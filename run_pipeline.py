"""
run_pipeline.py — Daily refresh: fetch OHLC, fetch OI, rebuild features.

Designed for a daily cron at 7am ET (Mon-Fri):

    TZ=America/New_York 0 7 * * 1-5 \
        flock -n /tmp/oi_research.lock \
        /Open_Interest/.venv/bin/python /Open_Interest/run_pipeline.py \
        >> /Open_Interest/logs/pipeline.log 2>&1

Behaviour
---------
- OHLC: refreshes underlying_ohlc for every ticker already in the table over
  a rolling 7-day window. Idempotent UPSERT, so re-fetches just overwrite
  any rows that previously existed.
- OI:   refreshes the parquet store for every ticker already under
  OI_RAW_DIR over the same rolling 7-day window. The parquet writer dedupes
  on (trade_date, expiration, strike, option_type) keeping last.
- Features: rebuilds daily_features for the intersection of those two ticker
  sets, over the last ~30 trading days. This is enough to (a) catch the new
  OI row for today, (b) keep ret_*_fwd_oc up-to-date as new OHLC arrives in
  the prior 20 days. The DuckDB query reads a 130-day buffer behind the
  range so window functions still see correct history; only the [start,end]
  slice is DELETE+INSERTed.

Today's row at 7am will have OI-only features populated and spot-derived
ones NULL — they fill in tomorrow when today's OHLC becomes available.

Tickers
-------
Operates on whatever's already loaded; doesn't add new tickers. To add a
new ticker, run fetch_ohlc.py / fetch_oi.py manually first.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

from build_features import build_for_ticker
from db import get_connection
from fetch_ohlc import run as run_ohlc_fetch
from fetch_oi import fetch_ticker as run_oi_history_fetch
from fetch_oi_snapshot import fetch_ticker as run_oi_snapshot_fetch
from lib.market_hours import get_trading_days, last_trading_day
from lib.parquet_store import list_tickers as list_oi_tickers
from lib.thetadata import test_connection as test_thetadata

# Ensure logs/ exists before anything else — the cron `>> .../pipeline.log`
# redirect will fail if the directory doesn't exist.
LOGS_DIR = Path(__file__).resolve().parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline")

# Calendar-day windows (extra slack covers holidays / non-trading days).
OHLC_LOOKBACK_DAYS     = 10   # ~7 trading days
OI_LOOKBACK_DAYS       = 10
FEATURES_LOOKBACK_DAYS = 45   # ~30 trading days


def get_ohlc_tickers(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT ticker FROM underlying_ohlc ORDER BY ticker")
        return [r[0] for r in cur.fetchall()]


def main() -> None:
    today = date.today()

    log.info("=" * 60)
    log.info("Daily pipeline starting (today = %s)", today)

    if not test_thetadata():
        log.error("ThetaData terminal not reachable — aborting")
        raise SystemExit(1)

    with get_connection() as conn:
        ohlc_tickers    = set(get_ohlc_tickers(conn))
        oi_tickers      = set(list_oi_tickers())
        feature_tickers = ohlc_tickers & oi_tickers

        log.info("OHLC tickers       : %s", sorted(ohlc_tickers) or "<none>")
        log.info("OI tickers         : %s", sorted(oi_tickers) or "<none>")
        log.info("Feature tickers    : %s", sorted(feature_tickers) or "<none>")

        if not ohlc_tickers and not oi_tickers:
            log.warning("Nothing to do — both stores are empty.")
            return

        # 1. OHLC fetch — yfinance only has data through yesterday at 7am.
        ohlc_end   = last_trading_day(today)
        ohlc_start = ohlc_end - timedelta(days=OHLC_LOOKBACK_DAYS)
        log.info("--- OHLC fetch: %s → %s ---", ohlc_start, ohlc_end)
        for t in sorted(ohlc_tickers):
            run_ohlc_fetch(conn, t, ohlc_start, ohlc_end)

        # 2a. OI history — fills/refreshes the prior week. The history
        #     endpoint typically lags by ~1 day so today usually returns
        #     nothing here; that's expected (snapshot covers today).
        oi_end   = today
        oi_start = oi_end - timedelta(days=OI_LOOKBACK_DAYS)
        oi_trading_days = get_trading_days(oi_start, oi_end)
        log.info("--- OI history: %s → %s (%d trading days) ---",
                 oi_start, oi_end, len(oi_trading_days))
        for t in sorted(oi_tickers):
            log.info("  %s ...", t)
            run_oi_history_fetch(t, oi_trading_days)

        # 2b. OI snapshot — today's chain (or last_trading_day(today) on
        #     weekends / holidays). Stamped onto today's row in the parquet
        #     store; tomorrow's history fetch will overwrite it with the
        #     authoritative EOD value (parquet dedupe keeps last).
        snapshot_td = last_trading_day(today)
        log.info("--- OI snapshot: trade_date = %s ---", snapshot_td)
        for t in sorted(oi_tickers):
            run_oi_snapshot_fetch(t, snapshot_td)

        # 3. build_features — rolling ~30-trading-day rebuild keeps recent
        #    rows current AND lets older ret_*_fwd_oc fill in as new OHLC
        #    arrives.
        feat_end   = today
        feat_start = feat_end - timedelta(days=FEATURES_LOOKBACK_DAYS)
        log.info("--- build_features: %s → %s ---", feat_start, feat_end)
        for t in sorted(feature_tickers):
            build_for_ticker(conn, t, start=feat_start, end=feat_end)

    log.info("Daily pipeline complete")


if __name__ == "__main__":
    main()
