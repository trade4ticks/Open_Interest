"""
run_pipeline.py — Daily two-tier pipeline.

Invoked twice per trading day by separate cron entries. Each invocation
runs only its tier's work and writes only that tier's columns of
daily_features (and that tier's bin assignments).

    EVENING run — after market close on T:
        python run_pipeline.py --tier EVENING
        1. fetch_ohlc            (today's finalized session: open + close)
        2. fetch_chain_eod       (today's EOD greeks chain → parquet)
        3. build_features --tier EVENING  (only EVENING_UPSERT_SQL fires)
        4. build_bin_tables --tier EVENING --build-tt

    MORNING run — before / shortly after market open on T:
        python run_pipeline.py --tier MORNING
        1. fetch_ohlc            (today's morning print — for spot_co = O_T)
        2. fetch_oi    (history) (rolling 10d refresh)
        3. fetch_oi_snapshot     (today's just-published OI chain)
        4. build_features --tier MORNING  (only MORNING_UPSERT_SQL fires)
        5. build_bin_tables --tier MORNING

Two-cron write contract (see build_features.py:1325-...):
    The two tiers' upserts touch DISJOINT columns of the same
    (ticker, trade_date) row. MORNING_UPSERT_SQL's DO UPDATE SET names
    only MORNING_COLS; EVENING_UPSERT_SQL names only EVENING_COLS.
    Neither tier can wipe the other's data; there is NO DELETE before
    either upsert.

Cross-tier date alignment:
    MORNING-tier vol_oi_ratio_* and net_new_oi_div_vol read vol data from
    chain_adj (the chain_eod parquet). Today's MORNING run reads chain
    rows written by YESTERDAY's EVENING run — chain rows are stamped
    with feature_date = next_trading_day(trade_date), so a row fetched
    in tonight's evening (trade_date = T) appears with feature_date =
    T+1 and is picked up by tomorrow morning's build_features.

    Operational consequence: the FIRST MORNING run after a fresh cron
    activation may have vol_oi_ratio_* / net_new_oi_div_vol returning
    NULL for one day (no prior evening run yet). Next morning is back
    to normal.

The cron schedule is managed externally; this script doesn't care what
times it runs at as long as the EVENING run is after T's market close
and the MORNING run is on T+1 (with T+1's overnight-published OI in
ThetaData's snapshot endpoint).
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

from build_features import build_for_ticker
from db import get_connection
from fetch_chain_eod import fetch_ticker as run_chain_fetch
from fetch_ohlc import run as run_ohlc_fetch
from fetch_oi import fetch_ticker as run_oi_history_fetch
from fetch_oi_snapshot import fetch_ticker as run_oi_snapshot_fetch
from lib.market_hours import get_trading_days, last_trading_day, next_trading_day
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

# Calendar-day rolling windows. Extra slack covers holidays / non-trading days.
OHLC_LOOKBACK_DAYS     = 10   # ~7 trading days
OI_LOOKBACK_DAYS       = 10
FEATURES_LOOKBACK_DAYS = 45   # ~30 trading days; lets older ret_*_fwd_* fill in


# ---------------------------------------------------------------------------
# Ticker discovery (shared by both tiers)
# ---------------------------------------------------------------------------

def _get_ohlc_tickers(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT ticker FROM underlying_ohlc ORDER BY ticker")
        return [r[0] for r in cur.fetchall()]


def _discover_tickers(conn) -> tuple[set, set, set]:
    """Return (ohlc_tickers, oi_tickers, feature_tickers).
    feature_tickers = intersection — only these have both feeds and can build features."""
    ohlc = set(_get_ohlc_tickers(conn))
    oi   = set(list_oi_tickers())
    log.info("OHLC tickers    : %s", sorted(ohlc) or "<none>")
    log.info("OI tickers      : %s", sorted(oi)   or "<none>")
    log.info("Feature tickers : %s", sorted(ohlc & oi) or "<none>")
    return ohlc, oi, ohlc & oi


# ---------------------------------------------------------------------------
# EVENING run
# ---------------------------------------------------------------------------

def run_evening(conn, today: date) -> None:
    """OHLC (today's full session) → chain_eod → build_features EVENING."""
    ohlc_tickers, oi_tickers, feature_tickers = _discover_tickers(conn)
    if not ohlc_tickers and not oi_tickers:
        log.warning("Nothing to do — both stores are empty.")
        return

    # 1. OHLC fetch — by evening, today's row has its finalized open + close
    #    available from yfinance (rolling 10-day window for resilience).
    ohlc_end   = last_trading_day(today)
    ohlc_start = ohlc_end - timedelta(days=OHLC_LOOKBACK_DAYS)
    log.info("--- OHLC fetch: %s → %s ---", ohlc_start, ohlc_end)
    for t in sorted(ohlc_tickers):
        run_ohlc_fetch(conn, t, ohlc_start, ohlc_end)

    # 2. fetch_chain_eod — today's session's full EOD greeks chain. By
    #    evening (post-17:15 ET) ThetaData has T's chain. The fetcher is
    #    resumable (skips already-loaded dates), so the rolling window
    #    only re-fetches actual new data.
    chain_end   = last_trading_day(today)
    chain_start = chain_end - timedelta(days=OI_LOOKBACK_DAYS)
    chain_days  = get_trading_days(chain_start, chain_end)
    log.info("--- chain_eod fetch: %s → %s (%d trading days) ---",
             chain_start, chain_end, len(chain_days))
    for t in sorted(oi_tickers):
        log.info("  %s ...", t)
        run_chain_fetch(t, chain_days)

    # 3. build_features for EVENING tier only — fires EVENING_UPSERT_SQL only.
    # Per docs/daily_features_data_dictionary.md (universal row invariant):
    # EVENING on calendar day X writes for trade_date T = next_trading_day(X).
    # The chain just fetched in step 2 is stamped feature_date=T, so extending
    # feat_end to T lets chain_adj's feature_date filter include it.  OHLC for
    # T does not yet exist on T-1 evening — build_for_ticker injects a NULL-
    # price placeholder ohlc row at T so the rolling-window OHLC SQL emits
    # T's backward-looking EVENING metrics from real T-1-and-earlier closes.
    # The next MORNING run on T fills OI into this same row.
    feat_end   = next_trading_day(today)
    feat_start = feat_end - timedelta(days=FEATURES_LOOKBACK_DAYS)
    log.info("--- build_features (EVENING): %s → %s ---", feat_start, feat_end)
    for t in sorted(feature_tickers):
        build_for_ticker(conn, t, start=feat_start, end=feat_end, tier="EVENING")


# ---------------------------------------------------------------------------
# MORNING run
# ---------------------------------------------------------------------------

def run_morning(conn, today: date) -> None:
    """OHLC (open print) → OI history → OI snapshot → build_features MORNING."""
    ohlc_tickers, oi_tickers, feature_tickers = _discover_tickers(conn)
    if not ohlc_tickers and not oi_tickers:
        log.warning("Nothing to do — both stores are empty.")
        return

    # 1. OHLC fetch — at MORNING cron time, today's row has its OPEN print
    #    (and possibly some intraday so far). spot_co = O_T depends on this.
    #    The evening run will overwrite today's row later with the finalized
    #    close; UPSERT (ticker, trade_date) handles that cleanly.
    ohlc_end   = last_trading_day(today)
    ohlc_start = ohlc_end - timedelta(days=OHLC_LOOKBACK_DAYS)
    log.info("--- OHLC fetch: %s → %s ---", ohlc_start, ohlc_end)
    for t in sorted(ohlc_tickers):
        run_ohlc_fetch(conn, t, ohlc_start, ohlc_end)

    # 2. OI history — fills/refreshes the prior week from the ThetaData history
    #    endpoint. Today's row typically isn't in history yet (~1-day lag);
    #    that's expected — the snapshot in step 3 covers today.
    oi_end   = today
    oi_start = oi_end - timedelta(days=OI_LOOKBACK_DAYS)
    oi_trading_days = get_trading_days(oi_start, oi_end)
    log.info("--- OI history: %s → %s (%d trading days) ---",
             oi_start, oi_end, len(oi_trading_days))
    for t in sorted(oi_tickers):
        log.info("  %s ...", t)
        run_oi_history_fetch(t, oi_trading_days)

    # 3. OI snapshot — today's chain via /v3/option/snapshot/open_interest.
    #    Stamped onto today's row in the parquet store. Tomorrow's history
    #    fetch will overwrite it with the authoritative value (parquet
    #    dedupe keep=last).
    snapshot_td = last_trading_day(today)
    log.info("--- OI snapshot: trade_date = %s ---", snapshot_td)
    for t in sorted(oi_tickers):
        run_oi_snapshot_fetch(t, snapshot_td)

    # 4. build_features for MORNING tier only — fires MORNING_UPSERT_SQL only.
    feat_end   = today
    feat_start = feat_end - timedelta(days=FEATURES_LOOKBACK_DAYS)
    log.info("--- build_features (MORNING): %s → %s ---", feat_start, feat_end)
    for t in sorted(feature_tickers):
        build_for_ticker(conn, t, start=feat_start, end=feat_end, tier="MORNING")


# ---------------------------------------------------------------------------
# Bin build attachment
# ---------------------------------------------------------------------------

def _run_bin_build(tier: str) -> int:
    """Subprocess to build_bin_tables.py for this tier. EVENING also rebuilds
    tt_thresholds (--build-tt).

    Returns the subprocess return code. Non-zero is logged but does NOT roll
    back daily_features — that's already committed via the conn-scoped block
    above. The cron's exit status reflects the bin build outcome so cron mail
    surfaces the failure; the next run re-fires the bin build (which is
    idempotent), so the table self-heals."""
    script_path = Path(__file__).resolve().parent / "build_bin_tables.py"
    cmd = [sys.executable, str(script_path), "--tier", tier]
    if tier == "EVENING":
        cmd.append("--build-tt")
    log.info("--- bin build: %s ---", " ".join(cmd))
    result = subprocess.run(cmd)
    if result.returncode != 0:
        log.error("bin build exited with code %d "
                  "(daily_features data preserved; re-run will recover)",
                  result.returncode)
    return result.returncode


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Daily two-tier pipeline.",
                                 formatter_class=argparse.RawDescriptionHelpFormatter,
                                 epilog=__doc__)
    ap.add_argument("--tier", choices=["MORNING", "EVENING"], required=True,
                    help="Which cron tier to run.")
    args = ap.parse_args()

    today = date.today()
    log.info("=" * 60)
    log.info("Pipeline starting (today = %s, tier = %s)", today, args.tier)

    if not test_thetadata():
        log.error("ThetaData terminal not reachable — aborting")
        return 1

    with get_connection() as conn:
        if args.tier == "EVENING":
            run_evening(conn, today)
        else:
            run_morning(conn, today)

    # Conn closed; daily_features writes are committed. Safe to invoke the
    # bin build as a subprocess (it opens its own connection).
    rc = _run_bin_build(args.tier)
    log.info("Pipeline complete (tier = %s, bin_build_rc = %d)", args.tier, rc)
    return rc


if __name__ == "__main__":
    sys.exit(main())
