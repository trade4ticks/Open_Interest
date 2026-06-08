"""
run_pipeline_early.py — 7 am pre-open pipeline.

Runs before the regular session opens (typically 6:30–7:30 ET) on trading
days only.  Its sole purpose is to populate enough features for an early
pre-market read — using a premarket open-price proxy and OI data published
overnight by the OCC.

Steps (in order):
    1. Trading-day guard — exits 0 immediately on weekends / holidays.
    2. ThetaData connection test.
    3. Premarket OHLC      — yfinance 1m prepost bars (04:00–09:29 ET),
                             writes open / open_source='premarket_1m' /
                             open_asof_ts.  Tickers with no premarket
                             activity are silently skipped.
    4. OI snapshot         — today's OCC-published chain via the snapshot
                             endpoint; covers today because the history
                             endpoint lags ~1 day.
    5. build_features MORNING — only MORNING_COLS; uses the premarket open
                             as the spot price anchor.
    6. build_bin_tables MORNING — per-ticker MORNING-tier bin assignments.

Deliberately omitted vs. the 9:35 MORNING run (run_pipeline.py --tier MORNING):
    - fetch_ohlc (regular daily bar) — regular-session open not yet printed.
    - fetch_oi history               — history endpoint lags ~1 day; the
                                       snapshot endpoint covers today.

The 9:35 MORNING run is still the authoritative MORNING run.  It overwrites
`open` with the official 9:30 print (open_source='daily_1d'), re-fetches OI
history for prior days, and rebuilds features + bins on the final open price.

Write contract
--------------
Same two-tier contract as run_pipeline.py.  MORNING_UPSERT_SQL touches only
MORNING_COLS — it never overwrites EVENING_COLS written by the prior night's
EVENING run.

The premarket OHLC upsert (PREMARKET_UPSERT_SQL in fetch_ohlc_premarket.py)
touches ONLY open / open_source / open_asof_ts — never high/low/close/volume.
"""
from __future__ import annotations

import logging
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

from build_features import build_for_ticker
from db import get_connection
import fetch_ohlc_premarket
from fetch_oi_snapshot import fetch_ticker as run_oi_snapshot_fetch
from lib.market_hours import get_trading_days, last_trading_day
from lib.parquet_store import list_tickers as list_oi_tickers
from lib.thetadata import test_connection as test_thetadata

# Ensure logs/ exists before anything else — the cron `>> .../pipeline_early.log`
# redirect will fail if the directory doesn't exist.
LOGS_DIR = Path(__file__).resolve().parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline_early")

# Rolling window for feature build.  Same as run_pipeline.py: ~30 trading
# days of history so that fwd-return fills and vol windows have enough data.
FEATURES_LOOKBACK_DAYS = 45


# ---------------------------------------------------------------------------
# Ticker discovery
# ---------------------------------------------------------------------------

def _get_ohlc_tickers(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT ticker FROM underlying_ohlc ORDER BY ticker")
        return [r[0] for r in cur.fetchall()]


def _discover_tickers(conn) -> tuple[set, set, set]:
    """Return (ohlc_tickers, oi_tickers, feature_tickers).

    feature_tickers = ohlc_tickers ∩ oi_tickers — only tickers with both
    feeds can meaningfully build OI-anchored features.
    """
    ohlc = set(_get_ohlc_tickers(conn))
    oi   = set(list_oi_tickers())
    log.info("OHLC tickers    : %s", sorted(ohlc) or "<none>")
    log.info("OI tickers      : %s", sorted(oi)   or "<none>")
    log.info("Feature tickers : %s", sorted(ohlc & oi) or "<none>")
    return ohlc, oi, ohlc & oi


# ---------------------------------------------------------------------------
# Bin build
# ---------------------------------------------------------------------------

def _run_bin_build() -> int:
    """Subprocess to build_bin_tables.py --tier MORNING.

    Returns the subprocess return code.  Non-zero is logged but does NOT
    roll back daily_features — those writes are already committed via the
    connection block above.  The pipeline's cron exit status reflects the
    bin-build outcome so cron mail surfaces the failure; the next run
    re-fires the build (which is idempotent), so the table self-heals.
    """
    script_path = Path(__file__).resolve().parent / "build_bin_tables.py"
    cmd = [sys.executable, str(script_path), "--tier", "MORNING"]
    log.info("--- bin build: %s ---", " ".join(str(c) for c in cmd))
    result = subprocess.run(cmd)
    if result.returncode != 0:
        log.error(
            "bin build exited with code %d "
            "(daily_features data preserved; re-run will recover)",
            result.returncode,
        )
    return result.returncode


# ---------------------------------------------------------------------------
# Early pipeline
# ---------------------------------------------------------------------------

def run_early(conn, today: date) -> None:
    """Three-step pre-open pipeline: premarket OHLC → OI snapshot → features."""
    ohlc_tickers, oi_tickers, feature_tickers = _discover_tickers(conn)

    if not ohlc_tickers and not oi_tickers:
        log.warning("Nothing to do — both stores are empty.")
        return

    trade_date = last_trading_day(today)   # today on a trading day

    # ------------------------------------------------------------------
    # Step 3: Premarket OHLC
    # Fetches yfinance 1m prepost bars (04:00–09:29 ET), takes the Close
    # of the last bar with nonzero volume, and writes:
    #     open          = premarket-proxy price
    #     open_source   = 'premarket_1m'
    #     open_asof_ts  = bar timestamp (ET, tz-aware)
    # Tickers with no premarket activity are silently skipped (correct
    # safe-failure mode — no price is better than a stale/zero price).
    # Per-ticker exceptions are caught inside fetch_ohlc_premarket.run().
    # ------------------------------------------------------------------
    log.info("--- premarket OHLC: trade_date = %s ---", trade_date)
    written = fetch_ohlc_premarket.run(conn, sorted(ohlc_tickers), trade_date)
    log.info("Premarket OHLC: %d/%d tickers written", written, len(ohlc_tickers))

    # ------------------------------------------------------------------
    # Step 4: OI snapshot
    # Fetches the current OI chain via the snapshot endpoint, which has
    # today's OCC-published overnight data.  We skip the history endpoint
    # here because it typically lags ~1 day (today's row isn't in it yet
    # at 7am).
    # Per-ticker exceptions are caught below so one bad ticker doesn't
    # abort the rest.
    # ------------------------------------------------------------------
    log.info("--- OI snapshot: trade_date = %s ---", trade_date)
    oi_ok = 0
    for t in sorted(oi_tickers):
        try:
            n = run_oi_snapshot_fetch(t, trade_date)
            if n:
                oi_ok += 1
            else:
                log.warning("  %s: snapshot returned 0 rows", t)
        except Exception as exc:
            log.warning("  %s: snapshot failed — %s", t, exc)
    log.info("OI snapshot: %d/%d tickers fetched", oi_ok, len(oi_tickers))

    # ------------------------------------------------------------------
    # Step 5: build_features (MORNING tier)
    # Uses the premarket open as the spot price anchor for all MORNING_COLS
    # (spot_co, oi_*, etc.).  MORNING_UPSERT_SQL touches only MORNING_COLS
    # — EVENING_COLS written by last night's run are left untouched.
    # Per-ticker exceptions are caught below.
    # ------------------------------------------------------------------
    feat_end   = today
    feat_start = feat_end - timedelta(days=FEATURES_LOOKBACK_DAYS)
    log.info("--- build_features (MORNING): %s → %s ---", feat_start, feat_end)
    feat_ok = 0
    for t in sorted(feature_tickers):
        try:
            build_for_ticker(conn, t, start=feat_start, end=feat_end, tier="MORNING")
            feat_ok += 1
        except Exception as exc:
            log.warning("  %s: build_features failed — %s", t, exc)
    log.info("build_features (MORNING): %d/%d tickers built", feat_ok, len(feature_tickers))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    today = date.today()

    # ------------------------------------------------------------------
    # Step 1: Trading-day guard
    # Exit 0 (not an error) on weekends and holidays — the cron will fire
    # every calendar day but should be a no-op on non-trading days.
    # ------------------------------------------------------------------
    if not get_trading_days(today, today):
        log.info("Today (%s) is not a trading day — exiting.", today)
        return 0

    log.info("=" * 60)
    log.info("Early pipeline starting (today = %s)", today)

    # ------------------------------------------------------------------
    # Step 2: ThetaData connection test
    # Abort early rather than fetching premarket OHLC and then failing
    # silently on the OI snapshot step.
    # ------------------------------------------------------------------
    if not test_thetadata():
        log.error("ThetaData terminal not reachable — aborting")
        return 1

    with get_connection() as conn:
        run_early(conn, today)

    # Conn closed; daily_features writes are committed.  Safe to invoke
    # the bin build as a subprocess (it opens its own connection).
    rc = _run_bin_build()
    log.info("Early pipeline complete (bin_build_rc = %d)", rc)
    return rc


if __name__ == "__main__":
    sys.exit(main())
