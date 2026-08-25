"""
run_live_pipeline.py — live capture, then metrics for what it just captured.

The cron entry for the intraday path. fetch_live_surface.py writes
equity_surface / equity_atm / equity_surface_diagnostics, but nothing computed
metrics from those rows, so equity_metrics had no intraday data at all. This
runs both stages in one process, under one log.

    stage 1  capture   fetch_live_surface.run() — 121 tickers, ~72s
    stage 2  metrics   build_equity_metrics.run_for_snapshot() per ticker

Stage 2 is a FUNCTION CALL, not a subprocess: run_for_snapshot was built as
the callable entry point for exactly this, and a subprocess would re-import
pandas/scipy per cycle and lose the structured return.

--- Why a metrics failure cannot cost the capture ---------------------------

The two stages use SEPARATE connections and the capture's is closed before
metrics opens one. lib/surface_store.write_snapshot commits per ticker, so
surface rows are durable the moment each ticker lands; but relying on that
alone would leave a shared transaction able to roll them back. Separate
connections make it structurally impossible instead of merely unlikely.

A metrics error is logged per ticker and the loop continues; the process exits
non-zero so cron mail fires, with the capture already safe on disk.

--- Scope -------------------------------------------------------------------

Metrics runs for exactly the (ticker, trade_date, snapshot) triples the
capture WROTE — totals["written"] — not a date range and not the ticker list.
Two reasons it cannot be derived from the clock instead:

  * a 72s capture can straddle a 5-minute boundary, so grid_bucket(now) after
    the capture is not necessarily the label the rows carry
  * captures include tickers whose fit or write later failed, and computing
    metrics for one of those reads a surface that is not there

--- Locking -----------------------------------------------------------------

Unchanged, and deliberately so. The cron wraps this in flock -n and that is
the authoritative guard; this script takes no lock of its own. Taking a second
one on the same path deadlocks, because flock locks are per open-file-
description and the descriptor flock(1) holds is inherited by this process.

Cron (replaces the fetch_live_surface entry):

    TZ=America/New_York */5 9-16 * * 1-5 flock -n /tmp/live_surface.lock \\
        /Open_Interest/.venv/bin/python /Open_Interest/run_live_pipeline.py \\
        >> /Open_Interest/logs/live_pipeline.log 2>&1

Usage:
    python run_live_pipeline.py
    python run_live_pipeline.py --force --tickers SPY,AAPL
    python run_live_pipeline.py --skip-metrics      # capture only
"""
from __future__ import annotations

import logging
import sys
import time

import fetch_live_surface as capture
from lib.chain_fetch_common import log_path, setup_file_logging

log = logging.getLogger("live_pipeline")


def run_metrics(written: list, with_z: bool) -> dict:
    """Metrics for each captured (ticker, trade_date, snapshot).

    Its own connection, opened after the capture's has closed — see the module
    docstring. Returns counts plus the tickers that failed.
    """
    from db import get_connection
    from build_equity_metrics import run_for_snapshot

    out = {"metrics": 0, "z": 0, "no_surface": [], "failed": [], "secs": 0.0}
    if not written:
        return out

    t0 = time.monotonic()
    with get_connection() as conn:
        for i, (tk, trade_date, snapshot) in enumerate(written, 1):
            try:
                # cache=None on purpose. A HistoryCache held across tickers
                # would have to be invalidated after every write, since the row
                # just written is part of the next lookback; at one row per
                # ticker per cycle the cache would be invalidated as often as
                # it was used.
                res = run_for_snapshot(conn, tk, trade_date, snapshot,
                                       cache=None, with_z=with_z)
            except Exception as exc:                          # noqa: BLE001
                # Per ticker, never fatal: the capture is already committed and
                # 120 good tickers must not be lost to one bad one.
                log.error("  %s %s %s: metrics FAILED — %s: %s",
                          tk, trade_date, snapshot,
                          type(exc).__name__, exc)
                out["failed"].append(tk)
                continue
            if res.get("reason"):
                log.warning("  %s %s %s: %s", tk, trade_date, snapshot,
                            res["reason"])
                out["no_surface"].append(tk)
                continue
            out["metrics"] += res.get("metrics", 0)
            out["z"] += res.get("z", 0)

            if i % capture.PROGRESS_EVERY == 0 or i == len(written):
                log.info("  metrics progress: %d/%d ticker(s)", i, len(written))
                capture.flush_log()

    out["secs"] = time.monotonic() - t0
    return out


def main() -> int:
    ap = capture.build_parser()
    ap.description = "Live intraday capture followed by metrics."
    ap.add_argument("--skip-metrics", action="store_true",
                    help="capture only; leaves equity_metrics without this "
                         "cycle")
    # Default ON now. zscore_rows scores every bucket against the
    # BASELINE_SNAPSHOT daily series, which IS the definition the dashboard
    # wanted and had been deriving for itself — so the stored value is now the
    # one to read, and there is no reason to withhold it from live rows.
    ap.add_argument("--no-z", action="store_true",
                    help="skip equity_metrics_z for live rows. The stored z is "
                         "now scored against the daily baseline, so there is "
                         "rarely a reason to.")
    args = ap.parse_args()

    # ONE log for both stages, so a stall is attributable without cross-
    # referencing two files. Named for the pipeline, not the fetcher.
    log_file = setup_file_logging("run_live_pipeline")
    t0 = time.monotonic()

    rc, totals = capture.run(args)
    cap_secs = time.monotonic() - t0
    written = totals.get("written", [])
    log.info("capture stage: %d ticker(s) written in %.0fs",
             len(written), cap_secs)
    capture.flush_log()

    if args.skip_metrics:
        print(f"\nmetrics skipped (--skip-metrics). Log: {log_path()}")
        return rc
    if not written:
        # Nothing landed, so there is nothing to compute. Preserve the
        # capture's own exit code rather than inventing a metrics failure.
        log.warning("capture wrote no tickers — skipping metrics")
        print(f"\nLog: {log_path()}")
        return rc

    m = run_metrics(written, with_z=not args.no_z)

    print(f"\n=== pipeline ===")
    print(f"  capture   {len(written):>4} ticker(s)   {cap_secs:>6.0f}s")
    print(f"  metrics   {m['metrics']:>4} row(s)      {m['secs']:>6.0f}s"
          + ("   (z skipped)" if args.no_z else f"   (+{m['z']} z)"))
    print(f"  total                       {time.monotonic() - t0:>6.0f}s")
    if m["no_surface"]:
        print(f"  no surface: {len(m['no_surface'])} ticker(s) — "
              f"{', '.join(m['no_surface'][:8])}")
    if m["failed"]:
        print(f"  metrics FAILED: {len(m['failed'])} ticker(s) — "
              f"{', '.join(m['failed'][:8])}")
    print(f"\nLog: {log_path()}")

    # Non-zero on a metrics failure so cron mail fires, but the capture is
    # already committed and is not affected by this exit code.
    if m["failed"]:
        return 2
    return rc


if __name__ == "__main__":
    sys.exit(main())
