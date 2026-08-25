"""
build_equity_metrics.py — derived vol metrics off the interpolated surface.

Stage 4 of the pipeline: fetch -> clean -> interpolate -> METRICS.

equity_surface holds 19 delta nodes x 17 tenors per (ticker, snapshot). That is
the right shape for exploring one ticker and the wrong shape for ranking 121 of
them on a derived quantity — a scanner would fan out into 121 x 323 rows and
recompute the same slopes every time. This stage collapses it to one wide row
per (ticker, trade_date, snapshot), so the scan is a single indexed read.

TWO CONSUMERS, ONE ENTRY POINT
------------------------------
    run_for_snapshot(conn, ticker, trade_date, snapshot)   <- the live pipeline
    build_range(...)                                       <- the batch backfill

run_live_pipeline.py calls the fetcher and then run_for_snapshot, in sequence.
Everything the CLI does goes through the same two functions, so the batch path
cannot drift from the live one.

ORDERING IS LOAD-BEARING: the z-scores are computed from equity_metrics AFTER
the base rows land, because today's own value is part of its own trailing
window. build_range therefore writes every base row for the range first and
z-scores the whole range second, per (ticker, snapshot) — which also means one
history read per series instead of one per date.

    python build_equity_metrics.py init-db
    python build_equity_metrics.py batch --start 20260601 --end 20260605
    python build_equity_metrics.py batch --start 20260601 --end 20260601 \
        --tickers SPY,AAPL --rebuild
    python build_equity_metrics.py catalog          # re-sync the picker only
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from collections import defaultdict
from datetime import date, datetime

from lib.market_hours import get_trading_days, last_trading_day
from lib.metrics_compute import HistoryCache, compute_metrics
from lib.metrics_store import (
    check_catalog_drift, snapshots_for_date, sync_all,
    write_metrics, write_zscores, zscore_rows,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("build_equity_metrics")


def _parse_day(s: str) -> date:
    return datetime.strptime(s.strip(), "%Y%m%d").date()


def _prompt_day(label: str, default: date) -> date:
    """Match the sibling fetchers: prompt rather than require flags."""
    raw = input(f"  {label} [YYYYMMDD, blank = {default:%Y%m%d}]: ").strip()
    return _parse_day(raw) if raw else default


# =============================================================================
# The two entry points
# =============================================================================
def run_for_snapshot(conn, ticker: str, trade_date, snapshot: str,
                     cache: HistoryCache | None = None,
                     with_z: bool = True) -> dict:
    """Compute, write and z-score ONE (ticker, trade_date, snapshot).

    This is what the live pipeline calls immediately after a surface capture.

    Returns {'metrics': n, 'z': n, 'reason': str|None}. A missing surface is
    reported, not raised: one ticker with no chain must not take down a cycle
    that has 120 good ones.

    A caller that holds a HistoryCache across cycles must invalidate(ticker)
    after each write — the cache holds the daily-baseline IV series, and a row
    just written AT THE BASELINE BUCKET is part of the next lookback. Writes at
    any other bucket no longer affect it, since spot-vol stopped reading the
    row's own snapshot; invalidating on every write is still the safe default.
    Passing None (the default) builds a fresh cache and is always correct.
    """
    row = compute_metrics(conn, ticker, trade_date, snapshot, cache=cache)
    if row is None:
        return {"metrics": 0, "z": 0, "reason": "no surface rows"}
    n_m = write_metrics(conn, [row])
    n_z = 0
    if with_z:
        n_z = write_zscores(conn, zscore_rows(conn, ticker, snapshot,
                                              [trade_date]))
    return {"metrics": n_m, "z": n_z, "reason": None}


def build_range(conn, days: list, tickers: list | None = None,
                snapshots: list | None = None, rebuild: bool = False,
                with_z: bool = True) -> dict:
    """Batch backfill. Base rows for the whole range first, then z-scores."""
    cache = HistoryCache(conn)
    stats = {"days": 0, "metrics": 0, "z": 0, "skipped": 0, "errors": 0}
    series = defaultdict(list)          # (ticker, snapshot) -> [trade_date]

    for day in days:
        pairs = _targets(conn, day, tickers, snapshots, rebuild)
        if not pairs:
            log.info("%s  nothing to do", day)
            continue
        rows, t0 = [], time.monotonic()
        for ticker, snap in pairs:
            try:
                row = compute_metrics(conn, ticker, day, snap, cache=cache)
            except Exception as exc:                          # noqa: BLE001
                # One bad ticker-snapshot must not abort the date. The
                # exception is logged with its key so it can be reproduced.
                log.error("  %s %s %s — %s: %s", ticker, day, snap,
                          type(exc).__name__, exc)
                stats["errors"] += 1
                continue
            if row is None:
                stats["skipped"] += 1
                continue
            rows.append(row)
            series[(ticker, snap)].append(day)
        n = write_metrics(conn, rows)
        stats["metrics"] += n
        stats["days"] += 1
        log.info("%s  %4d metric row(s) from %d target(s)  %.1fs",
                 day, n, len(pairs), time.monotonic() - t0)

    if with_z and series:
        log.info("z-scores over %d ticker-snapshot series ...", len(series))
        t0 = time.monotonic()
        for (ticker, snap), dates in sorted(series.items()):
            stats["z"] += write_zscores(conn,
                                        zscore_rows(conn, ticker, snap, dates))
        log.info("  %d z row(s)  %.1fs", stats["z"], time.monotonic() - t0)
    return stats


def _targets(conn, day, tickers, snapshots, rebuild) -> list:
    """(ticker, snapshot) pairs to process for one date.

    Incremental by default: diagnostics is the authority on what the surface
    stage completed, and anything already in equity_metrics is skipped. It
    holds a row even for a snapshot that produced no surface, which is what
    stops a barren date being retried forever.
    """
    if rebuild:
        with conn.cursor() as cur:
            sql = ("SELECT DISTINCT ticker, snapshot "
                   "FROM equity_surface_diagnostics WHERE trade_date = %s")
            params = [day]
            if tickers:
                sql += " AND ticker = ANY(%s)"
                params.append(list(tickers))
            cur.execute(sql + " ORDER BY 1, 2", params)
            pairs = [(r[0], r[1]) for r in cur.fetchall()]
    else:
        pairs = snapshots_for_date(conn, day)
        if tickers:
            want = set(tickers)
            pairs = [p for p in pairs if p[0] in want]
    if snapshots:
        want = set(snapshots)
        pairs = [p for p in pairs if p[1] in want]
    return pairs


# =============================================================================
# CLI
# =============================================================================
def main() -> int:
    ap = argparse.ArgumentParser(
        description="Derived vol metrics off the interpolated equity surface.")
    ap.add_argument("command", choices=["init-db", "batch", "catalog"])
    ap.add_argument("--start", help="YYYYMMDD")
    ap.add_argument("--end", help="YYYYMMDD")
    ap.add_argument("--tickers", help="comma-separated; default = all")
    ap.add_argument("--snapshots", help="comma-separated, e.g. 0945,1545")
    ap.add_argument("--rebuild", action="store_true",
                    help="reprocess dates that already have metric rows")
    ap.add_argument("--no-z", action="store_true",
                    help="base rows only; z-scores can be recomputed later")
    args = ap.parse_args()

    from db import get_connection
    from lib.metrics_store import init_db

    with get_connection() as conn:
        if args.command == "init-db":
            init_db(conn)
            print("equity_metrics / equity_metrics_z / catalog ready.")
            return 0

        if args.command == "catalog":
            # sync_all, not the steps inline — it also applies the metrics
            # migrations, whose position either side of the column sync is
            # load-bearing. `catalog` is the command reached for after a
            # registry change, which is exactly when a rename needs to land.
            n_base, n_z, n_cat = sync_all(conn)
            print(f"columns added: {n_base} base, {n_z} z; "
                  f"catalog rows: {n_cat}")
            return 0

        # Fail before doing any work rather than writing into a table whose
        # shape no longer matches the registry.
        check_catalog_drift(conn)

        default = last_trading_day()
        start = _parse_day(args.start) if args.start else None
        end = _parse_day(args.end) if args.end else None
        if start is None or end is None:
            print("Date range:")
            start = start or _prompt_day("start", default)
            end = end or _prompt_day("end", start)
        if end < start:
            print("end is before start", file=sys.stderr)
            return 2

        days = get_trading_days(start, end)
        if not days:
            print("no NYSE sessions in that range")
            return 0
        tickers = ([t.strip().upper() for t in args.tickers.split(",")]
                   if args.tickers else None)
        snaps = ([s.strip() for s in args.snapshots.split(",")]
                 if args.snapshots else None)

        log.info("%s .. %s  (%d session(s))  tickers=%s snapshots=%s "
                 "rebuild=%s", days[0], days[-1], len(days),
                 tickers or "all", snaps or "all", args.rebuild)
        t0 = time.monotonic()
        stats = build_range(conn, days, tickers, snaps, args.rebuild,
                            with_z=not args.no_z)
        log.info("done  %d metric row(s), %d z row(s), %d skipped, "
                 "%d error(s)  %.1fs", stats["metrics"], stats["z"],
                 stats["skipped"], stats["errors"], time.monotonic() - t0)
        return 1 if stats["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
