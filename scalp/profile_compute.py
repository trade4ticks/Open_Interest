"""Where does compute.py's 20 seconds per symbol-day go?

    python -m scalp.profile_compute --symbol FDX --date 2026-08-28

Answers three questions in order, and stops at the first one that explains the
time — no changes to metrics.py, this only measures.

  1. HOW IS THE TIME SPLIT between the daily row and the 26 intraday rows?
     They read the same data. If the intraday path costs ~26x the daily one,
     the cost is per-window fixed overhead paid 27 times, not data volume.

  2. WHAT IS NOT VECTORIZED? cProfile, sorted by cumulative time, with the
     row-wise `.apply(axis=1)` calls called out separately — those are Python
     function calls per row and nothing else in the path is.

  3. DID THE VECTORIZATION HOLD? The condition-code path is now vectorized in
     metrics.py. This times it against the old row-wise implementation (kept
     in this file) and asserts they still produce identical output — so a
     regression shows up as a failed comparison rather than as wrong numbers.

Then projects the 544 x 10 backfill at the current rate and at the rate each
fix would imply.

Reads parquet and computes. Touches no API, no database, and writes nothing.
"""
from __future__ import annotations

import argparse
import cProfile
import io
import pstats
import time
from datetime import date

import numpy as np
import pandas as pd

from scalp import compute, config, metrics, store

UNIVERSE = 544
BACKFILL_DAYS = 10


def _fmt(seconds: float) -> str:
    if seconds < 90:
        return f"{seconds:.1f}s"
    if seconds < 5400:
        return f"{seconds / 60:.1f} min"
    return f"{seconds / 3600:.2f} h"


def _rule(title: str) -> None:
    print()
    print("=" * 76)
    print(title)
    print("=" * 76)


# --- the vectorized candidate ------------------------------------------------
# Not applied to metrics.py. Defined here only so the speedup and the
# equivalence can be measured before anything is changed.

def legacy_condition_code_sets(df: pd.DataFrame,
                               condition_cols: list[str]) -> pd.Series:
    """The row-wise implementation that used to live in metrics.py.

    Kept HERE, not there, so the before/after comparison still runs after the
    fix landed — and so nothing in the pipeline can accidentally call it again.
    This was 134.8s of 159.7s cumulative on one symbol-day.
    """
    if not condition_cols:
        return pd.Series([frozenset()] * len(df), index=df.index, dtype=object)

    def codes(row) -> frozenset:
        out = set()
        for col in condition_cols:
            val = row[col]
            if pd.isna(val):
                continue
            try:
                code = int(val)
            except (TypeError, ValueError):
                continue
            if code not in config.NO_CONDITION_SENTINELS:
                out.add(code)
        return frozenset(out)

    return df[condition_cols].apply(codes, axis=1)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--symbol", default="FDX")
    ap.add_argument("--date", default="2026-08-28")
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--universe", type=int, default=UNIVERSE)
    ap.add_argument("--days", type=int, default=BACKFILL_DAYS)
    args = ap.parse_args()

    day = date.fromisoformat(args.date)
    raw = store.read_day(args.symbol, day)
    if raw.empty:
        raise SystemExit(f"nothing on disk for {args.symbol} {day}")

    df, cols = compute.prepare(raw, day)
    start, end = compute.session_bounds(day)

    _rule(f"INPUT — {args.symbol} {day}")
    print(f"rows                 : {len(df):,}")
    print(f"condition columns    : {cols.condition_cols}")
    print(f"intraday buckets     : "
          f"{int((end - start).total_seconds() // (config.INTRADAY_BUCKET_MINUTES * 60))}")

    # --- 1. daily vs intraday ----------------------------------------------
    _rule("1. WHERE THE TIME GOES")

    t0 = time.perf_counter()
    daily = metrics.compute_window(df, cols, start, end)
    t_daily = time.perf_counter() - t0

    t0 = time.perf_counter()
    buckets = metrics.compute_buckets(df, cols, start, end,
                                      config.INTRADAY_BUCKET_MINUTES)
    t_buckets = time.perf_counter() - t0

    total = t_daily + t_buckets
    print(f"daily row            : {t_daily:7.2f}s   "
          f"({100 * t_daily / total:4.1f}%)")
    print(f"{len(buckets):2d} intraday rows    : {t_buckets:7.2f}s   "
          f"({100 * t_buckets / total:4.1f}%)")
    print(f"total per symbol-day : {total:7.2f}s")
    print()
    print(f"intraday / daily     : {t_buckets / max(t_daily, 1e-9):.1f}x")
    print()
    print("Both read the SAME data — the intraday path just slices it. A ratio")
    print("near the bucket count means the cost is per-window fixed overhead")
    print("paid 27 times, not data volume, and computing bucket-level")
    print("aggregates once and rolling them up removes almost all of it.")

    # --- 2. what is not vectorized -----------------------------------------
    _rule("2. WHAT IS NOT VECTORIZED")

    pr = cProfile.Profile()
    pr.enable()
    metrics.compute_window(df, cols, start, end)
    metrics.compute_buckets(df, cols, start, end,
                            config.INTRADAY_BUCKET_MINUTES)
    pr.disable()

    buf = io.StringIO()
    pstats.Stats(pr, stream=buf).sort_stats("cumulative").print_stats(args.top)
    print(buf.getvalue())

    print("Read the `tottime` column, not `cumtime`: tottime is time spent IN")
    print("that function rather than in its callees, so a Python-level row loop")
    print("shows up there while a pandas call that does its work in C does not.")

    # --- 3. would vectorizing help, and is it safe? ------------------------
    _rule("3. THE CONDITION-CODE PATH — CURRENT vs VECTORIZED")

    excluded = config.EXCLUDED_CONDITION_CODES

    # `df` came back from compute.prepare with the flag columns already
    # attached, so this measures the path the pipeline actually takes.
    t0 = time.perf_counter()
    fast_mask = metrics.excluded_mask(df, cols.condition_cols)
    t_fast = time.perf_counter() - t0

    t0 = time.perf_counter()
    legacy_sets = legacy_condition_code_sets(df, cols.condition_cols)
    legacy_mask = legacy_sets.apply(lambda s: bool(s & excluded))
    t_legacy = time.perf_counter() - t0

    agree = bool((legacy_mask.values == fast_mask.values).all())
    print(f"legacy (row-wise apply)  : {t_legacy:7.3f}s   "
          f"{len(df) / max(t_legacy, 1e-9):>10,.0f} rows/s")
    print(f"current (vectorized)     : {t_fast:7.3f}s   "
          f"{len(df) / max(t_fast, 1e-9):>10,.0f} rows/s")
    print(f"speedup                  : {t_legacy / max(t_fast, 1e-9):7.1f}x")
    print()
    print(f"IDENTICAL RESULT         : {agree}")
    if not agree:
        diff = int((legacy_mask.values != fast_mask.values).sum())
        print(f"  !! {diff:,} rows differ. The vectorized path is live in")
        print("  metrics.py, so this is a REGRESSION, not a proposal — the")
        print("  numbers being written are wrong. Report it before running")
        print("  anything else.")

    # Per-code provenance counts, one per excluded code.
    t0 = time.perf_counter()
    for code in sorted(excluded):
        int(legacy_sets.apply(lambda s, c=code: c in s).sum())
    t_prov_legacy = time.perf_counter() - t0

    t0 = time.perf_counter()
    for code in sorted(excluded):
        int(metrics._flag(df, metrics.flag_col_for_code(code),
                          cols.condition_cols, {code}).sum())
    t_prov_fast = time.perf_counter() - t0

    print()
    print(f"provenance per-code counts ({len(excluded)} codes)")
    print(f"  legacy     : {t_prov_legacy:7.3f}s")
    print(f"  current    : {t_prov_fast:7.3f}s   "
          f"({t_prov_legacy / max(t_prov_fast, 1e-9):.1f}x)")

    # --- projection ---------------------------------------------------------
    _rule("PROJECTED BACKFILL")

    n = args.universe * args.days
    print(f"{args.universe} symbols x {args.days} days = {n:,} symbol-days")
    print()
    print(f"{'scenario':<42s} {'per s-d':>9s} {'serial':>11s} {'8 procs':>11s}")
    print("-" * 76)

    def row(label: str, per_day: float) -> None:
        print(f"{label:<42s} {per_day:>8.2f}s {_fmt(per_day * n):>11s} "
              f"{_fmt(per_day * n / 8):>11s}")

    row("measured now", total)
    row("daily only (no intraday)", t_daily)

    # What the single-pass restructure would add on top, if it is ever needed.
    # HELD deliberately: the median roll-up is the one part that cannot be done
    # naively, and it restructures reviewed code.
    single_pass = t_daily * 1.3
    row("if intraday folded into one pass (HELD)", single_pass)

    print()
    print("The 8-process column assumes near-linear scaling: the work is")
    print("independent per symbol-day and Python is pegged on one core, so a")
    print("process pool should be close to it. Check `nproc` before trusting")
    print("the divisor.")
    print()
    print("A fetch of the same data takes ~0.36s per symbol-day. Any compute")
    print("figure far above that is overhead, not arithmetic.")


if __name__ == "__main__":
    main()
