"""
diagnose_transform.py — why is write_expiration ~15x slower on the VPS?

Read-only diagnostic. Makes ONE enumeration call and ONE interval call, then
times each step of the transform on that real frame and on a synthetic frame
of identical shape, on the same machine. Writes nothing to the store (its
parquet test goes to a temp file, deleted afterwards).

Benchmarked on a dev box: 35 expirations x ~28K rows transformed in ~1.5s
(~21 ms per expiration). A VPS run measured 22.6s for 34 expirations
(~665 ms each) on the same row count. Same code, same shape, 15x apart — so
the difference is the environment or the data, not the algorithm.

The prime suspect is object-dtype columns. _project builds its derived columns
with factorize-and-take (np.where over a small unique array), which should
leave every row pointing at ONE shared Python object per distinct value. If
real vendor data takes a different path and produces a distinct object per
row, then sort_values, drop_duplicates and Table.from_pandas all fall off
their fast paths at once — and a synthetic benchmark built with np.full would
never show it.

Usage:
    python diagnose_transform.py
    python diagnose_transform.py --ticker SPY --date 20260804
"""
from __future__ import annotations

import argparse
import platform
import sys
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import CHAIN_INTRADAY_DIR
from fetch_chain_intraday import (
    INTRADAY_END_TIME,
    INTRADAY_INTERVAL,
    INTRADAY_START_TIME,
    _project,
)
from lib.chain_intraday_store import (
    COLUMNS, DEDUPE_KEYS, SORT_KEYS, _SCHEMA, coerce,
)
from lib.market_hours import get_trading_days
from lib.thetadata import (
    enumerate_expirations_eod,
    fetch_first_order_window,
    today_et,
)


def last_completed_session() -> date:
    """Most recent NYSE session strictly BEFORE today (ET).

    Not lib.market_hours.last_trading_day: despite its docstring saying
    "fully-completed", it returns TODAY when today is a trading day, because
    it takes the last row of a schedule ending at today. For an EOD
    enumeration that is the one date guaranteed to fail — the session has not
    closed, so /v3/option/history/eod answers 400, which is not in the
    retry-ladder's status map and surfaces as a raw HTTPError.

    Stepping strictly back one session is always safe and always closed,
    whatever time of day this runs.
    """
    today = today_et()
    sched = get_trading_days(today - timedelta(days=14), today - timedelta(days=1))
    if not sched:
        raise SystemExit("No completed NYSE session in the last 14 days.")
    return sched[-1]


def hdr(t: str) -> None:
    print("\n" + "=" * 68)
    print(t)
    print("=" * 68)


def t(fn, *a, **k):
    t0 = time.perf_counter()
    r = fn(*a, **k)
    return time.perf_counter() - t0, r


def time_transform(frame: pd.DataFrame, label: str) -> dict:
    """Exactly the steps SessionWriter.write_expiration performs, itemised."""
    t_co, f1 = t(coerce, frame)
    t_dd, f2 = t(lambda: f1.drop_duplicates(subset=DEDUPE_KEYS, keep="last"))
    t_so, f3 = t(lambda: f2.sort_values(SORT_KEYS, kind="mergesort"))
    t_ar, tb = t(lambda: pa.Table.from_pandas(f3, schema=_SCHEMA,
                                              preserve_index=False))
    total = t_co + t_dd + t_so + t_ar
    print(f"\n  {label}  ({len(frame):,} rows)")
    for name, v in [("coerce", t_co), ("drop_duplicates", t_dd),
                    ("sort_values", t_so), ("Table.from_pandas", t_ar)]:
        print(f"    {name:<22}{v:>8.3f}s  {100 * v / total if total else 0:>5.1f}%")
    print(f"    {'TOTAL':<22}{total:>8.3f}s   "
          f"({total / len(frame) * 1e6:.2f} us/row)")
    return {"coerce": t_co, "dedupe": t_dd, "sort": t_so, "arrow": t_ar,
            "total": total, "table": tb}


def describe_objects(df: pd.DataFrame, label: str) -> None:
    """Object columns: how many distinct values, and how many distinct OBJECTS.

    These should differ enormously. factorize-and-take gives one Python object
    per distinct value, reused across every row. One object per ROW means the
    fast paths are gone and every comparison is a Python call.
    """
    print(f"\n  {label} — object-dtype columns")
    any_obj = False
    for c in df.columns:
        if df[c].dtype != object:
            continue
        any_obj = True
        vals = df[c].to_numpy()
        sample = vals[: min(len(vals), 50_000)]
        n_ids = len({id(v) for v in sample})
        n_vals = len({v for v in sample})
        flag = ""
        if n_ids > n_vals * 4 and n_ids > 100:
            flag = "   <-- DISTINCT OBJECT PER ROW (slow path)"
        print(f"    {c:<22} distinct values {n_vals:>7,}   "
              f"distinct objects {n_ids:>7,}{flag}")
    if not any_obj:
        print("    (none)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ticker", default="QQQ")
    ap.add_argument("--date",
                    help=("YYYYMMDD. Default = most recent COMPLETED session; "
                          "today is excluded because an unclosed session "
                          "400s on the EOD enumeration."))
    ap.add_argument("--expiration", help="YYYYMMDD; default = first enumerated")
    args = ap.parse_args()

    hdr("ENVIRONMENT")
    print(f"  python        {sys.version.split()[0]}  ({platform.platform()})")
    print(f"  pandas        {pd.__version__}")
    print(f"  numpy         {np.__version__}")
    print(f"  pyarrow       {pa.__version__}")
    try:
        import os
        print(f"  cpu count     {os.cpu_count()}")
    except Exception:
        pass
    # pandas 3 defaults strings to Arrow-backed; pandas 2 does not. That single
    # difference changes the cost of every string column in the transform.
    try:
        print(f"  pd string dtype default  "
              f"{pd.options.future.infer_string}")
    except Exception:
        print("  pd string dtype default  (option not present — pandas < 2.1)")

    if args.date:
        sess = datetime.strptime(args.date, "%Y%m%d").date()
        why = "from --date"
    else:
        sess = last_completed_session()
        why = "most recent COMPLETED session (today is excluded — an unclosed "
        why += "session 400s on the EOD enumeration)"
    if not get_trading_days(sess, sess):
        raise SystemExit(f"{sess} is not a trading day.")
    if sess >= today_et():
        print(f"\n  WARNING: {sess} is today or later. The EOD enumeration will "
              f"400 because the session has not closed.")
    ticker = args.ticker.upper()

    hdr(f"FETCH  {ticker}  {sess}")
    print(f"  session       {sess}  ({why})")
    # Printed so the call can be compared against production by eye. This is
    # byte-identical to what fetch_chain_intraday.py:370 issues; the only
    # difference there is a track() wrapper, which adds no parameters.
    print(f"  enumeration   GET /v3/option/history/eod  "
          f"symbol={ticker} expiration=* "
          f"start_date={sess:%Y%m%d} end_date={sess:%Y%m%d}")
    print(f"                (same call as fetch_chain_intraday.py:370 — "
          f"enumerate_expirations_eod(ticker, sess, sess))")

    try:
        el, exps = t(enumerate_expirations_eod, ticker, sess, sess)
    except Exception as exc:
        raise SystemExit(
            f"\n  enumeration FAILED: {type(exc).__name__}: {exc}\n"
            f"  A 400 here almost always means {sess} has not closed. Pass a "
            f"known-good date, e.g. --date 20260804."
        )
    exps = sorted(e for e in exps if e >= sess)
    print(f"  enumerated {len(exps)} expirations in {el:.2f}s")
    if not exps:
        raise SystemExit("No expirations — nothing to diagnose.")
    exp = (datetime.strptime(args.expiration, "%Y%m%d").date()
           if args.expiration else exps[len(exps) // 2])
    print(f"  using expiration {exp}")

    el, raw = t(fetch_first_order_window, ticker, exp, sess,
                INTRADAY_START_TIME, INTRADAY_END_TIME, INTRADAY_INTERVAL)
    print(f"  fetched {len(raw):,} raw rows in {el:.2f}s")
    if raw.empty:
        raise SystemExit("Empty response — pick another expiration.")

    hdr("RAW FRAME (as returned by the vendor parse)")
    print(raw.dtypes.to_string())
    describe_objects(raw, "raw")

    el, proj = t(_project, raw, ticker, sess)
    hdr("PROJECTED FRAME (what enters write_expiration)")
    print(f"  _project took {el:.3f}s -> {len(proj):,} rows")
    print(proj.dtypes.to_string())
    print(f"\n  memory_usage(deep=False) {proj.memory_usage(deep=False).sum()/1e6:>8.1f} MB")
    print(f"  memory_usage(deep=True)  {proj.memory_usage(deep=True).sum()/1e6:>8.1f} MB")
    describe_objects(proj, "projected")

    hdr("TRANSFORM — REAL vs SYNTHETIC, same shape, same machine")
    real = time_transform(proj, "REAL frame")

    # Synthetic frame: identical shape and column names, but every object
    # column built with np.full so all rows share ONE object. This is what my
    # benchmark used. If the real frame is much slower, the difference is the
    # data, not the machine.
    n = len(proj)
    syn = pd.DataFrame(index=pd.RangeIndex(n))
    for c in COLUMNS:
        src = proj[c] if c in proj.columns else None
        if src is None:
            syn[c] = np.nan
            continue
        if src.dtype == object:
            first = next((v for v in src.to_numpy()[:1000] if v is not None), None)
            syn[c] = np.full(n, first, dtype=object)
        else:
            syn[c] = src.to_numpy()
    synth = time_transform(syn, "SYNTHETIC frame (shared objects)")

    hdr("VERDICT")
    for k in ("coerce", "dedupe", "sort", "arrow", "total"):
        r, s = real[k], synth[k]
        ratio = (r / s) if s > 0 else float("inf")
        mark = "   <-- REAL IS SLOWER" if ratio > 2 else ""
        print(f"  {k:<10} real {r:>7.3f}s   synthetic {s:>7.3f}s   "
              f"{ratio:>5.1f}x{mark}")
    print(f"\n  extrapolated to {len(exps)} expirations: "
          f"real {real['total'] * len(exps):.1f}s, "
          f"synthetic {synth['total'] * len(exps):.1f}s")

    hdr("DISK — does the transform touch it?")
    # coerce / from_pandas are pure memory; only write_table does I/O. Timing
    # a write to the store volume and to the system temp dir separates them.
    tbl = real["table"]
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "probe.parquet"
        el, _ = t(lambda: pq.write_table(tbl, p, compression="snappy"))
        print(f"  write_table -> system temp   {el:>7.3f}s "
              f"({p.stat().st_size/1e6:.1f} MB)")
    probe_dir = CHAIN_INTRADAY_DIR / "_diag"
    try:
        probe_dir.mkdir(parents=True, exist_ok=True)
        p = probe_dir / "probe.parquet"
        el, _ = t(lambda: pq.write_table(tbl, p, compression="snappy"))
        print(f"  write_table -> store volume  {el:>7.3f}s "
              f"({p.stat().st_size/1e6:.1f} MB)")
        p.unlink(missing_ok=True)
        probe_dir.rmdir()
    except Exception as exc:
        print(f"  store-volume probe skipped: {exc}")

    print("\nDone. Nothing was written to the store.")


if __name__ == "__main__":
    main()
