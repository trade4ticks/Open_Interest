"""
verify_csv_path.py — prove the CSV response path matches the JSON one.

Read-only. Fetches the SAME (ticker, session, expiration) twice, once with
format=json and once with format=csv, and compares:

  1. the raw frames        — shape, column names, dtypes, values
  2. the projected frames  — what actually reaches the store
  3. the Arrow tables      — schema and values after coerce/sort/dedupe

CSV type inference can differ from what the JSON path produces, particularly
on the timestamp columns and on `right` / `symbol`, which come back quoted.
The CSV reader is therefore given explicit column types (lib/thetadata.py:
_CSV_COLUMN_TYPES) with `timestamp` and `expiration` deliberately left as
strings, so the downstream projection parses them with exactly the same code
on both paths. This script is what checks that intent held.

Also times both paths, so the decode saving is measured on real data rather
than extrapolated from a synthetic payload.

Usage:
    python verify_csv_path.py
    python verify_csv_path.py --ticker QQQ --date 20260804
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime, timedelta

import pandas as pd
import pyarrow as pa

from fetch_chain_intraday import (
    INTRADAY_END_TIME,
    INTRADAY_INTERVAL,
    INTRADAY_START_TIME,
    _project,
)
from lib.chain_intraday_store import DEDUPE_KEYS, SORT_KEYS, _SCHEMA, coerce
from lib.market_hours import get_trading_days
from lib.thetadata import (
    enumerate_expirations_eod,
    fetch_first_order_window,
    response_format,
    set_response_format,
    today_et,
)


def hdr(t: str) -> None:
    print("\n" + "=" * 70)
    print(t)
    print("=" * 70)


def last_completed_session() -> date:
    today = today_et()
    sched = get_trading_days(today - timedelta(days=14), today - timedelta(days=1))
    if not sched:
        raise SystemExit("No completed NYSE session in the last 14 days.")
    return sched[-1]


def fetch_as(fmt: str, ticker: str, exp: date, sess: date):
    prev = response_format()
    set_response_format(fmt)
    try:
        t0 = time.perf_counter()
        raw = fetch_first_order_window(
            ticker, exp, sess,
            INTRADAY_START_TIME, INTRADAY_END_TIME, INTRADAY_INTERVAL)
        return time.perf_counter() - t0, raw
    finally:
        set_response_format(prev)


def to_table(frame: pd.DataFrame) -> pa.Table:
    f = coerce(frame)
    f = f.drop_duplicates(subset=DEDUPE_KEYS, keep="last")
    f = f.sort_values(SORT_KEYS, kind="mergesort")
    return pa.Table.from_pandas(f, schema=_SCHEMA, preserve_index=False)


def compare_frames(a: pd.DataFrame, b: pd.DataFrame, label: str) -> bool:
    """Column-by-column comparison that names what differs, not just that it did."""
    ok = True
    print(f"\n  {label}")
    print(f"    shape        json {a.shape}   csv {b.shape}   "
          f"{'MATCH' if a.shape == b.shape else 'DIFFER'}")
    if a.shape != b.shape:
        return False

    ca, cb = list(a.columns), list(b.columns)
    if ca != cb:
        ok = False
        print(f"    columns      DIFFER")
        print(f"      json only: {sorted(set(ca) - set(cb))}")
        print(f"      csv  only: {sorted(set(cb) - set(ca))}")
        return False
    print(f"    columns      MATCH ({len(ca)})")

    for c in ca:
        sa, sb = a[c], b[c]
        dt_same = str(sa.dtype) == str(sb.dtype)
        # Compare as values, tolerating float noise; NaN == NaN counts as equal.
        try:
            if pd.api.types.is_float_dtype(sa) and pd.api.types.is_float_dtype(sb):
                same = bool((((sa - sb).abs() < 1e-12)
                             | (sa.isna() & sb.isna())).all())
            else:
                same = bool((sa.eq(sb) | (sa.isna() & sb.isna())).all())
        except Exception as exc:
            same = False
            print(f"      {c:<22} comparison failed: {exc}")
        if not (dt_same and same):
            ok = False
            print(f"      {c:<22} dtype json={sa.dtype} csv={sb.dtype} "
                  f"{'' if dt_same else '<-- DTYPE'}  "
                  f"values {'match' if same else 'DIFFER'}")
            if not same:
                diff = sa.ne(sb) & ~(sa.isna() & sb.isna())
                idx = list(diff[diff].index[:3])
                for i in idx:
                    print(f"        row {i}: json={sa.loc[i]!r}  csv={sb.loc[i]!r}")
    if ok:
        print(f"    dtypes+values MATCH on all {len(ca)} columns")
    return ok


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ticker", default="QQQ")
    ap.add_argument("--date", help="YYYYMMDD; default = last completed session")
    ap.add_argument("--expiration", help="YYYYMMDD; default = middle of the chain")
    args = ap.parse_args()

    sess = (datetime.strptime(args.date, "%Y%m%d").date()
            if args.date else last_completed_session())
    if not get_trading_days(sess, sess):
        raise SystemExit(f"{sess} is not a trading day.")
    ticker = args.ticker.upper()

    hdr(f"VERIFY CSV vs JSON   {ticker}  {sess}")
    exps = sorted(e for e in enumerate_expirations_eod(ticker, sess, sess)
                  if e >= sess)
    if not exps:
        raise SystemExit("No expirations enumerated.")
    exp = (datetime.strptime(args.expiration, "%Y%m%d").date()
           if args.expiration else exps[len(exps) // 2])
    print(f"  expiration {exp}  (of {len(exps)} enumerated)")

    t_json, raw_json = fetch_as("json", ticker, exp, sess)
    t_csv, raw_csv = fetch_as("csv", ticker, exp, sess)
    print(f"\n  fetch+parse   json {t_json:.3f}s   csv {t_csv:.3f}s   "
          f"{t_json / t_csv if t_csv else 0:.2f}x")

    if raw_json.empty and raw_csv.empty:
        raise SystemExit("Both responses empty — pick another expiration.")
    if raw_json.empty != raw_csv.empty:
        raise SystemExit(f"MISMATCH: json empty={raw_json.empty} "
                         f"csv empty={raw_csv.empty}")

    hdr("1. RAW FRAMES")
    # Column ORDER may legitimately differ (CSV header order vs JSON format
    # list); align before comparing so a reordering is not reported as a
    # value mismatch.
    if set(raw_json.columns) == set(raw_csv.columns):
        raw_csv = raw_csv[list(raw_json.columns)]
    raw_ok = compare_frames(raw_json, raw_csv, "raw vendor frame")
    print("\n  dtypes:")
    for c in raw_json.columns:
        print(f"    {c:<22} json {str(raw_json[c].dtype):<18} "
              f"csv {str(raw_csv[c].dtype) if c in raw_csv else '-'}")

    hdr("2. PROJECTED FRAMES (what reaches the store)")
    p_json = _project(raw_json, ticker, sess)
    p_csv = _project(raw_csv, ticker, sess)
    proj_ok = compare_frames(p_json, p_csv, "projected frame")

    hdr("3. ARROW TABLES (after coerce/dedupe/sort)")
    t1, t2 = to_table(p_json), to_table(p_csv)
    schema_ok = t1.schema.equals(t2.schema)
    rows_ok = t1.num_rows == t2.num_rows
    vals_ok = t1.equals(t2)
    print(f"  rows          json {t1.num_rows:,}   csv {t2.num_rows:,}   "
          f"{'MATCH' if rows_ok else 'DIFFER'}")
    print(f"  schema        {'MATCH' if schema_ok else 'DIFFER'}")
    print(f"  table.equals  {'MATCH' if vals_ok else 'DIFFER'}")
    if not vals_ok and schema_ok and rows_ok:
        for name in t1.schema.names:
            c1, c2 = t1.column(name), t2.column(name)
            if not c1.equals(c2):
                print(f"    column {name} DIFFERS")
                print(f"      json[:3] {c1.slice(0, 3).to_pylist()}")
                print(f"      csv [:3] {c2.slice(0, 3).to_pylist()}")

    hdr("VERDICT")
    all_ok = raw_ok and proj_ok and schema_ok and rows_ok and vals_ok
    print(f"  raw frames        {'PASS' if raw_ok else 'FAIL'}")
    print(f"  projected frames  {'PASS' if proj_ok else 'FAIL'}")
    print(f"  arrow tables      {'PASS' if (schema_ok and rows_ok and vals_ok) else 'FAIL'}")
    print(f"\n  {'CSV path is equivalent — safe to leave as the default.'
            if all_ok else
            'NOT equivalent. Run with --response-format json until fixed.'}")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
