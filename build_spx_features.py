"""
build_spx_features.py — table A: daily SPX/SPXW open interest and volume
metrics, one row per trade_date, written to `spx_oi_daily`.

    python build_spx_features.py --start 20200102 --end 20201231
    python build_spx_features.py --start 20261001 --end 20261113 --dry-run
    python build_spx_features.py --print-ddl

Reads the raw stores only (SPX_OI_RAW_DIR, SPX_VOLUME_DIR); never fetches.
Writes to SPX_FEATURES_DSN. Definitions, timing and checks live in
lib/spx_features.py; this script runs them and writes the result.

Row T = OI labelled T + volume of session T-1. The LOOKAHEAD hard check
prints on every run; any hard-check failure exits 1 before anything is written.

IDEMPOTENT: inside one transaction, every row in [start, end] is deleted and
the freshly computed rows inserted. Re-running a range converges.

LEAD-IN: the 60 NYSE sessions before --start are read from the stores so d1,
d5 and the 60-session z-scores are complete on the first output row. Only
rows in [start, end] are written. A lead-in shorter than 60 (the stores begin
2020-01-02) leaves those z-scores NULL, never computed on a short window.

The split-factor machinery stays in the read path (lib.split_factors, from
underlying_ohlc in the project database); indices do not split, so any
factor other than 1.0 fails the run.
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime

import psycopg2
import psycopg2.extras

import config
import lib.spx_features as F
from db import get_connection
from lib.market_hours import get_trading_days
from lib.split_factors import load_splits


def _parse_day(s: str) -> date:
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    raise argparse.ArgumentTypeError(f"bad date {s!r}; use YYYYMMDD")


def ddl() -> str:
    cols = ["    trade_date DATE PRIMARY KEY", "    vol_session DATE"]
    for c in F.COLUMNS_A[2:]:
        cols.append(f"    {c} {'BIGINT' if c in F.COUNT_COLS else 'DOUBLE PRECISION'}")
    cols.append("    built_at TIMESTAMPTZ NOT NULL DEFAULT now()")
    return f"CREATE TABLE IF NOT EXISTS {F.TABLE_A} (\n" + ",\n".join(cols) + "\n);"


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(ddl())
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s AND table_schema = current_schema()", (F.TABLE_A,))
        have = {r[0] for r in cur.fetchall()}
    want = set(F.COLUMNS_A) | {"built_at"}
    if have != want:
        raise SystemExit(
            f"{F.TABLE_A} exists with a different column set.\n"
            f"  missing: {sorted(want - have)}\n  extra:   {sorted(have - want)}\n"
            "Not altering it automatically.")


def _row(rec: dict) -> tuple:
    out = []
    for c in F.COLUMNS_A:
        v = F.to_db_value(rec[c])
        if v is not None and c in F.COUNT_COLS:
            v = int(round(v))
        out.append(v)
    return tuple(out)


def write(dsn: str, frame, start: date, end: date) -> int:
    rows = [_row(r) for r in frame[F.COLUMNS_A].to_dict("records")]
    cols = ", ".join(F.COLUMNS_A)
    with psycopg2.connect(dsn) as conn:
        ensure_table(conn)
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {F.TABLE_A} WHERE trade_date BETWEEN %s AND %s", (start, end))
            deleted = cur.rowcount
            psycopg2.extras.execute_values(
                cur, f"INSERT INTO {F.TABLE_A} ({cols}) VALUES %s", rows, page_size=500)
        conn.commit()
    print(f"  replaced {deleted} existing row(s)")
    return len(rows)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--start", type=_parse_day)
    p.add_argument("--end", type=_parse_day)
    p.add_argument("--dry-run", action="store_true", help="compute and check; write nothing")
    p.add_argument("--print-ddl", action="store_true", help="print the CREATE TABLE and exit")
    args = p.parse_args()

    if args.print_ddl:
        print(ddl())
        return 0
    if not args.start or not args.end:
        p.error("--start and --end are required")
    if args.end < args.start:
        p.error("--end must be >= --start")
    dsn = config.SPX_FEATURES_DSN
    if not args.dry_run and not dsn:
        p.error("SPX_FEATURES_DSN is not set (.env); use --dry-run to compute without writing")

    t0 = time.time()
    print(f"=== SPX table A build  {datetime.now():%Y-%m-%d %H:%M:%S}  "
          f"[{args.start} → {args.end}]{'  DRY RUN' if args.dry_run else ''} ===")
    print(f"  SPX_OI_RAW_DIR : {config.SPX_OI_RAW_DIR}")
    print(f"  SPX_VOLUME_DIR : {config.SPX_VOLUME_DIR}")

    with get_connection() as conn:
        splits = {r: load_splits(conn, r) for r in F.ROOTS}

    res = F.compute_table_a(args.start, args.end,
                            oi_store=config.SPX_OI_RAW_DIR, vol_store=config.SPX_VOLUME_DIR,
                            splits=splits)
    for n in res.notes:
        print(f"  note: {n}")
    df = res.frame
    print(f"  rows computed  : {len(df)}  ({res.start} → {res.end_eff})")
    print(f"  lead-in        : {res.lead_in_used} of {res.lead_in_wanted} sessions before {res.start} carry OI")
    print(f"  volume store   : first session {res.vol_first}")
    print(f"  OI rows with expiration < trade_date : {int(df['oi_rows_neg_dte'].fillna(0).sum()):,}")
    print(f"  volume rows with expiration < session: {int(df['vol_rows_neg_dte'].fillna(0).sum()):,}")
    print(f"  computed in {time.time() - t0:.1f}s\n")

    print("--- hard checks ---")
    chk = F.Checker()
    chk.check(res.bad_split_factors == 0,
              f"split factors resolve to 1.0 (events: "
              + ", ".join(f"{r} {n}" for r, n in res.split_events.items())
              + f"; non-1.0 factors {res.bad_split_factors})")
    F.check_table_a(df, sessions=get_trading_days(res.start, res.end_eff),
                    vol_first=res.vol_first, chk=chk, full=res.full)

    if chk.failed:
        print(f"\n{len(chk.failed)} hard check(s) FAILED — nothing written.")
        return 1
    if args.dry_run:
        print("\nAll hard checks pass. DRY RUN — nothing written.")
        return 0

    n = write(dsn, df, res.start, res.end_eff)
    print(f"\nWROTE {n} rows to {F.TABLE_A}, {df['trade_date'].min()} → {df['trade_date'].max()}, "
          f"lead-in {res.lead_in_used}/{res.lead_in_wanted}  ({time.time() - t0:.1f}s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
