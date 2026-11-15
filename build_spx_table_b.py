"""
build_spx_table_b.py — table B: spot-relative SPX/SPXW OI and volume metrics
per 5-minute bar, one row per (trade_date, quote_time), written to
`spx_oi_5min`.

    python build_spx_table_b.py --start 20200102 --end 20201231
    python build_spx_table_b.py --start 20261001 --end 20261113 --dry-run
    python build_spx_table_b.py --print-ddl

Reads the raw stores (SPX_OI_RAW_DIR, SPX_VOLUME_DIR) and, read-only,
public.index_ohlc through SPX_INDEX_OHLC_DSN. Writes to SPX_FEATURES_DSN.
Definitions in lib/spx_table_b.py.

Every run prints the spot report: per session, bars of unknown spot and why,
early closes, and index_ohlc rows the grid did not read. Any hard-check
failure exits 1 before anything is written.

IDEMPOTENT: every row in [start, end] is deleted and the fresh rows inserted,
in one transaction.

LEAD-IN: 75 sessions before --start are computed and not written, so every
bar has 60 readings for its same-bar z-score on the first output day.
"""
from __future__ import annotations

import argparse
import sys
import time as _time
from datetime import datetime

import psycopg2
import psycopg2.extras

import config
import lib.spx_features as F
import lib.spx_table_b as B
from build_spx_features import _parse_day
from db import get_connection
from lib.market_hours import get_trading_days
from lib.split_factors import load_splits


def ddl() -> str:
    cols = ["    trade_date DATE NOT NULL", "    quote_time TIME NOT NULL",
            "    spot DOUBLE PRECISION", "    vol_session DATE"]
    cols += [f"    {c} DOUBLE PRECISION" for c in B.COLUMNS_B[4:]]
    cols += ["    built_at TIMESTAMPTZ NOT NULL DEFAULT now()",
             "    PRIMARY KEY (trade_date, quote_time)"]
    return f"CREATE TABLE IF NOT EXISTS {B.TABLE_B} (\n" + ",\n".join(cols) + "\n);"


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(ddl())
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s AND table_schema = current_schema()", (B.TABLE_B,))
        have = {r[0] for r in cur.fetchall()}
    want = set(B.COLUMNS_B) | {"built_at"}
    if have != want:
        raise SystemExit(f"{B.TABLE_B} exists with a different column set.\n"
                         f"  missing: {sorted(want - have)}\n  extra:   {sorted(have - want)}\n"
                         "Not altering it automatically.")


def write(dsn: str, frame, start, end) -> int:
    rows = [tuple(F.to_db_value(r[c]) for c in B.COLUMNS_B)
            for r in frame[B.COLUMNS_B].to_dict("records")]
    with psycopg2.connect(dsn) as conn:
        ensure_table(conn)
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {B.TABLE_B} WHERE trade_date BETWEEN %s AND %s", (start, end))
            deleted = cur.rowcount
            psycopg2.extras.execute_values(
                cur, f"INSERT INTO {B.TABLE_B} ({', '.join(B.COLUMNS_B)}) VALUES %s", rows, page_size=2000)
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
    if not config.SPX_INDEX_OHLC_DSN:
        p.error("SPX_INDEX_OHLC_DSN is not set (.env)")
    if not args.dry_run and not config.SPX_FEATURES_DSN:
        p.error("SPX_FEATURES_DSN is not set (.env); use --dry-run to compute without writing")

    t0 = _time.time()
    print(f"=== SPX table B build  {datetime.now():%Y-%m-%d %H:%M:%S}  "
          f"[{args.start} → {args.end}]{'  DRY RUN' if args.dry_run else ''} ===")
    with get_connection() as conn:
        splits = {r: load_splits(conn, r) for r in F.ROOTS}

    res = B.compute_table_b(
        args.start, args.end, oi_store=config.SPX_OI_RAW_DIR, vol_store=config.SPX_VOLUME_DIR,
        splits=splits, spot_source=lambda lo, hi: B.load_spot(config.SPX_INDEX_OHLC_DSN, lo, hi))
    for n in res.notes:
        print(f"  note: {n}")
    df = res.frame
    sessions = get_trading_days(res.start, res.end_eff)
    print(f"  rows computed  : {len(df):,}  ({res.start} → {res.end_eff}, {len(sessions)} sessions)")
    print(f"  lead-in        : {res.lead_in_sessions} sessions read; fewest readings any bar has "
          f"before {res.start}: {res.lead_in_min_readings} (60 needed for its z-score)")
    print(f"  computed in {_time.time() - t0:.1f}s")

    print("\n--- spot report (never fails the run) ---")
    B.spot_report(res.grid, res.spot_df, sessions)

    print("\n--- hard checks ---")
    chk = F.Checker()
    a = res.table_a
    chk.check(a.bad_split_factors == 0, f"split factors resolve to 1.0 (non-1.0: {a.bad_split_factors})")
    chk.check(not res.total_mismatch,
              f"per-strike OI / volume totals equal table A's total_oi / total_vol "
              f"({len(res.total_mismatch)} mismatch(es))"
              + "".join(f"\n    {d} {side}: {x} vs {y}" for d, side, x, y in res.total_mismatch[:5]))
    B.check_table_b(df, sessions=sessions, chk=chk, full=res.full)

    if chk.failed:
        print(f"\n{len(chk.failed)} hard check(s) FAILED — nothing written.")
        return 1
    if args.dry_run:
        print("\nAll hard checks pass. DRY RUN — nothing written.")
        return 0
    n = write(config.SPX_FEATURES_DSN, df, res.start, res.end_eff)
    print(f"\nWROTE {n:,} rows to {B.TABLE_B}, {df['trade_date'].min()} → {df['trade_date'].max()}, "
          f"lead-in {res.lead_in_sessions} sessions  ({_time.time() - t0:.1f}s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
