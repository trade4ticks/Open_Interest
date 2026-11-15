"""
build_spx_price_5min.py — SPX price metrics per 5-minute bar: one row per
(trade_date, quote_time) in `spx_price_5min`. Every value combines the frozen
state as of the prior session's close with the bar's own spx_open.

    python build_spx_price_5min.py --start 20200102 --end 20261113
    python build_spx_price_5min.py --start 20261001 --end 20261113 --dry-run
    python build_spx_price_5min.py --print-ddl

Reads public.index_ohlc read-only through SPX_INDEX_OHLC_DSN, always from
lib.spx_price.SEED_FROM (the recursion seed), whatever --start is. Writes to
SPX_FEATURES_DSN. Definitions in lib/spx_price.py.

Every run prints the daily-OHLC report, the spot report for the output
sessions, and the spec §4.1 report of bars outside the textbook range. Any
hard-check failure exits 1 before anything is written.

IDEMPOTENT: every row in [start, end] is deleted and the fresh rows inserted,
in one transaction.
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
import lib.spx_price as P
import lib.spx_table_b as B
from build_spx_features import _parse_day
from lib.market_hours import get_trading_days


def ddl() -> str:
    cols = ["    trade_date DATE NOT NULL", "    quote_time TIME NOT NULL", "    spot DOUBLE PRECISION",
            "    ohlc_session DATE NOT NULL"]
    cols += [f"    {c} DOUBLE PRECISION" for c in P.COLUMNS_P5[4:]]
    cols += ["    built_at TIMESTAMPTZ NOT NULL DEFAULT now()", "    PRIMARY KEY (trade_date, quote_time)"]
    return f"CREATE TABLE IF NOT EXISTS {P.TABLE_P5} (\n" + ",\n".join(cols) + "\n);"


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(ddl())
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s AND table_schema = current_schema()", (P.TABLE_P5,))
        have = {r[0] for r in cur.fetchall()}
    want = set(P.COLUMNS_P5) | {"built_at"}
    if have != want:
        raise SystemExit(f"{P.TABLE_P5} exists with a different column set.\n"
                         f"  missing: {sorted(want - have)}\n  extra:   {sorted(have - want)}\n"
                         "Not altering it automatically.")


def write(dsn: str, frame, start, end) -> int:
    rows = [tuple(F.to_db_value(r[c]) for c in P.COLUMNS_P5) for r in frame[P.COLUMNS_P5].to_dict("records")]
    with psycopg2.connect(dsn) as conn:
        ensure_table(conn)
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {P.TABLE_P5} WHERE trade_date BETWEEN %s AND %s", (start, end))
            deleted = cur.rowcount
            psycopg2.extras.execute_values(
                cur, f"INSERT INTO {P.TABLE_P5} ({', '.join(P.COLUMNS_P5)}) VALUES %s", rows, page_size=2000)
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
    print(f"=== SPX price 5-min build  {datetime.now():%Y-%m-%d %H:%M:%S}  "
          f"[{args.start} → {args.end}]{'  DRY RUN' if args.dry_run else ''} ===")
    bars = P.load_ohlc(config.SPX_INDEX_OHLC_DSN, P.SEED_FROM, args.end)
    print(f"  index_ohlc rows read: {len(bars):,}  ({bars['trade_date'].min()} → {bars['trade_date'].max()}) "
          f"in {_time.time() - t0:.1f}s")
    res = P.compute_price_5min(args.start, args.end, bars=bars)
    for n in res.notes:
        print(f"  note: {n}")
    df = res.frame
    sessions = get_trading_days(res.start, res.end_eff)
    print(f"  rows computed  : {len(df):,}  ({res.start} → {res.end_eff}, {len(sessions)} sessions)")
    print(f"  lead-in        : {res.lead_in_sessions} completed sessions before {res.start} "
          f"(from {res.daily['trade_date'].min()}, the recursion seed); the 52-week metrics need "
          f"{P.LEVEL_HISTORY_P5}; fewest known 52-week readings any bar has before {res.start}: "
          f"{res.lead_in_min_readings} ({P.Z_WINDOW} needed for its z-score)")
    if res.lead_in_min_readings < P.Z_WINDOW:
        print("  ! short lead-in: the first rows' z-scores are NULL (never a short window)")
    print(f"  last complete session in index_ohlc: {res.data_last}")
    print(f"  computed in {_time.time() - t0:.1f}s")

    print("\n--- daily OHLC report, every session from the seed (never fails the run) ---")
    P.daily_ohlc_report(res.daily)
    print("\n--- spot report, output sessions: P = spx_open (never fails the run) ---")
    B.spot_report(res.grid, bars, sessions)
    print("\n--- spec §4.1: bars outside the textbook range (expected; never clamped, never fails) ---")
    P.textbook_report(df)

    print("\n--- hard checks ---")
    chk = F.Checker()
    P.check_price_5min(df, sessions=sessions, chk=chk, full=res.full, daily=res.daily)
    probe_days = sorted({sessions[0], sessions[len(sessions) // 2], sessions[-1]})
    bad = P.lookahead_probe_5min(bars, probe_days, df)
    chk.check(not bad, f"LOOKAHEAD probe: {', '.join(map(str, probe_days))} rebuilt from earlier sessions plus "
                       f"only that day's spx_open through 12:00 (its high, low, close and later opens blanked); "
                       f"every bar through 12:00 matches ({len(bad)} mismatch(es))"
              + "".join(f"\n    {d} {q} {c}: built {a!r}, probe {b!r}" for d, q, c, a, b in bad[:10]))

    if chk.failed:
        print(f"\n{len(chk.failed)} hard check(s) FAILED — nothing written.")
        return 1
    if args.dry_run:
        print(f"\nAll hard checks pass. DRY RUN — nothing written.  ({_time.time() - t0:.1f}s)")
        return 0
    n = write(config.SPX_FEATURES_DSN, df, res.start, res.end_eff)
    print(f"\nWROTE {n:,} rows to {P.TABLE_P5}, {df['trade_date'].min()} → {df['trade_date'].max()}, "
          f"lead-in {res.lead_in_sessions} sessions  ({_time.time() - t0:.1f}s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
