"""
build_spx_price_daily.py — SPX price metrics, daily: one row per trade_date in
`spx_price_daily`, every value as of the prior session's close.

    python build_spx_price_daily.py --start 20200102 --end 20261113
    python build_spx_price_daily.py --start 20261001 --end 20261113 --dry-run
    python build_spx_price_daily.py --print-ddl

Reads public.index_ohlc read-only through SPX_INDEX_OHLC_DSN, always from
lib.spx_price.SEED_FROM (the recursion seed), whatever --start is. Writes to
SPX_FEATURES_DSN. Definitions in lib/spx_price.py.

Every run prints the daily-OHLC report (sessions with no usable bar or only
part of one) and the spot report for the sessions feeding the output rows.
Any hard-check failure exits 1 before anything is written.

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
    cols = ["    trade_date DATE NOT NULL", "    ohlc_session DATE NOT NULL"]
    cols += [f"    {c} DOUBLE PRECISION" for c in P.COLUMNS_PD[2:]]
    cols += ["    built_at TIMESTAMPTZ NOT NULL DEFAULT now()", "    PRIMARY KEY (trade_date)"]
    return f"CREATE TABLE IF NOT EXISTS {P.TABLE_PD} (\n" + ",\n".join(cols) + "\n);"


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(ddl())
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s AND table_schema = current_schema()", (P.TABLE_PD,))
        have = {r[0] for r in cur.fetchall()}
    want = set(P.COLUMNS_PD) | {"built_at"}
    if have != want:
        raise SystemExit(f"{P.TABLE_PD} exists with a different column set.\n"
                         f"  missing: {sorted(want - have)}\n  extra:   {sorted(have - want)}\n"
                         "Not altering it automatically.")


def write(dsn: str, frame, start, end) -> int:
    rows = [tuple(F.to_db_value(r[c]) for c in P.COLUMNS_PD) for r in frame[P.COLUMNS_PD].to_dict("records")]
    with psycopg2.connect(dsn) as conn:
        ensure_table(conn)
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {P.TABLE_PD} WHERE trade_date BETWEEN %s AND %s", (start, end))
            deleted = cur.rowcount
            psycopg2.extras.execute_values(
                cur, f"INSERT INTO {P.TABLE_PD} ({', '.join(P.COLUMNS_PD)}) VALUES %s", rows, page_size=2000)
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
    print(f"=== SPX price daily build  {datetime.now():%Y-%m-%d %H:%M:%S}  "
          f"[{args.start} → {args.end}]{'  DRY RUN' if args.dry_run else ''} ===")
    bars = P.load_ohlc(config.SPX_INDEX_OHLC_DSN, P.SEED_FROM, args.end)
    print(f"  index_ohlc rows read: {len(bars):,}  ({bars['trade_date'].min()} → {bars['trade_date'].max()}) "
          f"in {_time.time() - t0:.1f}s")
    res = P.compute_price_daily(args.start, args.end, bars=bars)
    for n in res.notes:
        print(f"  note: {n}")
    df = res.frame
    sessions = get_trading_days(res.start, res.end_eff)
    first_in = res.daily["trade_date"].min()
    print(f"  rows computed  : {len(df):,}  ({res.start} → {res.end_eff}, {len(sessions)} sessions)")
    print(f"  lead-in        : {res.lead_in_sessions} completed sessions before {res.start} "
          f"(from {first_in}, the recursion seed); a row needs {P.LEVEL_HISTORY} for every level "
          f"and {P.LEVEL_HISTORY + P.Z_WINDOW - 1} for every z-score")
    if res.lead_in_sessions < P.LEVEL_HISTORY + P.Z_WINDOW - 1:
        print("  ! short lead-in: the first rows have NULL levels / z-scores (never a short window)")
    print(f"  last complete session in index_ohlc: {res.data_last}")
    print(f"  computed in {_time.time() - t0:.1f}s")

    print("\n--- daily OHLC report, every session from the seed (never fails the run) ---")
    P.daily_ohlc_report(res.daily)

    print("\n--- spot report, sessions feeding the output rows (never fails the run) ---")
    feed_in = get_trading_days(df["ohlc_session"].min(), df["ohlc_session"].max()) if len(df) else []
    if feed_in:
        sp = bars[(bars["trade_date"] >= feed_in[0]) & (bars["trade_date"] <= feed_in[-1])]
        B.spot_report(B.classify_spot(B.expected_grid(feed_in), sp), sp, feed_in)

    print("\n--- hard checks ---")
    chk = F.Checker()
    P.check_price_daily(df, sessions=sessions, chk=chk, full=res.full, daily=res.daily)
    probe_days = sorted({sessions[0], sessions[len(sessions) // 2], sessions[-1]})
    bad = P.lookahead_probe(bars, probe_days, df)
    chk.check(not bad, f"LOOKAHEAD probe: rows {', '.join(map(str, probe_days))} recomputed from index_ohlc "
                       f"bars strictly before their trade_date match the build ({len(bad)} mismatch(es))"
              + "".join(f"\n    {d} {c}: built {a!r}, probe {b!r}" for d, c, a, b in bad[:10]))

    if chk.failed:
        print(f"\n{len(chk.failed)} hard check(s) FAILED — nothing written.")
        return 1
    if args.dry_run:
        print(f"\nAll hard checks pass. DRY RUN — nothing written.  ({_time.time() - t0:.1f}s)")
        return 0
    n = write(config.SPX_FEATURES_DSN, df, res.start, res.end_eff)
    print(f"\nWROTE {n:,} rows to {P.TABLE_PD}, {df['trade_date'].min()} → {df['trade_date'].max()}, "
          f"lead-in {res.lead_in_sessions} sessions  ({_time.time() - t0:.1f}s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
