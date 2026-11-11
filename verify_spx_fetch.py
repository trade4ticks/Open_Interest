"""
verify_spx_fetch.py — checks on the raw SPX / SPXW OI and volume stores.

    python verify_spx_fetch.py --start 20200102 --end 20201231
    python verify_spx_fetch.py --start 20200102 --end 20261030 --dates 20261026,20230315

Reads the parquet files directly with DuckDB, not through lib.parquet_store /
lib.spx_volume_store, so a writer bug cannot hide itself. Read-only, apart
from one SELECT against underlying_ohlc for the split check.

HARD checks (exit 1 on failure):
  * every NYSE session in range present, per root, per store
  * (root, expiration, strike, right, trade_date) unique in both stores
  * root is load-bearing: dropping it from the key collides, with differing OI
  * no stored volume <= 0
  * split factors resolve to exactly 1.0 for both roots

REPORTED for eyeballing (never fail the run):
  * rows / sessions / expirations / OI by root by year
  * SPX OI at DTE <= 7, with each expiration classed monthly or not
  * SPXW expiration cadence (expirations per week, weekday mix) by year
  * the §3 table (rows, total OI, expirations, OI at DTE<=7 / >90) for --dates
  * bytes on disk and a projection for the full backfill
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta

import duckdb
import pandas as pd
import pyarrow.parquet as pq

from config import SPX_OI_RAW_DIR, SPX_ROOTS, SPX_VOLUME_DIR
from lib.market_hours import get_trading_days

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 30)
pd.set_option("display.max_rows", 200)

FAILURES: list[str] = []


def _parse_day(s: str) -> date:
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    raise argparse.ArgumentTypeError(f"bad date {s!r}; use YYYYMMDD")


def check(ok: bool, label: str) -> None:
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}")
    if not ok:
        FAILURES.append(label)


def section(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def _files(store_dir, roots) -> list[str]:
    return sorted(str(p) for r in roots for p in (store_dir / r).glob("*.parquet"))


def _register(con, name: str, store_dir, value_col: str, start: date, end: date) -> bool:
    files = _files(store_dir, SPX_ROOTS)
    if not files:
        print(f"  no files under {store_dir} for {SPX_ROOTS}")
        return False
    # Views cannot hold prepared parameters, so the literals are inlined.
    # root = the directory above {year}.parquet; the regexp accepts / and \.
    file_list = ", ".join("'" + f.replace("'", "''") + "'" for f in files)
    con.execute(f"""
        CREATE OR REPLACE VIEW {name} AS
        SELECT regexp_extract(filename, '([^/\\\\]+)[/\\\\][0-9]{{4}}\\.parquet$', 1) AS root,
               trade_date, expiration, strike, option_type, {value_col} AS v,
               (expiration - trade_date) AS dte
        FROM read_parquet([{file_list}], filename = true)
        WHERE trade_date BETWEEN DATE '{start.isoformat()}' AND DATE '{end.isoformat()}'
    """)
    return True


def _monthly_expiry(d: date) -> date:
    """Third Friday of d's month, or the prior trading day if it is a holiday."""
    first = d.replace(day=1)
    fri = first + timedelta(days=(4 - first.weekday()) % 7 + 14)
    days = get_trading_days(fri - timedelta(days=7), fri)
    return days[-1] if days else fri


# --- checks shared by both stores -------------------------------------------

def common_checks(con, view: str, label: str, sessions: list[date]) -> None:
    section(f"{label}: coverage, by root by year")
    print(con.execute(f"""
        SELECT root, year(trade_date) AS year,
               count(*) AS rows, count(DISTINCT trade_date) AS sessions,
               count(DISTINCT expiration) AS expirations,
               round(count(*) / count(DISTINCT trade_date)) AS rows_per_session,
               round(sum(v) / count(DISTINCT trade_date)) AS {'oi' if label == 'OI' else 'volume'}_per_session
        FROM {view} GROUP BY 1, 2 ORDER BY 1, 2
    """).df().to_string(index=False))

    section(f"{label}: hard checks")
    want = set(sessions)
    for root in SPX_ROOTS:
        have = {r[0] for r in con.execute(
            f"SELECT DISTINCT trade_date FROM {view} WHERE root = ?", [root]).fetchall()}
        missing = sorted(want - have)
        extra = sorted(have - want)
        check(not missing, f"{label} {root}: all {len(want)} sessions present"
              + (f" — MISSING {len(missing)}: {', '.join(map(str, missing[:15]))}"
                 f"{' …' if len(missing) > 15 else ''}" if missing else ""))
        if extra:
            print(f"  [NOTE] {label} {root}: {len(extra)} non-NYSE-session dates "
                  f"stored, e.g. {extra[:5]}")

    dupes = con.execute(f"""
        SELECT count(*) FROM (
            SELECT 1 FROM {view}
            GROUP BY root, expiration, strike, option_type, trade_date
            HAVING count(*) > 1)
    """).fetchone()[0]
    check(dupes == 0, f"{label}: (root, expiration, strike, right, trade_date) unique "
          f"({dupes:,} duplicate keys)")

    shared = con.execute(f"""
        WITH k AS (
            SELECT trade_date, expiration, strike, option_type,
                   count(DISTINCT root) AS n_roots,
                   count(DISTINCT v)    AS n_values
            FROM {view}
            GROUP BY trade_date, expiration, strike, option_type)
        SELECT count(*) FILTER (WHERE n_roots = 2),
               count(*) FILTER (WHERE n_roots = 2 AND n_values = 2),
               count(DISTINCT trade_date) FILTER (WHERE n_roots = 2),
               count(DISTINCT trade_date)
        FROM k
    """).fetchone()
    n_shared, n_diff, d_shared, d_all = shared
    per_day = n_shared / d_all if d_all else 0
    check(n_shared > 0 and n_diff > 0,
          f"{label}: root is load-bearing — dropping it collides on {n_shared:,} "
          f"keys ({per_day:,.0f}/session, on {d_shared}/{d_all} sessions), "
          f"{n_diff:,} with a different value per root")

    if n_shared:
        print("\n  Sample keys present under both roots (latest session):")
        print(con.execute(f"""
            WITH last AS (SELECT max(trade_date) AS d FROM {view}),
                 shared_keys AS (
                    SELECT trade_date, expiration, strike, option_type
                    FROM {view}, last WHERE trade_date = last.d
                    GROUP BY ALL HAVING count(DISTINCT root) = 2)
            SELECT b.trade_date, b.expiration, b.strike, b.option_type,
                   max(v) FILTER (WHERE root = 'SPX')  AS spx,
                   max(v) FILTER (WHERE root = 'SPXW') AS spxw
            FROM shared_keys b JOIN {view} USING (trade_date, expiration, strike, option_type)
            GROUP BY ALL
            ORDER BY abs(coalesce(spx, 0) - coalesce(spxw, 0)) DESC
            LIMIT 5
        """).df().to_string(index=False))


# --- OI-only reports ---------------------------------------------------------

def oi_reports(con, dates: list[date]) -> None:
    section("OI: SPX root at DTE <= 7 (AM-settled monthlies in expiration week)")
    spx_short = con.execute("""
        SELECT expiration, count(DISTINCT trade_date) AS sessions,
               min(dte) AS min_dte, max(dte) AS max_dte, sum(v) AS oi_summed
        FROM oi WHERE root = 'SPX' AND dte <= 7
        GROUP BY 1 ORDER BY 1
    """).df()
    if spx_short.empty:
        print("  none — SPX holds no OI within 7 days anywhere in range")
    else:
        spx_short["expiration"] = pd.to_datetime(spx_short["expiration"]).dt.date
        spx_short["is_monthly"] = [e == _monthly_expiry(e) for e in spx_short["expiration"]]
        n_sessions = con.execute(
            "SELECT count(DISTINCT trade_date) FROM oi WHERE root='SPX' AND dte <= 7"
        ).fetchone()[0]
        non_monthly = spx_short[~spx_short["is_monthly"]]
        print(f"  {n_sessions} sessions carry SPX OI at DTE <= 7, across "
              f"{len(spx_short)} expirations; {len(non_monthly)} are NOT the "
              "monthly (third-Friday) expiry.")
        print("  Expected: the SPX root holds the monthly right up to its AM "
              "settlement, so OI inside 7 days appears every expiration week. "
              "Only non-monthly expirations here would be surprising.")
        if not non_monthly.empty:
            print(non_monthly.to_string(index=False))
        else:
            print(spx_short.tail(6).to_string(index=False))

    section("OI: SPXW expiration cadence by year (expirations listed in that year)")
    cad = con.execute("""
        WITH e AS (SELECT DISTINCT expiration FROM oi WHERE root = 'SPXW')
        SELECT year(expiration) AS year,
               count(*) AS expirations,
               count(DISTINCT yearweek(expiration)) AS weeks,
               round(count(*) / count(DISTINCT yearweek(expiration)), 2) AS per_week,
               count(*) FILTER (WHERE dayofweek(expiration) = 1) AS mon,
               count(*) FILTER (WHERE dayofweek(expiration) = 2) AS tue,
               count(*) FILTER (WHERE dayofweek(expiration) = 3) AS wed,
               count(*) FILTER (WHERE dayofweek(expiration) = 4) AS thu,
               count(*) FILTER (WHERE dayofweek(expiration) = 5) AS fri
        FROM e GROUP BY 1 ORDER BY 1
    """).df()
    print(cad.to_string(index=False))
    print("  Expected: Mon/Wed/Fri (~3/week) before 2022, Tue/Thu added in "
          "April–May 2022 (~5/week after). Years outside the fetched range show "
          "only far-dated listings.")

    section("OI: per-date table (compare against the §3 sample)")
    rows = []
    for d in dates:
        df = con.execute("""
            SELECT root, count(*) AS rows, sum(v) AS total_oi,
                   count(DISTINCT expiration) AS expirations,
                   coalesce(sum(v) FILTER (WHERE dte <= 7), 0) AS oi_dte_le_7,
                   coalesce(sum(v) FILTER (WHERE dte > 90), 0) AS oi_dte_gt_90
            FROM oi WHERE trade_date = ? GROUP BY root ORDER BY root
        """, [d]).df()
        if df.empty:
            print(f"  {d}: no OI rows")
            continue
        df.insert(0, "trade_date", d)
        rows.append(df)
    if rows:
        print(pd.concat(rows).to_string(index=False))
        print("  Note: the store holds OI > 0 only (fetch_oi drops zero rows), so "
              "`rows` can sit below a raw-response count.")


# --- split factors -----------------------------------------------------------

def split_check(sessions: list[date]) -> None:
    section("Split factors (index options must resolve to 1.0)")
    try:
        from db import get_connection
        from lib.split_factors import load_splits, make_split_factor_map
        with get_connection() as conn:
            for root in SPX_ROOTS:
                splits = load_splits(conn, root)
                fmap = make_split_factor_map(splits, sessions)
                bad = {d: f for d, f in fmap.items() if f != 1.0}
                check(not bad, f"{root}: {len(splits)} split events, "
                      f"{len(fmap)} session factors, {len(bad)} != 1.0")
    except Exception as exc:
        check(False, f"split check could not run: {type(exc).__name__}: {exc}")


# --- disk ---------------------------------------------------------------------

def disk_report(project_sessions: int) -> None:
    section(f"Disk (all stored years) and projection to {project_sessions:,} sessions")
    total_proj = 0.0
    for label, store_dir in (("OI", SPX_OI_RAW_DIR), ("volume", SPX_VOLUME_DIR)):
        for root in SPX_ROOTS:
            files = list((store_dir / root).glob("*.parquet"))
            if not files:
                print(f"  {label:<7}{root:<6} no files")
                continue
            nbytes = sum(f.stat().st_size for f in files)
            nrows = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
            nsess = len({d for f in files for d in
                         pq.read_table(f, columns=["trade_date"])
                           .column("trade_date").unique().to_pylist()})
            per_sess = nbytes / nsess if nsess else 0
            proj = per_sess * project_sessions
            total_proj += proj
            print(f"  {label:<7}{root:<6}{nbytes / 1e6:9.1f} MB  {nrows:>12,} rows  "
                  f"{nsess:>5} sessions  {nbytes / max(nrows, 1):5.2f} B/row  "
                  f"→ {proj / 1e6:8.1f} MB projected")
    print(f"  projected total: {total_proj / 1e9:.2f} GB  (scaled from the "
          "sessions stored so far; recent years are the densest, so a recent "
          "sample over-estimates)")


# --- main ---------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--start", type=_parse_day, required=True)
    p.add_argument("--end", type=_parse_day, required=True)
    p.add_argument("--dates", default="",
                   help="comma-separated dates for the per-date OI table "
                        "(default: 20261026 if in range, plus first and last session)")
    p.add_argument("--project-sessions", type=int, default=1510,
                   help="sessions to project disk use to (default ~6 years)")
    args = p.parse_args()

    sessions = get_trading_days(args.start, args.end)
    if not sessions:
        print("No NYSE sessions in range.")
        return 2
    if args.dates:
        dates = [_parse_day(s) for s in args.dates.split(",") if s.strip()]
    else:
        dates = sorted({d for d in (date(2026, 10, 26), sessions[0], sessions[-1])
                        if args.start <= d <= args.end})

    print(f"SPX_OI_RAW_DIR : {SPX_OI_RAW_DIR}")
    print(f"SPX_VOLUME_DIR : {SPX_VOLUME_DIR}")
    print(f"range          : {sessions[0]} → {sessions[-1]} ({len(sessions)} sessions)")

    con = duckdb.connect()
    if _register(con, "oi", SPX_OI_RAW_DIR, "open_interest", args.start, args.end):
        common_checks(con, "oi", "OI", sessions)
        oi_reports(con, dates)
    else:
        FAILURES.append("OI store empty")

    if _register(con, "vol", SPX_VOLUME_DIR, "volume", args.start, args.end):
        common_checks(con, "vol", "volume", sessions)
        nonpos = con.execute("SELECT count(*) FROM vol WHERE v <= 0").fetchone()[0]
        check(nonpos == 0, f"volume: no rows with volume <= 0 ({nonpos:,})")
        section("volume: put/call split by root by year")
        print(con.execute("""
            SELECT root, year(trade_date) AS year,
                   round(sum(v) FILTER (WHERE option_type = 'C') / count(DISTINCT trade_date)) AS call_vol_per_session,
                   round(sum(v) FILTER (WHERE option_type = 'P') / count(DISTINCT trade_date)) AS put_vol_per_session,
                   round(sum(v) FILTER (WHERE option_type = 'P') / sum(v) FILTER (WHERE option_type = 'C'), 3) AS put_call
            FROM vol GROUP BY 1, 2 ORDER BY 1, 2
        """).df().to_string(index=False))
    else:
        FAILURES.append("volume store empty")

    split_check(sessions)
    disk_report(args.project_sessions)

    section("Result")
    if FAILURES:
        print(f"  {len(FAILURES)} FAILED:")
        for f in FAILURES:
            print(f"    - {f}")
        return 1
    print("  all hard checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
