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
  * no stored volume <= 0; trade_count on every row; trade_count <= volume
  * SPX holds OI at DTE <= 7 only in its monthly expiry (SPXW carries the
    short-dated book on every other week)
  * split factors resolve to exactly 1.0 for both roots

REPORTED for eyeballing (never fail the run):
  * rows / sessions / expirations / OI by root by year
  * book split per year: SPXW share of DTE <= 7 OI, SPX share of DTE > 90 OI
  * rows with volume > 0 but trade_count = 0 (count and rate per root per year)
  * average trade size (volume / trade_count, count > 0 rows only) by root by
    year; by month with --trade-size-monthly
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


def _register(con, name: str, store_dir, value_col: str, start: date, end: date,
              extra: str = "") -> bool:
    files = _files(store_dir, SPX_ROOTS)
    if not files:
        print(f"  no files under {store_dir} for {SPX_ROOTS}")
        return False
    # Views cannot hold prepared parameters, so the literals are inlined.
    # root = the directory above {year}.parquet; the regexp accepts / and \.
    # union_by_name: volume files written before trade_count existed read it
    # as NULL instead of failing the whole scan.
    file_list = ", ".join("'" + f.replace("'", "''") + "'" for f in files)
    con.execute(f"""
        CREATE OR REPLACE VIEW {name} AS
        SELECT regexp_extract(filename, '([^/\\\\]+)[/\\\\][0-9]{{4}}\\.parquet$', 1) AS root,
               trade_date, expiration, strike, option_type, {value_col} AS v,
               (expiration - trade_date) AS dte{extra}
        FROM read_parquet([{file_list}], filename = true, union_by_name = true)
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

def oi_book_split(con) -> None:
    """SPX holds OI inside 7 days only in a monthly-expiration week (its
    AM-settled monthly runs to settlement); on every other week SPXW carries
    the whole short-dated book. SPX carries the long-dated book throughout."""
    section("OI: SPX / SPXW book split")
    exps = con.execute("""
        SELECT DISTINCT expiration FROM oi WHERE root = 'SPX' AND dte <= 7
    """).df()
    exps["expiration"] = pd.to_datetime(exps["expiration"]).dt.date
    non_monthly = sorted(e for e in exps["expiration"] if e != _monthly_expiry(e))
    check(not non_monthly,
          "SPX holds OI at DTE <= 7 only in its monthly (third-Friday) expiry"
          + (f" — {len(non_monthly)} other expirations: "
             f"{', '.join(map(str, non_monthly[:10]))}" if non_monthly else ""))

    per = con.execute("""
        SELECT trade_date, root,
               coalesce(sum(v) FILTER (WHERE dte <= 7), 0) AS short_oi,
               coalesce(sum(v) FILTER (WHERE dte > 90), 0) AS long_oi
        FROM oi GROUP BY 1, 2
    """).df()
    if per.empty:
        return
    per["trade_date"] = pd.to_datetime(per["trade_date"]).dt.date
    wide = per.pivot(index="trade_date", columns="root",
                     values=["short_oi", "long_oi"]).fillna(0)
    out = pd.DataFrame(index=wide.index)
    # A third Friday is on day 15-21, so only the current month's monthly can
    # fall within 7 days of a session.
    out["monthly_week"] = [0 <= (_monthly_expiry(d) - d).days <= 7 for d in wide.index]
    short_tot = wide["short_oi"].sum(axis=1)
    long_tot = wide["long_oi"].sum(axis=1)
    out["spxw_short_share"] = wide["short_oi"].get("SPXW", 0) / short_tot.where(short_tot > 0)
    out["spx_long_share"] = wide["long_oi"].get("SPX", 0) / long_tot.where(long_tot > 0)
    out["year"] = [d.year for d in out.index]

    rows = []
    for y, g in out.groupby("year"):
        nm, mw = g[~g["monthly_week"]], g[g["monthly_week"]]
        rows.append({
            "year": y,
            "non_monthly_wks": len(nm),
            "spxw_short_share_min": round(nm["spxw_short_share"].min(), 4) if len(nm) else None,
            "monthly_wks": len(mw),
            "spxw_short_share_median": round(mw["spxw_short_share"].median(), 4) if len(mw) else None,
            "spx_long_share_median": round(g["spx_long_share"].median(), 4),
            "spx_long_share_min": round(g["spx_long_share"].min(), 4),
        })
    print("  Per session: SPXW share of OI at DTE <= 7, SPX share of OI at DTE > 90.")
    print("  Expected: spxw_short_share_min = 1.0 on non-monthly weeks (the hard check")
    print("  above, seen per session); SPX the large majority of DTE > 90 every year.")
    print(pd.DataFrame(rows).to_string(index=False))


def oi_reports(con, dates: list[date]) -> None:
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
    p.add_argument("--trade-size-monthly", action="store_true",
                   help="also print average trade size by month per root, to "
                        "tell a step change from a gradual slide")
    args = p.parse_args()
    trade_size_monthly = args.trade_size_monthly

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
        oi_book_split(con)
        oi_reports(con, dates)
    else:
        FAILURES.append("OI store empty")

    if _register(con, "vol", SPX_VOLUME_DIR, "volume", args.start, args.end,
                 extra=", trade_count"):
        common_checks(con, "vol", "volume", sessions)
        nonpos = con.execute("SELECT count(*) FROM vol WHERE v <= 0").fetchone()[0]
        check(nonpos == 0, f"volume: no rows with volume <= 0 ({nonpos:,})")
        n_null, n_over, null_days = con.execute("""
            SELECT count(*) FILTER (WHERE trade_count IS NULL),
                   count(*) FILTER (WHERE trade_count > v),
                   count(DISTINCT trade_date) FILTER (WHERE trade_count IS NULL)
            FROM vol
        """).fetchone()
        check(n_null == 0, f"volume: trade_count present on every row ({n_null:,} NULL "
              f"on {null_days} sessions — written before trade_count was stored; "
              "re-fetch those sessions)")
        check(n_over == 0, f"volume: trade_count <= volume on every row "
              f"({n_over:,} violations — a trade is at least one contract)")

        # trade_count = 0 with volume > 0 is a legitimate vendor state, not an
        # error: volume can carry trades whose conditions add size without
        # adding a counted trade (packages / blocks seen in Oct 2026). The rows
        # stay; only volume / trade_count is undefined for them.
        section("volume: rows with volume > 0 and trade_count = 0 (report only)")
        print(con.execute("""
            SELECT root, year(trade_date) AS year,
                   count(*) FILTER (WHERE trade_count = 0) AS rows_zero_count,
                   round(100.0 * count(*) FILTER (WHERE trade_count = 0) / count(*), 4) AS pct_rows,
                   coalesce(sum(v) FILTER (WHERE trade_count = 0), 0) AS volume_zero_count,
                   round(100.0 * coalesce(sum(v) FILTER (WHERE trade_count = 0), 0) / sum(v), 4) AS pct_volume
            FROM vol WHERE trade_count IS NOT NULL
            GROUP BY 1, 2 ORDER BY 1, 2
        """).df().to_string(index=False))

        # Same rule phase 2 uses: rows with trade_count = 0 leave BOTH
        # numerator and denominator, so their volume cannot inflate the size.
        section("volume: average trade size (volume / trade_count, count > 0 rows) by root by year")
        print(con.execute("""
            SELECT root, year(trade_date) AS year,
                   round(sum(v) / sum(trade_count), 2) AS avg_trade_size,
                   round(sum(v) FILTER (WHERE option_type = 'C')
                         / sum(trade_count) FILTER (WHERE option_type = 'C'), 2) AS call,
                   round(sum(v) FILTER (WHERE option_type = 'P')
                         / sum(trade_count) FILTER (WHERE option_type = 'P'), 2) AS put,
                   round(sum(trade_count) / count(DISTINCT trade_date)) AS trades_per_session
            FROM vol WHERE trade_count > 0
            GROUP BY 1, 2 ORDER BY 1, 2
        """).df().to_string(index=False))
        if trade_size_monthly:
            section("volume: average trade size by root by month (step vs slide)")
            m = con.execute("""
                SELECT strftime(date_trunc('month', trade_date), '%Y-%m') AS month, root,
                       round(sum(v) / sum(trade_count), 2) AS avg_trade_size
                FROM vol WHERE trade_count > 0
                GROUP BY 1, 2
            """).df()
            wide = m.pivot(index="month", columns="root", values="avg_trade_size").sort_index()
            for r in wide.columns:
                wide[f"{r}_mom_pct"] = (wide[r].pct_change() * 100).round(1)
            print(wide.to_string())
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
