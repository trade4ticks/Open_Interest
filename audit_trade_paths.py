"""
audit_trade_paths.py — correctness and coverage checks for trade_paths.

Read-only. Four checks, the first two of which are the approved regression
tests.

  1. FORWARD-RETURN AGREEMENT
     For entry_anchor='open', max_days=N must reproduce daily_features'
     ret_Nd_fwd_oc: both are "enter at O_T, exit at the close N-1 sessions
     later". This single check validates the day-counting convention, the
     split adjustment and the price basis at once, which is why it is worth
     more than a split test alone.

     It will not match to the penny — daily_features is built on yfinance
     OHLC and trade_paths on Polygon minute bars — so it asserts a tolerance
     and reports the distribution. The outliers ARE the diagnostic: a large
     mismatch means a split-basis problem or genuine vendor disagreement, and
     both are worth seeing.

  2. SPLIT-SPANNING HOLDS
     A hold whose window contains a split must NOT show a return near
     -(1 - 1/ratio). equity_1min is stored as-traded, so an unadjusted 10:1
     split reads as -90%; that would not merely be one bad row, it would fire
     a stop that never should have fired and corrupt the exit statistics.

  3. HORIZON INVARIANT
     Every path_status='ok' row must have a non-NULL horizon exit. This is
     what makes the structural backstop in build_combine_sql meaningful: if
     the horizon column can be NULL on a resolved path, a combine can still
     produce a trade with no exit.

  4. JOIN COVERAGE
     tt_bins vs trade_paths on (ticker, trade_date), both sides.

Usage:
    python audit_trade_paths.py
    python audit_trade_paths.py --tolerance 0.002
"""
from __future__ import annotations

import argparse
import logging
import sys

import pandas as pd

from lib.chain_fetch_common import log_path, setup_file_logging
from lib.trade_path_rules import BY_KEY, HORIZON_RULE_KEY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("audit_trade_paths")

# max_days N -> the daily_features forward-return column it must reproduce.
FWD_PAIRS = {1: "ret_1d_fwd_oc", 3: "ret_3d_fwd_oc", 5: "ret_5d_fwd_oc",
             7: "ret_7d_fwd_oc", 10: "ret_10d_fwd_oc", 20: "ret_20d_fwd_oc"}


def check_forward_returns(conn, tol: float) -> int:
    from db import read_sql_df
    print("\n" + "=" * 70)
    print("1. FORWARD-RETURN AGREEMENT  (anchor='open': max_days=N vs ret_Nd_fwd_oc)")
    print("=" * 70)
    print(f"  {'N':<4}{'compared':>10}{'median |d|':>13}{'p99 |d|':>11}"
          f"{'> tol':>9}{'  worst'}")
    bad_total = 0
    for n, col in FWD_PAIRS.items():
        xr = BY_KEY[f"max_days__{n}"].ret_col
        df = read_sql_df(conn, f"""
            SELECT p.ticker, p.trade_date, p.{xr} AS got, f.{col} AS want
            FROM trade_paths p
            JOIN daily_features f
              ON f.ticker = p.ticker AND f.trade_date = p.trade_date
            WHERE p.entry_anchor = 'open'
              AND p.path_status = 'ok'
              AND p.{xr} IS NOT NULL
              AND f.{col} IS NOT NULL
        """)
        if df.empty:
            print(f"  {n:<4}{'0':>10}   (no overlapping rows)")
            continue
        d = (df["got"].astype(float) - df["want"].astype(float)).abs()
        over = int((d > tol).sum())
        bad_total += over
        worst = df.loc[d.idxmax()]
        print(f"  {n:<4}{len(df):>10,}{d.median():>13.5f}{d.quantile(0.99):>11.5f}"
              f"{over:>9,}  {worst['ticker']} {worst['trade_date']} "
              f"d={d.max():.4f}")
    if bad_total:
        print(f"\n  {bad_total:,} row(s) exceed tolerance {tol}. Small counts are "
              f"vendor disagreement;")
        print("  clusters on one ticker or around one date are a basis problem.")
    else:
        print(f"\n  All compared rows within {tol}.")
    return bad_total


def check_basis_continuity(conn, tol: float = 0.25) -> int:
    """entry_price must not step by the split ratio across an ex-date.

    This is the primary split check, and it replaces reasoning from returns.
    A return-based test is drift-dependent: it compares the observed return
    against 1/ratio - 1, so it only fires when the underlying happened not to
    move much over the hold, and it is blind to the case where BOTH ends of a
    hold carry the same wrong factor (they cancel, and the return is
    numerically identical to no adjustment at all).

    entry_price is stored per session, already adjusted, so the ratio of
    consecutive sessions' entry prices across the ex-date tests the basis
    directly. It should be ~1 give or take one day's move, never ~ratio or
    ~1/ratio. This fires on unadjusted history AND on an over-adjusted
    ex-date, which is what the return-band test could not separate.
    """
    from db import read_sql_df
    print("\n" + "=" * 70)
    print("2a. BASIS CONTINUITY  (entry_price must not step across an ex-date)")
    print("=" * 70)
    df = read_sql_df(conn, """
        WITH s AS (
            SELECT ticker, trade_date AS split_date, splits
            FROM underlying_ohlc
            WHERE splits IS NOT NULL AND splits <> 0 AND splits <> 1
        ),
        b AS (
            SELECT s.ticker, s.split_date, s.splits,
                   (SELECT p.entry_price FROM trade_paths p
                     WHERE p.ticker = s.ticker AND p.entry_anchor = 'open'
                       AND p.trade_date < s.split_date
                     ORDER BY p.trade_date DESC LIMIT 1) AS px_before,
                   (SELECT p.entry_price FROM trade_paths p
                     WHERE p.ticker = s.ticker AND p.entry_anchor = 'open'
                       AND p.trade_date >= s.split_date
                     ORDER BY p.trade_date ASC LIMIT 1) AS px_on_after
            FROM s
        )
        SELECT * FROM b WHERE px_before IS NOT NULL AND px_on_after IS NOT NULL
    """)
    if df.empty:
        print("  No split dates with entry prices on both sides — nothing to check.")
        return 0
    df["step"] = df["px_before"].astype(float) / df["px_on_after"].astype(float)
    df["expected_if_raw"] = df["splits"].astype(float)
    df["bad"] = (df["step"] - 1.0).abs() > tol
    n_bad = int(df["bad"].sum())
    print(f"  {'ticker':<8}{'ex-date':<12}{'ratio':>7}{'px_before':>11}"
          f"{'px_on/after':>13}{'step':>8}  verdict")
    for _, r in df.iterrows():
        v = "OK" if not r["bad"] else (
            "RAW/UNADJUSTED" if abs(r["step"] - r["expected_if_raw"]) < 0.3
            else "EX-DATE OVER-ADJUSTED"
            if abs(r["step"] - 1.0 / r["expected_if_raw"]) < 0.3 else "DISCONTINUOUS")
        print(f"  {r['ticker']:<8}{str(r['split_date']):<12}"
              f"{r['splits']:>7.2f}{r['px_before']:>11.2f}"
              f"{r['px_on_after']:>13.2f}{r['step']:>8.3f}  {v}")
    if n_bad:
        print(f"\n  FAIL — {n_bad} split boundary(ies) show a price basis step.")
        print("  step ~= ratio  means the history was never adjusted.")
        print("  step ~= 1/ratio means the EX-DATE was adjusted when it should")
        print("  not have been (make_split_factors inclusive boundary applied")
        print("  to a price series).")
    else:
        print("\n  OK — price basis is continuous across every split.")
    return n_bad


def check_split_spanning(conn) -> int:
    from db import read_sql_df
    print("\n" + "=" * 70)
    print("2b. SPLIT-SPANNING HOLDS  (must NOT read as a large negative return)")
    print("=" * 70)
    xr = BY_KEY[HORIZON_RULE_KEY].ret_col
    df = read_sql_df(conn, f"""
        WITH s AS (
            SELECT ticker, trade_date AS split_date, splits
            FROM underlying_ohlc
            WHERE splits IS NOT NULL AND splits <> 0 AND splits <> 1
        )
        SELECT p.ticker, p.trade_date, s.split_date, s.splits,
               p.{xr} AS horizon_return
        FROM trade_paths p
        JOIN s ON s.ticker = p.ticker
              AND s.split_date > p.trade_date
              AND s.split_date <= p.trade_date + INTERVAL '30 days'
        WHERE p.entry_anchor = 'open' AND p.path_status = 'ok'
          AND p.{xr} IS NOT NULL
    """)
    if df.empty:
        print("  No split-spanning holds found. Either no splits are in range,")
        print("  or trade_paths has not been built for those tickers yet.")
        return 0
    df["expected_if_unadjusted"] = 1.0 / df["splits"].astype(float) - 1.0
    df["looks_unadjusted"] = (
        (df["horizon_return"].astype(float)
         - df["expected_if_unadjusted"]).abs() < 0.05)
    n_bad = int(df["looks_unadjusted"].sum())
    print(f"  {len(df):,} hold(s) span a split, across "
          f"{df['ticker'].nunique()} ticker(s)")
    print(f"  median horizon return: {df['horizon_return'].median():.4f}")
    print(f"  holds reading as an UNADJUSTED split: {n_bad:,}")
    if n_bad:
        print("\n  FAIL — split adjustment is not being applied. Sample:")
        for _, r in df[df["looks_unadjusted"]].head(5).iterrows():
            print(f"    {r['ticker']:<7}{r['trade_date']} split {r['splits']}:1 "
                  f"on {r['split_date']} -> return {r['horizon_return']:.4f}")
    else:
        print("  OK — no hold reads as a raw split move.")
    return n_bad


def check_horizon_invariant(conn) -> int:
    print("\n" + "=" * 70)
    print("3. HORIZON INVARIANT  (resolved paths must always have an exit)")
    print("=" * 70)
    xb = BY_KEY[HORIZON_RULE_KEY].bar_col
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM trade_paths "
                    f"WHERE path_status = 'ok' AND {xb} IS NULL")
        n = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM trade_paths WHERE path_status = 'ok'")
        total = cur.fetchone()[0]
    print(f"  resolved rows              {total:>12,}")
    print(f"  with NULL horizon exit     {n:>12,}")
    if n:
        print("\n  FAIL — the structural backstop in build_combine_sql assumes")
        print(f"  {xb} is never NULL on a resolved path. A combine could now")
        print("  return a trade with no exit at all.")
    else:
        print("  OK — every resolved path has a horizon exit.")
    return n


def check_coverage(conn) -> None:
    from build_trade_paths import coverage_report
    coverage_report(conn)


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit trade_paths (read-only).")
    ap.add_argument("--tolerance", type=float, default=0.002,
                    help="absolute return tolerance for check 1 (default 0.002 "
                         "= 20 bps, loose enough for vendor disagreement)")
    ap.add_argument("--skip-coverage", action="store_true")
    args = ap.parse_args()

    log_file = setup_file_logging("audit_trade_paths")
    print("=== trade_paths audit (read-only) ===")
    print(f"Log: {log_file}")

    from db import get_connection
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.trade_paths')")
            if cur.fetchone()[0] is None:
                raise SystemExit("trade_paths does not exist — run "
                                 "build_trade_paths.py first.")
            cur.execute("SELECT count(*) FROM trade_paths")
            if cur.fetchone()[0] == 0:
                raise SystemExit("trade_paths is empty — run "
                                 "build_trade_paths.py first.")

        n_fwd = check_forward_returns(conn, args.tolerance)
        n_basis = check_basis_continuity(conn)
        n_split = check_split_spanning(conn)
        n_horizon = check_horizon_invariant(conn)
        n_split += n_basis
        if not args.skip_coverage:
            check_coverage(conn)

    print(f"\nLog: {log_path()}")
    # Only the two hard invariants fail the run. Forward-return drift is
    # reported but not fatal: some disagreement between two vendors is
    # expected, and turning it into an exit code would train you to ignore it.
    if n_split or n_horizon:
        print("\nFAILED: a correctness invariant is broken (see above).")
        return 1
    if n_fwd:
        print(f"\nPASSED with {n_fwd:,} forward-return row(s) over tolerance — "
              f"review the worst offenders above.")
    else:
        print("\nAll checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
