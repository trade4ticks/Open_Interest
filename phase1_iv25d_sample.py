"""
phase1_iv25d_sample.py — Phase 1 verification of the proposed 25-delta IV
metrics (14d and 30d tenors).  Computes iv_25d_call/put, rr, bf, skew for
both tenors against the existing chain_eod parquet for a small sample of
tickers and dates.  Read-only.

This script DOES NOT:
  - modify build_features.py (the new SQL lives only here for Phase 1)
  - modify daily_features schema (no ALTER TABLE)
  - modify EVENING_COLS (no new persisted writes)
  - touch metric_classification (no eligibility flag flips)
  - run any backfill

Once the sample output reads correctly to you, Phase 2 moves the SQL into
build_features.py:IV_FEATURES_SQL, adds the schema columns, updates
EVENING_COLS, applies the schema via init_db.py, and runs the full backfill.

Open question for Phase 2: atm_iv_14d.  bf_25d_14d / skew_25p_atm_14d /
skew_atm_25c_14d need a 14-day ATM body for tenor consistency.  This
script computes atm_iv_14d in-memory and uses it as the body.  The
printed output shows both atm_iv_14d and atm_iv_30d so the difference
is visible before deciding whether to store atm_iv_14d as an 8th column.

Method (mirrors what the eventual IV_FEATURES_SQL will do):

  Strike locator — stored chain delta directly.  Calls target signed
  delta = +0.25; puts target |delta| = 0.25 (robust to whether the
  vendor stores signed [-1,0] or unsigned [0,1] put delta; the
  diagnostic earlier confirmed the chain stores positive-magnitude
  put delta, but ABS is convention-agnostic).

  Per (feature_date, expiration), bracket two adjacent strikes around
  the 25-delta point.  Linear interpolation in delta at exactly 0.25
  gives the per-expiration 25d IV.  No nearest-neighbour fallback —
  NULL when the chain lacks strikes bracketing 25-delta on that side.

  DTE bracketing — per (feature_date, target_dte ∈ {14, 30}), pick
  exp_lower (max DTE <= target) and exp_upper (min DTE > target);
  linearly interpolate the per-expiration 25d IVs across DTE to get
  the final tenored value.  Same convention as the existing
  atm_iv_7d/30d/90d.

  Split-adjustment uses the same chain_adj view SQL as build_features.

Usage:
    python phase1_iv25d_sample.py
    python phase1_iv25d_sample.py --tickers SPY,AAPL,IONQ
    python phase1_iv25d_sample.py --tickers SPY --dates 2026-06-05,2026-06-09
    python phase1_iv25d_sample.py --n-dates 10
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta

import duckdb
import pandas as pd

from db import get_connection, read_sql_df
from lib.chain_store import has_data as chain_has_data
from lib.chain_store import parquet_glob as chain_parquet_glob
from lib.split_factors import load_splits, make_split_factors

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("phase1_iv25d")

# Suggested defaults — one from each liquidity tier per the iv25d quality
# report.  Override via --tickers.
DEFAULT_TICKERS = ["SPY", "AAPL", "IONQ"]
DEFAULT_N_DATES = 5  # most recent feature_dates with chain data, per ticker


# ---------------------------------------------------------------------------
# The candidate IV interpolation SQL.  When you sign off on Phase 1, this
# string moves into build_features.py:IV_FEATURES_SQL, replacing the current
# version (which hardcodes iv_25d_call/put_30d = NULL).  Until then it lives
# only here so the sample run can't accidentally persist anything new.
#
# Inputs registered by the caller (chain_adj + ohlc views) match the views
# build_features.py uses, so the SQL is portable as-is.
# ---------------------------------------------------------------------------
NEW_IV_SQL = """
WITH calls AS (
    SELECT trade_date, feature_date, expiration, strike, implied_vol, delta
    FROM chain_adj
    WHERE option_type = 'C' AND implied_vol > 0
),
calls_for_delta AS (
    SELECT * FROM calls
    WHERE delta IS NOT NULL AND ABS(delta) <= 1.0
),
puts AS (
    SELECT trade_date, feature_date, expiration, strike, implied_vol,
           ABS(delta) AS abs_delta
    FROM chain_adj
    WHERE option_type = 'P'
      AND implied_vol > 0
      AND delta IS NOT NULL
      AND ABS(delta) <= 1.0
),

-- ============================================================
-- ATM path: mirrors build_features.py:IV_FEATURES_SQL exactly,
-- with `14` added to the target_dte set so atm_iv_14d is computed.
-- ============================================================
calls_with_spot AS (
    SELECT c.feature_date, c.expiration, c.strike, c.implied_vol,
           o.close AS spot,
           (c.expiration - c.trade_date)::INTEGER AS exp_dte
    FROM calls c
    JOIN ohlc o ON o.trade_date = c.trade_date
),
atm_strike_brackets AS (
    SELECT feature_date, expiration,
           ANY_VALUE(spot)    AS spot,
           ANY_VALUE(exp_dte) AS exp_dte,
           MAX(strike) FILTER (WHERE strike <= spot) AS s_low,
           MIN(strike) FILTER (WHERE strike >  spot) AS s_high
    FROM calls_with_spot
    GROUP BY feature_date, expiration
),
atm_strike_brackets_iv AS (
    SELECT sb.feature_date, sb.expiration, sb.spot, sb.exp_dte,
           sb.s_low, sb.s_high,
           cl.implied_vol AS iv_low,
           ch.implied_vol AS iv_high
    FROM atm_strike_brackets sb
    LEFT JOIN calls_with_spot cl
      ON cl.feature_date = sb.feature_date
     AND cl.expiration   = sb.expiration
     AND cl.strike       = sb.s_low
    LEFT JOIN calls_with_spot ch
      ON ch.feature_date = sb.feature_date
     AND ch.expiration   = sb.expiration
     AND ch.strike       = sb.s_high
),
atm_per_exp AS (
    SELECT feature_date, expiration, exp_dte, spot,
           CASE WHEN s_low IS NULL OR s_high IS NULL THEN NULL
                WHEN s_low = s_high                  THEN iv_low
                ELSE iv_low + (iv_high - iv_low) * (spot - s_low) / (s_high - s_low) END
             AS atm_iv
    FROM atm_strike_brackets_iv
),
atm_targets AS (
    SELECT * FROM (VALUES (7), (14), (30), (90)) AS t(target_dte)
),
atm_exp_brackets AS (
    SELECT a.feature_date, t.target_dte,
           MAX(a.exp_dte) FILTER (WHERE a.exp_dte <= t.target_dte) AS d_low,
           MIN(a.exp_dte) FILTER (WHERE a.exp_dte >  t.target_dte) AS d_high
    FROM atm_per_exp a CROSS JOIN atm_targets t
    WHERE a.atm_iv IS NOT NULL
    GROUP BY a.feature_date, t.target_dte
),
atm_exp_brackets_iv AS (
    SELECT eb.feature_date, eb.target_dte, eb.d_low, eb.d_high,
           al.atm_iv AS iv_low,
           ah.atm_iv AS iv_high
    FROM atm_exp_brackets eb
    LEFT JOIN atm_per_exp al
      ON al.feature_date = eb.feature_date AND al.exp_dte = eb.d_low
    LEFT JOIN atm_per_exp ah
      ON ah.feature_date = eb.feature_date AND ah.exp_dte = eb.d_high
),
atm_by_dte AS (
    SELECT feature_date, target_dte,
           CASE WHEN d_low IS NULL OR d_high IS NULL THEN NULL
                WHEN d_low = d_high                  THEN iv_low
                ELSE iv_low + (iv_high - iv_low) * (target_dte - d_low) / (d_high - d_low) END
             AS atm_iv_value
    FROM atm_exp_brackets_iv
),
atm_pivoted AS (
    SELECT feature_date AS trade_date,
           MAX(atm_iv_value) FILTER (WHERE target_dte = 7)  AS atm_iv_7d,
           MAX(atm_iv_value) FILTER (WHERE target_dte = 14) AS atm_iv_14d,
           MAX(atm_iv_value) FILTER (WHERE target_dte = 30) AS atm_iv_30d,
           MAX(atm_iv_value) FILTER (WHERE target_dte = 90) AS atm_iv_90d
    FROM atm_by_dte
    GROUP BY feature_date
),

-- ============================================================
-- 25-delta CALL path: bracket-by-delta in strike, then
-- bracket-by-DTE across expirations.  Targets 14 and 30 days.
-- Calls only — call delta is non-increasing in strike.
-- ============================================================
call_25d_strike_brackets AS (
    SELECT feature_date, expiration,
           ANY_VALUE((expiration - trade_date)::INTEGER) AS exp_dte,
           -- s_lo (lower strike, higher delta) = highest strike where delta >= 0.25
           -- s_hi (higher strike, lower delta) = lowest strike where delta <= 0.25
           MAX(strike) FILTER (WHERE delta >= 0.25) AS s_lo,
           MIN(strike) FILTER (WHERE delta <= 0.25) AS s_hi
    FROM calls_for_delta
    GROUP BY feature_date, expiration
),
call_25d_strike_brackets_iv AS (
    SELECT sb.feature_date, sb.expiration, sb.exp_dte, sb.s_lo, sb.s_hi,
           cl.implied_vol AS iv_lo, cl.delta AS delta_lo,
           ch.implied_vol AS iv_hi, ch.delta AS delta_hi
    FROM call_25d_strike_brackets sb
    LEFT JOIN calls_for_delta cl
      ON cl.feature_date = sb.feature_date AND cl.expiration = sb.expiration AND cl.strike = sb.s_lo
    LEFT JOIN calls_for_delta ch
      ON ch.feature_date = sb.feature_date AND ch.expiration = sb.expiration AND ch.strike = sb.s_hi
),
call_25d_per_exp AS (
    -- Linear-in-delta at target delta = 0.25.  NULL when bracketing strikes
    -- missing (no nearest-neighbour fallback — same convention as atm_iv).
    SELECT feature_date, expiration, exp_dte,
           CASE WHEN s_lo IS NULL OR s_hi IS NULL                THEN NULL
                WHEN delta_lo IS NULL OR delta_hi IS NULL        THEN NULL
                WHEN delta_lo = delta_hi                          THEN iv_lo
                ELSE iv_lo + (iv_hi - iv_lo) * (0.25 - delta_lo) / (delta_hi - delta_lo)
           END AS iv_25d_call
    FROM call_25d_strike_brackets_iv
),
delta_targets AS (
    SELECT * FROM (VALUES (14), (30)) AS t(target_dte)
),
call_25d_exp_brackets AS (
    SELECT cpe.feature_date, t.target_dte,
           MAX(cpe.exp_dte) FILTER (WHERE cpe.exp_dte <= t.target_dte) AS d_lo,
           MIN(cpe.exp_dte) FILTER (WHERE cpe.exp_dte >  t.target_dte) AS d_hi
    FROM call_25d_per_exp cpe CROSS JOIN delta_targets t
    WHERE cpe.iv_25d_call IS NOT NULL
    GROUP BY cpe.feature_date, t.target_dte
),
call_25d_exp_brackets_iv AS (
    SELECT eb.feature_date, eb.target_dte, eb.d_lo, eb.d_hi,
           cl.iv_25d_call AS iv_lo, ch.iv_25d_call AS iv_hi
    FROM call_25d_exp_brackets eb
    LEFT JOIN call_25d_per_exp cl ON cl.feature_date = eb.feature_date AND cl.exp_dte = eb.d_lo
    LEFT JOIN call_25d_per_exp ch ON ch.feature_date = eb.feature_date AND ch.exp_dte = eb.d_hi
),
call_25d_by_dte AS (
    SELECT feature_date, target_dte,
           CASE WHEN d_lo IS NULL OR d_hi IS NULL THEN NULL
                WHEN d_lo = d_hi                  THEN iv_lo
                ELSE iv_lo + (iv_hi - iv_lo) * (target_dte - d_lo) / (d_hi - d_lo) END
             AS iv_25d_call_value
    FROM call_25d_exp_brackets_iv
),
call_25d_pivoted AS (
    SELECT feature_date AS trade_date,
           MAX(iv_25d_call_value) FILTER (WHERE target_dte = 14) AS iv_25d_call_14d,
           MAX(iv_25d_call_value) FILTER (WHERE target_dte = 30) AS iv_25d_call_30d
    FROM call_25d_by_dte
    GROUP BY feature_date
),

-- ============================================================
-- 25-delta PUT path: same shape, on |delta|.
-- Puts only — |put_delta| is non-decreasing in strike (OTM low
-- strike = small |delta|; ITM high strike = large |delta|).
-- ============================================================
put_25d_strike_brackets AS (
    SELECT feature_date, expiration,
           ANY_VALUE((expiration - trade_date)::INTEGER) AS exp_dte,
           -- s_lo (lower strike, smaller |delta|) = highest strike where |delta| <= 0.25
           -- s_hi (higher strike, larger |delta|) = lowest strike where |delta| >= 0.25
           MAX(strike) FILTER (WHERE abs_delta <= 0.25) AS s_lo,
           MIN(strike) FILTER (WHERE abs_delta >= 0.25) AS s_hi
    FROM puts
    GROUP BY feature_date, expiration
),
put_25d_strike_brackets_iv AS (
    SELECT sb.feature_date, sb.expiration, sb.exp_dte, sb.s_lo, sb.s_hi,
           pl.implied_vol AS iv_lo, pl.abs_delta AS abs_delta_lo,
           ph.implied_vol AS iv_hi, ph.abs_delta AS abs_delta_hi
    FROM put_25d_strike_brackets sb
    LEFT JOIN puts pl
      ON pl.feature_date = sb.feature_date AND pl.expiration = sb.expiration AND pl.strike = sb.s_lo
    LEFT JOIN puts ph
      ON ph.feature_date = sb.feature_date AND ph.expiration = sb.expiration AND ph.strike = sb.s_hi
),
put_25d_per_exp AS (
    SELECT feature_date, expiration, exp_dte,
           CASE WHEN s_lo IS NULL OR s_hi IS NULL                       THEN NULL
                WHEN abs_delta_lo IS NULL OR abs_delta_hi IS NULL       THEN NULL
                WHEN abs_delta_lo = abs_delta_hi                         THEN iv_lo
                ELSE iv_lo + (iv_hi - iv_lo) * (0.25 - abs_delta_lo)
                                                / (abs_delta_hi - abs_delta_lo)
           END AS iv_25d_put
    FROM put_25d_strike_brackets_iv
),
put_25d_exp_brackets AS (
    SELECT ppe.feature_date, t.target_dte,
           MAX(ppe.exp_dte) FILTER (WHERE ppe.exp_dte <= t.target_dte) AS d_lo,
           MIN(ppe.exp_dte) FILTER (WHERE ppe.exp_dte >  t.target_dte) AS d_hi
    FROM put_25d_per_exp ppe CROSS JOIN delta_targets t
    WHERE ppe.iv_25d_put IS NOT NULL
    GROUP BY ppe.feature_date, t.target_dte
),
put_25d_exp_brackets_iv AS (
    SELECT eb.feature_date, eb.target_dte, eb.d_lo, eb.d_hi,
           pl.iv_25d_put AS iv_lo, ph.iv_25d_put AS iv_hi
    FROM put_25d_exp_brackets eb
    LEFT JOIN put_25d_per_exp pl ON pl.feature_date = eb.feature_date AND pl.exp_dte = eb.d_lo
    LEFT JOIN put_25d_per_exp ph ON ph.feature_date = eb.feature_date AND ph.exp_dte = eb.d_hi
),
put_25d_by_dte AS (
    SELECT feature_date, target_dte,
           CASE WHEN d_lo IS NULL OR d_hi IS NULL THEN NULL
                WHEN d_lo = d_hi                  THEN iv_lo
                ELSE iv_lo + (iv_hi - iv_lo) * (target_dte - d_lo) / (d_hi - d_lo) END
             AS iv_25d_put_value
    FROM put_25d_exp_brackets_iv
),
put_25d_pivoted AS (
    SELECT feature_date AS trade_date,
           MAX(iv_25d_put_value) FILTER (WHERE target_dte = 14) AS iv_25d_put_14d,
           MAX(iv_25d_put_value) FILTER (WHERE target_dte = 30) AS iv_25d_put_30d
    FROM put_25d_by_dte
    GROUP BY feature_date
),

-- ============================================================
-- Final assembly.  bf and skew use the SAME-tenor ATM as the body
-- (atm_iv_14d for 14d, atm_iv_30d for 30d) so the metric is
-- tenor-consistent.  Whether atm_iv_14d gets stored as a daily_features
-- column is the Phase 2 decision; for this sample it's printed alongside.
-- ============================================================
all_iv AS (
    SELECT a.trade_date,
           a.atm_iv_7d, a.atm_iv_14d, a.atm_iv_30d, a.atm_iv_90d,
           c.iv_25d_call_14d, c.iv_25d_call_30d,
           p.iv_25d_put_14d,  p.iv_25d_put_30d
    FROM atm_pivoted a
    LEFT JOIN call_25d_pivoted c ON a.trade_date = c.trade_date
    LEFT JOIN put_25d_pivoted  p ON a.trade_date = p.trade_date
)
SELECT trade_date,
       atm_iv_7d, atm_iv_14d, atm_iv_30d, atm_iv_90d,
       iv_25d_call_14d, iv_25d_call_30d,
       iv_25d_put_14d,  iv_25d_put_30d,
       -- 14d derivatives
       iv_25d_call_14d - iv_25d_put_14d                        AS rr_25d_14d,
       0.5 * (iv_25d_call_14d + iv_25d_put_14d) - atm_iv_14d   AS bf_25d_14d,
       iv_25d_put_14d  - atm_iv_14d                            AS skew_25p_atm_14d,
       atm_iv_14d      - iv_25d_call_14d                       AS skew_atm_25c_14d,
       -- 30d derivatives
       iv_25d_call_30d - iv_25d_put_30d                        AS rr_25d_30d,
       0.5 * (iv_25d_call_30d + iv_25d_put_30d) - atm_iv_30d   AS bf_25d_30d,
       iv_25d_put_30d  - atm_iv_30d                            AS skew_25p_atm_30d,
       atm_iv_30d      - iv_25d_call_30d                       AS skew_atm_25c_30d
FROM all_iv
ORDER BY trade_date
"""


# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------

def setup_ticker(con, pg_conn, ticker, feature_dates):
    """Register chain_adj + ohlc views in DuckDB for one ticker, filtered
    to the requested feature_dates so memory stays small for the sample.
    Returns True on success, False if data is missing.
    """
    if not chain_has_data(ticker):
        log.warning("  %s: no chain_eod parquet — skipping", ticker)
        return False

    # The chain_adj view filters parquet by feature_date, but split_factors
    # is keyed on the chain row's SOURCE trade_date (= feature_date - 1
    # trading day under build_features's convention).  Find the source
    # trade_dates we'll need.
    feature_dates_sql = ", ".join(f"DATE '{d.isoformat()}'" for d in feature_dates)
    rows = con.execute(
        f"SELECT DISTINCT trade_date FROM read_parquet('{chain_parquet_glob(ticker)}') "
        f"WHERE feature_date IN ({feature_dates_sql})"
    ).fetchall()
    chain_source_dates = [r[0] for r in rows]
    if not chain_source_dates:
        log.warning("  %s: no chain rows for the requested feature_dates", ticker)
        return False

    splits_df = load_splits(pg_conn, ticker)
    sf_df = make_split_factors(splits_df, chain_source_dates)
    con.register("split_factors", sf_df)

    # chain_adj — mirrors build_features.py:1253-1263 exactly, with a
    # feature_date pre-filter pushed into the parquet scan.
    con.execute(
        f"CREATE OR REPLACE VIEW chain_adj AS "
        f"SELECT raw.trade_date, raw.source_session, raw.feature_date, "
        f"       raw.expiration, "
        f"       raw.strike * COALESCE(sf.adj_factor, 1.0) AS strike, "
        f"       raw.option_type, "
        f"       raw.volume / COALESCE(sf.adj_factor, 1.0) AS volume, "
        f"       raw.implied_vol, raw.delta, raw.iv_error "
        f"FROM (SELECT * FROM read_parquet('{chain_parquet_glob(ticker)}') "
        f"      WHERE feature_date IN ({feature_dates_sql})) raw "
        f"LEFT JOIN split_factors sf ON raw.trade_date = sf.trade_date"
    )

    # underlying_ohlc — needed by the atm_iv path (calls_with_spot joins on
    # trade_date).  Small (one row per session per ticker), load full history.
    ohlc_df = read_sql_df(
        pg_conn,
        "SELECT trade_date, close FROM underlying_ohlc "
        "WHERE ticker = %(t)s ORDER BY trade_date",
        {"t": ticker},
    )
    if ohlc_df.empty:
        log.warning("  %s: no underlying_ohlc rows — skipping", ticker)
        return False
    ohlc_df["trade_date"] = pd.to_datetime(ohlc_df["trade_date"]).dt.date
    con.register("ohlc", ohlc_df)
    return True


def pick_recent_feature_dates(ticker, n):
    """Return the n most recent feature_dates in the ticker's chain parquet."""
    con = duckdb.connect(database=":memory:")
    try:
        rows = con.execute(
            f"SELECT DISTINCT feature_date FROM "
            f"read_parquet('{chain_parquet_glob(ticker)}') "
            f"ORDER BY feature_date DESC LIMIT {n}"
        ).fetchall()
    finally:
        con.close()
    return sorted([r[0] for r in rows])


def print_ticker_result(ticker, result):
    """Print one ticker's sample table — split into 14d and 30d blocks for
    readability (a single wide table is hard to scan at the terminal)."""
    print()
    print("=" * 100)
    print(f"=== {ticker}")
    print("=" * 100)

    # The atm_iv_14d column is informational — Phase 2 decision is whether
    # to store it; it's used here as the body for 14d bf/skew metrics.
    print()
    print("ATM term structure (for context — atm_iv_14d not yet a stored column):")
    cols = ["trade_date", "atm_iv_7d", "atm_iv_14d", "atm_iv_30d", "atm_iv_90d"]
    cols = [c for c in cols if c in result.columns]
    print(result[cols].to_string(index=False))

    for tenor in ("14d", "30d"):
        print()
        print(f"25-delta IV @ {tenor}:")
        cols = [
            "trade_date",
            f"iv_25d_call_{tenor}",
            f"iv_25d_put_{tenor}",
            f"rr_25d_{tenor}",
            f"bf_25d_{tenor}",
            f"skew_25p_atm_{tenor}",
            f"skew_atm_25c_{tenor}",
        ]
        cols = [c for c in cols if c in result.columns]
        print(result[cols].to_string(index=False))


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--tickers", default=",".join(DEFAULT_TICKERS),
                    help=f"Comma-separated tickers (default: {','.join(DEFAULT_TICKERS)})")
    ap.add_argument("--dates", default="",
                    help="Comma-separated YYYY-MM-DD feature_dates "
                         f"(default: last {DEFAULT_N_DATES} per ticker)")
    ap.add_argument("--n-dates", type=int, default=DEFAULT_N_DATES,
                    help=f"How many recent feature_dates per ticker if --dates "
                         f"not given (default: {DEFAULT_N_DATES})")
    args = ap.parse_args()

    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    user_dates = []
    if args.dates:
        user_dates = [datetime.strptime(d.strip(), "%Y-%m-%d").date()
                      for d in args.dates.split(",") if d.strip()]

    log.info("Tickers : %s", tickers)
    if user_dates:
        log.info("Dates   : %s (explicit)",
                 ", ".join(d.isoformat() for d in user_dates))
    else:
        log.info("Dates   : last %d feature_dates per ticker", args.n_dates)

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    pd.set_option("display.float_format", lambda x: f"{x:.4f}")

    with get_connection() as pg_conn:
        for ticker in tickers:
            try:
                feature_dates = (user_dates if user_dates
                                 else pick_recent_feature_dates(ticker, args.n_dates))
                if not feature_dates:
                    print()
                    print(f"=== {ticker}: no chain_eod data — skipping")
                    continue

                con = duckdb.connect(database=":memory:")
                try:
                    if not setup_ticker(con, pg_conn, ticker, feature_dates):
                        continue
                    result = con.execute(NEW_IV_SQL).df()
                finally:
                    con.close()

                if result.empty:
                    print()
                    print(f"=== {ticker}: SQL returned no rows — skipping")
                    continue
                result["trade_date"] = pd.to_datetime(result["trade_date"]).dt.date
                # Restrict to requested feature_dates (atm path runs on every
                # available date the spot+chain join produces, which can
                # include dates beyond the requested set).
                result = result[result["trade_date"].isin(set(feature_dates))]
                if result.empty:
                    print()
                    print(f"=== {ticker}: no rows for requested feature_dates")
                    continue
                print_ticker_result(ticker, result)
            except Exception as exc:
                log.warning("  %s: failed — %s", ticker, exc)
                continue

    print()
    log.info("Done.  Read-only — no data persisted.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
