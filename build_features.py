"""
build_features.py — Recompute the daily_features table from the raw parquet
store and underlying_ohlc, in one pass per ticker.

Reads:
    {OI_RAW_DIR}/{ticker}/*.parquet   raw OI rows
    underlying_ohlc                   daily open/close per ticker (Postgres)

Writes:
    daily_features                    one row per (ticker, trade_date)

All percentage features use the FULL UNFILTERED raw chain as the denominator.
The legacy `option_oi_surface` table is no longer used.

Usage:
    python build_features.py
    (prompts for tickers — blank = every ticker found in OI_RAW_DIR)
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import duckdb
import pandas as pd
import psycopg2.extras

from db import get_connection, read_sql_df
from lib.expirations import build_next_monthly_lookup
from lib.parquet_store import list_tickers, parquet_glob

# Backward window buffer (calendar days) when running with a date range.
# 60-day z-scores need ~65 trading-day inputs; 130 calendar days covers that
# comfortably even across long weekends / holidays.
LOOKBACK_BUFFER_DAYS = 130

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# OI features (DuckDB on parquet, LEFT JOINed to ohlc + next_monthly DFs).
# Inputs registered by the caller:
#   oi              view over read_parquet(...)   — raw chain
#   ohlc            pandas DF [trade_date, close]
#   next_monthly_df pandas DF [trade_date, next_monthly] — covers ALL OI dates
#                   (independent of OHLC, so today's OI features still resolve
#                   the next-monthly node even before today's OHLC arrives)
#
# LEFT JOIN ohlc means OI rows for today (no OHLC yet at 7am) survive the
# join; spot_close becomes NULL and moneyness-dependent aggregates are
# NULL-guarded below so they're properly NULL (not 0) when spot is unknown.
# ---------------------------------------------------------------------------
OI_FEATURES_SQL = """
WITH joined AS (
    SELECT
        oi.trade_date,
        oi.expiration,
        oi.strike,
        oi.option_type,
        oi.open_interest,
        ohlc.close                                       AS spot_close,
        nm.next_monthly                                  AS next_monthly,
        (oi.expiration - oi.trade_date)::INTEGER         AS dte,
        CASE WHEN ohlc.close > 0
             THEN oi.strike / ohlc.close - 1.0
             ELSE NULL END                               AS moneyness
    FROM oi
    LEFT JOIN ohlc            USING (trade_date)
    LEFT JOIN next_monthly_df nm USING (trade_date)
),
per_day_agg AS (
    SELECT
        trade_date,
        ANY_VALUE(spot_close)                                                          AS spot_close,
        ANY_VALUE(next_monthly)                                                        AS next_monthly,
        SUM(open_interest)                                                             AS total_oi,
        SUM(CASE WHEN option_type = 'C' THEN open_interest ELSE 0 END)                 AS call_oi,
        SUM(CASE WHEN option_type = 'P' THEN open_interest ELSE 0 END)                 AS put_oi,
        -- moneyness IS NULL on a date with no OHLC yet (today, before market open).
        -- Returning NULL (instead of summing the ELSE 0 branch) keeps the column
        -- honest — "we don't know spot" rather than "0 OI within 5%".
        SUM(CASE WHEN moneyness IS NULL          THEN NULL
                 WHEN ABS(moneyness) <= 0.05     THEN open_interest
                 ELSE 0 END)                                                           AS oi_within_5pct,
        SUM(CASE WHEN moneyness IS NULL          THEN NULL
                 WHEN ABS(moneyness) <= 0.10     THEN open_interest
                 ELSE 0 END)                                                           AS oi_within_10pct,
        SUM(CASE WHEN moneyness IS NULL          THEN NULL
                 WHEN moneyness > 0              THEN open_interest
                 ELSE 0 END)                                                           AS oi_above_spot,
        SUM(CASE WHEN moneyness IS NULL          THEN NULL
                 WHEN moneyness < 0              THEN open_interest
                 ELSE 0 END)                                                           AS oi_below_spot,
        SUM(CASE WHEN dte BETWEEN 0  AND 30  THEN open_interest ELSE 0 END)            AS oi_0_30,
        SUM(CASE WHEN dte BETWEEN 31 AND 90  THEN open_interest ELSE 0 END)            AS oi_31_90,
        SUM(CASE WHEN dte BETWEEN 91 AND 365 THEN open_interest ELSE 0 END)            AS oi_91_365,
        -- OI-weighted strikes (all DTE)
        SUM(strike * open_interest)::DOUBLE
            / NULLIF(SUM(open_interest), 0)                                            AS oi_weighted_strike_all,
        SUM(CASE WHEN option_type = 'C' THEN strike * open_interest ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN option_type = 'C' THEN open_interest ELSE 0 END), 0) AS oi_weighted_strike_call,
        SUM(CASE WHEN option_type = 'P' THEN strike * open_interest ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN option_type = 'P' THEN open_interest ELSE 0 END), 0) AS oi_weighted_strike_put,
        -- OI-weighted strikes (DTE 0-30)
        SUM(CASE WHEN dte BETWEEN 0 AND 30 THEN strike * open_interest ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN dte BETWEEN 0 AND 30 THEN open_interest ELSE 0 END), 0)
                                                                                       AS oi_weighted_strike_all_0_30d,
        SUM(CASE WHEN dte BETWEEN 0 AND 30 AND option_type = 'C' THEN strike * open_interest ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN dte BETWEEN 0 AND 30 AND option_type = 'C' THEN open_interest ELSE 0 END), 0)
                                                                                       AS oi_weighted_strike_call_0_30d,
        SUM(CASE WHEN dte BETWEEN 0 AND 30 AND option_type = 'P' THEN strike * open_interest ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN dte BETWEEN 0 AND 30 AND option_type = 'P' THEN open_interest ELSE 0 END), 0)
                                                                                       AS oi_weighted_strike_put_0_30d,
        -- OI-weighted strikes (DTE 31-90)
        SUM(CASE WHEN dte BETWEEN 31 AND 90 THEN strike * open_interest ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN dte BETWEEN 31 AND 90 THEN open_interest ELSE 0 END), 0)
                                                                                       AS oi_weighted_strike_all_31_90d,
        SUM(CASE WHEN dte BETWEEN 31 AND 90 AND option_type = 'C' THEN strike * open_interest ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN dte BETWEEN 31 AND 90 AND option_type = 'C' THEN open_interest ELSE 0 END), 0)
                                                                                       AS oi_weighted_strike_call_31_90d,
        SUM(CASE WHEN dte BETWEEN 31 AND 90 AND option_type = 'P' THEN strike * open_interest ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN dte BETWEEN 31 AND 90 AND option_type = 'P' THEN open_interest ELSE 0 END), 0)
                                                                                       AS oi_weighted_strike_put_31_90d,
        SUM(dte * open_interest)::DOUBLE
            / NULLIF(SUM(open_interest), 0)                                            AS weighted_avg_dte
    FROM joined
    GROUP BY trade_date
),
-- Layer derived ratios on top of the GROUP BY so the windowing CTEs below
-- can LAG / window over them (DuckDB can't do that inside an aggregating SELECT).
per_day AS (
    SELECT
        a.*,
        a.put_oi::DOUBLE       / NULLIF(a.call_oi, 0)        AS put_call_oi_ratio,
        a.oi_above_spot::DOUBLE / NULLIF(a.oi_below_spot, 0) AS oi_above_below_ratio,
        a.oi_weighted_strike_all / NULLIF(a.spot_close, 0)   AS oi_weighted_strike_all_div_spot
    FROM per_day_agg a
),
strike_agg AS (
    SELECT trade_date, strike, SUM(open_interest) AS strike_oi
    FROM joined
    GROUP BY trade_date, strike
),
strike_ranked AS (
    SELECT trade_date, strike_oi,
           ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY strike_oi DESC) AS rk
    FROM strike_agg
),
top_strikes AS (
    SELECT trade_date,
           SUM(CASE WHEN rk <= 5  THEN strike_oi ELSE 0 END) AS top5_oi,
           SUM(CASE WHEN rk <= 10 THEN strike_oi ELSE 0 END) AS top10_oi
    FROM strike_ranked
    GROUP BY trade_date
),
call_strike_agg AS (
    SELECT trade_date, strike, SUM(open_interest) AS oi
    FROM joined WHERE option_type = 'C'
    GROUP BY trade_date, strike
),
max_call AS (
    SELECT trade_date, strike AS max_oi_strike_call
    FROM (
        SELECT trade_date, strike,
               ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY oi DESC, strike) AS rk
        FROM call_strike_agg
    ) WHERE rk = 1
),
put_strike_agg AS (
    SELECT trade_date, strike, SUM(open_interest) AS oi
    FROM joined WHERE option_type = 'P'
    GROUP BY trade_date, strike
),
max_put AS (
    SELECT trade_date, strike AS max_oi_strike_put
    FROM (
        SELECT trade_date, strike,
               ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY oi DESC, strike) AS rk
        FROM put_strike_agg
    ) WHERE rk = 1
),
front_expiry AS (
    SELECT trade_date, MIN(expiration) AS front_exp
    FROM joined WHERE dte >= 0
    GROUP BY trade_date
),
front_oi_q AS (
    SELECT j.trade_date, SUM(j.open_interest) AS front_oi
    FROM joined j
    JOIN front_expiry f
      ON f.trade_date = j.trade_date AND f.front_exp = j.expiration
    GROUP BY j.trade_date
),
next_monthly_oi AS (
    SELECT j.trade_date,
           SUM(j.open_interest)              AS nm_oi,
           SUM(j.strike * j.open_interest)   AS nm_strike_oi
    FROM joined j
    WHERE j.expiration = j.next_monthly
    GROUP BY j.trade_date
),
oi_lags AS (
    SELECT
        trade_date,
        total_oi,
        -- Existing absolute changes
        total_oi - LAG(total_oi, 1)  OVER w_t                                AS d1_total_oi_change,
        total_oi - LAG(total_oi, 5)  OVER w_t                                AS d5_total_oi_change,
        total_oi - LAG(total_oi, 20) OVER w_t                                AS d20_total_oi_change,
        -- New pct changes
        (total_oi - LAG(total_oi, 1) OVER w_t)::DOUBLE
            / NULLIF(LAG(total_oi, 1) OVER w_t, 0)                           AS d1_total_oi_pct_change,
        (total_oi - LAG(total_oi, 5) OVER w_t)::DOUBLE
            / NULLIF(LAG(total_oi, 5) OVER w_t, 0)                           AS d5_total_oi_pct_change,
        -- New d1/d5 changes of derived ratios
        oi_weighted_strike_all_div_spot
            - LAG(oi_weighted_strike_all_div_spot, 1) OVER w_t               AS d1_oi_weighted_strike_all_div_spot_change,
        oi_weighted_strike_all_div_spot
            - LAG(oi_weighted_strike_all_div_spot, 5) OVER w_t               AS d5_oi_weighted_strike_all_div_spot_change,
        put_call_oi_ratio - LAG(put_call_oi_ratio, 1) OVER w_t               AS d1_put_call_oi_ratio_change,
        put_call_oi_ratio - LAG(put_call_oi_ratio, 5) OVER w_t               AS d5_put_call_oi_ratio_change
    FROM per_day
    WINDOW w_t AS (ORDER BY trade_date)
),
-- 60-trading-day (~3-month) z-scores and the d1/d5 pct-change ratio.
-- Each z-score is gated by COUNT(col) >= 60 so we don't emit a "z-score" off
-- 2-3 observations early in the series — those would be noise, not signal.
-- Joined back to per_day so we can z-score the derived ratios in the same pass.
oi_zscores AS (
    SELECT
        trade_date,
        d1_total_oi_pct_change / NULLIF(d5_total_oi_pct_change, 0)                  AS d1_d5_ratio_total_oi_pct_change,
        CASE WHEN COUNT(d1_total_oi_change) OVER w60 >= 60
             THEN (d1_total_oi_change - AVG(d1_total_oi_change) OVER w60)
                  / NULLIF(STDDEV_SAMP(d1_total_oi_change) OVER w60, 0)
             ELSE NULL
        END                                                                         AS zscore_d1_oi_change_3m,
        CASE WHEN COUNT(d5_total_oi_change) OVER w60 >= 60
             THEN (d5_total_oi_change - AVG(d5_total_oi_change) OVER w60)
                  / NULLIF(STDDEV_SAMP(d5_total_oi_change) OVER w60, 0)
             ELSE NULL
        END                                                                         AS zscore_d5_oi_change_3m,
        CASE WHEN COUNT(oi_weighted_strike_all_div_spot) OVER w60 >= 60
             THEN (oi_weighted_strike_all_div_spot
                   - AVG(oi_weighted_strike_all_div_spot) OVER w60)
                  / NULLIF(STDDEV_SAMP(oi_weighted_strike_all_div_spot) OVER w60, 0)
             ELSE NULL
        END                                                                         AS zscore_oi_weighted_strike_all_div_spot_3m,
        CASE WHEN COUNT(put_call_oi_ratio) OVER w60 >= 60
             THEN (put_call_oi_ratio - AVG(put_call_oi_ratio) OVER w60)
                  / NULLIF(STDDEV_SAMP(put_call_oi_ratio) OVER w60, 0)
             ELSE NULL
        END                                                                         AS zscore_put_call_oi_ratio_3m,
        CASE WHEN COUNT(oi_above_below_ratio) OVER w60 >= 60
             THEN (oi_above_below_ratio - AVG(oi_above_below_ratio) OVER w60)
                  / NULLIF(STDDEV_SAMP(oi_above_below_ratio) OVER w60, 0)
             ELSE NULL
        END                                                                         AS zscore_oi_above_below_ratio_3m
    FROM oi_lags JOIN per_day USING (trade_date)
    WINDOW w60 AS (ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
)
SELECT
    p.trade_date,
    p.spot_close,
    p.total_oi,
    p.call_oi,
    p.put_oi,
    p.put_oi::DOUBLE / NULLIF(p.call_oi, 0)                                AS put_call_oi_ratio,
    mc.max_oi_strike_call,
    mp.max_oi_strike_put,
    p.oi_weighted_strike_call,
    p.oi_weighted_strike_put,
    p.oi_weighted_strike_all,
    p.oi_weighted_strike_call - p.spot_close                               AS oi_weighted_strike_call_minus_spot,
    p.oi_weighted_strike_put  - p.spot_close                               AS oi_weighted_strike_put_minus_spot,
    p.oi_weighted_strike_all  - p.spot_close                               AS oi_weighted_strike_all_minus_spot,
    p.oi_weighted_strike_call / NULLIF(p.spot_close, 0)                    AS oi_weighted_strike_call_div_spot,
    p.oi_weighted_strike_put  / NULLIF(p.spot_close, 0)                    AS oi_weighted_strike_put_div_spot,
    p.oi_weighted_strike_all  / NULLIF(p.spot_close, 0)                    AS oi_weighted_strike_all_div_spot,
    p.oi_within_5pct,
    p.oi_within_10pct,
    fo.front_oi::DOUBLE / NULLIF(p.total_oi, 0)                            AS pct_oi_in_front_expiry,
    p.oi_above_spot,
    p.oi_below_spot,
    p.oi_above_spot::DOUBLE / NULLIF(p.oi_below_spot, 0)                   AS oi_above_below_ratio,
    p.oi_weighted_strike_all_0_30d,
    p.oi_weighted_strike_call_0_30d,
    p.oi_weighted_strike_put_0_30d,
    p.oi_weighted_strike_all_0_30d  / NULLIF(p.spot_close, 0)              AS oi_weighted_strike_all_0_30d_div_spot,
    p.oi_weighted_strike_call_0_30d / NULLIF(p.spot_close, 0)              AS oi_weighted_strike_call_0_30d_div_spot,
    p.oi_weighted_strike_put_0_30d  / NULLIF(p.spot_close, 0)              AS oi_weighted_strike_put_0_30d_div_spot,
    p.oi_weighted_strike_all_31_90d,
    p.oi_weighted_strike_call_31_90d,
    p.oi_weighted_strike_put_31_90d,
    p.oi_weighted_strike_all_31_90d  / NULLIF(p.spot_close, 0)             AS oi_weighted_strike_all_31_90d_div_spot,
    p.oi_weighted_strike_call_31_90d / NULLIF(p.spot_close, 0)             AS oi_weighted_strike_call_31_90d_div_spot,
    p.oi_weighted_strike_put_31_90d  / NULLIF(p.spot_close, 0)             AS oi_weighted_strike_put_31_90d_div_spot,
    ol.d1_total_oi_change,
    ol.d5_total_oi_change,
    ol.d20_total_oi_change,
    -- New percentage / aggregate features (full unfiltered chain in denominator)
    p.oi_within_5pct::DOUBLE  / NULLIF(p.total_oi, 0)                      AS pct_oi_within_5pct,
    p.oi_within_10pct::DOUBLE / NULLIF(p.total_oi, 0)                      AS pct_oi_within_10pct,
    p.oi_above_spot::DOUBLE / NULLIF(p.total_oi, 0)                        AS pct_oi_above_spot,
    p.oi_below_spot::DOUBLE / NULLIF(p.total_oi, 0)                        AS pct_oi_below_spot,
    ts.top5_oi::DOUBLE  / NULLIF(p.total_oi, 0)                            AS top5_strikes_pct_total_oi,
    ts.top10_oi::DOUBLE / NULLIF(p.total_oi, 0)                            AS top10_strikes_pct_total_oi,
    p.weighted_avg_dte,
    p.oi_0_30::DOUBLE   / NULLIF(p.total_oi, 0)                            AS pct_oi_0_30d,
    p.oi_31_90::DOUBLE  / NULLIF(p.total_oi, 0)                            AS pct_oi_31_90d,
    p.oi_91_365::DOUBLE / NULLIF(p.total_oi, 0)                            AS pct_oi_91_365d,
    nm.nm_oi::DOUBLE / NULLIF(p.total_oi, 0)                               AS pct_oi_next_monthly,
    nm.nm_strike_oi::DOUBLE
        / NULLIF(nm.nm_oi, 0)
        / NULLIF(p.spot_close, 0)                                          AS oi_weighted_strike_next_monthly_div_spot,
    -- 2026-04-28 — pct changes / derived-ratio changes / 90-day z-scores
    ol.d1_total_oi_pct_change,
    ol.d5_total_oi_pct_change,
    z.d1_d5_ratio_total_oi_pct_change,
    ol.d1_oi_weighted_strike_all_div_spot_change,
    ol.d5_oi_weighted_strike_all_div_spot_change,
    ol.d1_put_call_oi_ratio_change,
    ol.d5_put_call_oi_ratio_change,
    z.zscore_d1_oi_change_3m,
    z.zscore_d5_oi_change_3m,
    z.zscore_oi_weighted_strike_all_div_spot_3m,
    z.zscore_put_call_oi_ratio_3m,
    z.zscore_oi_above_below_ratio_3m
FROM per_day p
LEFT JOIN max_call         mc USING (trade_date)
LEFT JOIN max_put          mp USING (trade_date)
LEFT JOIN top_strikes      ts USING (trade_date)
LEFT JOIN front_oi_q       fo USING (trade_date)
LEFT JOIN next_monthly_oi  nm USING (trade_date)
LEFT JOIN oi_lags          ol USING (trade_date)
LEFT JOIN oi_zscores       z  USING (trade_date)
ORDER BY p.trade_date
"""


# ---------------------------------------------------------------------------
# OHLC-derived features (rv, fwd oc returns) — DuckDB on ohlc_full DF
# Input: ohlc_full pandas DF [trade_date, open, close]
#
# Forward returns: entry = open of trade_date (OI for trade_date is published
# overnight and visible on broker platforms when the market opens), exit =
# close of trade_date + (N-1). So ret_1d is intraday open-to-close on
# trade_date itself, ret_3d is from trade_date open to close[+2], etc.
# ---------------------------------------------------------------------------
OHLC_FEATURES_SQL = """
WITH ret AS (
    SELECT trade_date, open, close,
           LN(NULLIF(close, 0)
              / NULLIF(LAG(close) OVER (ORDER BY trade_date), 0)) AS log_ret
    FROM ohlc_full
)
SELECT
    trade_date,
    STDDEV_SAMP(log_ret) OVER w5  * SQRT(252)                              AS rv_5d,
    STDDEV_SAMP(log_ret) OVER w20 * SQRT(252)                              AS rv_20d,
    close                         / NULLIF(open, 0) - 1                    AS ret_1d_fwd_oc,
    LEAD(close,  2) OVER w_t      / NULLIF(open, 0) - 1                    AS ret_3d_fwd_oc,
    LEAD(close,  4) OVER w_t      / NULLIF(open, 0) - 1                    AS ret_5d_fwd_oc,
    LEAD(close,  6) OVER w_t      / NULLIF(open, 0) - 1                    AS ret_7d_fwd_oc,
    LEAD(close,  9) OVER w_t      / NULLIF(open, 0) - 1                    AS ret_10d_fwd_oc,
    LEAD(close, 19) OVER w_t      / NULLIF(open, 0) - 1                    AS ret_20d_fwd_oc
FROM ret
WINDOW
    w_t AS (ORDER BY trade_date),
    w5  AS (ORDER BY trade_date ROWS BETWEEN 4  PRECEDING AND CURRENT ROW),
    w20 AS (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
"""


# ---------------------------------------------------------------------------
# Postgres write
# ---------------------------------------------------------------------------
INSERT_COLS = [
    "ticker", "trade_date", "spot_close",
    "total_oi", "call_oi", "put_oi", "put_call_oi_ratio",
    "max_oi_strike_call", "max_oi_strike_put",
    "oi_weighted_strike_call", "oi_weighted_strike_put", "oi_weighted_strike_all",
    "oi_weighted_strike_call_minus_spot", "oi_weighted_strike_put_minus_spot",
    "oi_weighted_strike_all_minus_spot",
    "oi_weighted_strike_call_div_spot", "oi_weighted_strike_put_div_spot",
    "oi_weighted_strike_all_div_spot",
    "oi_within_5pct", "oi_within_10pct", "pct_oi_in_front_expiry",
    "oi_above_spot", "oi_below_spot", "oi_above_below_ratio",
    "oi_weighted_strike_all_0_30d", "oi_weighted_strike_call_0_30d",
    "oi_weighted_strike_put_0_30d",
    "oi_weighted_strike_all_0_30d_div_spot", "oi_weighted_strike_call_0_30d_div_spot",
    "oi_weighted_strike_put_0_30d_div_spot",
    "oi_weighted_strike_all_31_90d", "oi_weighted_strike_call_31_90d",
    "oi_weighted_strike_put_31_90d",
    "oi_weighted_strike_all_31_90d_div_spot", "oi_weighted_strike_call_31_90d_div_spot",
    "oi_weighted_strike_put_31_90d_div_spot",
    "d1_total_oi_change", "d5_total_oi_change", "d20_total_oi_change",
    "rv_5d", "rv_20d",
    "ret_1d_fwd_oc", "ret_3d_fwd_oc", "ret_5d_fwd_oc",
    "ret_7d_fwd_oc", "ret_10d_fwd_oc", "ret_20d_fwd_oc",
    "pct_oi_within_5pct", "pct_oi_within_10pct",
    "pct_oi_above_spot", "pct_oi_below_spot",
    "top5_strikes_pct_total_oi", "top10_strikes_pct_total_oi",
    "weighted_avg_dte",
    "pct_oi_0_30d", "pct_oi_31_90d", "pct_oi_91_365d",
    "pct_oi_next_monthly", "oi_weighted_strike_next_monthly_div_spot",
    # 2026-04-28 — pct changes / derived-ratio changes / 90-day z-scores
    "d1_total_oi_pct_change", "d5_total_oi_pct_change",
    "d1_d5_ratio_total_oi_pct_change",
    "d1_oi_weighted_strike_all_div_spot_change",
    "d5_oi_weighted_strike_all_div_spot_change",
    "d1_put_call_oi_ratio_change", "d5_put_call_oi_ratio_change",
    "zscore_d1_oi_change_3m", "zscore_d5_oi_change_3m",
    "zscore_oi_weighted_strike_all_div_spot_3m",
    "zscore_put_call_oi_ratio_3m",
    "zscore_oi_above_below_ratio_3m",
]

INSERT_SQL = f"INSERT INTO daily_features ({', '.join(INSERT_COLS)}) VALUES %s"
CLEAR_SQL  = "DELETE FROM daily_features WHERE ticker = %(ticker)s"


# ---------------------------------------------------------------------------
# Per-ticker pipeline
# ---------------------------------------------------------------------------

def load_ohlc(conn, ticker: str) -> pd.DataFrame:
    """Pull (trade_date, open, close) for one ticker out of underlying_ohlc."""
    df = read_sql_df(
        conn,
        "SELECT trade_date, open, close FROM underlying_ohlc "
        "WHERE ticker = %(ticker)s ORDER BY trade_date",
        {"ticker": ticker},
    )
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return df


def listed_expirations_from_parquet(con: duckdb.DuckDBPyConnection,
                                    ticker: str) -> set:
    rows = con.execute(
        f"SELECT DISTINCT expiration FROM read_parquet('{parquet_glob(ticker)}')"
    ).fetchall()
    return {r[0] for r in rows}


def build_for_ticker(pg_conn, ticker: str,
                     start: date | None = None,
                     end:   date | None = None) -> int:
    """
    Recompute daily_features for one ticker.

    - start/end both None: full rebuild (entire history, fastest path through
      a single DuckDB pass).
    - start set: rebuild only [start, end] (end defaults to today). DuckDB
      reads parquet+OHLC from (start - LOOKBACK_BUFFER_DAYS) so window
      functions (LAG, 60-day z-scores) still see enough history; the result
      is then sliced to [start, end] before INSERT, and only that range is
      DELETEd from daily_features.
    """
    log.info("--- %s ---", ticker)

    ohlc_full_df = load_ohlc(pg_conn, ticker)
    if ohlc_full_df.empty:
        log.warning("  no OHLC for %s — skipping (run fetch_ohlc.py first)", ticker)
        return 0

    end_eff = end or date.today()
    if start is not None:
        buffer_start = start - timedelta(days=LOOKBACK_BUFFER_DAYS)
        ohlc = ohlc_full_df[
            (ohlc_full_df["trade_date"] >= buffer_start)
            & (ohlc_full_df["trade_date"] <= end_eff)
        ].reset_index(drop=True)
        date_filter_sql = (
            f" WHERE trade_date >= DATE '{buffer_start.isoformat()}'"
            f" AND trade_date <= DATE '{end_eff.isoformat()}'"
        )
    else:
        ohlc = ohlc_full_df
        date_filter_sql = ""

    con = duckdb.connect(database=":memory:")

    listed = listed_expirations_from_parquet(con, ticker)
    if not listed:
        log.warning("  no parquet rows for %s — skipping", ticker)
        con.close()
        return 0

    # next_monthly must cover every trade_date that appears in the OI parquet
    # (NOT just OHLC dates), so today's OI row resolves a next-monthly node
    # even when today's OHLC hasn't been published yet.
    oi_dates_rows = con.execute(
        f"SELECT DISTINCT trade_date FROM read_parquet('{parquet_glob(ticker)}')"
        f"{date_filter_sql} ORDER BY trade_date"
    ).fetchall()
    oi_dates = [r[0] for r in oi_dates_rows]
    all_dates = sorted(set(ohlc["trade_date"].tolist()) | set(oi_dates))
    nm_lookup = build_next_monthly_lookup(all_dates, listed)
    nm_df = pd.DataFrame({
        "trade_date":   list(nm_lookup.keys()),
        "next_monthly": list(nm_lookup.values()),
    })

    con.register("ohlc",            ohlc[["trade_date", "close"]])
    con.register("ohlc_full",       ohlc[["trade_date", "open", "close"]])
    con.register("next_monthly_df", nm_df)
    con.execute(
        f"CREATE OR REPLACE VIEW oi AS "
        f"SELECT * FROM read_parquet('{parquet_glob(ticker)}'){date_filter_sql}"
    )

    log.info("  computing OI features ...")
    oi_feats = con.execute(OI_FEATURES_SQL).df()
    log.info("  computing OHLC features ...")
    ohlc_feats = con.execute(OHLC_FEATURES_SQL).df()
    con.close()

    if oi_feats.empty:
        log.warning("  no OI rows in range for %s — skipping", ticker)
        return 0

    feats = oi_feats.merge(ohlc_feats, on="trade_date", how="left")
    # DuckDB returns DATE columns as datetime64[us]; normalise to Python date
    # so `>= start` (Python date) comparisons work and psycopg2 sees a clean
    # DATE value at INSERT time.
    feats["trade_date"] = pd.to_datetime(feats["trade_date"]).dt.date
    feats.insert(0, "ticker", ticker)

    # Drop the lookback buffer rows — they were only there for window context.
    if start is not None:
        feats = feats[feats["trade_date"] >= start].reset_index(drop=True)
        if feats.empty:
            log.info("  no rows in [%s, %s] for %s", start, end_eff, ticker)
            return 0

    rows = [
        tuple(_pgify(r.get(c)) for c in INSERT_COLS)
        for r in feats.to_dict(orient="records")
    ]

    with pg_conn.cursor() as cur:
        if start is None:
            cur.execute(CLEAR_SQL, {"ticker": ticker})
        else:
            cur.execute(
                "DELETE FROM daily_features "
                "WHERE ticker = %(ticker)s "
                "  AND trade_date BETWEEN %(start)s AND %(end)s",
                {"ticker": ticker, "start": start, "end": end_eff},
            )
        psycopg2.extras.execute_values(cur, INSERT_SQL, rows, page_size=500)
    pg_conn.commit()

    log.info("  wrote %d rows to daily_features", len(rows))
    return len(rows)


def _pgify(v):
    """numpy/pandas → native; NaN/NaT → None."""
    if v is None:
        return None
    if isinstance(v, float):
        return None if v != v else v
    try:
        import numpy as np
        if isinstance(v, np.floating):
            f = float(v)
            return None if f != f else f
        if isinstance(v, np.integer):
            return int(v)
        if isinstance(v, np.bool_):
            return bool(v)
    except Exception:
        pass
    if pd.isna(v):
        return None
    return v


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def prompt_tickers() -> list[str]:
    raw = input(
        "Tickers (comma-separated; blank = all tickers in OI_RAW_DIR): "
    ).strip()
    if raw:
        return [t.strip().upper() for t in raw.split(",") if t.strip()]
    return list_tickers()


def prompt_date_range() -> tuple[date | None, date | None]:
    """Returns (start, end) or (None, None) for a full-history rebuild."""
    raw_start = input("Start date (blank = full history rebuild): ").strip()
    if not raw_start:
        return None, None
    try:
        start = datetime.strptime(raw_start, "%Y-%m-%d").date()
    except ValueError:
        raise SystemExit("Start date must be YYYY-MM-DD.")
    raw_end = input("End date   (blank = today): ").strip()
    if raw_end:
        try:
            end = datetime.strptime(raw_end, "%Y-%m-%d").date()
        except ValueError:
            raise SystemExit("End date must be YYYY-MM-DD.")
    else:
        end = date.today()
    if end < start:
        raise SystemExit("End date must be >= start date.")
    return start, end


def main() -> None:
    print("=== OI_Research — Build daily_features (parquet → Postgres) ===\n")
    tickers = prompt_tickers()
    if not tickers:
        print("No tickers in OI_RAW_DIR — run fetch_oi.py first.")
        return
    start, end = prompt_date_range()
    range_label = f"{start} → {end}" if start else "full history"
    print(f"\nRebuilding features ({range_label}) for: {', '.join(tickers)}\n")

    with get_connection() as conn:
        for t in tickers:
            build_for_ticker(conn, t, start=start, end=end)
    print("\nDone.")


if __name__ == "__main__":
    main()
