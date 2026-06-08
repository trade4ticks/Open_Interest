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
from lib.chain_store import has_data as chain_has_data, parquet_glob as chain_parquet_glob
from lib.expirations import build_next_monthly_lookup
from lib.market_hours import next_trading_day
from lib.parquet_store import list_tickers, parquet_glob
from lib.split_factors import load_splits, make_split_factors

# Backward window buffer (calendar days) when running with a date range.
# 60-day z-scores need ~65 trading-day inputs; 130 calendar days covers that
# comfortably even across long weekends / holidays.
LOOKBACK_BUFFER_DAYS = 130

# Wider buffer applied only to the OHLC pandas slice.
# 52-week metrics require 252 trading days ≈ 365 calendar days of OHLC;
# 400 adds ~10% slack for holidays and non-trading periods.
OHLC_LOOKBACK_BUFFER_DAYS = 400

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# OI features (DuckDB on parquet, LEFT JOINed to ohlc + next_monthly DFs).
# Inputs registered by the caller:
#   oi              view over read_parquet(...)        — raw chain
#   ohlc            pandas DF [trade_date, open, close, prev_close]
#   next_monthly_df pandas DF [trade_date, next_monthly] — covers ALL OI dates
#                   (independent of OHLC, so today's OI features still resolve
#                   the next-monthly node even before today's OHLC arrives)
#
# Two spot definitions:
#   spot_pc = ohlc.prev_close  — close of the previous trading day (KNOWN at
#             7am, when OI is published; this is the price a trader sees at
#             the moment OI[X] becomes visible)
#   spot_co = ohlc.open        — open of trade_date X (the realistic entry
#             price after seeing OI[X]; NOT yet known at 7am for today)
#
# LEFT JOIN means OI rows for today (no OHLC yet at 7am) still survive — but
# spot_co will be NULL while spot_pc populates from yesterday's close.
# Moneyness-dependent SUMs are NULL-guarded so each version is properly NULL
# (not 0) when its spot is unknown.
# ---------------------------------------------------------------------------
OI_FEATURES_SQL = """
WITH joined AS (
    -- spot_pc and spot_co need different JOINs:
    --   spot_pc = close of the most recent OHLC row STRICTLY BEFORE the OI's
    --            trade_date. ASOF LEFT JOIN handles this directly. For today
    --            (no OHLC row yet at 7am ET), this still resolves to
    --            yesterday's close — which is what we want.
    --   spot_co = open of the SAME trade_date. Plain equality LEFT JOIN.
    --            Today's row gets NULL until today's OHLC is ingested.
    SELECT
        oi.trade_date,
        oi.expiration,
        oi.strike,
        oi.option_type,
        oi.open_interest,
        ohlc_pc.close                                    AS spot_pc,
        ohlc_co.open                                     AS spot_co,
        nm.next_monthly                                  AS next_monthly,
        (oi.expiration - oi.trade_date)::INTEGER         AS dte,
        CASE WHEN ohlc_pc.close > 0
             THEN oi.strike / ohlc_pc.close - 1.0
             ELSE NULL END                               AS moneyness_pc,
        CASE WHEN ohlc_co.open > 0
             THEN oi.strike / ohlc_co.open - 1.0
             ELSE NULL END                               AS moneyness_co
    FROM oi
    ASOF LEFT JOIN ohlc ohlc_pc      ON oi.trade_date > ohlc_pc.trade_date
    LEFT JOIN      ohlc ohlc_co      ON oi.trade_date = ohlc_co.trade_date
    LEFT JOIN      next_monthly_df nm ON oi.trade_date = nm.trade_date
),
per_day_agg AS (
    SELECT
        trade_date,
        ANY_VALUE(spot_pc)                                                             AS spot_pc,
        ANY_VALUE(spot_co)                                                             AS spot_co,
        ANY_VALUE(next_monthly)                                                        AS next_monthly,
        SUM(open_interest)                                                             AS total_oi,
        SUM(CASE WHEN option_type = 'C' THEN open_interest ELSE 0 END)                 AS call_oi,
        SUM(CASE WHEN option_type = 'P' THEN open_interest ELSE 0 END)                 AS put_oi,
        -- Moneyness IS NULL on a date with no OHLC yet (today's spot_co at 7am).
        -- The CASE WHEN ... IS NULL THEN NULL guard makes the SUM honest:
        -- "we don't know spot" rather than "0 OI within 5%".
        SUM(CASE WHEN moneyness_pc IS NULL          THEN NULL
                 WHEN ABS(moneyness_pc) <= 0.05     THEN open_interest
                 ELSE 0 END)                                                           AS oi_within_5pct_pc,
        SUM(CASE WHEN moneyness_co IS NULL          THEN NULL
                 WHEN ABS(moneyness_co) <= 0.05     THEN open_interest
                 ELSE 0 END)                                                           AS oi_within_5pct_co,
        SUM(CASE WHEN moneyness_pc IS NULL          THEN NULL
                 WHEN ABS(moneyness_pc) <= 0.10     THEN open_interest
                 ELSE 0 END)                                                           AS oi_within_10pct_pc,
        SUM(CASE WHEN moneyness_co IS NULL          THEN NULL
                 WHEN ABS(moneyness_co) <= 0.10     THEN open_interest
                 ELSE 0 END)                                                           AS oi_within_10pct_co,
        SUM(CASE WHEN moneyness_pc IS NULL          THEN NULL
                 WHEN moneyness_pc > 0              THEN open_interest
                 ELSE 0 END)                                                           AS oi_above_spot_pc,
        SUM(CASE WHEN moneyness_co IS NULL          THEN NULL
                 WHEN moneyness_co > 0              THEN open_interest
                 ELSE 0 END)                                                           AS oi_above_spot_co,
        SUM(CASE WHEN moneyness_pc IS NULL          THEN NULL
                 WHEN moneyness_pc < 0              THEN open_interest
                 ELSE 0 END)                                                           AS oi_below_spot_pc,
        SUM(CASE WHEN moneyness_co IS NULL          THEN NULL
                 WHEN moneyness_co < 0              THEN open_interest
                 ELSE 0 END)                                                           AS oi_below_spot_co,
        SUM(CASE WHEN dte BETWEEN 0  AND 30  THEN open_interest ELSE 0 END)            AS oi_0_30,
        SUM(CASE WHEN dte BETWEEN 31 AND 90  THEN open_interest ELSE 0 END)            AS oi_31_90,
        SUM(CASE WHEN dte BETWEEN 91 AND 365 THEN open_interest ELSE 0 END)            AS oi_91_365,
        -- OI-weighted strikes (no spot dependency — same value for pc/co)
        SUM(strike * open_interest)::DOUBLE
            / NULLIF(SUM(open_interest), 0)                                            AS oi_weighted_all,
        SUM(CASE WHEN option_type = 'C' THEN strike * open_interest ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN option_type = 'C' THEN open_interest ELSE 0 END), 0) AS oi_weighted_call,
        SUM(CASE WHEN option_type = 'P' THEN strike * open_interest ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN option_type = 'P' THEN open_interest ELSE 0 END), 0) AS oi_weighted_put,
        SUM(CASE WHEN dte BETWEEN 0 AND 30 THEN strike * open_interest ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN dte BETWEEN 0 AND 30 THEN open_interest ELSE 0 END), 0)
                                                                                       AS oi_weighted_all_0_30d,
        SUM(CASE WHEN dte BETWEEN 0 AND 30 AND option_type = 'C' THEN strike * open_interest ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN dte BETWEEN 0 AND 30 AND option_type = 'C' THEN open_interest ELSE 0 END), 0)
                                                                                       AS oi_weighted_call_0_30d,
        SUM(CASE WHEN dte BETWEEN 0 AND 30 AND option_type = 'P' THEN strike * open_interest ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN dte BETWEEN 0 AND 30 AND option_type = 'P' THEN open_interest ELSE 0 END), 0)
                                                                                       AS oi_weighted_put_0_30d,
        SUM(CASE WHEN dte BETWEEN 31 AND 90 THEN strike * open_interest ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN dte BETWEEN 31 AND 90 THEN open_interest ELSE 0 END), 0)
                                                                                       AS oi_weighted_all_31_90d,
        SUM(CASE WHEN dte BETWEEN 31 AND 90 AND option_type = 'C' THEN strike * open_interest ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN dte BETWEEN 31 AND 90 AND option_type = 'C' THEN open_interest ELSE 0 END), 0)
                                                                                       AS oi_weighted_call_31_90d,
        SUM(CASE WHEN dte BETWEEN 31 AND 90 AND option_type = 'P' THEN strike * open_interest ELSE 0 END)::DOUBLE
            / NULLIF(SUM(CASE WHEN dte BETWEEN 31 AND 90 AND option_type = 'P' THEN open_interest ELSE 0 END), 0)
                                                                                       AS oi_weighted_put_31_90d,
        SUM(dte * open_interest)::DOUBLE
            / NULLIF(SUM(open_interest), 0)                                            AS weighted_avg_dte
    FROM joined
    GROUP BY trade_date
),
-- Layer derived ratios on top of the GROUP BY so the windowing CTEs below can
-- LAG / window over them. _pc and _co are split versions of the spot-divided
-- ones; put_call_oi_ratio is spot-independent.
per_day AS (
    SELECT
        a.*,
        a.put_oi::DOUBLE       / NULLIF(a.call_oi, 0)              AS put_call_oi_ratio,
        a.oi_above_spot_pc::DOUBLE / NULLIF(a.oi_below_spot_pc, 0) AS oi_above_below_ratio_pc,
        a.oi_above_spot_co::DOUBLE / NULLIF(a.oi_below_spot_co, 0) AS oi_above_below_ratio_co,
        a.oi_weighted_all / NULLIF(a.spot_pc, 0)                   AS oi_weighted_all_div_spot_pc,
        a.oi_weighted_all / NULLIF(a.spot_co, 0)                   AS oi_weighted_all_div_spot_co
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
        -- Spot-independent absolute / pct changes
        total_oi - LAG(total_oi, 1)  OVER w_t                                AS d1_total_oi_change,
        total_oi - LAG(total_oi, 5)  OVER w_t                                AS d5_total_oi_change,
        total_oi - LAG(total_oi, 20) OVER w_t                                AS d20_total_oi_change,
        (total_oi - LAG(total_oi, 1) OVER w_t)::DOUBLE
            / NULLIF(LAG(total_oi, 1) OVER w_t, 0)                           AS d1_total_oi_pct_change,
        (total_oi - LAG(total_oi, 5) OVER w_t)::DOUBLE
            / NULLIF(LAG(total_oi, 5) OVER w_t, 0)                           AS d5_total_oi_pct_change,
        put_call_oi_ratio - LAG(put_call_oi_ratio, 1) OVER w_t               AS d1_put_call_oi_ratio_change,
        put_call_oi_ratio - LAG(put_call_oi_ratio, 5) OVER w_t               AS d5_put_call_oi_ratio_change,
        -- Spot-dependent — _pc and _co versions
        oi_weighted_all_div_spot_pc
            - LAG(oi_weighted_all_div_spot_pc, 1) OVER w_t                   AS d1_oi_weighted_all_div_spot_change_pc,
        oi_weighted_all_div_spot_pc
            - LAG(oi_weighted_all_div_spot_pc, 5) OVER w_t                   AS d5_oi_weighted_all_div_spot_change_pc,
        oi_weighted_all_div_spot_co
            - LAG(oi_weighted_all_div_spot_co, 1) OVER w_t                   AS d1_oi_weighted_all_div_spot_change_co,
        oi_weighted_all_div_spot_co
            - LAG(oi_weighted_all_div_spot_co, 5) OVER w_t                   AS d5_oi_weighted_all_div_spot_change_co
    FROM per_day
    WINDOW w_t AS (ORDER BY trade_date)
),
-- 60-trading-day (~3-month) z-scores. Each is gated by COUNT(col) >= 60 so
-- early rows in the series stay NULL until 60 prior observations exist.
-- Spot-dependent z-scores have _pc and _co versions.
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
        CASE WHEN COUNT(put_call_oi_ratio) OVER w60 >= 60
             THEN (put_call_oi_ratio - AVG(put_call_oi_ratio) OVER w60)
                  / NULLIF(STDDEV_SAMP(put_call_oi_ratio) OVER w60, 0)
             ELSE NULL
        END                                                                         AS zscore_put_call_oi_ratio_3m,
        -- Spot-dependent z-scores (_pc and _co)
        CASE WHEN COUNT(oi_weighted_all_div_spot_pc) OVER w60 >= 60
             THEN (oi_weighted_all_div_spot_pc
                   - AVG(oi_weighted_all_div_spot_pc) OVER w60)
                  / NULLIF(STDDEV_SAMP(oi_weighted_all_div_spot_pc) OVER w60, 0)
             ELSE NULL
        END                                                                         AS zscore_oi_weighted_all_div_spot_3m_pc,
        CASE WHEN COUNT(oi_weighted_all_div_spot_co) OVER w60 >= 60
             THEN (oi_weighted_all_div_spot_co
                   - AVG(oi_weighted_all_div_spot_co) OVER w60)
                  / NULLIF(STDDEV_SAMP(oi_weighted_all_div_spot_co) OVER w60, 0)
             ELSE NULL
        END                                                                         AS zscore_oi_weighted_all_div_spot_3m_co,
        CASE WHEN COUNT(oi_above_below_ratio_pc) OVER w60 >= 60
             THEN (oi_above_below_ratio_pc - AVG(oi_above_below_ratio_pc) OVER w60)
                  / NULLIF(STDDEV_SAMP(oi_above_below_ratio_pc) OVER w60, 0)
             ELSE NULL
        END                                                                         AS zscore_oi_above_below_ratio_3m_pc,
        CASE WHEN COUNT(oi_above_below_ratio_co) OVER w60 >= 60
             THEN (oi_above_below_ratio_co - AVG(oi_above_below_ratio_co) OVER w60)
                  / NULLIF(STDDEV_SAMP(oi_above_below_ratio_co) OVER w60, 0)
             ELSE NULL
        END                                                                         AS zscore_oi_above_below_ratio_3m_co
    FROM oi_lags JOIN per_day USING (trade_date)
    WINDOW w60 AS (ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
)
SELECT
    p.trade_date,
    p.spot_pc,
    p.spot_co,
    p.total_oi,
    p.call_oi,
    p.put_oi,
    p.put_call_oi_ratio,
    mc.max_oi_strike_call,
    mp.max_oi_strike_put,
    -- OI-weighted strikes (no spot dependency)
    p.oi_weighted_call,
    p.oi_weighted_put,
    p.oi_weighted_all,
    -- minus_spot (pc / co)
    p.oi_weighted_call - p.spot_pc                              AS oi_weighted_call_minus_spot_pc,
    p.oi_weighted_call - p.spot_co                              AS oi_weighted_call_minus_spot_co,
    p.oi_weighted_put  - p.spot_pc                              AS oi_weighted_put_minus_spot_pc,
    p.oi_weighted_put  - p.spot_co                              AS oi_weighted_put_minus_spot_co,
    p.oi_weighted_all  - p.spot_pc                              AS oi_weighted_all_minus_spot_pc,
    p.oi_weighted_all  - p.spot_co                              AS oi_weighted_all_minus_spot_co,
    -- div_spot (pc / co)
    p.oi_weighted_call / NULLIF(p.spot_pc, 0)                   AS oi_weighted_call_div_spot_pc,
    p.oi_weighted_call / NULLIF(p.spot_co, 0)                   AS oi_weighted_call_div_spot_co,
    p.oi_weighted_put  / NULLIF(p.spot_pc, 0)                   AS oi_weighted_put_div_spot_pc,
    p.oi_weighted_put  / NULLIF(p.spot_co, 0)                   AS oi_weighted_put_div_spot_co,
    p.oi_weighted_all_div_spot_pc,
    p.oi_weighted_all_div_spot_co,
    -- Moneyness counts (pc / co)
    p.oi_within_5pct_pc,  p.oi_within_5pct_co,
    p.oi_within_10pct_pc, p.oi_within_10pct_co,
    fo.front_oi::DOUBLE / NULLIF(p.total_oi, 0)                 AS pct_oi_in_front_expiry,
    p.oi_above_spot_pc, p.oi_above_spot_co,
    p.oi_below_spot_pc, p.oi_below_spot_co,
    p.oi_above_below_ratio_pc, p.oi_above_below_ratio_co,
    -- DTE-bucketed weighted strikes (no spot)
    p.oi_weighted_all_0_30d, p.oi_weighted_call_0_30d, p.oi_weighted_put_0_30d,
    -- DTE-bucketed div_spot variants (pc / co)
    p.oi_weighted_all_0_30d  / NULLIF(p.spot_pc, 0)             AS oi_weighted_all_0_30d_div_spot_pc,
    p.oi_weighted_all_0_30d  / NULLIF(p.spot_co, 0)             AS oi_weighted_all_0_30d_div_spot_co,
    p.oi_weighted_call_0_30d / NULLIF(p.spot_pc, 0)             AS oi_weighted_call_0_30d_div_spot_pc,
    p.oi_weighted_call_0_30d / NULLIF(p.spot_co, 0)             AS oi_weighted_call_0_30d_div_spot_co,
    p.oi_weighted_put_0_30d  / NULLIF(p.spot_pc, 0)             AS oi_weighted_put_0_30d_div_spot_pc,
    p.oi_weighted_put_0_30d  / NULLIF(p.spot_co, 0)             AS oi_weighted_put_0_30d_div_spot_co,
    p.oi_weighted_all_31_90d, p.oi_weighted_call_31_90d, p.oi_weighted_put_31_90d,
    p.oi_weighted_all_31_90d  / NULLIF(p.spot_pc, 0)            AS oi_weighted_all_31_90d_div_spot_pc,
    p.oi_weighted_all_31_90d  / NULLIF(p.spot_co, 0)            AS oi_weighted_all_31_90d_div_spot_co,
    p.oi_weighted_call_31_90d / NULLIF(p.spot_pc, 0)            AS oi_weighted_call_31_90d_div_spot_pc,
    p.oi_weighted_call_31_90d / NULLIF(p.spot_co, 0)            AS oi_weighted_call_31_90d_div_spot_co,
    p.oi_weighted_put_31_90d  / NULLIF(p.spot_pc, 0)            AS oi_weighted_put_31_90d_div_spot_pc,
    p.oi_weighted_put_31_90d  / NULLIF(p.spot_co, 0)            AS oi_weighted_put_31_90d_div_spot_co,
    ol.d1_total_oi_change,
    ol.d5_total_oi_change,
    ol.d20_total_oi_change,
    -- pct features (denominator = total_oi)
    p.oi_within_5pct_pc::DOUBLE  / NULLIF(p.total_oi, 0)        AS pct_oi_within_5pct_pc,
    p.oi_within_5pct_co::DOUBLE  / NULLIF(p.total_oi, 0)        AS pct_oi_within_5pct_co,
    p.oi_within_10pct_pc::DOUBLE / NULLIF(p.total_oi, 0)        AS pct_oi_within_10pct_pc,
    p.oi_within_10pct_co::DOUBLE / NULLIF(p.total_oi, 0)        AS pct_oi_within_10pct_co,
    p.oi_above_spot_pc::DOUBLE / NULLIF(p.total_oi, 0)          AS pct_oi_above_spot_pc,
    p.oi_above_spot_co::DOUBLE / NULLIF(p.total_oi, 0)          AS pct_oi_above_spot_co,
    p.oi_below_spot_pc::DOUBLE / NULLIF(p.total_oi, 0)          AS pct_oi_below_spot_pc,
    p.oi_below_spot_co::DOUBLE / NULLIF(p.total_oi, 0)          AS pct_oi_below_spot_co,
    ts.top5_oi::DOUBLE  / NULLIF(p.total_oi, 0)                 AS top5_strikes_pct_total_oi,
    ts.top10_oi::DOUBLE / NULLIF(p.total_oi, 0)                 AS top10_strikes_pct_total_oi,
    p.weighted_avg_dte,
    p.oi_0_30::DOUBLE   / NULLIF(p.total_oi, 0)                 AS pct_oi_0_30d,
    p.oi_31_90::DOUBLE  / NULLIF(p.total_oi, 0)                 AS pct_oi_31_90d,
    p.oi_91_365::DOUBLE / NULLIF(p.total_oi, 0)                 AS pct_oi_91_365d,
    nm.nm_oi::DOUBLE / NULLIF(p.total_oi, 0)                    AS pct_oi_next_monthly,
    nm.nm_strike_oi::DOUBLE
        / NULLIF(nm.nm_oi, 0)
        / NULLIF(p.spot_pc, 0)                                  AS oi_weighted_next_monthly_div_spot_pc,
    nm.nm_strike_oi::DOUBLE
        / NULLIF(nm.nm_oi, 0)
        / NULLIF(p.spot_co, 0)                                  AS oi_weighted_next_monthly_div_spot_co,
    -- pct changes / derived ratio changes / z-scores
    ol.d1_total_oi_pct_change,
    ol.d5_total_oi_pct_change,
    z.d1_d5_ratio_total_oi_pct_change,
    ol.d1_oi_weighted_all_div_spot_change_pc,
    ol.d1_oi_weighted_all_div_spot_change_co,
    ol.d5_oi_weighted_all_div_spot_change_pc,
    ol.d5_oi_weighted_all_div_spot_change_co,
    ol.d1_put_call_oi_ratio_change,
    ol.d5_put_call_oi_ratio_change,
    z.zscore_d1_oi_change_3m,
    z.zscore_d5_oi_change_3m,
    z.zscore_oi_weighted_all_div_spot_3m_pc,
    z.zscore_oi_weighted_all_div_spot_3m_co,
    z.zscore_put_call_oi_ratio_3m,
    z.zscore_oi_above_below_ratio_3m_pc,
    z.zscore_oi_above_below_ratio_3m_co
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
# OHLC-derived features (rv, fwd oc/cc returns) — DuckDB on the unified ohlc DF.
# Input: ohlc pandas DF [trade_date, open, high, low, close, volume]
#
# Timing convention (lookahead-free): every OHLC signal in row T uses only
# data through C_{T-1} (prior session's close). The named windows all end at
# 1 PRECEDING so the most recent close included is always C_{T-1}, never C_T.
# prev_close = LAG(close) is the shared price anchor for derived ratios.
#
# Two forward-return series, same exit closes, different entry anchors:
#   _oc  entry = open of trade_date (O_T). OI is published overnight and visible
#        when the market opens, so O_T is the realistic entry price.
#   _cc  entry = prior close C_{T-1}. Pair with _oc to isolate the overnight
#        gap from the intraday component. NOT named _pc — the _pc/_co suffix
#        convention elsewhere means spot-reference, not entry-price anchor.
# ---------------------------------------------------------------------------
OHLC_FEATURES_SQL = """
WITH base AS (
    SELECT
        trade_date, open, high, low, close, volume,
        LN(
            NULLIF(close, 0) /
            NULLIF(LAG(close) OVER (ORDER BY trade_date), 0)
        ) AS log_ret,
        GREATEST(
            high - low,
            ABS(high - LAG(close) OVER (ORDER BY trade_date)),
            ABS(low  - LAG(close) OVER (ORDER BY trade_date))
        ) AS true_range,
        LAG(close) OVER (ORDER BY trade_date)                       AS prev_close
    FROM ohlc
),
windowed AS (
    SELECT
        trade_date, open, close, prev_close, volume, log_ret, true_range,
        -- Realized vol (sessions T-5..T-1 and T-20..T-1 — lookahead-free)
        STDDEV_SAMP(log_ret) OVER w5  * SQRT(252)                  AS rv_5d,
        STDDEV_SAMP(log_ret) OVER w20 * SQRT(252)                  AS rv_20d,
        -- Moving averages (windows end at T-1)
        AVG(close) OVER w20                                         AS ma20,
        AVG(close) OVER w50                                         AS ma50,
        -- 52-week extremes (window ends at T-1)
        MAX(close) OVER w252                                        AS hi52,
        MIN(close) OVER w252                                        AS lo52,
        -- Donchian channel components (window ends at T-1)
        MAX(high)  OVER w20                                         AS hi20,
        MIN(low)   OVER w20                                         AS lo20,
        -- Average true range (14 sessions ending at T-1)
        AVG(true_range) OVER w14                                    AS atr_14d,
        -- Backward returns (entry C_{T-6}/C_{T-11}/C_{T-21}, exit C_{T-1})
        prev_close / NULLIF(LAG(close,  6) OVER w_t, 0) - 1        AS ret_5d,
        prev_close / NULLIF(LAG(close, 11) OVER w_t, 0) - 1        AS ret_10d,
        prev_close / NULLIF(LAG(close, 21) OVER w_t, 0) - 1        AS ret_20d,
        -- Forward returns — _oc: entry = open of trade_date (O_T)
        close                      / NULLIF(open, 0) - 1           AS ret_1d_fwd_oc,
        LEAD(close,  2) OVER w_t   / NULLIF(open, 0) - 1           AS ret_3d_fwd_oc,
        LEAD(close,  4) OVER w_t   / NULLIF(open, 0) - 1           AS ret_5d_fwd_oc,
        LEAD(close,  6) OVER w_t   / NULLIF(open, 0) - 1           AS ret_7d_fwd_oc,
        LEAD(close,  9) OVER w_t   / NULLIF(open, 0) - 1           AS ret_10d_fwd_oc,
        LEAD(close, 19) OVER w_t   / NULLIF(open, 0) - 1           AS ret_20d_fwd_oc,
        -- Forward returns — _cc: entry = prior close C_{T-1} (same exit closes)
        close                        / NULLIF(LAG(close, 1) OVER w_t, 0) - 1   AS ret_1d_fwd_cc,
        LEAD(close,  2) OVER w_t     / NULLIF(LAG(close, 1) OVER w_t, 0) - 1   AS ret_3d_fwd_cc,
        LEAD(close,  4) OVER w_t     / NULLIF(LAG(close, 1) OVER w_t, 0) - 1   AS ret_5d_fwd_cc,
        LEAD(close,  6) OVER w_t     / NULLIF(LAG(close, 1) OVER w_t, 0) - 1   AS ret_7d_fwd_cc,
        LEAD(close,  9) OVER w_t     / NULLIF(LAG(close, 1) OVER w_t, 0) - 1   AS ret_10d_fwd_cc,
        LEAD(close, 19) OVER w_t     / NULLIF(LAG(close, 1) OVER w_t, 0) - 1   AS ret_20d_fwd_cc,
        -- Up-day frequency (20 sessions ending at T-1)
        AVG(CASE WHEN log_ret > 0 THEN 1.0 ELSE 0.0 END) OVER w20  AS pct_up_days_20d,
        -- Volume-weighted directional indicator (OBV-style, normalized; T-1..T-20)
        SUM(SIGN(log_ret) * volume::DOUBLE) OVER w20
            / NULLIF(SUM(volume::DOUBLE) OVER w20, 0)               AS cum_signed_vol_20d
    FROM base
    WINDOW
        w_t  AS (ORDER BY trade_date),
        w5   AS (ORDER BY trade_date ROWS BETWEEN   5 PRECEDING AND 1 PRECEDING),
        w14  AS (ORDER BY trade_date ROWS BETWEEN  14 PRECEDING AND 1 PRECEDING),
        w20  AS (ORDER BY trade_date ROWS BETWEEN  20 PRECEDING AND 1 PRECEDING),
        w50  AS (ORDER BY trade_date ROWS BETWEEN  50 PRECEDING AND 1 PRECEDING),
        w252 AS (ORDER BY trade_date ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING)
),
derived AS (
    SELECT
        *,
        -- All ratios use prev_close = C_{T-1} so no session-T data is required.
        prev_close / NULLIF(ma20, 0) - 1                            AS pct_from_ma20,
        prev_close / NULLIF(ma50, 0) - 1                            AS pct_from_ma50,
        prev_close / NULLIF(hi52, 0) - 1                            AS pct_from_52w_high,
        prev_close / NULLIF(lo52, 0) - 1                            AS pct_from_52w_low,
        (prev_close - lo20) / NULLIF(hi20 - lo20, 0)                AS donchian_pos_20d,
        ma20 / NULLIF(LAG(ma20, 5) OVER (ORDER BY trade_date), 0) - 1 AS ma20_slope_5d,
        rv_5d / NULLIF(rv_20d, 0)                                   AS rv_ratio_5d_20d,
        -- atr_14d / prev_close normalises ATR from dollar-units to a fraction of
        -- prior-session price, making the ratio dimensionless and cross-ticker-
        -- comparable. Both numerator (ret_5d: C_{T-1}/C_{T-6}-1) and denominator
        -- (atr_14d / prev_close) share prev_close = C_{T-1} as their price anchor,
        -- so the ratio is split-safe and lookahead-free.
        ret_5d / NULLIF(atr_14d / NULLIF(prev_close, 0), 0)         AS atr_normalized_ret_5d
    FROM windowed
),
zscores AS (
    SELECT
        trade_date,
        CASE WHEN COUNT(pct_from_ma20) OVER w60 >= 60
             THEN (pct_from_ma20 - AVG(pct_from_ma20) OVER w60)
                  / NULLIF(STDDEV_SAMP(pct_from_ma20) OVER w60, 0)
             ELSE NULL END                                          AS zscore_price_vs_ma20,
        CASE WHEN COUNT(pct_from_ma50) OVER w60 >= 60
             THEN (pct_from_ma50 - AVG(pct_from_ma50) OVER w60)
                  / NULLIF(STDDEV_SAMP(pct_from_ma50) OVER w60, 0)
             ELSE NULL END                                          AS zscore_price_vs_ma50,
        CASE WHEN COUNT(rv_20d) OVER w60 >= 60
             THEN (rv_20d - AVG(rv_20d) OVER w60)
                  / NULLIF(STDDEV_SAMP(rv_20d) OVER w60, 0)
             ELSE NULL END                                          AS zscore_underlying_vol_20d
    FROM derived
    WINDOW w60 AS (ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
)
SELECT
    d.trade_date,
    d.rv_5d,
    d.rv_20d,
    d.ret_1d_fwd_oc, d.ret_3d_fwd_oc,  d.ret_5d_fwd_oc,
    d.ret_7d_fwd_oc, d.ret_10d_fwd_oc, d.ret_20d_fwd_oc,
    d.ret_1d_fwd_cc, d.ret_3d_fwd_cc,  d.ret_5d_fwd_cc,
    d.ret_7d_fwd_cc, d.ret_10d_fwd_cc, d.ret_20d_fwd_cc,
    d.ret_5d,
    d.ret_10d,
    d.ret_20d,
    d.pct_from_ma20,
    d.pct_from_ma50,
    d.pct_from_52w_high,
    d.pct_from_52w_low,
    d.donchian_pos_20d,
    d.ma20_slope_5d,
    d.pct_up_days_20d,
    d.rv_ratio_5d_20d,
    d.cum_signed_vol_20d,
    d.atr_normalized_ret_5d,
    z.zscore_price_vs_ma20,
    z.zscore_price_vs_ma50,
    z.zscore_underlying_vol_20d
FROM derived d
JOIN zscores z USING (trade_date)
"""


# ---------------------------------------------------------------------------
# Bucket 2: Option volume EOD features (refactored to read raw chain)
# Inputs registered by the caller:
#   chain_adj   DuckDB view over data/chain_eod/{ticker}/*.parquet with
#               strike already multiplied by split_factors.adj_factor.
#               Columns: trade_date, source_session, feature_date, expiration,
#                        strike (adjusted), option_type, volume, implied_vol,
#                        delta, iv_error.
#               trade_date = actual session the data is from (T-1 in pipeline
#                            terms); feature_date = next_trading_day(trade_date)
#                            = the daily_features trade_date (T) this row
#                            contributes to.
#   ohlc        pandas DF with [trade_date, open, high, low, close, volume]
#               for the underlying (already registered for OHLC features).
#               JOIN on ohlc.trade_date = chain_adj.trade_date gives spot_pc
#               (the close of the actual session whose chain data this is).
#   base_feats  merged oi+ohlc DF (used for vol_oi_ratio_* and net_new_oi_div_vol
#               which need OI quantities at the consumer date T).
#               JOIN on base_feats.trade_date = chain_adj.feature_date.
# ---------------------------------------------------------------------------
VOL_FEATURES_SQL = """
WITH chain AS (
    SELECT c.trade_date,
           c.feature_date,
           c.expiration,
           c.strike,
           c.option_type,
           c.volume,
           o.close AS spot,
           (c.expiration - c.trade_date)::INTEGER AS dte
    FROM chain_adj c
    JOIN ohlc o ON o.trade_date = c.trade_date
),
per_day AS (
    -- All vol aggregates that used to live in fetch_volume_eod.py:_aggregate.
    SELECT feature_date AS trade_date,
           SUM(CASE WHEN option_type = 'C' THEN volume ELSE 0 END)::BIGINT  AS total_call_vol,
           SUM(CASE WHEN option_type = 'P' THEN volume ELSE 0 END)::BIGINT  AS total_put_vol,
           SUM(volume)::BIGINT                                              AS total_vol,
           SUM(CASE WHEN dte BETWEEN 0  AND 30  THEN volume ELSE 0 END)::BIGINT AS vol_0_30d,
           SUM(CASE WHEN dte BETWEEN 31 AND 90  THEN volume ELSE 0 END)::BIGINT AS vol_31_90d,
           SUM(CASE WHEN option_type = 'C' THEN strike * volume ELSE 0 END)::DOUBLE
               / NULLIF(SUM(CASE WHEN option_type = 'C' THEN volume ELSE 0 END), 0) AS vol_weighted_strike_call,
           SUM(CASE WHEN option_type = 'P' THEN strike * volume ELSE 0 END)::DOUBLE
               / NULLIF(SUM(CASE WHEN option_type = 'P' THEN volume ELSE 0 END), 0) AS vol_weighted_strike_put,
           SUM(strike * volume)::DOUBLE / NULLIF(SUM(volume), 0)             AS vol_weighted_strike_all,
           SUM(CASE WHEN strike > spot THEN volume ELSE 0 END)::BIGINT       AS vol_above_spot,
           SUM(CASE WHEN strike < spot THEN volume ELSE 0 END)::BIGINT       AS vol_below_spot,
           SUM(CASE WHEN ABS(strike / spot - 1) <= 0.05 THEN volume ELSE 0 END)::BIGINT AS vol_within_5pct,
           SUM(CASE WHEN ABS(strike / spot - 1) <= 0.10 THEN volume ELSE 0 END)::BIGINT AS vol_within_10pct,
           SUM(dte * volume)::DOUBLE / NULLIF(SUM(volume), 0)                AS weighted_avg_dte_vol,
           ANY_VALUE(spot)                                                   AS spot_pc
    FROM chain
    GROUP BY feature_date
),
joined AS (
    -- Pull OI quantities at the consumer date T for the vol/OI cross-metrics.
    SELECT p.*, f.total_oi, f.call_oi, f.put_oi, f.d1_total_oi_change
    FROM per_day p
    LEFT JOIN base_feats f ON p.trade_date = f.trade_date
),
ratios AS (
    SELECT
        trade_date,
        total_put_vol::DOUBLE  / NULLIF(total_call_vol, 0)     AS put_call_ratio_vol,
        total_vol::DOUBLE      / NULLIF(total_oi, 0)           AS vol_oi_ratio_all,
        total_call_vol::DOUBLE / NULLIF(call_oi, 0)            AS vol_oi_ratio_call,
        total_put_vol::DOUBLE  / NULLIF(put_oi, 0)             AS vol_oi_ratio_put,
        -- vol_weighted_*_div_spot_pc: divides by spot_pc (= close of T-1 = the
        -- close of chain_adj.trade_date), consistent with the strike-vs-spot
        -- comparison above that produced vol_above_spot / vol_within_*.
        vol_weighted_strike_call / NULLIF(spot_pc, 0)          AS vol_weighted_call_div_spot_pc,
        vol_weighted_strike_put  / NULLIF(spot_pc, 0)          AS vol_weighted_put_div_spot_pc,
        vol_weighted_strike_all  / NULLIF(spot_pc, 0)          AS vol_weighted_all_div_spot_pc,
        vol_above_spot::DOUBLE / NULLIF(vol_below_spot, 0)     AS vol_above_below_ratio_pc,
        vol_within_5pct::DOUBLE  / NULLIF(total_vol, 0)        AS pct_vol_within_5pct_pc,
        vol_within_10pct::DOUBLE / NULLIF(total_vol, 0)        AS pct_vol_within_10pct_pc,
        vol_0_30d::DOUBLE  / NULLIF(total_vol, 0)              AS pct_vol_0_30d,
        vol_31_90d::DOUBLE / NULLIF(total_vol, 0)              AS pct_vol_31_90d,
        d1_total_oi_change::DOUBLE / NULLIF(total_vol, 0)      AS net_new_oi_div_vol,
        weighted_avg_dte_vol
    FROM joined
),
zscores AS (
    SELECT
        trade_date,
        CASE WHEN COUNT(put_call_ratio_vol) OVER w60 >= 60
             THEN (put_call_ratio_vol - AVG(put_call_ratio_vol) OVER w60)
                  / NULLIF(STDDEV_SAMP(put_call_ratio_vol) OVER w60, 0)
             ELSE NULL END                                      AS zscore_put_call_ratio_vol,
        CASE WHEN COUNT(vol_oi_ratio_all) OVER w60 >= 60
             THEN (vol_oi_ratio_all - AVG(vol_oi_ratio_all) OVER w60)
                  / NULLIF(STDDEV_SAMP(vol_oi_ratio_all) OVER w60, 0)
             ELSE NULL END                                      AS zscore_vol_oi_ratio_all,
        CASE WHEN COUNT(vol_oi_ratio_call) OVER w60 >= 60
             THEN (vol_oi_ratio_call - AVG(vol_oi_ratio_call) OVER w60)
                  / NULLIF(STDDEV_SAMP(vol_oi_ratio_call) OVER w60, 0)
             ELSE NULL END                                      AS zscore_vol_oi_ratio_call,
        CASE WHEN COUNT(vol_oi_ratio_put) OVER w60 >= 60
             THEN (vol_oi_ratio_put - AVG(vol_oi_ratio_put) OVER w60)
                  / NULLIF(STDDEV_SAMP(vol_oi_ratio_put) OVER w60, 0)
             ELSE NULL END                                      AS zscore_vol_oi_ratio_put,
        CASE WHEN COUNT(vol_above_below_ratio_pc) OVER w60 >= 60
             THEN (vol_above_below_ratio_pc - AVG(vol_above_below_ratio_pc) OVER w60)
                  / NULLIF(STDDEV_SAMP(vol_above_below_ratio_pc) OVER w60, 0)
             ELSE NULL END                                      AS zscore_vol_above_below_ratio_pc
    FROM ratios
    WINDOW w60 AS (ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
)
SELECT
    r.trade_date,
    r.put_call_ratio_vol,
    r.vol_oi_ratio_all, r.vol_oi_ratio_call, r.vol_oi_ratio_put,
    r.vol_weighted_call_div_spot_pc, r.vol_weighted_put_div_spot_pc,
    r.vol_weighted_all_div_spot_pc,
    r.vol_above_below_ratio_pc,
    r.pct_vol_within_5pct_pc, r.pct_vol_within_10pct_pc,
    r.pct_vol_0_30d, r.pct_vol_31_90d,
    r.net_new_oi_div_vol,
    r.weighted_avg_dte_vol,
    z.zscore_put_call_ratio_vol,
    z.zscore_vol_oi_ratio_all, z.zscore_vol_oi_ratio_call, z.zscore_vol_oi_ratio_put,
    z.zscore_vol_above_below_ratio_pc
FROM ratios r
JOIN zscores z USING (trade_date)
"""

# ---------------------------------------------------------------------------
# Bucket 3: IV chain features (refactored to read raw chain + interpolate in SQL)
# Inputs registered by the caller:
#   chain_adj   DuckDB view over data/chain_eod/{ticker}/*.parquet with
#               strike already split-adjusted. See VOL_FEATURES_SQL header
#               for full schema notes.
#   ohlc        pandas DF (already registered for OHLC features). spot_pc =
#               ohlc.close at chain_adj.trade_date.
#   base_feats  merged oi+ohlc DF — used here only for rv_20d (needed for
#               vrp_30d and iv_rv_ratio_30d). JOIN on base_feats.trade_date
#               = chain_adj.feature_date.
#
# ATM interpolation: bracket-and-interpolate. For each (feature_date, expiration)
# find the two CALL strikes bracketing spot_pc, linearly interpolate IV at
# strike=spot. Then for each (feature_date, target_dte in {7,30,90}) bracket
# expirations around feature_date's target DTE and linearly interpolate.
#
# No boundary fallback: if either strikes or expirations don't bracket the
# target, the metric is NULL. With split adjustment correct, bracketing
# should rarely fail; failures indicate genuinely sparse / questionable data.
# This is a deliberate change from the legacy Python (_atm_iv_for_expiration)
# which returned the nearest-strike IV — that fallback masked the split bug.
#
# iv_25d_* / rr / bf / skew columns remain NULL pending a future 15:45
# endpoint migration (see test_iv_endpoint.py); EOD greeks are not trusted
# in the wings.
# ---------------------------------------------------------------------------
IV_FEATURES_SQL = """
WITH calls AS (
    -- Calls only (matches legacy _compute_day_metrics:
    -- calls = day_df[day_df['option_type'] == 'C']).
    -- Filter on implied_vol > 0 only; iv_error filter is deferred per
    -- initial-backfill decision — apply at this CTE later once threshold
    -- is selected from the empirical distribution.
    SELECT trade_date, feature_date, expiration, strike, implied_vol
    FROM chain_adj
    WHERE option_type = 'C'
      AND implied_vol > 0
),
calls_with_spot AS (
    SELECT c.feature_date,
           c.expiration,
           c.strike,
           c.implied_vol,
           o.close AS spot,
           (c.expiration - c.trade_date)::INTEGER AS exp_dte
    FROM calls c
    JOIN ohlc o ON o.trade_date = c.trade_date
),
strike_brackets AS (
    -- Per (feature_date, expiration): largest strike <= spot and smallest > spot.
    SELECT feature_date,
           expiration,
           ANY_VALUE(spot)    AS spot,
           ANY_VALUE(exp_dte) AS exp_dte,
           MAX(strike) FILTER (WHERE strike <= spot) AS s_low,
           MIN(strike) FILTER (WHERE strike >  spot) AS s_high
    FROM calls_with_spot
    GROUP BY feature_date, expiration
),
strike_brackets_iv AS (
    -- Pull the IVs at the two bracketing strikes.
    SELECT sb.feature_date, sb.expiration, sb.spot, sb.exp_dte,
           sb.s_low, sb.s_high,
           cl.implied_vol AS iv_low,
           ch.implied_vol AS iv_high
    FROM strike_brackets sb
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
    -- Linear interpolation in strike at x = spot. NULL when not bracketed.
    SELECT feature_date, expiration, exp_dte, spot,
           CASE
             WHEN s_low IS NULL OR s_high IS NULL                       THEN NULL
             WHEN s_low = s_high                                        THEN iv_low
             ELSE iv_low + (iv_high - iv_low) * (spot - s_low) / (s_high - s_low)
           END AS atm_iv
    FROM strike_brackets_iv
),
targets AS (
    SELECT * FROM (VALUES (7), (30), (90)) AS t(target_dte)
),
exp_brackets AS (
    -- Per (feature_date, target_dte): largest exp_dte <= target and smallest > target,
    -- restricted to expirations that produced a valid atm_iv.
    SELECT a.feature_date, t.target_dte,
           MAX(a.exp_dte) FILTER (WHERE a.exp_dte <= t.target_dte) AS d_low,
           MIN(a.exp_dte) FILTER (WHERE a.exp_dte >  t.target_dte) AS d_high
    FROM atm_per_exp a CROSS JOIN targets t
    WHERE a.atm_iv IS NOT NULL
    GROUP BY a.feature_date, t.target_dte
),
exp_brackets_iv AS (
    SELECT eb.feature_date, eb.target_dte, eb.d_low, eb.d_high,
           al.atm_iv AS iv_low,
           ah.atm_iv AS iv_high
    FROM exp_brackets eb
    LEFT JOIN atm_per_exp al
        ON al.feature_date = eb.feature_date AND al.exp_dte = eb.d_low
    LEFT JOIN atm_per_exp ah
        ON ah.feature_date = eb.feature_date AND ah.exp_dte = eb.d_high
),
atm_by_dte AS (
    -- Linear interpolation in DTE at x = target_dte. NULL when not bracketed.
    SELECT feature_date, target_dte,
           CASE
             WHEN d_low IS NULL OR d_high IS NULL                       THEN NULL
             WHEN d_low = d_high                                        THEN iv_low
             ELSE iv_low + (iv_high - iv_low) * (target_dte - d_low) / (d_high - d_low)
           END AS atm_iv_value
    FROM exp_brackets_iv
),
atm_pivoted AS (
    SELECT feature_date AS trade_date,
           MAX(atm_iv_value) FILTER (WHERE target_dte =  7) AS atm_iv_7d,
           MAX(atm_iv_value) FILTER (WHERE target_dte = 30) AS atm_iv_30d,
           MAX(atm_iv_value) FILTER (WHERE target_dte = 90) AS atm_iv_90d
    FROM atm_by_dte
    GROUP BY feature_date
),
iv_joined AS (
    -- Match the column shape the downstream `derived` CTE expects.
    SELECT
        a.trade_date,
        a.atm_iv_7d,
        a.atm_iv_30d,
        a.atm_iv_90d,
        CAST(NULL AS DOUBLE) AS iv_25d_call_30d,
        CAST(NULL AS DOUBLE) AS iv_25d_put_30d,
        f.rv_20d
    FROM atm_pivoted a
    LEFT JOIN base_feats f ON a.trade_date = f.trade_date
),
derived AS (
    SELECT
        trade_date,
        atm_iv_7d,
        atm_iv_30d,
        atm_iv_90d,
        iv_25d_call_30d,
        iv_25d_put_30d,
        rv_20d,
        iv_25d_call_30d - iv_25d_put_30d                                AS rr_25d_30d,
        0.5 * (iv_25d_call_30d + iv_25d_put_30d) - atm_iv_30d          AS bf_25d_30d,
        iv_25d_put_30d - atm_iv_30d                                     AS skew_25p_atm_30d,
        atm_iv_30d - iv_25d_call_30d                                    AS skew_atm_25c_30d,
        atm_iv_7d  - atm_iv_30d                                         AS term_7d_30d,
        atm_iv_30d - atm_iv_90d                                         AS term_30d_90d,
        atm_iv_30d - rv_20d                                             AS vrp_30d,
        atm_iv_30d / NULLIF(rv_20d, 0)                                  AS iv_rv_ratio_30d,
        atm_iv_7d  - LAG(atm_iv_7d,  1) OVER (ORDER BY trade_date)      AS d1_atm_iv_7d_change,
        atm_iv_7d  - LAG(atm_iv_7d,  5) OVER (ORDER BY trade_date)      AS d5_atm_iv_7d_change,
        atm_iv_30d - LAG(atm_iv_30d, 1) OVER (ORDER BY trade_date)      AS d1_atm_iv_30d_change,
        atm_iv_30d - LAG(atm_iv_30d, 5) OVER (ORDER BY trade_date)      AS d5_atm_iv_30d_change
    FROM iv_joined
),
zscores AS (
    SELECT
        trade_date,
        CASE WHEN COUNT(atm_iv_7d) OVER w60 >= 60
             THEN (atm_iv_7d - AVG(atm_iv_7d) OVER w60)
                  / NULLIF(STDDEV_SAMP(atm_iv_7d) OVER w60, 0)
             ELSE NULL END                                              AS zscore_iv_7d,
        CASE WHEN COUNT(atm_iv_30d) OVER w60 >= 60
             THEN (atm_iv_30d - AVG(atm_iv_30d) OVER w60)
                  / NULLIF(STDDEV_SAMP(atm_iv_30d) OVER w60, 0)
             ELSE NULL END                                              AS zscore_iv_30d,
        CASE WHEN COUNT(atm_iv_90d) OVER w60 >= 60
             THEN (atm_iv_90d - AVG(atm_iv_90d) OVER w60)
                  / NULLIF(STDDEV_SAMP(atm_iv_90d) OVER w60, 0)
             ELSE NULL END                                              AS zscore_iv_90d,
        CASE WHEN COUNT(rr_25d_30d) OVER w60 >= 60
             THEN (rr_25d_30d - AVG(rr_25d_30d) OVER w60)
                  / NULLIF(STDDEV_SAMP(rr_25d_30d) OVER w60, 0)
             ELSE NULL END                                              AS zscore_rr_25d_30d,
        CASE WHEN COUNT(term_7d_30d) OVER w60 >= 60
             THEN (term_7d_30d - AVG(term_7d_30d) OVER w60)
                  / NULLIF(STDDEV_SAMP(term_7d_30d) OVER w60, 0)
             ELSE NULL END                                              AS zscore_term_7d_30d,
        CASE WHEN COUNT(term_30d_90d) OVER w60 >= 60
             THEN (term_30d_90d - AVG(term_30d_90d) OVER w60)
                  / NULLIF(STDDEV_SAMP(term_30d_90d) OVER w60, 0)
             ELSE NULL END                                              AS zscore_term_30d_90d,
        CASE WHEN COUNT(vrp_30d) OVER w60 >= 60
             THEN (vrp_30d - AVG(vrp_30d) OVER w60)
                  / NULLIF(STDDEV_SAMP(vrp_30d) OVER w60, 0)
             ELSE NULL END                                              AS zscore_vrp_30d,
        CASE WHEN COUNT(iv_rv_ratio_30d) OVER w60 >= 60
             THEN (iv_rv_ratio_30d - AVG(iv_rv_ratio_30d) OVER w60)
                  / NULLIF(STDDEV_SAMP(iv_rv_ratio_30d) OVER w60, 0)
             ELSE NULL END                                              AS zscore_iv_rv_ratio_30d
    FROM derived
    WINDOW w60 AS (ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
)
SELECT
    d.trade_date,
    d.atm_iv_7d, d.atm_iv_30d, d.atm_iv_90d,
    d.iv_25d_call_30d, d.iv_25d_put_30d,
    d.rr_25d_30d, d.bf_25d_30d,
    d.skew_25p_atm_30d, d.skew_atm_25c_30d,
    d.term_7d_30d, d.term_30d_90d,
    d.vrp_30d, d.iv_rv_ratio_30d,
    d.d1_atm_iv_7d_change, d.d5_atm_iv_7d_change,
    d.d1_atm_iv_30d_change, d.d5_atm_iv_30d_change,
    z.zscore_iv_7d, z.zscore_iv_30d, z.zscore_iv_90d,
    z.zscore_rr_25d_30d,
    z.zscore_term_7d_30d, z.zscore_term_30d_90d,
    z.zscore_vrp_30d, z.zscore_iv_rv_ratio_30d
FROM derived d
JOIN zscores z USING (trade_date)
"""


# ---------------------------------------------------------------------------
# Postgres write — two-cron column partition
# ---------------------------------------------------------------------------
# Two crons write disjoint column sets to the same (ticker, trade_date) row
# using INSERT … ON CONFLICT DO UPDATE SET.  Each cron's DO UPDATE SET clause
# lists ONLY that cron's own columns, so neither cron can wipe the other's data.
#
# MORNING_COLS  — OI-tier.  Written by the morning cron (~7am on T) after
#                 ThetaData publishes OI for the upcoming session.  Also
#                 includes vol/OI cross-metrics (vol_oi_ratio_*, net_new_oi_div_vol)
#                 because they require T's OI, which is not available at the
#                 evening run.  Vol quantities for these come from the chain_adj
#                 view over data/chain_eod/ (filtered to feature_date=T).
#
# EVENING_COLS  — Non-OI-tier.  Written by the evening cron (~5:30pm on T-1)
#                 after the prior session's close.  Includes OHLC-derived
#                 features, all IV metrics, and vol metrics that do not need
#                 today's OI (pure vol ratios, spot-referenced vol strikes, etc.).
#
# HARD RULE: never add DELETE before either upsert.  A DELETE+INSERT would
# wipe whichever cron's columns were written first.  See F1/F2/F6 design notes.
# ---------------------------------------------------------------------------

def _make_upsert_sql(cols: list) -> str:
    """
    Build an execute_values-compatible upsert for the given column list.
    cols must start with ["ticker", "trade_date"] (the conflict key);
    those two are excluded from the DO UPDATE SET clause.
    """
    data_cols = [c for c in cols if c not in ("ticker", "trade_date")]
    set_clause = ",\n        ".join(f"{c} = EXCLUDED.{c}" for c in data_cols)
    return (
        f"INSERT INTO daily_features ({', '.join(cols)}) VALUES %s\n"
        f"ON CONFLICT (ticker, trade_date) DO UPDATE SET\n"
        f"    {set_clause}"
    )


# ---------------------------------------------------------------------------
# MORNING_COLS — OI-tier (morning cron, ~7am on T)
# ---------------------------------------------------------------------------
MORNING_COLS = [
    "ticker", "trade_date",
    "spot_pc", "spot_co",
    "total_oi", "call_oi", "put_oi", "put_call_oi_ratio",
    "max_oi_strike_call", "max_oi_strike_put",
    # OI-weighted strikes (spot-independent)
    "oi_weighted_call", "oi_weighted_put", "oi_weighted_all",
    # minus_spot pc / co
    "oi_weighted_call_minus_spot_pc", "oi_weighted_call_minus_spot_co",
    "oi_weighted_put_minus_spot_pc",  "oi_weighted_put_minus_spot_co",
    "oi_weighted_all_minus_spot_pc",  "oi_weighted_all_minus_spot_co",
    # div_spot pc / co
    "oi_weighted_call_div_spot_pc",   "oi_weighted_call_div_spot_co",
    "oi_weighted_put_div_spot_pc",    "oi_weighted_put_div_spot_co",
    "oi_weighted_all_div_spot_pc",    "oi_weighted_all_div_spot_co",
    # Moneyness-dependent counts (pc / co)
    "oi_within_5pct_pc",  "oi_within_5pct_co",
    "oi_within_10pct_pc", "oi_within_10pct_co",
    "pct_oi_in_front_expiry",
    "oi_above_spot_pc",   "oi_above_spot_co",
    "oi_below_spot_pc",   "oi_below_spot_co",
    "oi_above_below_ratio_pc", "oi_above_below_ratio_co",
    # DTE-bucketed weighted strikes (spot-independent)
    "oi_weighted_all_0_30d",  "oi_weighted_call_0_30d",  "oi_weighted_put_0_30d",
    # DTE-bucketed div_spot pc / co
    "oi_weighted_all_0_30d_div_spot_pc",   "oi_weighted_all_0_30d_div_spot_co",
    "oi_weighted_call_0_30d_div_spot_pc",  "oi_weighted_call_0_30d_div_spot_co",
    "oi_weighted_put_0_30d_div_spot_pc",   "oi_weighted_put_0_30d_div_spot_co",
    "oi_weighted_all_31_90d", "oi_weighted_call_31_90d", "oi_weighted_put_31_90d",
    "oi_weighted_all_31_90d_div_spot_pc",  "oi_weighted_all_31_90d_div_spot_co",
    "oi_weighted_call_31_90d_div_spot_pc", "oi_weighted_call_31_90d_div_spot_co",
    "oi_weighted_put_31_90d_div_spot_pc",  "oi_weighted_put_31_90d_div_spot_co",
    "d1_total_oi_change", "d5_total_oi_change", "d20_total_oi_change",
    # pct features (denominator = total_oi)
    "pct_oi_within_5pct_pc",  "pct_oi_within_5pct_co",
    "pct_oi_within_10pct_pc", "pct_oi_within_10pct_co",
    "pct_oi_above_spot_pc",   "pct_oi_above_spot_co",
    "pct_oi_below_spot_pc",   "pct_oi_below_spot_co",
    "top5_strikes_pct_total_oi", "top10_strikes_pct_total_oi",
    "weighted_avg_dte",
    "pct_oi_0_30d", "pct_oi_31_90d", "pct_oi_91_365d",
    "pct_oi_next_monthly",
    "oi_weighted_next_monthly_div_spot_pc", "oi_weighted_next_monthly_div_spot_co",
    # pct changes / derived-ratio changes / 60-day z-scores (OI)
    "d1_total_oi_pct_change", "d5_total_oi_pct_change",
    "d1_d5_ratio_total_oi_pct_change",
    "d1_oi_weighted_all_div_spot_change_pc", "d1_oi_weighted_all_div_spot_change_co",
    "d5_oi_weighted_all_div_spot_change_pc", "d5_oi_weighted_all_div_spot_change_co",
    "d1_put_call_oi_ratio_change", "d5_put_call_oi_ratio_change",
    "zscore_d1_oi_change_3m", "zscore_d5_oi_change_3m",
    "zscore_oi_weighted_all_div_spot_3m_pc", "zscore_oi_weighted_all_div_spot_3m_co",
    "zscore_put_call_oi_ratio_3m",
    "zscore_oi_above_below_ratio_3m_pc", "zscore_oi_above_below_ratio_3m_co",
    # Vol/OI cross-metrics: require T's OI → morning cron only.
    # Vol quantities are aggregated from the chain_adj view at build time.
    "vol_oi_ratio_all", "vol_oi_ratio_call", "vol_oi_ratio_put",
    "net_new_oi_div_vol",
    "zscore_vol_oi_ratio_all", "zscore_vol_oi_ratio_call", "zscore_vol_oi_ratio_put",
]

# ---------------------------------------------------------------------------
# EVENING_COLS — Non-OI-tier (evening cron, ~5:30pm on T-1)
# ---------------------------------------------------------------------------
EVENING_COLS = [
    "ticker", "trade_date",
    # Bucket 1: OHLC-derived (all use only closes / opens / OHLC available at T-1)
    "rv_5d", "rv_20d",
    # _oc = entry O_T, _cc = entry C_{T-1}; both self-heal as future closes land
    "ret_1d_fwd_oc",  "ret_3d_fwd_oc",  "ret_5d_fwd_oc",
    "ret_7d_fwd_oc",  "ret_10d_fwd_oc", "ret_20d_fwd_oc",
    "ret_1d_fwd_cc",  "ret_3d_fwd_cc",  "ret_5d_fwd_cc",
    "ret_7d_fwd_cc",  "ret_10d_fwd_cc", "ret_20d_fwd_cc",
    "ret_5d", "ret_10d", "ret_20d",
    "pct_from_ma20", "pct_from_ma50",
    "pct_from_52w_high", "pct_from_52w_low",
    "donchian_pos_20d",
    "ma20_slope_5d",
    "pct_up_days_20d",
    "rv_ratio_5d_20d",
    "cum_signed_vol_20d",
    "atr_normalized_ret_5d",
    "zscore_price_vs_ma20", "zscore_price_vs_ma50",
    "zscore_underlying_vol_20d",
    "relative_strength_vs_spy_20d",
    # Bucket 2: option volume EOD (pure vol metrics — no OI denominator)
    "put_call_ratio_vol",
    # _pc suffix: upstream vol_above_spot / vol_within_* used prior_close = spot_pc
    "vol_weighted_call_div_spot_pc", "vol_weighted_put_div_spot_pc",
    "vol_weighted_all_div_spot_pc",
    "vol_above_below_ratio_pc",
    "pct_vol_within_5pct_pc", "pct_vol_within_10pct_pc",
    "pct_vol_0_30d", "pct_vol_31_90d",
    "weighted_avg_dte_vol",
    "zscore_put_call_ratio_vol",
    "zscore_vol_above_below_ratio_pc",
    # Bucket 3: IV chain (15:45 snapshot)
    "atm_iv_7d", "atm_iv_30d", "atm_iv_90d",
    "iv_25d_call_30d", "iv_25d_put_30d",
    "rr_25d_30d", "bf_25d_30d",
    "skew_25p_atm_30d", "skew_atm_25c_30d",
    "term_7d_30d", "term_30d_90d",
    "vrp_30d", "iv_rv_ratio_30d",
    "d1_atm_iv_7d_change", "d5_atm_iv_7d_change",
    "d1_atm_iv_30d_change", "d5_atm_iv_30d_change",
    "zscore_iv_7d", "zscore_iv_30d", "zscore_iv_90d",
    "zscore_rr_25d_30d",
    "zscore_term_7d_30d", "zscore_term_30d_90d",
    "zscore_vrp_30d", "zscore_iv_rv_ratio_30d",
]

MORNING_UPSERT_SQL = _make_upsert_sql(MORNING_COLS)
EVENING_UPSERT_SQL = _make_upsert_sql(EVENING_COLS)


# ---------------------------------------------------------------------------
# Per-ticker pipeline
# ---------------------------------------------------------------------------

def load_ohlc(conn, ticker: str) -> pd.DataFrame:
    """Pull OHLC + volume for one ticker out of underlying_ohlc."""
    df = read_sql_df(
        conn,
        "SELECT trade_date, open, high, low, close, volume FROM underlying_ohlc "
        "WHERE ticker = %(ticker)s ORDER BY trade_date",
        {"ticker": ticker},
    )
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return df


def load_ohlc_spy(conn) -> pd.DataFrame:
    """Return (trade_date, spy_ret_20d) for all available SPY history.

    spy_ret_20d = C_{T-1} / C_{T-21} - 1 (lookahead-free, matching the shifted
    ret_20d convention in OHLC_FEATURES_SQL).  relative_strength_vs_spy_20d is
    then ret_20d(ticker) - spy_ret_20d, both anchored to the same T-1 close.
    """
    df = read_sql_df(
        conn,
        "SELECT trade_date, close FROM underlying_ohlc "
        "WHERE ticker = 'SPY' ORDER BY trade_date",
    )
    if df.empty:
        return df
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df = df.sort_values("trade_date").reset_index(drop=True)
    df["spy_ret_20d"] = df["close"].shift(1) / df["close"].shift(21) - 1
    return df[["trade_date", "spy_ret_20d"]]


def listed_expirations_from_parquet(con: duckdb.DuckDBPyConnection,
                                    ticker: str) -> set:
    rows = con.execute(
        f"SELECT DISTINCT expiration FROM read_parquet('{parquet_glob(ticker)}')"
    ).fetchall()
    return {r[0] for r in rows}


def build_for_ticker(pg_conn, ticker: str,
                     start: date | None = None,
                     end:   date | None = None,
                     tier:  str = "BOTH") -> int:
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

    # prev_close is computed as LAG(close) inside DuckDB later (see registration
    # block below) — doing it there instead of via pandas .shift(1) avoids a
    # round-trip quirk where a pandas-shifted column with NaN at position 0
    # was coming back as all-NULL on the DuckDB side.
    ohlc_full_df = ohlc_full_df.sort_values("trade_date").reset_index(drop=True)

    end_eff = end or date.today()
    if start is not None:
        oi_buffer_start   = start - timedelta(days=LOOKBACK_BUFFER_DAYS)
        ohlc_buffer_start = start - timedelta(days=OHLC_LOOKBACK_BUFFER_DAYS)
        ohlc = ohlc_full_df[
            (ohlc_full_df["trade_date"] >= ohlc_buffer_start)
            & (ohlc_full_df["trade_date"] <= end_eff)
        ].reset_index(drop=True)
        date_filter_sql = (
            f" WHERE trade_date >= DATE '{oi_buffer_start.isoformat()}'"
            f" AND trade_date <= DATE '{end_eff.isoformat()}'"
        )
    else:
        oi_buffer_start   = None
        ohlc_buffer_start = None
        ohlc = ohlc_full_df
        date_filter_sql = ""

    # ------------------------------------------------------------------------
    # Placeholder-anchor row injection (EVENING-first compliance).
    #
    # On the T-1 evening EVENING run, the universal row invariant requires us
    # to write trade_date T's EVENING_COLS from data through T-1's close.
    # Chain-derived EVENING cols (vol, IV from chain_adj) land naturally via
    # the feature_date=T offset stored in the chain parquet — a one-to-one
    # source→consumer mapping.  OHLC's own EVENING cols (rv_*, pct_from_ma*,
    # donchian_pos_20d, ret_5/10/20d, z-scores, etc.) do NOT have a stored
    # feature_date offset because OHLC bars feed many feature_dates via
    # rolling windows (T-1's close participates in T's, T+1's, ... T+19's
    # rv_20d/donchian/returns/z-scores).  The mapping is many-to-many,
    # unlike chain's one-to-one, so a stored relabel is not applicable.
    #
    # The gap is structural: OHLC_FEATURES_SQL is `... FROM ohlc` and only
    # emits an output row at trade_date D if an input row exists at D.  On
    # T-1 evening, no bar at T exists yet (T is the future) — so the window
    # functions emit nothing for T, even though every input value they need
    # (closes/highs/lows through T-1) is fully present.  A relabel cannot
    # create the missing anchor at T; only injection can.
    #
    # Injection rule: add a single placeholder row at trade_date = end_eff
    # with price columns NULL, ONLY when end_eff is exactly the next trading
    # day after the last real OHLC row.  This guarantees the placeholder is
    # the terminal row of the ohlc view, so no other row's rolling-window
    # aggregate can look back through its NULL close.  All EVENING-tier
    # window bounds end at "1 PRECEDING", so the placeholder's own NULL is
    # excluded from its own windows — backward-looking metrics at T compute
    # from real T-1-and-earlier inputs.
    #
    # The placeholder is in-memory only — never persisted.  When T's real
    # bar later lands, the next EVENING run recomputes T's metrics from
    # real data; backward-looking values are byte-identical because their
    # inputs are unchanged.  The diagnostic immediately after OHLC features
    # are computed verifies this idempotency against the actual data.
    placeholder_injected = False
    if (not ohlc.empty
        and end_eff > ohlc["trade_date"].iloc[-1]
        and end_eff == next_trading_day(ohlc["trade_date"].iloc[-1])):
        placeholder = pd.DataFrame([{
            "trade_date": end_eff,
            "open": None, "high": None, "low": None,
            "close": None, "volume": None,
        }])
        ohlc = pd.concat([ohlc, placeholder], ignore_index=True)
        placeholder_injected = True
        log.info("  injected placeholder OHLC row at %s "
                 "(evening-first; price cols NULL, backward EVENING "
                 "metrics from real T-1-and-earlier closes)", end_eff)

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

    # Build per-trade_date strike adjustment factors to normalise pre-split OI
    # strikes to current (post-split) price terms.  ThetaData does not adjust
    # historical strikes for splits, so without this, any metric that divides
    # strike by spot (OI-weighted strikes, moneyness bands, above/below-spot OI)
    # is wrong for all trade_dates before a split event.  The 1-day CBOE
    # publication lag means trade_date == split_date is still pre-split OI, so
    # the boundary is: adjust for trade_date <= split_date.
    splits_df = load_splits(pg_conn, ticker)
    sf_df     = make_split_factors(splits_df, oi_dates)
    if not splits_df.empty:
        log.info("  applying split adjustments for %d event(s): %s",
                 len(splits_df),
                 ", ".join(f"{r.trade_date}×{r.splits}" for r in splits_df.itertuples()))

    # Register the raw OHLC. OI_FEATURES_SQL uses (trade_date, open, close);
    # OHLC_FEATURES_SQL also uses high, low, volume for Bucket 1 metrics;
    # VOL/IV SQL join on ohlc.trade_date = chain_adj.trade_date for spot_pc.
    con.register("ohlc",            ohlc[["trade_date", "open", "high", "low", "close", "volume"]])
    con.register("next_monthly_df", nm_df)
    con.register("split_factors",   sf_df)
    # The oi view applies split-adjustment to strike AND open_interest via a
    # LEFT JOIN on split_factors. Rows with no split event in their history get
    # adj_factor=1.0 (COALESCE guard), so both scalings collapse to no-ops.
    #
    # Count adjustment is the algebraic inverse of strike adjustment:
    #   strike     := raw_strike     * adj_factor       (4:1 → ×0.25)
    #   count      := raw_count      / adj_factor       (4:1 → ×4   )
    # Applied universally to every row. Metrics expressed as ratios or
    # weighted averages (e.g. put_call_oi_ratio, oi_weighted_*, pct_oi_*)
    # cancel the factor between numerator and denominator and are unchanged.
    # Only raw-count metrics (total_oi, call_oi, put_oi, max_oi_strike_*,
    # d{1,5,20}_total_oi_change) become continuous across split boundaries —
    # the intended effect.
    #
    # Pre-split count values are now expressed in current (post-split-
    # equivalent) contract units. The raw parquet retains the as-of-date
    # counts underneath, so original values remain recoverable.
    con.execute(
        f"CREATE OR REPLACE VIEW oi AS "
        f"SELECT raw.trade_date, raw.expiration, "
        f"       raw.strike * COALESCE(sf.adj_factor, 1.0) AS strike, "
        f"       raw.option_type, "
        f"       raw.open_interest / COALESCE(sf.adj_factor, 1.0) AS open_interest "
        f"FROM (SELECT * FROM read_parquet('{parquet_glob(ticker)}'){date_filter_sql}) raw "
        f"LEFT JOIN split_factors sf ON raw.trade_date = sf.trade_date"
    )

    # The chain_adj view applies the same strike + count adjustments (count
    # here is `volume`). trade_date is the actual session (T-1); feature_date
    # is the daily_features consumer date (T). Vol/IV SQL filters by
    # feature_date against the build range. May not exist for tickers not yet
    # backfilled.
    #
    # Scaling OI and volume TOGETHER is required, not optional:
    # vol_oi_ratio_* = volume / OI. Both quantities mechanically multiply at
    # a split; scaling both keeps the ratio continuous via cancellation.
    # Scaling only one would inject a discontinuity into the ratio.
    chain_present = chain_has_data(ticker)
    if chain_present:
        chain_date_filter = (
            f" WHERE feature_date >= DATE '{oi_buffer_start.isoformat()}'"
            f"   AND feature_date <= DATE '{end_eff.isoformat()}'"
            if start is not None else ""
        )
        con.execute(
            f"CREATE OR REPLACE VIEW chain_adj AS "
            f"SELECT raw.trade_date, raw.source_session, raw.feature_date, "
            f"       raw.expiration, "
            f"       raw.strike * COALESCE(sf.adj_factor, 1.0) AS strike, "
            f"       raw.option_type, "
            f"       raw.volume / COALESCE(sf.adj_factor, 1.0) AS volume, "
            f"       raw.implied_vol, raw.delta, raw.iv_error "
            f"FROM (SELECT * FROM read_parquet('{chain_parquet_glob(ticker)}'){chain_date_filter}) raw "
            f"LEFT JOIN split_factors sf ON raw.trade_date = sf.trade_date"
        )

    log.info("  computing OI features ...")
    oi_feats = con.execute(OI_FEATURES_SQL).df()
    log.info("  computing OHLC features ...")
    ohlc_feats = con.execute(OHLC_FEATURES_SQL).df()

    # ------------------------------------------------------------------------
    # DIAGNOSTIC (temporary — remove after one clean production evening run).
    # Confirms the placeholder-idempotency proof against the actual SQL and
    # data: reruns OHLC_FEATURES_SQL on an ohlc view without the placeholder
    # and verifies that the row at the last REAL OHLC date is byte-identical
    # to the with-placeholder version.  If any metric differs, the placeholder
    # is contaminating the last real row's window (e.g. a window bound of
    # CURRENT ROW instead of 1 PRECEDING would silently admit the NULL).
    # Cost: one extra OHLC_FEATURES_SQL pass per ticker per evening run.
    if placeholder_injected:
        last_real_date = ohlc["trade_date"].iloc[-2]
        log.info("  [diagnostic] verifying placeholder idempotency at %s ...",
                 last_real_date)
        scratch = duckdb.connect(database=":memory:")
        scratch.register("ohlc", ohlc.iloc[:-1].reset_index(drop=True))
        baseline = scratch.execute(OHLC_FEATURES_SQL).df()
        scratch.close()
        row_w = ohlc_feats[ohlc_feats["trade_date"] == last_real_date].squeeze()
        row_b = baseline[baseline["trade_date"] == last_real_date].squeeze()
        diffs = []
        for col in row_w.index:
            if col == "trade_date":
                continue
            a, b = row_w[col], row_b[col]
            if pd.isna(a) and pd.isna(b):
                continue
            if a != b:
                diffs.append((col, a, b))
        if diffs:
            log.error("  [diagnostic] FAIL — placeholder contaminates "
                      "%d metric(s) at %s:", len(diffs), last_real_date)
            for col, a, b in diffs:
                log.error("    %s: with=%r  without=%r", col, a, b)
        else:
            log.info("  [diagnostic] OK — placeholder idempotent at %s "
                     "(all %d backward-window metrics byte-identical)",
                     last_real_date, len(row_w) - 1)

    # Per docs/daily_features_data_dictionary.md ("As-of semantics — the universal
    # row invariant" and "Cron / write architecture"): a daily_features row for
    # date T is a composite — its existence must NOT be gated on any single
    # contributing family.  Build the trade_date spine as the UNION across all
    # families, then LEFT-join each family onto it.  This is what allows the
    # EVENING cron to create the T row from T-1's close before OI for T is
    # published, and the MORNING cron to later fill OI into that same row.
    def _dates_of(df: pd.DataFrame) -> set:
        if df.empty:
            return set()
        return set(pd.to_datetime(df["trade_date"]).dt.date.tolist())

    spine_set = _dates_of(oi_feats) | _dates_of(ohlc_feats)

    # chain_adj.feature_date is the EVENING-tier row driver: a chain row from
    # session T-1 has feature_date = T and lands EVENING vol/IV in row T.
    # Without including these, an EVENING run with no OI for T yet would have
    # nothing to attach vol/IV to.
    if chain_present:
        chain_dates_rows = con.execute(
            "SELECT DISTINCT feature_date FROM chain_adj ORDER BY feature_date"
        ).fetchall()
        spine_set |= {r[0] for r in chain_dates_rows}

    if not spine_set:
        log.warning("  no trade_dates from any family for %s — skipping", ticker)
        con.close()
        return 0

    spine = pd.DataFrame({"trade_date": sorted(spine_set)})

    # LEFT-join each family onto the spine.  A date present only in OHLC and
    # chain (e.g. tonight's EVENING build of row T+1 before OI lands) gets OI
    # cols NULL; the next MORNING run's MORNING_UPSERT_SQL fills them.  A date
    # present only in OI (rare backfill case) gets OHLC/vol/IV NULL until those
    # land.
    feats = spine.merge(oi_feats,   on="trade_date", how="left")
    feats = feats.merge(ohlc_feats, on="trade_date", how="left")
    # DuckDB returns DATE columns as datetime64[us]; normalise to Python date
    # so `>= start` (Python date) comparisons work and psycopg2 sees a clean
    # DATE value at INSERT time.
    feats["trade_date"] = pd.to_datetime(feats["trade_date"]).dt.date
    feats.insert(0, "ticker", ticker)

    # Register AFTER the spine is built so VOL/IV SQL joins to base_feats see
    # every spine date.  Where OI cols are NULL, the vol/OI cross-metrics
    # (vol_oi_ratio_*, net_new_oi_div_vol — all MORNING-tier) evaluate to NULL
    # via the division — correct: the next MORNING run fills them.
    con.register("base_feats", feats)

    if chain_present:
        log.info("  computing vol features ...")
        vol_feats = con.execute(VOL_FEATURES_SQL).df()
        vol_feats["trade_date"] = pd.to_datetime(vol_feats["trade_date"]).dt.date
        # LEFT MERGE: missing chain data for a date leaves vol columns NULL
        # rather than dropping the daily_features row.
        feats = feats.merge(vol_feats, on="trade_date", how="left")

        log.info("  computing IV features ...")
        iv_feats = con.execute(IV_FEATURES_SQL).df()
        iv_feats["trade_date"] = pd.to_datetime(iv_feats["trade_date"]).dt.date
        feats = feats.merge(iv_feats, on="trade_date", how="left")
    else:
        log.warning("  no chain_eod parquet for %s — vol/IV columns will be NULL", ticker)

    con.close()

    # SPY relative strength: ret_20d(ticker) - ret_20d(SPY).
    spy_df = load_ohlc_spy(pg_conn)
    if not spy_df.empty:
        if ticker == "SPY":
            feats["relative_strength_vs_spy_20d"] = 0.0
        else:
            feats = feats.merge(spy_df, on="trade_date", how="left")
            feats["relative_strength_vs_spy_20d"] = (
                feats["ret_20d"] - feats["spy_ret_20d"]
            )
            feats = feats.drop(columns=["spy_ret_20d"], errors="ignore")

    # Drop the lookback buffer rows — they were only there for window context.
    if start is not None:
        feats = feats[feats["trade_date"] >= start].reset_index(drop=True)
        if feats.empty:
            log.info("  no rows in [%s, %s] for %s", start, end_eff, ticker)
            return 0

    tier_norm = (tier or "BOTH").strip().upper()
    if tier_norm not in ("MORNING", "EVENING", "BOTH"):
        raise ValueError(f"tier must be MORNING / EVENING / BOTH; got {tier!r}")

    records = feats.to_dict(orient="records")

    with pg_conn.cursor() as cur:
        # CONTRACT: MORNING_UPSERT_SQL updates ONLY MORNING_COLS; EVENING_UPSERT_SQL
        # updates ONLY EVENING_COLS.  Neither upsert touches the other tier's columns.
        # Do NOT add DELETE before either upsert — that would wipe whichever tier's
        # data landed first, breaking the two-cron write contract.
        #
        # tier='MORNING' or 'EVENING' fires ONLY that tier's upsert (used by the
        # two-cron orchestrator in run_pipeline.py).  tier='BOTH' fires both in
        # one transaction and is preserved for ad-hoc / interactive rebuilds.
        if tier_norm in ("MORNING", "BOTH"):
            morning_rows = [tuple(_pgify(r.get(c)) for c in MORNING_COLS) for r in records]
            psycopg2.extras.execute_values(cur, MORNING_UPSERT_SQL, morning_rows, page_size=500)
        if tier_norm in ("EVENING", "BOTH"):
            evening_rows = [tuple(_pgify(r.get(c)) for c in EVENING_COLS) for r in records]
            psycopg2.extras.execute_values(cur, EVENING_UPSERT_SQL, evening_rows, page_size=500)
    pg_conn.commit()

    log.info("  wrote %d rows to daily_features (tier=%s)", len(records), tier_norm)
    return len(records)


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
    raw_start = input("Start date YYYYMMDD (blank = full history rebuild): ").strip()
    if not raw_start:
        return None, None
    try:
        start = datetime.strptime(raw_start, "%Y%m%d").date()
    except ValueError:
        raise SystemExit("Start date must be YYYYMMDD.")
    raw_end = input("End date YYYYMMDD   (blank = today): ").strip()
    if raw_end:
        try:
            end = datetime.strptime(raw_end, "%Y%m%d").date()
        except ValueError:
            raise SystemExit("End date must be YYYYMMDD.")
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
