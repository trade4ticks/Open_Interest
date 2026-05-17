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

import bisect
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
# OHLC-derived features (rv, fwd oc returns) — DuckDB on the unified ohlc DF.
# Input: ohlc pandas DF [trade_date, open, close, prev_close]
#
# Forward returns: entry = open of trade_date (OI for trade_date is published
# overnight and visible on broker platforms when the market opens), exit =
# close of trade_date + (N-1). So ret_1d is intraday open-to-close on
# trade_date itself, ret_3d is from trade_date open to close[+2], etc.
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
        ) AS true_range
    FROM ohlc
),
windowed AS (
    SELECT
        trade_date, open, close, volume, log_ret, true_range,
        -- Realized vol
        STDDEV_SAMP(log_ret) OVER w5  * SQRT(252)                  AS rv_5d,
        STDDEV_SAMP(log_ret) OVER w20 * SQRT(252)                  AS rv_20d,
        -- Moving averages
        AVG(close) OVER w20                                         AS ma20,
        AVG(close) OVER w50                                         AS ma50,
        -- 52-week extremes
        MAX(close) OVER w252                                        AS hi52,
        MIN(close) OVER w252                                        AS lo52,
        -- Donchian channel components
        MAX(high)  OVER w20                                         AS hi20,
        MIN(low)   OVER w20                                         AS lo20,
        -- Average true range (14-day)
        AVG(true_range) OVER w14                                    AS atr_14d,
        -- Backward returns
        close / NULLIF(LAG(close,  5) OVER w_t, 0) - 1             AS ret_5d,
        close / NULLIF(LAG(close, 10) OVER w_t, 0) - 1             AS ret_10d,
        close / NULLIF(LAG(close, 20) OVER w_t, 0) - 1             AS ret_20d,
        -- Forward returns (entry = open of trade_date)
        close                      / NULLIF(open, 0) - 1           AS ret_1d_fwd_oc,
        LEAD(close,  2) OVER w_t   / NULLIF(open, 0) - 1           AS ret_3d_fwd_oc,
        LEAD(close,  4) OVER w_t   / NULLIF(open, 0) - 1           AS ret_5d_fwd_oc,
        LEAD(close,  6) OVER w_t   / NULLIF(open, 0) - 1           AS ret_7d_fwd_oc,
        LEAD(close,  9) OVER w_t   / NULLIF(open, 0) - 1           AS ret_10d_fwd_oc,
        LEAD(close, 19) OVER w_t   / NULLIF(open, 0) - 1           AS ret_20d_fwd_oc,
        -- Up-day frequency (20-day)
        AVG(CASE WHEN log_ret > 0 THEN 1.0 ELSE 0.0 END) OVER w20  AS pct_up_days_20d,
        -- Volume-weighted directional indicator (OBV-style, normalized)
        SUM(SIGN(log_ret) * volume::DOUBLE) OVER w20
            / NULLIF(SUM(volume::DOUBLE) OVER w20, 0)               AS cum_signed_vol_20d
    FROM base
    WINDOW
        w_t  AS (ORDER BY trade_date),
        w5   AS (ORDER BY trade_date ROWS BETWEEN   4 PRECEDING AND CURRENT ROW),
        w14  AS (ORDER BY trade_date ROWS BETWEEN  13 PRECEDING AND CURRENT ROW),
        w20  AS (ORDER BY trade_date ROWS BETWEEN  19 PRECEDING AND CURRENT ROW),
        w50  AS (ORDER BY trade_date ROWS BETWEEN  49 PRECEDING AND CURRENT ROW),
        w252 AS (ORDER BY trade_date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW)
),
derived AS (
    SELECT
        *,
        close / NULLIF(ma20, 0) - 1                                 AS pct_from_ma20,
        close / NULLIF(ma50, 0) - 1                                 AS pct_from_ma50,
        close / NULLIF(hi52, 0) - 1                                 AS pct_from_52w_high,
        close / NULLIF(lo52, 0) - 1                                 AS pct_from_52w_low,
        (close - lo20) / NULLIF(hi20 - lo20, 0)                     AS donchian_pos_20d,
        ma20 / NULLIF(LAG(ma20, 5) OVER (ORDER BY trade_date), 0) - 1 AS ma20_slope_5d,
        rv_5d / NULLIF(rv_20d, 0)                                   AS rv_ratio_5d_20d,
        ret_5d / NULLIF(atr_14d, 0)                                 AS atr_normalized_ret_5d
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
# Bucket 2: Option volume EOD features
# Inputs registered by the caller:
#   vol_daily   pandas DF from option_volume_daily (trade_date + aggregates)
#   base_feats  merged oi+ohlc DF (trade_date, total_oi, call_oi, put_oi,
#               spot_co, d1_total_oi_change, ...)
# ---------------------------------------------------------------------------
VOL_FEATURES_SQL = """
WITH vol_joined AS (
    SELECT
        v.trade_date,
        v.total_call_vol,
        v.total_put_vol,
        v.total_vol,
        v.vol_0_30d,
        v.vol_31_90d,
        v.vol_weighted_strike_call,
        v.vol_weighted_strike_put,
        v.vol_weighted_strike_all,
        v.vol_above_spot,
        v.vol_below_spot,
        v.vol_within_5pct,
        v.vol_within_10pct,
        f.total_oi,
        f.call_oi,
        f.put_oi,
        f.spot_co,
        f.d1_total_oi_change
    FROM vol_daily v
    LEFT JOIN base_feats f ON v.trade_date = f.trade_date
),
ratios AS (
    SELECT
        trade_date,
        total_put_vol::DOUBLE  / NULLIF(total_call_vol, 0)     AS put_call_ratio_vol,
        total_vol::DOUBLE      / NULLIF(total_oi, 0)           AS vol_oi_ratio_all,
        total_call_vol::DOUBLE / NULLIF(call_oi, 0)            AS vol_oi_ratio_call,
        total_put_vol::DOUBLE  / NULLIF(put_oi, 0)             AS vol_oi_ratio_put,
        vol_weighted_strike_call / NULLIF(spot_co, 0)          AS vol_weighted_call_div_spot_co,
        vol_weighted_strike_put  / NULLIF(spot_co, 0)          AS vol_weighted_put_div_spot_co,
        vol_weighted_strike_all  / NULLIF(spot_co, 0)          AS vol_weighted_all_div_spot_co,
        vol_above_spot::DOUBLE / NULLIF(vol_below_spot, 0)     AS vol_above_below_ratio_co,
        vol_within_5pct::DOUBLE  / NULLIF(total_vol, 0)        AS pct_vol_within_5pct_co,
        vol_within_10pct::DOUBLE / NULLIF(total_vol, 0)        AS pct_vol_within_10pct_co,
        vol_0_30d::DOUBLE  / NULLIF(total_vol, 0)              AS pct_vol_0_30d,
        vol_31_90d::DOUBLE / NULLIF(total_vol, 0)              AS pct_vol_31_90d,
        d1_total_oi_change::DOUBLE / NULLIF(total_vol, 0)      AS net_new_oi_div_vol
    FROM vol_joined
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
        CASE WHEN COUNT(vol_above_below_ratio_co) OVER w60 >= 60
             THEN (vol_above_below_ratio_co - AVG(vol_above_below_ratio_co) OVER w60)
                  / NULLIF(STDDEV_SAMP(vol_above_below_ratio_co) OVER w60, 0)
             ELSE NULL END                                      AS zscore_vol_above_below_ratio_co
    FROM ratios
    WINDOW w60 AS (ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
)
SELECT
    r.trade_date,
    r.put_call_ratio_vol,
    r.vol_oi_ratio_all, r.vol_oi_ratio_call, r.vol_oi_ratio_put,
    r.vol_weighted_call_div_spot_co, r.vol_weighted_put_div_spot_co,
    r.vol_weighted_all_div_spot_co,
    r.vol_above_below_ratio_co,
    r.pct_vol_within_5pct_co, r.pct_vol_within_10pct_co,
    r.pct_vol_0_30d, r.pct_vol_31_90d,
    r.net_new_oi_div_vol,
    z.zscore_put_call_ratio_vol,
    z.zscore_vol_oi_ratio_all, z.zscore_vol_oi_ratio_call, z.zscore_vol_oi_ratio_put,
    z.zscore_vol_above_below_ratio_co
FROM ratios r
JOIN zscores z USING (trade_date)
"""

# ---------------------------------------------------------------------------
# Bucket 3: IV chain (15:45) features
# Inputs registered by the caller:
#   iv_daily    pandas DF from option_iv_daily (trade_date + 5 IV metrics)
#   base_feats  merged oi+ohlc DF (for rv_20d used in VRP / iv_rv_ratio)
# ---------------------------------------------------------------------------
IV_FEATURES_SQL = """
WITH iv_joined AS (
    SELECT
        i.trade_date,
        i.atm_iv_7d,
        i.atm_iv_30d,
        i.atm_iv_90d,
        i.iv_25d_call_30d,
        i.iv_25d_put_30d,
        f.rv_20d
    FROM iv_daily i
    LEFT JOIN base_feats f ON i.trade_date = f.trade_date
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
# Postgres write
# ---------------------------------------------------------------------------
INSERT_COLS = [
    "ticker", "trade_date",
    "spot_pc", "spot_co",
    "total_oi", "call_oi", "put_oi", "put_call_oi_ratio",
    "max_oi_strike_call", "max_oi_strike_put",
    # OI-weighted strikes (no spot — same value regardless of pc/co)
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
    # DTE-bucketed weighted strikes (no spot)
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
    "rv_5d", "rv_20d",
    "ret_1d_fwd_oc",  "ret_3d_fwd_oc",  "ret_5d_fwd_oc",
    "ret_7d_fwd_oc",  "ret_10d_fwd_oc", "ret_20d_fwd_oc",
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
    # pct changes / derived-ratio changes / 60-day z-scores
    "d1_total_oi_pct_change", "d5_total_oi_pct_change",
    "d1_d5_ratio_total_oi_pct_change",
    "d1_oi_weighted_all_div_spot_change_pc", "d1_oi_weighted_all_div_spot_change_co",
    "d5_oi_weighted_all_div_spot_change_pc", "d5_oi_weighted_all_div_spot_change_co",
    "d1_put_call_oi_ratio_change", "d5_put_call_oi_ratio_change",
    "zscore_d1_oi_change_3m", "zscore_d5_oi_change_3m",
    "zscore_oi_weighted_all_div_spot_3m_pc", "zscore_oi_weighted_all_div_spot_3m_co",
    "zscore_put_call_oi_ratio_3m",
    "zscore_oi_above_below_ratio_3m_pc", "zscore_oi_above_below_ratio_3m_co",
    # Bucket 1: OHLC-derived
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
    # Bucket 2: option volume EOD
    "put_call_ratio_vol",
    "vol_oi_ratio_all", "vol_oi_ratio_call", "vol_oi_ratio_put",
    "vol_weighted_call_div_spot_co", "vol_weighted_put_div_spot_co",
    "vol_weighted_all_div_spot_co",
    "vol_above_below_ratio_co",
    "pct_vol_within_5pct_co", "pct_vol_within_10pct_co",
    "pct_vol_0_30d", "pct_vol_31_90d",
    "net_new_oi_div_vol",
    "zscore_put_call_ratio_vol",
    "zscore_vol_oi_ratio_all", "zscore_vol_oi_ratio_call", "zscore_vol_oi_ratio_put",
    "zscore_vol_above_below_ratio_co",
    # Bucket 3: IV chain (15:45)
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

INSERT_SQL = f"INSERT INTO daily_features ({', '.join(INSERT_COLS)}) VALUES %s"
CLEAR_SQL  = "DELETE FROM daily_features WHERE ticker = %(ticker)s"


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
    """Return (trade_date, spy_ret_20d) for all available SPY history."""
    df = read_sql_df(
        conn,
        "SELECT trade_date, close FROM underlying_ohlc "
        "WHERE ticker = 'SPY' ORDER BY trade_date",
    )
    if df.empty:
        return df
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df = df.sort_values("trade_date").reset_index(drop=True)
    df["spy_ret_20d"] = df["close"] / df["close"].shift(20) - 1
    return df[["trade_date", "spy_ret_20d"]]


def load_vol_daily(conn, ticker: str,
                   start: date | None, end: date) -> pd.DataFrame:
    """Pull option_volume_daily rows for one ticker within [start, end]."""
    if start is not None:
        df = read_sql_df(
            conn,
            "SELECT * FROM option_volume_daily "
            "WHERE ticker = %(t)s AND trade_date >= %(s)s AND trade_date <= %(e)s "
            "ORDER BY trade_date",
            {"t": ticker, "s": start, "e": end},
        )
    else:
        df = read_sql_df(
            conn,
            "SELECT * FROM option_volume_daily "
            "WHERE ticker = %(t)s ORDER BY trade_date",
            {"t": ticker},
        )
    if not df.empty:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return df


def load_iv_daily(conn, ticker: str,
                  start: date | None, end: date) -> pd.DataFrame:
    """Pull option_iv_daily rows for one ticker within [start, end]."""
    if start is not None:
        df = read_sql_df(
            conn,
            "SELECT * FROM option_iv_daily "
            "WHERE ticker = %(t)s AND trade_date >= %(s)s AND trade_date <= %(e)s "
            "ORDER BY trade_date",
            {"t": ticker, "s": start, "e": end},
        )
    else:
        df = read_sql_df(
            conn,
            "SELECT * FROM option_iv_daily "
            "WHERE ticker = %(t)s ORDER BY trade_date",
            {"t": ticker},
        )
    if not df.empty:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return df


def load_splits(conn, ticker: str) -> pd.DataFrame:
    """Pull non-zero split events for one ticker, sorted ascending by date."""
    df = read_sql_df(
        conn,
        "SELECT trade_date, splits FROM underlying_ohlc "
        "WHERE ticker = %(ticker)s AND splits IS NOT NULL AND splits != 0 "
        "ORDER BY trade_date",
        {"ticker": ticker},
    )
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return df


def make_split_factors(splits_df: pd.DataFrame, oi_dates: list) -> pd.DataFrame:
    """Return DataFrame(trade_date, adj_factor) for each date in oi_dates.

    adj_factor = product of (1/ratio) for all splits on or after trade_date.
    Converts pre-split strikes to current (post-split) terms.
    Handles forward splits (ratio > 1) and reverse splits (ratio < 1) uniformly.
    For tickers with no splits every factor is 1.0 (no-op).
    """
    if splits_df.empty:
        return pd.DataFrame({"trade_date": oi_dates,
                             "adj_factor":  [1.0] * len(oi_dates)})

    split_dates  = splits_df["trade_date"].tolist()   # sorted asc by query
    split_ratios = splits_df["splits"].tolist()

    # Build suffix cumulative product: suffix_factors[i] = prod(1/ratio for splits[i:])
    # Boundary: trade_date <= split_date → adjust (bisect_left returns that split's idx)
    #           trade_date >  split_date → no adjustment (idx past the split)
    n = len(split_dates)
    suffix_factors = [1.0] * (n + 1)
    for i in range(n - 1, -1, -1):
        suffix_factors[i] = suffix_factors[i + 1] / split_ratios[i]

    factors = [suffix_factors[bisect.bisect_left(split_dates, td)] for td in oi_dates]
    return pd.DataFrame({"trade_date": oi_dates, "adj_factor": factors})


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
    # OHLC_FEATURES_SQL also uses high, low, volume for Bucket 1 metrics.
    con.register("ohlc",            ohlc[["trade_date", "open", "high", "low", "close", "volume"]])
    con.register("next_monthly_df", nm_df)
    con.register("split_factors",   sf_df)
    # The oi view applies split-adjustment to strike via a LEFT JOIN on split_factors.
    # Rows with no split event in their history get adj_factor=1.0 (COALESCE guard).
    con.execute(
        f"CREATE OR REPLACE VIEW oi AS "
        f"SELECT raw.trade_date, raw.expiration, "
        f"       raw.strike * COALESCE(sf.adj_factor, 1.0) AS strike, "
        f"       raw.option_type, raw.open_interest "
        f"FROM (SELECT * FROM read_parquet('{parquet_glob(ticker)}'){date_filter_sql}) raw "
        f"LEFT JOIN split_factors sf ON raw.trade_date = sf.trade_date"
    )

    # Load vol and IV data (include OI lookback buffer for z-score warm-up).
    vol_daily_df = load_vol_daily(pg_conn, ticker, oi_buffer_start, end_eff)
    iv_daily_df  = load_iv_daily(pg_conn, ticker,  oi_buffer_start, end_eff)

    log.info("  computing OI features ...")
    oi_feats = con.execute(OI_FEATURES_SQL).df()
    log.info("  computing OHLC features ...")
    ohlc_feats = con.execute(OHLC_FEATURES_SQL).df()

    if oi_feats.empty:
        log.warning("  no OI rows in range for %s — skipping", ticker)
        con.close()
        return 0

    feats = oi_feats.merge(ohlc_feats, on="trade_date", how="left")
    # DuckDB returns DATE columns as datetime64[us]; normalise to Python date
    # so `>= start` (Python date) comparisons work and psycopg2 sees a clean
    # DATE value at INSERT time.
    feats["trade_date"] = pd.to_datetime(feats["trade_date"]).dt.date
    feats.insert(0, "ticker", ticker)

    # Register the merged base for vol/IV queries, then compute Bucket 2 & 3.
    con.register("base_feats", feats)

    if not vol_daily_df.empty:
        log.info("  computing vol features ...")
        con.register("vol_daily", vol_daily_df)
        vol_feats = con.execute(VOL_FEATURES_SQL).df()
        vol_feats["trade_date"] = pd.to_datetime(vol_feats["trade_date"]).dt.date
        feats = feats.merge(vol_feats, on="trade_date", how="left")

    if not iv_daily_df.empty:
        log.info("  computing IV features ...")
        con.register("iv_daily", iv_daily_df)
        iv_feats = con.execute(IV_FEATURES_SQL).df()
        iv_feats["trade_date"] = pd.to_datetime(iv_feats["trade_date"]).dt.date
        feats = feats.merge(iv_feats, on="trade_date", how="left")

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
