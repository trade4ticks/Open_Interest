-- =============================================================================
-- 03_new_metrics.sql — Three new metric buckets (Bucket 1: OHLC-derived,
--   Bucket 2: option volume EOD, Bucket 3: IV chain 15:45).
--
-- Run via init_db.py (idempotent — IF NOT EXISTS / IF NOT EXISTS guards).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- New aggregated tables (raw per-strike data is NOT stored)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS option_volume_daily (
    ticker                      TEXT             NOT NULL,
    trade_date                  DATE             NOT NULL,
    total_call_vol              BIGINT,
    total_put_vol               BIGINT,
    total_vol                   BIGINT,
    vol_0_30d                   BIGINT,
    vol_31_90d                  BIGINT,
    vol_weighted_strike_call     DOUBLE PRECISION,
    vol_weighted_strike_put      DOUBLE PRECISION,
    vol_weighted_strike_all      DOUBLE PRECISION,
    vol_above_spot              BIGINT,
    vol_below_spot              BIGINT,
    vol_within_5pct             BIGINT,
    vol_within_10pct            BIGINT,
    weighted_avg_dte_vol        DOUBLE PRECISION,
    PRIMARY KEY (ticker, trade_date)
);

CREATE TABLE IF NOT EXISTS option_iv_daily (
    ticker          TEXT             NOT NULL,
    trade_date      DATE             NOT NULL,
    atm_iv_7d       DOUBLE PRECISION,
    atm_iv_30d      DOUBLE PRECISION,
    atm_iv_90d      DOUBLE PRECISION,
    iv_25d_call_30d DOUBLE PRECISION,
    iv_25d_put_30d  DOUBLE PRECISION,
    PRIMARY KEY (ticker, trade_date)
);

-- ---------------------------------------------------------------------------
-- Bucket 1: OHLC-derived columns added to daily_features
-- ---------------------------------------------------------------------------

-- backward returns (entry = close of trade_date - N trading days, exit = close of trade_date)
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS ret_5d              DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS ret_10d             DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS ret_20d             DOUBLE PRECISION;

-- distance from moving averages and 52-week extremes
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_from_ma20       DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_from_ma50       DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_from_52w_high   DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_from_52w_low    DOUBLE PRECISION;

-- range / momentum / volatility structure
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS donchian_pos_20d    DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS ma20_slope_5d       DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_up_days_20d     DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS rv_ratio_5d_20d     DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS cum_signed_vol_20d  DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS atr_normalized_ret_5d DOUBLE PRECISION;

-- z-scores (60-day window)
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_price_vs_ma20          DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_price_vs_ma50          DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_underlying_vol_20d     DOUBLE PRECISION;

-- relative strength vs SPY
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS relative_strength_vs_spy_20d DOUBLE PRECISION;

-- ---------------------------------------------------------------------------
-- Bucket 2: option volume EOD columns added to daily_features
-- ---------------------------------------------------------------------------

-- put/call volume ratio
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS put_call_ratio_vol              DOUBLE PRECISION;

-- volume / OI ratios
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS vol_oi_ratio_all                DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS vol_oi_ratio_call               DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS vol_oi_ratio_put                DOUBLE PRECISION;

-- volume-weighted strike vs spot (spot_co variant only — _pc is spot-independent)
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS vol_weighted_call_div_spot_co   DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS vol_weighted_put_div_spot_co    DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS vol_weighted_all_div_spot_co    DOUBLE PRECISION;

-- directional volume split
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS vol_above_below_ratio_co        DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_vol_within_5pct_co          DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_vol_within_10pct_co         DOUBLE PRECISION;

-- term structure of volume
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_vol_0_30d                   DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_vol_31_90d                  DOUBLE PRECISION;

-- OI absorption: how much of daily OI change is explained by volume
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS net_new_oi_div_vol              DOUBLE PRECISION;

-- z-scores (60-day window)
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_put_call_ratio_vol        DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_vol_oi_ratio_all          DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_vol_oi_ratio_call         DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_vol_oi_ratio_put          DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_vol_above_below_ratio_co  DOUBLE PRECISION;

-- ---------------------------------------------------------------------------
-- Bucket 3: IV chain (15:45 snapshot) columns added to daily_features
-- ---------------------------------------------------------------------------

-- stored ATM IV by tenor (from option_iv_daily)
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS atm_iv_7d              DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS atm_iv_30d             DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS atm_iv_90d             DOUBLE PRECISION;

-- stored 25-delta IV (from option_iv_daily)
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS iv_25d_call_30d        DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS iv_25d_put_30d         DOUBLE PRECISION;

-- derived IV structure metrics (computed in build_features.py)
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS rr_25d_30d             DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS bf_25d_30d             DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS skew_25p_atm_30d       DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS skew_atm_25c_30d       DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS term_7d_30d            DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS term_30d_90d           DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS vrp_30d                DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS iv_rv_ratio_30d        DOUBLE PRECISION;

-- day-over-day IV changes
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS d1_atm_iv_7d_change    DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS d5_atm_iv_7d_change    DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS d1_atm_iv_30d_change   DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS d5_atm_iv_30d_change   DOUBLE PRECISION;

-- z-scores (60-day window)
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_iv_7d           DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_iv_30d          DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_iv_90d          DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_rr_25d_30d      DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_term_7d_30d     DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_term_30d_90d    DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_vrp_30d         DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_iv_rv_ratio_30d DOUBLE PRECISION;
