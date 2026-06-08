-- =============================================================================
-- OI_Research schema — research-only, drop the whole DB to clean up.
--
-- Tables:
--   underlying_ohlc      daily OHLC per ticker (yfinance)
--   option_oi_raw        raw daily OI per (ticker, trade_date, expiration, strike, option_type)
--   option_oi_surface    filtered subset of option_oi_raw, joined to spot/moneyness/DTE
--   daily_features       per (ticker, trade_date) feature row for AI/stat analysis
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Daily underlying OHLC (long format, one row per ticker per date)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS underlying_ohlc (
    ticker      TEXT             NOT NULL,
    trade_date  DATE             NOT NULL,
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    adj_close   DOUBLE PRECISION,
    volume      BIGINT,
    dividends   DOUBLE PRECISION,
    splits      DOUBLE PRECISION,
    PRIMARY KEY (ticker, trade_date)
);

CREATE INDEX IF NOT EXISTS underlying_ohlc_date_idx
    ON underlying_ohlc (trade_date);

-- ---------------------------------------------------------------------------
-- 2. Raw daily option OI — one row per contract per trading day
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS option_oi_raw (
    ticker         TEXT             NOT NULL,
    trade_date     DATE             NOT NULL,
    expiration     DATE             NOT NULL,
    strike         DOUBLE PRECISION NOT NULL,
    option_type    CHAR(1)          NOT NULL CHECK (option_type IN ('C','P')),
    open_interest  BIGINT           NOT NULL,
    PRIMARY KEY (ticker, trade_date, expiration, strike, option_type)
);

CREATE INDEX IF NOT EXISTS option_oi_raw_lookup
    ON option_oi_raw (ticker, trade_date);
CREATE INDEX IF NOT EXISTS option_oi_raw_expiry
    ON option_oi_raw (ticker, expiration);

-- ---------------------------------------------------------------------------
-- 3. Filtered OI surface — meaningful nodes only, denormalised with spot/DTE
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS option_oi_surface (
    ticker         TEXT             NOT NULL,
    trade_date     DATE             NOT NULL,
    expiration     DATE             NOT NULL,
    dte            SMALLINT         NOT NULL,
    strike         DOUBLE PRECISION NOT NULL,
    option_type    CHAR(1)          NOT NULL CHECK (option_type IN ('C','P')),
    open_interest  BIGINT           NOT NULL,
    spot_close     DOUBLE PRECISION,
    moneyness      DOUBLE PRECISION,
    PRIMARY KEY (ticker, trade_date, expiration, strike, option_type)
);

CREATE INDEX IF NOT EXISTS option_oi_surface_lookup
    ON option_oi_surface (ticker, trade_date);
CREATE INDEX IF NOT EXISTS option_oi_surface_dte
    ON option_oi_surface (ticker, trade_date, dte);

-- ---------------------------------------------------------------------------
-- 4. Daily features — one row per (ticker, trade_date) for stat / AI analysis
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_features (
    ticker                  TEXT             NOT NULL,
    trade_date              DATE             NOT NULL,
    spot_close              DOUBLE PRECISION,

    -- aggregate OI (over the surface table — i.e. after filter)
    total_oi                BIGINT,
    call_oi                 BIGINT,
    put_oi                  BIGINT,
    put_call_oi_ratio       DOUBLE PRECISION,

    -- pinning / gamma-wall candidates
    max_oi_strike_call      DOUBLE PRECISION,
    max_oi_strike_put       DOUBLE PRECISION,

    -- OI-weighted strikes (centre of mass)
    oi_weighted_strike_call DOUBLE PRECISION,
    oi_weighted_strike_put  DOUBLE PRECISION,
    oi_weighted_strike_all  DOUBLE PRECISION,

    -- distance of OI-weighted strike from spot (absolute and relative)
    oi_weighted_strike_call_minus_spot DOUBLE PRECISION,
    oi_weighted_strike_put_minus_spot  DOUBLE PRECISION,
    oi_weighted_strike_all_minus_spot  DOUBLE PRECISION,
    oi_weighted_strike_call_div_spot   DOUBLE PRECISION,
    oi_weighted_strike_put_div_spot    DOUBLE PRECISION,
    oi_weighted_strike_all_div_spot    DOUBLE PRECISION,

    -- concentration of OI near spot
    oi_within_5pct          BIGINT,
    oi_within_10pct         BIGINT,
    pct_oi_in_front_expiry  DOUBLE PRECISION,

    -- OI split above vs below spot
    oi_above_spot           BIGINT,
    oi_below_spot           BIGINT,
    oi_above_below_ratio    DOUBLE PRECISION,

    -- OI-weighted strikes by DTE bucket (0-30d)
    oi_weighted_strike_all_0_30d            DOUBLE PRECISION,
    oi_weighted_strike_call_0_30d           DOUBLE PRECISION,
    oi_weighted_strike_put_0_30d            DOUBLE PRECISION,
    oi_weighted_strike_all_0_30d_div_spot   DOUBLE PRECISION,
    oi_weighted_strike_call_0_30d_div_spot  DOUBLE PRECISION,
    oi_weighted_strike_put_0_30d_div_spot   DOUBLE PRECISION,

    -- OI-weighted strikes by DTE bucket (31-90d)
    oi_weighted_strike_all_31_90d           DOUBLE PRECISION,
    oi_weighted_strike_call_31_90d          DOUBLE PRECISION,
    oi_weighted_strike_put_31_90d           DOUBLE PRECISION,
    oi_weighted_strike_all_31_90d_div_spot  DOUBLE PRECISION,
    oi_weighted_strike_call_31_90d_div_spot DOUBLE PRECISION,
    oi_weighted_strike_put_31_90d_div_spot  DOUBLE PRECISION,

    -- OI build-up signals (vs N trading days back)
    d1_total_oi_change      BIGINT,
    d5_total_oi_change      BIGINT,
    d20_total_oi_change     BIGINT,

    -- realised vol (computed from underlying_ohlc)
    rv_5d                   DOUBLE PRECISION,
    rv_20d                  DOUBLE PRECISION,

    -- forward returns: entry = open of trade_date, exit = close of trade_date+(N-1).
    -- (OI is published overnight and visible on broker platforms when the market
    -- opens on trade_date, so the realistic entry is that day's open.)
    ret_1d_fwd_oc           DOUBLE PRECISION,
    ret_3d_fwd_oc           DOUBLE PRECISION,
    ret_5d_fwd_oc           DOUBLE PRECISION,
    ret_7d_fwd_oc           DOUBLE PRECISION,
    ret_10d_fwd_oc          DOUBLE PRECISION,
    ret_20d_fwd_oc          DOUBLE PRECISION,

    PRIMARY KEY (ticker, trade_date)
);

CREATE INDEX IF NOT EXISTS daily_features_date_idx
    ON daily_features (trade_date);

-- ---------------------------------------------------------------------------
-- 5. New feature columns added 2026-04-28 — populated by build_features.py
--    using the FULL UNFILTERED parquet chain as the denominator.
-- ---------------------------------------------------------------------------
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_oi_within_5pct                       DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_oi_within_10pct                      DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_oi_above_spot                        DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_oi_below_spot                        DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS top5_strikes_pct_total_oi                DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS top10_strikes_pct_total_oi               DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS weighted_avg_dte                         DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_oi_0_30d                             DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_oi_31_90d                            DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_oi_91_365d                           DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_oi_next_monthly                      DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS oi_weighted_strike_next_monthly_div_spot DOUBLE PRECISION;

-- ---------------------------------------------------------------------------
-- 6. Forward-return semantics change + lag/zscore feature additions (2026-04-28)
--    OI for trade_date is visible on broker platforms when the market opens
--    that morning, so the realistic entry price is open[trade_date], not the
--    next day's open. Drop the close-to-close columns (no longer meaningful).
-- ---------------------------------------------------------------------------
-- CASCADE because v_features_with_returns (SELECT f.* from daily_features) holds
-- references to these columns. 02_views.sql recreates the view immediately after.
ALTER TABLE daily_features DROP COLUMN IF EXISTS ret_1d_fwd_cc  CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS ret_3d_fwd_cc  CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS ret_5d_fwd_cc  CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS ret_7d_fwd_cc  CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS ret_10d_fwd_cc CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS ret_20d_fwd_cc CASCADE;

-- Pct-change / derived-ratio change / 60-day (~3-month) z-score features.
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS d1_total_oi_pct_change                       DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS d5_total_oi_pct_change                       DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS d1_d5_ratio_total_oi_pct_change              DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS d1_oi_weighted_strike_all_div_spot_change    DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS d5_oi_weighted_strike_all_div_spot_change    DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS d1_put_call_oi_ratio_change                  DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS d5_put_call_oi_ratio_change                  DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_d1_oi_change_3m                       DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_d5_oi_change_3m                       DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_oi_weighted_strike_all_div_spot_3m    DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_put_call_oi_ratio_3m                  DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_oi_above_below_ratio_3m               DOUBLE PRECISION;

-- ---------------------------------------------------------------------------
-- 7. Spot-dual-version migration (2026-04-30)
--    Replace the single close[X]-based "spot" with explicit _pc (prior-day
--    close) and _co (current-day open) variants for every spot-dependent
--    metric, so we can backtest both entry models. Also drop "_strike" from
--    the oi_weighted_strike_* names since it was redundant in long names.
--
--    DROPs use CASCADE because v_features_with_returns (SELECT f.*) pins
--    these columns. 02_views.sql recreates the view immediately after.
-- ---------------------------------------------------------------------------

-- 7a. Drop obsolete close[X]-based spot-dependent columns.
ALTER TABLE daily_features DROP COLUMN IF EXISTS spot_close                                  CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_within_5pct                              CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_within_10pct                             CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_above_spot                               CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_below_spot                               CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_above_below_ratio                        CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS pct_oi_within_5pct                          CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS pct_oi_within_10pct                         CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS pct_oi_above_spot                           CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS pct_oi_below_spot                           CASCADE;

-- 7b. Drop obsolete _strike-named columns (renamed to drop "_strike").
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_call                     CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_put                      CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_all                      CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_call_minus_spot          CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_put_minus_spot           CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_all_minus_spot           CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_call_div_spot            CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_put_div_spot             CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_all_div_spot             CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_all_0_30d                CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_call_0_30d               CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_put_0_30d                CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_all_0_30d_div_spot       CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_call_0_30d_div_spot      CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_put_0_30d_div_spot       CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_all_31_90d               CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_call_31_90d              CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_put_31_90d               CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_all_31_90d_div_spot      CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_call_31_90d_div_spot     CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_put_31_90d_div_spot      CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS oi_weighted_strike_next_monthly_div_spot    CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS d1_oi_weighted_strike_all_div_spot_change   CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS d5_oi_weighted_strike_all_div_spot_change   CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS zscore_oi_weighted_strike_all_div_spot_3m   CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS zscore_oi_above_below_ratio_3m              CASCADE;

-- 7c. Renamed (_strike dropped) — spot-independent OI-weighted strike values.
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_call                        DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_put                         DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_all                         DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_all_0_30d                   DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_call_0_30d                  DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_put_0_30d                   DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_all_31_90d                  DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_call_31_90d                 DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_put_31_90d                  DOUBLE PRECISION;

-- 7d. Spot in two flavours.
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS spot_pc                                 DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS spot_co                                 DOUBLE PRECISION;

-- 7e. Moneyness-dependent counts and pct-of-total — _pc and _co versions.
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_within_5pct_pc                       BIGINT;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_within_5pct_co                       BIGINT;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_within_10pct_pc                      BIGINT;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_within_10pct_co                      BIGINT;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_above_spot_pc                        BIGINT;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_above_spot_co                        BIGINT;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_below_spot_pc                        BIGINT;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_below_spot_co                        BIGINT;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_above_below_ratio_pc                 DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_above_below_ratio_co                 DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS pct_oi_within_5pct_pc                   DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS pct_oi_within_5pct_co                   DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS pct_oi_within_10pct_pc                  DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS pct_oi_within_10pct_co                  DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS pct_oi_above_spot_pc                    DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS pct_oi_above_spot_co                    DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS pct_oi_below_spot_pc                    DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS pct_oi_below_spot_co                    DOUBLE PRECISION;

-- 7f. OI-weighted strikes vs spot (minus_spot and div_spot, _pc and _co).
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_call_minus_spot_pc          DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_call_minus_spot_co          DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_put_minus_spot_pc           DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_put_minus_spot_co           DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_all_minus_spot_pc           DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_all_minus_spot_co           DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_call_div_spot_pc            DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_call_div_spot_co            DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_put_div_spot_pc             DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_put_div_spot_co             DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_all_div_spot_pc             DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_all_div_spot_co             DOUBLE PRECISION;

-- 7g. DTE-bucketed div_spot variants — _pc and _co.
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_all_0_30d_div_spot_pc       DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_all_0_30d_div_spot_co       DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_call_0_30d_div_spot_pc      DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_call_0_30d_div_spot_co      DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_put_0_30d_div_spot_pc       DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_put_0_30d_div_spot_co       DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_all_31_90d_div_spot_pc      DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_all_31_90d_div_spot_co      DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_call_31_90d_div_spot_pc     DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_call_31_90d_div_spot_co     DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_put_31_90d_div_spot_pc      DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_put_31_90d_div_spot_co      DOUBLE PRECISION;

-- 7h. Next-monthly div_spot variants — _pc and _co.
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_next_monthly_div_spot_pc    DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS oi_weighted_next_monthly_div_spot_co    DOUBLE PRECISION;

-- 7i. Lag changes and z-scores of spot-dependent ratios — _pc and _co.
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS d1_oi_weighted_all_div_spot_change_pc   DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS d1_oi_weighted_all_div_spot_change_co   DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS d5_oi_weighted_all_div_spot_change_pc   DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS d5_oi_weighted_all_div_spot_change_co   DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS zscore_oi_weighted_all_div_spot_3m_pc   DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS zscore_oi_weighted_all_div_spot_3m_co   DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS zscore_oi_above_below_ratio_3m_pc       DOUBLE PRECISION;
ALTER TABLE daily_features ADD  COLUMN IF NOT EXISTS zscore_oi_above_below_ratio_3m_co       DOUBLE PRECISION;

-- ---------------------------------------------------------------------------
-- 8. Vol spot-reference standardization (2026-05-26)
--    The six vol _co derived columns in daily_features had wrong suffixes:
--    - vol_above_below_ratio_co / pct_vol_within_*_co: upstream computation in
--      fetch_volume_eod.py used prior_close (= C_{T-1} = spot_pc) as the spot
--      reference, so _co suffix was incorrect from the start.
--    - vol_weighted_*_div_spot_co: denominator changed from O_T (spot_co) to
--      C_{T-1} (spot_pc) for consistency with the standardized spot reference.
--    Upstream option_volume_daily columns (vol_above_spot, vol_within_*) keep
--    their names — only the derived daily_features columns are renamed/changed.
--    CASCADE: v_features_with_returns uses SELECT f.* — drop+recreate via
--    02_views.sql after running this file.
-- ---------------------------------------------------------------------------

-- 8a. Drop old _co named columns (stale data; rebuild daily_features after).
ALTER TABLE daily_features DROP COLUMN IF EXISTS vol_weighted_call_div_spot_co   CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS vol_weighted_put_div_spot_co    CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS vol_weighted_all_div_spot_co    CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS vol_above_below_ratio_co        CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS pct_vol_within_5pct_co          CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS pct_vol_within_10pct_co         CASCADE;
ALTER TABLE daily_features DROP COLUMN IF EXISTS zscore_vol_above_below_ratio_co CASCADE;

-- 8b. Add new _pc named replacements.
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS vol_weighted_call_div_spot_pc   DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS vol_weighted_put_div_spot_pc    DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS vol_weighted_all_div_spot_pc    DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS vol_above_below_ratio_pc        DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_vol_within_5pct_pc          DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS pct_vol_within_10pct_pc         DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_vol_above_below_ratio_pc DOUBLE PRECISION;

-- 8c. weighted_avg_dte_vol: present in option_volume_daily since 03_new_metrics,
--     now surfaced in daily_features as an evening-tier feature.
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS weighted_avg_dte_vol            DOUBLE PRECISION;

-- ---------------------------------------------------------------------------
-- 9. Forward-return close-to-close columns re-added (2026-05-26)
--    Previously dropped in section 6 with a different entry-anchor rationale.
--    Re-added with entry = C_{T-1} (prior close). Suffix _cc = close-to-close
--    entry anchor. The _pc/_co suffix convention is reserved for spot-reference
--    meaning elsewhere and is NOT used here.
-- ---------------------------------------------------------------------------
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS ret_1d_fwd_cc   DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS ret_3d_fwd_cc   DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS ret_5d_fwd_cc   DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS ret_7d_fwd_cc   DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS ret_10d_fwd_cc  DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS ret_20d_fwd_cc  DOUBLE PRECISION;

-- ---------------------------------------------------------------------------
-- 10. source_session columns on vol/IV upstream tables
--     Both option_volume_daily and option_iv_daily store rows under
--     trade_date = next_trading_day(fetch_date), so the fetched session
--     (T-1) is otherwise invisible once the row lands. source_session
--     records the actual session whose data this row contains.
--     Existing rows: NULL until the next re-fetch overwrites them.
-- ---------------------------------------------------------------------------
ALTER TABLE option_volume_daily ADD COLUMN IF NOT EXISTS source_session DATE;
ALTER TABLE option_iv_daily     ADD COLUMN IF NOT EXISTS source_session DATE;

-- ---------------------------------------------------------------------------
-- 11. Open-price provenance on underlying_ohlc (2026-06-07)
--     The 7am premarket run writes open = premarket proxy and stamps the
--     source so downstream consumers know the price isn't the official open.
--     The 9:35 MORNING and EVENING runs overwrite both columns with
--     open_source = 'daily_1d' when they correct open to the official bar.
--
--     open_source  values: 'premarket_1m' | 'daily_1d' | NULL (legacy rows)
--     open_asof_ts: wall-clock timestamp of the bar used (ET, tz-aware).
--                   NULL for daily_1d rows (bar time is just the trade date).
-- ---------------------------------------------------------------------------
ALTER TABLE underlying_ohlc ADD COLUMN IF NOT EXISTS open_source   TEXT;
ALTER TABLE underlying_ohlc ADD COLUMN IF NOT EXISTS open_asof_ts  TIMESTAMPTZ;
