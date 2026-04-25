-- =============================================================================
-- OI_Research schema — research-only, drop the whole DB to clean up.
--
-- Tables:
--   underlying_ohlc      daily OHLC per ticker (yfinance)
--   option_oi_raw        raw daily OI per (ticker, trade_date, expiration, strike, right)
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
    right          CHAR(1)          NOT NULL CHECK (right IN ('C','P')),
    open_interest  BIGINT           NOT NULL,
    PRIMARY KEY (ticker, trade_date, expiration, strike, right)
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
    right          CHAR(1)          NOT NULL CHECK (right IN ('C','P')),
    open_interest  BIGINT           NOT NULL,
    spot_close     DOUBLE PRECISION,
    moneyness      DOUBLE PRECISION,
    PRIMARY KEY (ticker, trade_date, expiration, strike, right)
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

    -- concentration of OI near spot
    oi_within_5pct          BIGINT,
    oi_within_10pct         BIGINT,
    pct_oi_in_front_expiry  DOUBLE PRECISION,

    -- OI build-up signals (vs N trading days back)
    d1_total_oi_change      BIGINT,
    d5_total_oi_change      BIGINT,
    d20_total_oi_change     BIGINT,

    -- response variables (computed from underlying_ohlc)
    rv_5d                   DOUBLE PRECISION,
    rv_20d                  DOUBLE PRECISION,
    ret_1d_fwd              DOUBLE PRECISION,
    ret_5d_fwd              DOUBLE PRECISION,
    ret_20d_fwd             DOUBLE PRECISION,

    PRIMARY KEY (ticker, trade_date)
);

CREATE INDEX IF NOT EXISTS daily_features_date_idx
    ON daily_features (trade_date);
