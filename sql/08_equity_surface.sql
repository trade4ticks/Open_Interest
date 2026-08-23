-- =============================================================================
-- 08_equity_surface.sql — interpolated equity option surface (pipeline stage 3).
--
-- equity_surface              one row per (ticker, date, snapshot, dte, delta)
-- equity_atm                  one row per (ticker, date, snapshot, dte)
-- equity_surface_diagnostics  one row per fitted expiry; doubles as pipeline
--                             state — incremental runs resume from its max
--                             trade_date, and the intraday job compares its
--                             row count per snapshot against the expirations
--                             on disk to decide what still needs work.
--
-- ticker is part of every primary key and every unique constraint.
--
-- The first two are monthly RANGE partitions on trade_date; child tables are
-- created on demand by the ensure_*_partition functions below, so a backfill
-- into a new month does not need a migration.
-- =============================================================================

CREATE TABLE IF NOT EXISTS equity_surface (
    ticker        TEXT             NOT NULL,
    trade_date    DATE             NOT NULL,
    snapshot      TEXT             NOT NULL,
    dte           SMALLINT         NOT NULL,
    put_delta     SMALLINT         NOT NULL,
    iv            DOUBLE PRECISION NOT NULL,
    strike        DOUBLE PRECISION,
    forward       DOUBLE PRECISION,
    log_moneyness DOUBLE PRECISION,
    price         DOUBLE PRECISION,
    theta         DOUBLE PRECISION,
    vega          DOUBLE PRECISION,
    gamma         DOUBLE PRECISION,
    dte_actual    DOUBLE PRECISION,
    -- TRUE means the delta solved outside the fitted smile's [k_min, k_max],
    -- where the spline is pinned flat at its boundary value. The IV is then
    -- the last real strike's IV, not an observation. Rows are kept rather than
    -- dropped: a gap breaks rolling percentiles downstream, and the metrics
    -- layer can decide what to do with a flagged value.
    extrapolated  BOOLEAN          NOT NULL DEFAULT FALSE,
    UNIQUE (ticker, trade_date, snapshot, dte, put_delta)
) PARTITION BY RANGE (trade_date);

CREATE TABLE IF NOT EXISTS equity_atm (
    ticker           TEXT             NOT NULL,
    trade_date       DATE             NOT NULL,
    snapshot         TEXT             NOT NULL,
    dte              SMALLINT         NOT NULL,
    atm_put_delta    DOUBLE PRECISION NOT NULL,
    atm_strike       DOUBLE PRECISION NOT NULL,
    atm_iv           DOUBLE PRECISION NOT NULL,
    atm_forward      DOUBLE PRECISION NOT NULL,
    total_var        DOUBLE PRECISION,
    underlying_price DOUBLE PRECISION,
    price            DOUBLE PRECISION,
    theta            DOUBLE PRECISION,
    vega             DOUBLE PRECISION,
    gamma            DOUBLE PRECISION,
    dte_actual       DOUBLE PRECISION,
    UNIQUE (ticker, trade_date, snapshot, dte)
) PARTITION BY RANGE (trade_date);

CREATE TABLE IF NOT EXISTS equity_surface_diagnostics (
    ticker             TEXT             NOT NULL,
    trade_date         DATE             NOT NULL,
    snapshot           TEXT             NOT NULL,
    expiry             DATE             NOT NULL,
    dte_actual         DOUBLE PRECISION,
    forward_price      DOUBLE PRECISION,
    risk_free_rate     DOUBLE PRECISION,
    -- 'pcp' | 'spot_fallback'. The fallback ignores dividends, so on a
    -- dividend-paying name it overstates the forward by roughly the dividend
    -- inside the tenor. This column is how its frequency gets measured.
    forward_method     TEXT,
    n_strikes_raw      INTEGER,
    n_strikes_clean    INTEGER,
    k_min              DOUBLE PRECISION,
    k_max              DOUBLE PRECISION,
    spline_rmse        DOUBLE PRECISION,
    calendar_arb_flag  BOOLEAN NOT NULL DEFAULT FALSE,
    butterfly_arb_flag BOOLEAN NOT NULL DEFAULT FALSE,
    skipped            BOOLEAN NOT NULL DEFAULT FALSE,
    skip_reason        TEXT,
    PRIMARY KEY (ticker, trade_date, snapshot, expiry)
);

CREATE INDEX IF NOT EXISTS ix_equity_surface_diag_date
    ON equity_surface_diagnostics (trade_date, snapshot);

-- --- Partition helpers ------------------------------------------------------
-- Called before each date is processed. Idempotent: the child may already
-- exist from an earlier run or a concurrent worker, so the duplicate_table
-- exception is swallowed rather than racing.

CREATE OR REPLACE FUNCTION ensure_equity_surface_partition(d DATE)
RETURNS VOID AS $$
DECLARE
    month_start DATE := date_trunc('month', d)::DATE;
    month_end   DATE := (date_trunc('month', d) + INTERVAL '1 month')::DATE;
    part_name   TEXT := 'equity_surface_' || to_char(month_start, 'YYYYMM');
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF equity_surface '
        'FOR VALUES FROM (%L) TO (%L)', part_name, month_start, month_end);
EXCEPTION WHEN duplicate_table OR invalid_object_definition THEN
    NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ensure_equity_atm_partition(d DATE)
RETURNS VOID AS $$
DECLARE
    month_start DATE := date_trunc('month', d)::DATE;
    month_end   DATE := (date_trunc('month', d) + INTERVAL '1 month')::DATE;
    part_name   TEXT := 'equity_atm_' || to_char(month_start, 'YYYYMM');
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF equity_atm '
        'FOR VALUES FROM (%L) TO (%L)', part_name, month_start, month_end);
EXCEPTION WHEN duplicate_table OR invalid_object_definition THEN
    NULL;
END;
$$ LANGUAGE plpgsql;
