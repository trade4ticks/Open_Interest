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
    price         DOUBLE PRECISION,   -- Black-Scholes PUT at this node
    -- The CALL at the same node, same F/K/T/r/sigma. Needed by the metrics
    -- stage for risk-reversal structure prices; parity downstream would need
    -- r, which is not stored on the row.
    call_price    DOUBLE PRECISION,
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
    -- The instant the chain was actually captured. `snapshot` is the 5-minute
    -- GRID BUCKET the capture belongs to and stays part of the key, so a live
    -- row at 13:47:30 occupies slot '1345' and a later exact rebuild upserts
    -- over it. captured_at is the truth; snapshot is the join key.
    captured_at   TIMESTAMP,
    -- 'live'  — captured intraday from the snapshot endpoint, approximate time
    -- 'exact' — rebuilt from the historical 5-minute record, on the grid
    source        TEXT,
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
    captured_at      TIMESTAMP,
    source           TEXT,
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
    -- What the put-call parity regression's rate WOULD have been, recorded
    -- even when rejected or unused. On American options early-exercise premium
    -- tilts the regression and biases r downward through R_MIN, and the
    -- distribution of these rejected rates is the only place that bias is
    -- visible -- every rejection otherwise reads only 'spot_fallback'.
    r_solved_raw       DOUBLE PRECISION,
    n_strikes_raw      INTEGER,
    n_strikes_clean    INTEGER,
    k_min              DOUBLE PRECISION,
    k_max              DOUBLE PRECISION,
    spline_rmse        DOUBLE PRECISION,
    calendar_arb_flag  BOOLEAN NOT NULL DEFAULT FALSE,
    butterfly_arb_flag BOOLEAN NOT NULL DEFAULT FALSE,
    skipped            BOOLEAN NOT NULL DEFAULT FALSE,
    skip_reason        TEXT,
    -- Put-side domain reach in sigma: |k_min| / sqrt(w_atm). Raw k_min is not
    -- comparable across expiries; this is, because sqrt(w) = sigma*sqrt(T).
    -- A 25-delta put sits near 0.67 sigma, a 10-delta near 1.28.
    domain_reach       DOUBLE PRECISION,
    -- TRUE when the narrow-domain rule removed this fit from the interpolation
    -- candidate pool. The fit was still computed and still calendar-checked;
    -- it was only barred from being a bracketing endpoint, where its narrow
    -- domain would have clipped the blend's wing.
    excluded_from_bracketing BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (ticker, trade_date, snapshot, expiry)
);

-- Migration for tables created before these two columns existed. CREATE TABLE
-- IF NOT EXISTS above is a no-op on an existing table, so the columns have to
-- be added separately; both forms are idempotent.
ALTER TABLE equity_surface_diagnostics
    ADD COLUMN IF NOT EXISTS domain_reach DOUBLE PRECISION;
ALTER TABLE equity_surface_diagnostics
    ADD COLUMN IF NOT EXISTS excluded_from_bracketing BOOLEAN NOT NULL DEFAULT FALSE;

-- Same migration for the live-capture columns. Adding to a partitioned parent
-- cascades to every existing child partition.
ALTER TABLE equity_surface ADD COLUMN IF NOT EXISTS call_price DOUBLE PRECISION;
ALTER TABLE equity_surface ADD COLUMN IF NOT EXISTS captured_at TIMESTAMP;
ALTER TABLE equity_surface ADD COLUMN IF NOT EXISTS source TEXT;
ALTER TABLE equity_atm     ADD COLUMN IF NOT EXISTS captured_at TIMESTAMP;
ALTER TABLE equity_atm     ADD COLUMN IF NOT EXISTS source TEXT;

-- The dashboard reads "most recent slot for this ticker", never a fixed one:
-- a skipped cycle leaves a grid slot empty by design.
CREATE INDEX IF NOT EXISTS ix_equity_surface_latest
    ON equity_surface (ticker, trade_date, snapshot DESC);

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

-- Added with the three surface fixes. Idempotent, so an existing deployment
-- picks up the column without a rebuild.
ALTER TABLE equity_surface_diagnostics
    ADD COLUMN IF NOT EXISTS r_solved_raw DOUBLE PRECISION;
