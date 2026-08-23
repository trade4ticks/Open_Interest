-- =============================================================================
-- 09_equity_metrics.sql — derived vol metrics off the interpolated surface
-- (pipeline stage 4: fetch -> clean -> interpolate -> metrics).
--
-- equity_metrics          one row per (ticker, trade_date, snapshot)
-- equity_metrics_z        same key, PK-joined; rolling z-scores
-- equity_metrics_catalog  one row per column, driving the dashboard's picker
--
-- THESE ARE SKELETONS. Only the key columns live here. The ~600 metric columns
-- are added idempotently by lib/metrics_store.sync_metrics_schema() from
-- lib/metrics_config.py, exactly as sql/07_trade_paths.sql and
-- lib/trade_path_schema.py do for the per-rule columns. Defining them here as
-- well would put the same list in two places and let them drift silently — and
-- a drifted metric column is not an error, it is a permanently NULL column.
--
-- WHY equity_metrics_z IS A SEPARATE TABLE
-- The z window is the most likely thing to change: adding a 126-day window, or
-- excluding low-quality days from the baseline. Isolating it makes that a
-- TRUNCATE-and-recompute instead of an UPDATE across 370 columns.
--
-- Both fact tables are monthly RANGE partitions on trade_date, matching
-- equity_surface. A partitioned table's primary key must contain the partition
-- key; (ticker, trade_date, snapshot) does.
-- =============================================================================

CREATE TABLE IF NOT EXISTS equity_metrics (
    ticker     TEXT NOT NULL,
    trade_date DATE NOT NULL,
    snapshot   TEXT NOT NULL,
    built_at   TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, trade_date, snapshot)
) PARTITION BY RANGE (trade_date);

CREATE TABLE IF NOT EXISTS equity_metrics_z (
    ticker     TEXT NOT NULL,
    trade_date DATE NOT NULL,
    snapshot   TEXT NOT NULL,
    built_at   TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, trade_date, snapshot)
) PARTITION BY RANGE (trade_date);

-- The scanner's access pattern is "rank every ticker on one metric at one
-- (date, snapshot)", which is the opposite of the PK's leading column.
CREATE INDEX IF NOT EXISTS ix_equity_metrics_scan
    ON equity_metrics (trade_date, snapshot);
CREATE INDEX IF NOT EXISTS ix_equity_metrics_z_scan
    ON equity_metrics_z (trade_date, snapshot);

-- --- Catalog ----------------------------------------------------------------
-- Not partitioned and not keyed on a date: it describes columns, not rows.
-- Regenerated from lib/metrics_config.py on every run, with a drift check
-- against information_schema that fails loudly rather than serving a picker
-- entry for a column that does not exist.
CREATE TABLE IF NOT EXISTS equity_metrics_catalog (
    column_name TEXT PRIMARY KEY,
    table_name  TEXT NOT NULL,      -- equity_metrics | equity_metrics_z
    family      TEXT NOT NULL,      -- level_iv, skew, structure, quality, ...
    tenor       SMALLINT,           -- NULL for term-structure and OHLC metrics
    wing        TEXT,               -- '10p', '25p_atm', 'short', ...
    form        TEXT NOT NULL,      -- 'base' | 'z_63' | 'z_252'
    -- Points at itself for base rows, so GROUP BY base_column returns a metric
    -- together with its z variants — the grouping a metric picker wants.
    base_column TEXT NOT NULL,
    -- Drives rendering: 'vol_decimal' as a percentage, 'ratio' as a number,
    -- 'z_score' on a diverging scale, 'bool' as a flag.
    units       TEXT NOT NULL,
    description TEXT,
    formula     TEXT
);

CREATE INDEX IF NOT EXISTS ix_equity_metrics_catalog_family
    ON equity_metrics_catalog (family, tenor);

-- --- Partition helpers ------------------------------------------------------
-- Idempotent, and tolerant of a concurrent worker creating the same child.

CREATE OR REPLACE FUNCTION ensure_equity_metrics_partition(d DATE)
RETURNS VOID AS $$
DECLARE
    month_start DATE := date_trunc('month', d)::DATE;
    month_end   DATE := (date_trunc('month', d) + INTERVAL '1 month')::DATE;
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF equity_metrics '
        'FOR VALUES FROM (%L) TO (%L)',
        'equity_metrics_' || to_char(month_start, 'YYYYMM'),
        month_start, month_end);
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF equity_metrics_z '
        'FOR VALUES FROM (%L) TO (%L)',
        'equity_metrics_z_' || to_char(month_start, 'YYYYMM'),
        month_start, month_end);
EXCEPTION WHEN duplicate_table OR invalid_object_definition THEN
    NULL;
END;
$$ LANGUAGE plpgsql;
