-- =============================================================================
-- Rebuild intraday_metrics as WIDE + DAILY-PARTITIONED, and add the rollup.
--   psql -d equities_scalp -f migrations/rebuild_intraday_metrics.sql
--
-- SAFE BECAUSE THE TABLE IS EMPTY. It was truncated when the root disk hit
-- 100%, so there is no data migration — this is a drop and recreate. If it
-- ever holds rows again, they are rebuildable from parquet inside
-- RAW_RETENTION_DAYS, which is the design assumption that makes a schema
-- change here cheap and makes daily_metrics' long format worth keeping.
--
-- WHY. Eleven days of long-format intraday at 232 metrics produced 32M rows
-- and 5,995 MB — 96% of the database — against daily_metrics' 1.2M rows and
-- 204 MB for the same period. Wide, an 18-metric subset, and daily partitions
-- bring that to 15,262 rows/day and ~3 MB.
--
-- Run init_schema() afterwards (any scalp script does it) to create the
-- partitions for the dates you are about to compute — or let compute.py do it,
-- which it does automatically for every day in its range.
-- =============================================================================

BEGIN;

DO $$
BEGIN
    IF to_regclass('public.intraday_metrics') IS NOT NULL THEN
        IF (SELECT count(*) FROM intraday_metrics) > 0 THEN
            RAISE EXCEPTION
                'intraday_metrics is not empty — refusing to drop it. '
                'Confirm the rows are rebuildable, then TRUNCATE first.';
        END IF;
    END IF;
END $$;

DROP TABLE IF EXISTS intraday_metrics;

COMMIT;

-- The parent, the rollup and fetch_runs are created by db.init_schema(), which
-- generates their columns from config.INTRADAY_COLUMNS so the pinned noise
-- definition cannot drift between the schema and the writer. Run:
--
--     python -c "from scalp import db; db.init_schema()"
--
-- Then rebuild 14 days of intraday and the August rollup:
--
--     python -m scalp.compute --start 2026-08-17 --end 2026-08-31
--
-- Aug 17 is included deliberately: it is one day outside the 14-day window
-- from Aug 31 and will be pruned on the next --intraday run, which is
-- preferable to a gap while the dashboard's Phase 3 is being built.
