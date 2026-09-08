-- =============================================================================
-- Rebuild intraday_metrics as WIDE + DAILY-PARTITIONED, and add the rollup.
--   psql -d equities_scalp -f migrations/rebuild_intraday_metrics.sql
--
-- YOU PROBABLY DO NOT NEED THIS FOR AN ADDED METRIC. db.init_schema() now
-- reconciles the column set on every run — it emits ADD COLUMN IF NOT EXISTS
-- for every entry in config.INTRADAY_COLUMNS, against both intraday_metrics
-- and intraday_monthly, and then verifies that none is still missing. Adding
-- a metric is therefore:
--
--     python -c "from scalp import db; db.init_schema()"
--
-- and nothing else. No drop, no truncate, no data loss.
--
-- THAT RECONCILIATION DID NOT EXIST WHEN THE QUIET-WINDOW COLUMNS LANDED, and
-- the cost of its absence is the reason this header now leads with it.
-- CREATE TABLE IF NOT EXISTS does nothing to a table that already exists, so
-- init_schema silently no-opped, every symbol-day computed correctly, and each
-- one then failed in the INSERT with
--
--     UndefinedColumn: column "quiet_windows_60s_10" of relation
--                      "intraday_metrics" does not exist
--
-- Five workers, 100% CPU, 32 minutes, one row written, no output.
--
-- WHEN THIS FILE IS STILL THE RIGHT TOOL
--   * a column TYPE changes (ADD COLUMN cannot retype)
--   * a column must genuinely be REMOVED rather than left holding history
--   * the partitioning or primary key changes
--   * the stored table has drifted far enough that a clean shape is wanted
--
-- Renaming a metric — the noise pin moving from _rms to _p75 — needs neither:
-- the new column is added, the old one keeps the history it already has, and
-- intraday_monthly retains that history indefinitely, which is the point.
--
-- THE GUARD BELOW REFUSES TO RUN ON A NON-EMPTY TABLE. That is deliberate,
-- and it means a real rebuild is a TWO-STEP operation. Do not discover this
-- from the exception:
--
--     -- 1. confirm the rows are rebuildable: every date in intraday_metrics
--     --    must still be inside RAW_RETENTION_DAYS (45) in the parquet store,
--     --    or it cannot be recomputed and this is data loss.
--     SELECT min(trade_date), max(trade_date), count(*) FROM intraday_metrics;
--
--     -- 2. then, and only then:
--     TRUNCATE intraday_metrics;
--     \i migrations/rebuild_intraday_metrics.sql
--
-- intraday_monthly is NOT truncated by any of this, and must not be: it is
-- kept indefinitely and can only be built from intraday rows still inside
-- retention. A month dropped here can never be rebuilt.
--
-- WHY WIDE AND PARTITIONED. Eleven days of long-format intraday at 232 metrics
-- produced 32M rows and 5,995 MB — 96% of the database — against
-- daily_metrics' 1.2M rows and 204 MB for the same period. Wide, a subset of
-- the metric set, and daily partitions bring that to 15,262 rows/day and ~3 MB.
-- =============================================================================

BEGIN;

DO $$
BEGIN
    IF to_regclass('public.intraday_metrics') IS NOT NULL THEN
        IF (SELECT count(*) FROM intraday_metrics) > 0 THEN
            RAISE EXCEPTION
                'intraday_metrics is not empty — refusing to drop it. %',
                'Confirm every stored date is still inside RAW_RETENTION_DAYS '
                'in the parquet store, then TRUNCATE intraday_metrics and '
                're-run this file. See the header for the two-step form.';
        END IF;
    END IF;
END $$;

DROP TABLE IF EXISTS intraday_metrics;

COMMIT;

-- The parent, the rollup and fetch_runs are all created by db.init_schema(),
-- which GENERATES their columns from config.INTRADAY_COLUMNS. That is why this
-- file contains no column list of its own: a list here would be a second copy
-- of the metric set, free to drift from the writer, which is exactly the
-- failure this migration's header describes.
--
--     python -c "from scalp import db; db.init_schema()"
--
-- Then rebuild the intraday window and the month's rollup:
--
--     python -m scalp.compute --start 2026-08-17 --end 2026-08-31
--
-- Aug 17 is included deliberately: it is one day outside the 14-day window
-- from Aug 31 and will be pruned on the next --intraday run, which is
-- preferable to a gap while the dashboard's Phase 3 is being built.
