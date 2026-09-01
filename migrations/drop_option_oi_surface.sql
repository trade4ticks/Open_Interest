-- =============================================================================
-- Drop option_oi_surface and the five views that read it.
-- Run against the options database:  psql -d open_interest -f drop_surface.sql
--
-- Dependency order: views first, then the table. Each view is a plain
-- dependency on the table, so dropping the table first would fail without
-- CASCADE — and CASCADE would drop the views silently, which is the same
-- outcome achieved without a record of what went.
--
-- Verified before writing this:
--   * 15,303,161 rows, same grain as option_oi_raw (filtered, not aggregated)
--   * reconstructible from the raw parquet store, which is a superset
--   * the filter predicate is recorded in migrations/README.md
--   * no Python in the repo queries any of these five views
-- =============================================================================

BEGIN;

-- Sanity check: fails loudly if this is run against the wrong database.
DO $$
BEGIN
    IF to_regclass('public.option_oi_surface') IS NULL THEN
        RAISE EXCEPTION 'option_oi_surface does not exist here — wrong database?';
    END IF;
END $$;

-- Report what is about to go, so the transcript records it.
SELECT count(*) AS rows_about_to_be_dropped FROM option_oi_surface;
SELECT pg_size_pretty(pg_total_relation_size('option_oi_surface')) AS size_to_reclaim;

-- 1. The five dependent views.
DROP VIEW IF EXISTS v_pin_candidates;
DROP VIEW IF EXISTS v_oi_concentration;
DROP VIEW IF EXISTS v_oi_changes_daily;
DROP VIEW IF EXISTS v_oi_top_nodes_latest;
DROP VIEW IF EXISTS v_oi_surface_latest;

-- 2. The table. RESTRICT is the default and is what we want: if anything
--    still depends on it that this script did not account for, this aborts
--    the whole transaction rather than dropping it too.
DROP TABLE option_oi_surface RESTRICT;

COMMIT;

-- Space is returned to the filesystem immediately on DROP TABLE; no VACUUM
-- FULL is needed. v_features_with_returns is untouched — it reads
-- daily_features.
