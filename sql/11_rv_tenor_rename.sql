-- =============================================================================
-- Realized-vol / VRP family: period labels -> tenor labels.
--
-- RENAME, not recompute. Three of the six windows already existed under period
-- names, and they are the SAME arithmetic on the SAME closes:
--
--     rv_1w   5 trading days, measured against the  7d ATM IV  ->  rv_7d
--     rv_1m  21 trading days, measured against the 30d ATM IV  ->  rv_30d
--     rv_3m  63 trading days, measured against the 90d ATM IV  ->  rv_90d
--
-- So renaming carries the whole history forward untouched, and only the three
-- genuinely new windows (14d/10td, 21d/15td, 60d/42td) need a rebuild. Adding
-- rv_30d as a new column and recomputing it instead would have thrown away
-- months of identical values for nothing.
--
-- vrp_1m and vrp_30d were always the same number — 30-day implied against 21
-- trading days of realized — which is why this renames rather than keeping
-- both. Two columns holding one number under two names is the duplication
-- pattern that has caused problems here before.
--
-- ALTER TABLE ... RENAME COLUMN on a partitioned parent cascades to every
-- existing partition, so this is safe against a table already holding history.
--
-- Idempotent: each rename fires only if the old name is still present and the
-- new one is not, so re-running is a no-op and a half-applied migration
-- finishes cleanly.
-- =============================================================================

DO $$
DECLARE
    tbl   TEXT;
    pair  TEXT[];
    pairs TEXT[][] := ARRAY[
        -- 1w -> 7d  (5 trading days)
        ARRAY['rv_1w',        'rv_7d'],
        ARRAY['rv_park_1w',   'rv_park_7d'],
        ARRAY['rv_gk_1w',     'rv_gk_7d'],
        ARRAY['vrp_1w',       'vrp_7d'],
        ARRAY['vrp_ratio_1w', 'vrp_ratio_7d'],
        -- 1m -> 30d (21 trading days)
        ARRAY['rv_1m',        'rv_30d'],
        ARRAY['rv_park_1m',   'rv_park_30d'],
        ARRAY['rv_gk_1m',     'rv_gk_30d'],
        ARRAY['vrp_1m',       'vrp_30d'],
        ARRAY['vrp_ratio_1m', 'vrp_ratio_30d'],
        -- 3m -> 90d (63 trading days)
        ARRAY['rv_3m',        'rv_90d'],
        ARRAY['rv_park_3m',   'rv_park_90d'],
        ARRAY['rv_gk_3m',     'rv_gk_90d'],
        ARRAY['vrp_3m',       'vrp_90d'],
        ARRAY['vrp_ratio_3m', 'vrp_ratio_90d']
    ];
    suffix TEXT;
    oldn   TEXT;
    newn   TEXT;
BEGIN
    FOREACH pair SLICE 1 IN ARRAY pairs LOOP
        -- equity_metrics holds the base column; equity_metrics_z holds one
        -- column per z window, named <base>_z_<window>. Both must move
        -- together or check_catalog_drift() fails on the half that did not.
        FOR tbl, suffix IN
            SELECT 'equity_metrics', ''
            UNION ALL SELECT 'equity_metrics_z', '_z_63'
            UNION ALL SELECT 'equity_metrics_z', '_z_252'
        LOOP
            oldn := pair[1] || suffix;
            newn := pair[2] || suffix;
            -- pg_attribute via to_regclass rather than
            -- information_schema.columns: the latter matches on bare
            -- table_name across every schema on the search_path, so a
            -- same-named table elsewhere would answer for this one.
            IF EXISTS (SELECT 1 FROM pg_attribute
                       WHERE attrelid = to_regclass(tbl) AND attname = oldn
                         AND attnum > 0 AND NOT attisdropped)
               AND NOT EXISTS (SELECT 1 FROM pg_attribute
                               WHERE attrelid = to_regclass(tbl)
                                 AND attname = newn
                                 AND attnum > 0 AND NOT attisdropped)
            THEN
                EXECUTE format('ALTER TABLE %I RENAME COLUMN %I TO %I',
                               tbl, oldn, newn);
                RAISE NOTICE 'renamed %.% -> %', tbl, oldn, newn;
            END IF;
        END LOOP;
    END LOOP;
END $$;
