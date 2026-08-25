-- =============================================================================
-- log_ret / downside_semivol: period labels -> tenor labels.
--
-- RENAME, not recompute, for the same reason as sql/11: these three windows
-- already held exactly the right numbers under the old names.
--
--     log_ret_1w           5 trading days  ->  log_ret_7d
--     log_ret_1m          21 trading days  ->  log_ret_30d
--     downside_semivol_1m 21 trading days  ->  downside_semivol_30d
--
-- log_ret_d is NOT renamed and NOT extended. A one-day return has no tenor
-- analogue — TENORS starts at 7 and there is no 1-calendar-day option tenor to
-- pair it with — so it stays a fixed quantity inside a tenor-bearing family.
--
-- Applied BEFORE sync_metrics_schema (see metrics_store.PRE_SYNC_SQL): sync
-- only ever ADDs, so running it first would create log_ret_7d empty, this
-- rename would then skip, and log_ret_1w would be orphaned holding the history.
--
-- Idempotent: each rename fires only if the old name is present and the new one
-- is not.
-- =============================================================================

DO $$
DECLARE
    tbl    TEXT;
    pair   TEXT[];
    pairs  TEXT[][] := ARRAY[
        ARRAY['log_ret_1w',          'log_ret_7d'],
        ARRAY['log_ret_1m',          'log_ret_30d'],
        ARRAY['downside_semivol_1m', 'downside_semivol_30d']
    ];
    suffix TEXT;
    oldn   TEXT;
    newn   TEXT;
BEGIN
    FOREACH pair SLICE 1 IN ARRAY pairs LOOP
        FOR tbl, suffix IN
            SELECT 'equity_metrics', ''
            UNION ALL SELECT 'equity_metrics_z', '_z_63'
            UNION ALL SELECT 'equity_metrics_z', '_z_252'
        LOOP
            oldn := pair[1] || suffix;
            newn := pair[2] || suffix;
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
