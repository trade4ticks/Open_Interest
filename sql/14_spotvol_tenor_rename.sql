-- =============================================================================
-- spot-vol: expose the TENOR, keep the estimation window.
--
-- These columns always had two window dimensions and the name showed the wrong
-- one:
--
--     spotvol_beta_{TENOR}d_{WINDOW}
--                   |          `-- estimation window, 21td / 63td of daily
--                   |              observations. A sample-size choice, NOT a
--                   |              tenor. Keeps its period label to mark that.
--                   `------------- the ATM IV being explained. A real tenor.
--
-- The tenor was hardcoded to 30d and invisible. Short-dated ATM IV responds far
-- more strongly to spot than long-dated -- beta_7d typically runs 2-3x beta_90d
-- -- so a 7 DTE short-vega position sized off a 30d beta understates the vega
-- P&L of a gap by that factor.
--
-- RENAME, not recompute: the four existing columns ARE the 30d readings, so
-- their history carries forward untouched and only the other five tenors need
-- computing.
--
--     spotvol_beta_1m  ->  spotvol_beta_30d_1m
--     spotvol_r2_1m    ->  spotvol_r2_30d_1m
--     spotvol_beta_3m  ->  spotvol_beta_30d_3m
--     spotvol_r2_3m    ->  spotvol_r2_30d_3m
--
-- vov_30d_1m is NOT renamed. It already named its IV tenor first and its
-- estimation window second, which is the convention the others are moving to --
-- it was the one column in the family that had it right.
--
-- Applied BEFORE sync_metrics_schema (metrics_store.PRE_SYNC_SQL). Idempotent.
-- =============================================================================

DO $$
DECLARE
    tbl    TEXT;
    pair   TEXT[];
    pairs  TEXT[][] := ARRAY[
        ARRAY['spotvol_beta_1m', 'spotvol_beta_30d_1m'],
        ARRAY['spotvol_r2_1m',   'spotvol_r2_30d_1m'],
        ARRAY['spotvol_beta_3m', 'spotvol_beta_30d_3m'],
        ARRAY['spotvol_r2_3m',   'spotvol_r2_30d_3m']
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
