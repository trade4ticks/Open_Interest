-- ---------------------------------------------------------------------------
-- 06_25d_skew_metrics.sql — Phase 2 of the 25-delta skew metric enable.
--
-- Adds the 7d skew column set to daily_features (the 30d set already exists
-- in sql/03_new_metrics.sql) and flips metric_classification.eligible_as_metric
-- to TRUE for all 14 metrics (7 new 7d + 7 formerly dormant 30d) so the
-- downstream bin_schema sync picks them up.
--
-- Run via init_db.py — idempotent (uses IF NOT EXISTS for columns and a
-- conditional INSERT + UPDATE for the metric_classification rows, so reruns
-- are safe).  Order in init_db.py's FILES list places this AFTER
-- 05_bin_tables.sql so the wf_bins / is_bins / tt_bins skeletons already
-- exist when bin_schema.sync runs immediately after.
-- ---------------------------------------------------------------------------

-- 7d skew columns — DOUBLE PRECISION, mirrors the existing 30d types in
-- sql/03_new_metrics.sql.
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS iv_25d_call_7d    DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS iv_25d_put_7d     DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS rr_25d_7d         DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS bf_25d_7d         DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS skew_25p_atm_7d   DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS skew_atm_25c_7d   DOUBLE PRECISION;
ALTER TABLE daily_features ADD COLUMN IF NOT EXISTS zscore_rr_25d_7d  DOUBLE PRECISION;


-- metric_classification flag flips for both tenor sets.
--
-- All 14 metrics belong to Family 13 ("Implied volatility chain") per the
-- data dictionary and are EVENING-tier (no OI dependency).  `metric` is the
-- table's PRIMARY KEY so ON CONFLICT (metric) DO UPDATE handles the split
-- cleanly in a single statement:
--   - 7 new 7d rows get full INSERTs (every NOT-NULL column populated).
--   - 7 existing 30d rows (eligible_as_metric currently FALSE) get the flag
--     flipped to TRUE via the ON CONFLICT DO UPDATE branch.
--
-- DO UPDATE only sets eligible_as_metric — leaves family_num / family_name
-- / tier untouched on existing rows so we never clobber the source-of-truth
-- classification.  The INSERT values for the 30d rows match what's already
-- in the table (family_num=13, etc.), so even if a row somehow re-routed
-- through the INSERT path the contents would be identical.
--
-- updated_at is left to its DEFAULT now() on new rows; not in the SET
-- clause so existing-row timestamps are preserved.  If you want every
-- touched row to record now() as its update time, add updated_at = now()
-- to the SET clause.
INSERT INTO metric_classification
    (metric, family_num, family_name, tier, eligible_as_metric)
VALUES
    -- 7d skew metrics (NEW this release — full INSERT path).
    ('iv_25d_call_7d',     13, 'Implied volatility chain', 'EVENING', TRUE),
    ('iv_25d_put_7d',      13, 'Implied volatility chain', 'EVENING', TRUE),
    ('rr_25d_7d',          13, 'Implied volatility chain', 'EVENING', TRUE),
    ('bf_25d_7d',          13, 'Implied volatility chain', 'EVENING', TRUE),
    ('skew_25p_atm_7d',    13, 'Implied volatility chain', 'EVENING', TRUE),
    ('skew_atm_25c_7d',    13, 'Implied volatility chain', 'EVENING', TRUE),
    ('zscore_rr_25d_7d',   13, 'Implied volatility chain', 'EVENING', TRUE),
    -- 30d skew metrics (rows already exist with eligible_as_metric=FALSE;
    -- ON CONFLICT branch flips them to TRUE).
    ('iv_25d_call_30d',    13, 'Implied volatility chain', 'EVENING', TRUE),
    ('iv_25d_put_30d',     13, 'Implied volatility chain', 'EVENING', TRUE),
    ('rr_25d_30d',         13, 'Implied volatility chain', 'EVENING', TRUE),
    ('bf_25d_30d',         13, 'Implied volatility chain', 'EVENING', TRUE),
    ('skew_25p_atm_30d',   13, 'Implied volatility chain', 'EVENING', TRUE),
    ('skew_atm_25c_30d',   13, 'Implied volatility chain', 'EVENING', TRUE),
    ('zscore_rr_25d_30d',  13, 'Implied volatility chain', 'EVENING', TRUE)
ON CONFLICT (metric) DO UPDATE SET
    eligible_as_metric = EXCLUDED.eligible_as_metric;
