-- =============================================================================
-- Helper views for AI Explorer / ad-hoc queries.
--
-- We DROP IF EXISTS up front because CREATE OR REPLACE VIEW in Postgres
-- cannot change a view's column NAMES (only types/positions). Whenever the
-- underlying tables get a column rename or `f.*` resolves differently,
-- CREATE OR REPLACE fails with "cannot change name of view column ..." —
-- DROP+CREATE sidesteps that.
--
-- REMOVED 2026-09-01, together with option_oi_surface: v_oi_surface_latest,
-- v_oi_top_nodes_latest, v_oi_changes_daily, v_oi_concentration and
-- v_pin_candidates. All five read the surface table and would have errored on
-- use once it was dropped.
--
-- That was a CAPABILITY, not only storage. Those five were the per-node view
-- of the OI surface, and daily_features holds derived metrics rather than
-- nodes, so nothing currently replaces them. Rebuilding means pointing them
-- at the parquet store through DuckDB; the filter that produced the surface
-- is recorded verbatim in migrations/README.md so that stays possible.
--
-- v_features_with_returns reads daily_features and is unaffected.
-- =============================================================================

DROP VIEW IF EXISTS v_features_with_returns CASCADE;

-- Dropped alongside option_oi_surface. Kept as statements so re-running this
-- file also cleans a database created before 2026-09-01.
DROP VIEW IF EXISTS v_pin_candidates      CASCADE;
DROP VIEW IF EXISTS v_oi_concentration    CASCADE;
DROP VIEW IF EXISTS v_oi_changes_daily    CASCADE;
DROP VIEW IF EXISTS v_oi_top_nodes_latest CASCADE;
DROP VIEW IF EXISTS v_oi_surface_latest   CASCADE;

-- ---------------------------------------------------------------------------
-- v_features_with_returns
-- daily_features joined with realised forward returns from underlying_ohlc.
-- The forward-return columns inside daily_features are populated by
-- build_features.py; this view also exposes them inline with spot_close so
-- the AI can correlate features against future moves in one query.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_features_with_returns AS
SELECT
    f.*,
    o.close       AS close_today,
    o.adj_close   AS adj_close_today
FROM daily_features f
LEFT JOIN underlying_ohlc o USING (ticker, trade_date)
ORDER BY f.ticker, f.trade_date;
