-- =============================================================================
-- Compatibility views for the rv/vrp tenor rename.
--
-- Applied AFTER sync_metrics_schema, not with the renames in 11: on a fresh
-- database 09 creates only the key skeleton and every metric column arrives
-- from the registry, so a view naming rv_7d would fail against a table that
-- does not have it yet.
--
-- DROP then CREATE, never CREATE OR REPLACE. These views select m.*, which is
-- expanded once at creation; the next metric column added to the registry
-- would land in the middle of the output list and CREATE OR REPLACE refuses to
-- reorder view columns. Rebuilding outright keeps the shim in step with the
-- table. No CASCADE -- if something has come to depend on these, that should
-- surface as an error rather than be dropped quietly.
-- =============================================================================

DROP VIEW IF EXISTS equity_metrics_compat;
DROP VIEW IF EXISTS equity_metrics_z_compat;

-- The old names are gone from the tables, and anything with them hardcoded --
-- the dashboard's non-catalog SQL, a notebook, a saved query -- would break.
-- These expose the whole table plus the old names as aliases, so a reader
-- keeps working by changing only its FROM clause.
--
-- A VIEW rather than duplicate columns on purpose: an alias column would be a
-- second physical copy that can drift, needs writing, and would trip
-- check_catalog_drift() as an orphan. A view cannot diverge from what it
-- selects, costs no storage, and can be dropped the day the last reader moves
-- over. A migration aid with an expiry date, not part of the schema.

CREATE VIEW equity_metrics_compat AS
SELECT m.*,
       m.rv_7d          AS rv_1w,
       m.rv_park_7d     AS rv_park_1w,
       m.rv_gk_7d       AS rv_gk_1w,
       m.vrp_7d         AS vrp_1w,
       m.vrp_ratio_7d   AS vrp_ratio_1w,
       m.rv_30d         AS rv_1m,
       m.rv_park_30d    AS rv_park_1m,
       m.rv_gk_30d      AS rv_gk_1m,
       m.vrp_30d        AS vrp_1m,
       m.vrp_ratio_30d  AS vrp_ratio_1m,
       m.rv_90d         AS rv_3m,
       m.rv_park_90d    AS rv_park_3m,
       m.rv_gk_90d      AS rv_gk_3m,
       m.vrp_90d        AS vrp_3m,
       m.vrp_ratio_90d  AS vrp_ratio_3m,
       -- sql/13: log_ret / downside_semivol joined the tenor grid.
       m.log_ret_7d              AS log_ret_1w,
       m.log_ret_30d             AS log_ret_1m,
       m.downside_semivol_30d    AS downside_semivol_1m
FROM equity_metrics m;

COMMENT ON VIEW equity_metrics_compat IS
    'Deprecated-name shim for the rv/vrp tenor rename. rv_1w/rv_1m/rv_3m and '
    'their vrp siblings are aliases of rv_7d/rv_30d/rv_90d. Point readers at '
    'the real column names and drop this view.';

-- The z table needs the same shim: a reader of vrp_1m_z_252 is in exactly the
-- same position as a reader of vrp_1m.
CREATE VIEW equity_metrics_z_compat AS
SELECT z.*,
       z.rv_7d_z_63          AS rv_1w_z_63,
       z.rv_7d_z_252         AS rv_1w_z_252,
       z.rv_park_7d_z_63     AS rv_park_1w_z_63,
       z.rv_park_7d_z_252    AS rv_park_1w_z_252,
       z.rv_gk_7d_z_63       AS rv_gk_1w_z_63,
       z.rv_gk_7d_z_252      AS rv_gk_1w_z_252,
       z.vrp_7d_z_63         AS vrp_1w_z_63,
       z.vrp_7d_z_252        AS vrp_1w_z_252,
       z.vrp_ratio_7d_z_63   AS vrp_ratio_1w_z_63,
       z.vrp_ratio_7d_z_252  AS vrp_ratio_1w_z_252,
       z.rv_30d_z_63         AS rv_1m_z_63,
       z.rv_30d_z_252        AS rv_1m_z_252,
       z.rv_park_30d_z_63    AS rv_park_1m_z_63,
       z.rv_park_30d_z_252   AS rv_park_1m_z_252,
       z.rv_gk_30d_z_63      AS rv_gk_1m_z_63,
       z.rv_gk_30d_z_252     AS rv_gk_1m_z_252,
       z.vrp_30d_z_63        AS vrp_1m_z_63,
       z.vrp_30d_z_252       AS vrp_1m_z_252,
       z.vrp_ratio_30d_z_63  AS vrp_ratio_1m_z_63,
       z.vrp_ratio_30d_z_252 AS vrp_ratio_1m_z_252,
       z.rv_90d_z_63         AS rv_3m_z_63,
       z.rv_90d_z_252        AS rv_3m_z_252,
       z.rv_park_90d_z_63    AS rv_park_3m_z_63,
       z.rv_park_90d_z_252   AS rv_park_3m_z_252,
       z.rv_gk_90d_z_63      AS rv_gk_3m_z_63,
       z.rv_gk_90d_z_252     AS rv_gk_3m_z_252,
       z.vrp_90d_z_63        AS vrp_3m_z_63,
       z.vrp_90d_z_252       AS vrp_3m_z_252,
       z.vrp_ratio_90d_z_63  AS vrp_ratio_3m_z_63,
       z.vrp_ratio_90d_z_252 AS vrp_ratio_3m_z_252,
       z.log_ret_7d_z_63             AS log_ret_1w_z_63,
       z.log_ret_7d_z_252            AS log_ret_1w_z_252,
       z.log_ret_30d_z_63            AS log_ret_1m_z_63,
       z.log_ret_30d_z_252           AS log_ret_1m_z_252,
       z.downside_semivol_30d_z_63   AS downside_semivol_1m_z_63,
       z.downside_semivol_30d_z_252  AS downside_semivol_1m_z_252
FROM equity_metrics_z z;

COMMENT ON VIEW equity_metrics_z_compat IS
    'Deprecated-name shim for the rv/vrp tenor rename, z variants. See '
    'equity_metrics_compat.';
