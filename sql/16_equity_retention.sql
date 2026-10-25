-- =============================================================================
-- 16_equity_retention.sql — ledger of equity_surface partitions slimmed by
-- prune_equity_surface.py.
--
-- One row per monthly partition that holds only RETAINED_SNAPSHOTS
-- ('0945', '1545'). Two methods:
--
--   'swap'          prune_equity_surface.py rewrote it: copy the kept rows,
--                   then detach / drop / rename / attach in one transaction.
--   'already_slim'  scanned and found no intraday rows — pre-2026-08 months
--                   that never had any, or a partition slimmed by hand.
--
-- READ BY THE BUILDERS, not just written. build_equity_metrics.py batch skips
-- any (trade_date, snapshot) inside a logged range whose snapshot is not
-- retained: with the surface gone and equity_atm still present,
-- compute_metrics returns an ATM-only row, and the upsert would overwrite a
-- good metrics row with NULL skew. build_equity_surface.py refuses
-- --source intraday into a logged range for the same reason in reverse.
--
-- Applied by `prune_equity_surface.py --delete` (CREATE IF NOT EXISTS) and by
-- init_db.py. A missing table reads as "nothing slimmed".
-- =============================================================================

CREATE TABLE IF NOT EXISTS equity_retention_log (
    partition_name TEXT        PRIMARY KEY,
    range_start    DATE        NOT NULL,
    range_end      DATE        NOT NULL,          -- exclusive, as the bound is
    method         TEXT        NOT NULL,          -- 'swap' | 'already_slim'
    slimmed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    rows_before    BIGINT,
    rows_after     BIGINT,
    bytes_before   BIGINT,
    bytes_after    BIGINT
);
