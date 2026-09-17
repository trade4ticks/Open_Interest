-- =============================================================================
-- split_repairs — ledger of split events and the rebuild each one obliges.
--
-- See lib/split_repairs.py for why this exists and for the stage order.
--
-- One row per (ticker, split_date). Each stage column is stamped when that
-- stage completes for the ticker; NULL means outstanding, and the next
-- pipeline run retries it.
--
-- MIGRATION, NOT PART OF init_db.py — deliberately. The seed below is only
-- correct against an underlying_ohlc that already holds its history. On a fresh
-- database the seed inserts nothing, and the initial OHLC backfill would then
-- queue every historical split as new: a full repair of every ticker that has
-- ever split, on the first pipeline run.
-- =============================================================================

CREATE TABLE IF NOT EXISTS split_repairs (
    ticker              TEXT             NOT NULL,
    split_date          DATE             NOT NULL,
    -- As yfinance reports it: 4.0 for a 4:1 forward split, 0.125 for 1:8
    -- reverse, 1.054 for a spin-off adjustment.
    ratio               DOUBLE PRECISION NOT NULL,
    detected_at         TIMESTAMPTZ      NOT NULL DEFAULT now(),
    ohlc_refetched_at   TIMESTAMPTZ,
    features_rebuilt_at TIMESTAMPTZ,
    bins_rebuilt_at     TIMESTAMPTZ,
    paths_rebuilt_at    TIMESTAMPTZ,
    note                TEXT,
    PRIMARY KEY (ticker, split_date)
);

-- What a monitor should watch: a repair that has been outstanding for longer
-- than one full EVENING + MORNING cycle.
CREATE OR REPLACE VIEW split_repairs_outstanding AS
SELECT ticker, split_date, ratio, detected_at,
       ohlc_refetched_at, features_rebuilt_at, bins_rebuilt_at,
       paths_rebuilt_at, note
FROM split_repairs
WHERE ohlc_refetched_at IS NULL
   OR features_rebuilt_at IS NULL
   OR bins_rebuilt_at IS NULL
   OR paths_rebuilt_at IS NULL;

-- SEED. Every split already in underlying_ohlc is recorded as complete, so the
-- first run does not read the existing ~40 events as new.
--
-- Apply AFTER the manual CRWD / CVNA / FDX repair: this marks those three
-- complete as well. ON CONFLICT DO NOTHING means re-applying the file can never
-- reset a genuine in-flight repair.
--
-- The 1e-4 threshold matches lib/split_repairs.RATIO_TOL.
INSERT INTO split_repairs
    (ticker, split_date, ratio,
     ohlc_refetched_at, features_rebuilt_at, bins_rebuilt_at, paths_rebuilt_at,
     note)
SELECT ticker, trade_date, splits,
       now(), now(), now(), now(),
       'seeded: present in underlying_ohlc before the ledger existed'
FROM underlying_ohlc
WHERE splits IS NOT NULL
  AND splits <> 0
  AND abs(splits - 1) > 1e-4
ON CONFLICT (ticker, split_date) DO NOTHING;
