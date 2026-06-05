-- =============================================================================
-- 05_bin_tables.sql — Precomputed bin tables consumed by the dashboard.
--
-- wf_bins:        walk-forward, per-row, WIDE.  One row per (ticker, trade_date)
--                 with two columns per eligible metric (frac_<m>, bin20_<m>).
--                 Per-metric columns are added by lib/bin_schema.py based on
--                 metric_classification (eligible_as_metric = TRUE).  Skeleton
--                 here is just the identity columns + PK.
--
-- tt_thresholds:  train-test, per-(metric, ticker, cutoff_date).  Stores the
--                 sorted pre-cutoff history per ticker so the dashboard can
--                 reproduce _bin_for_value (rank = bisect_left(history_vals,
--                 value); bin = min(int(rank/n_train * k) + 1, k); None if
--                 n_train < k) without recomputing.
-- =============================================================================

CREATE TABLE IF NOT EXISTS wf_bins (
    ticker     TEXT NOT NULL,
    trade_date DATE NOT NULL,
    PRIMARY KEY (ticker, trade_date)
);

CREATE TABLE IF NOT EXISTS tt_thresholds (
    metric       TEXT               NOT NULL,
    ticker       TEXT               NOT NULL,
    cutoff_date  DATE               NOT NULL,
    history_vals DOUBLE PRECISION[] NOT NULL,
    n_train      INTEGER            NOT NULL,
    PRIMARY KEY (metric, ticker, cutoff_date)
);

CREATE INDEX IF NOT EXISTS ix_tt_thresholds_ticker
    ON tt_thresholds (ticker, metric, cutoff_date);

-- is_bins: in-sample bin table.  Same shape as wf_bins; one row per
-- (ticker, trade_date) with frac_<metric> / bin20_<metric> per eligible metric.
-- Populated by build_bin_tables.py --tier {MORNING,EVENING}.
-- Per-metric column pairs are added dynamically by lib/bin_schema.sync_is_bins_schema.
CREATE TABLE IF NOT EXISTS is_bins (
    ticker     TEXT NOT NULL,
    trade_date DATE NOT NULL,
    PRIMARY KEY (ticker, trade_date)
);

-- tt_bins: train-test bin table.  Same wide shape as wf_bins / is_bins —
-- one row per (ticker, trade_date) with frac_<metric> / bin20_<metric>
-- per eligible metric (added dynamically by
-- lib/bin_schema.sync_tt_bins_schema).  Populated by
-- build_bin_tables.py --build-tt-bins.
--
-- Difference from wf_bins / is_bins:
--   * cutoff_date column, defaulted to 2024-01-01.  Single value across
--     the whole table; present per-row for self-documentation (dashboard
--     reads cutoff from any row, not from a hardcoded constant).
--   * No warmup.  bin20=0 / frac=NULL means NULL source value OR
--     (ticker, metric) had fewer than 500 valid pre-cutoff training rows.
--     Single sentinel meaning, matched to the wf/is convention so the
--     dashboard's existing frac/bin handling works identically.
--
-- Method: per (ticker, metric), the frozen ruler = sorted pre-cutoff
-- valid values; applied to BOTH train (pre-cutoff) and test (post-cutoff)
-- rows.  See lib/bin_compute.py:train_test_series.
CREATE TABLE IF NOT EXISTS tt_bins (
    ticker      TEXT NOT NULL,
    trade_date  DATE NOT NULL,
    cutoff_date DATE NOT NULL DEFAULT '2024-01-01',
    PRIMARY KEY (ticker, trade_date)
);
