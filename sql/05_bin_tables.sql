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
