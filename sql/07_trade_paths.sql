-- =============================================================================
-- 07_trade_paths.sql — precomputed exit outcomes for every possible entry.
--
-- trade_paths:      one row per (ticker, trade_date, entry_anchor), WIDE, with
--                   two columns per exit rule: xb_<rule> (the bar the rule
--                   fired) and xr_<rule> (the return actually realised at that
--                   exit). Per-rule columns are added by
--                   lib/trade_path_schema.py from the registry in
--                   lib/trade_path_rules.py — the skeleton here is identity +
--                   entry context only, exactly as wf_bins/is_bins do it.
--
--                   entry_anchor is in the KEY, not in column names, so adding
--                   an anchor later is new rows rather than a migration.
--
-- trade_path_rules: catalog of the registry, so the dashboard builds column
--                   names from data instead of hardcoding 118 strings. The
--                   same registry drives the compute and the UI.
--
-- WHY BOTH xb_ AND xr_ ARE STORED
--   The return is NOT derivable from the rule's price level. If a bar opens
--   past the stop, the fill is the OPEN, not the stop — and on a multi-day
--   hold that gap-through difference is the single largest source of error.
--   The dashboard takes min() over the xb_ columns and reads the xr_ belonging
--   to whichever rule won.
--
-- NULL SEMANTICS
--   xb_<rule> IS NULL means that rule never fired inside the path horizon.
--   Postgres LEAST ignores NULLs, which is the desired behaviour, and
--   lib.trade_path_rules.build_combine_sql always appends the horizon rule so
--   a selection whose rules all miss can never yield a trade with no exit.
-- =============================================================================

CREATE TABLE IF NOT EXISTS trade_paths (
    ticker        TEXT     NOT NULL,
    trade_date    DATE     NOT NULL,
    entry_anchor  TEXT     NOT NULL,

    -- Entry context. Split-adjusted, in the same basis as the path itself.
    entry_price   REAL,
    entry_bar_ts  TIMESTAMP,          -- naive ET, matching the equity_1min store
    atr_14d       REAL,               -- ATR(14) over sessions T-14..T-1
    swing_low_1   REAL,
    swing_low_3   REAL,
    swing_low_5   REAL,

    -- Path extent. A path whose 20-session horizon runs past the end of
    -- available data is NOT resolved; its horizon exit is genuinely unknown
    -- and must not be mixed into realised statistics.
    n_bars        INTEGER,
    n_sessions    SMALLINT,
    path_status   TEXT     NOT NULL DEFAULT 'ok',   -- ok | truncated | no_minute_data

    built_at      TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, trade_date, entry_anchor)
);

CREATE INDEX IF NOT EXISTS ix_trade_paths_date
    ON trade_paths (trade_date, entry_anchor);
CREATE INDEX IF NOT EXISTS ix_trade_paths_status
    ON trade_paths (path_status);

CREATE TABLE IF NOT EXISTS trade_path_rules (
    rule_key        TEXT PRIMARY KEY,
    family          TEXT    NOT NULL,
    side            TEXT    NOT NULL,   -- stop | target | time | trend
    fill_mode       TEXT    NOT NULL,   -- stop | target | close
    params          JSONB   NOT NULL,
    exit_bar_col    TEXT    NOT NULL,
    exit_return_col TEXT    NOT NULL,
    is_horizon      BOOLEAN NOT NULL DEFAULT FALSE
);

-- Per-ticker build bookkeeping. Resumability is keyed at the unit a failure
-- actually occurs at: one ticker's paths are computed and written together, so
-- a ticker is the thing that succeeds or fails.
CREATE TABLE IF NOT EXISTS trade_paths_manifest (
    ticker       TEXT NOT NULL,
    entry_anchor TEXT NOT NULL,
    status       TEXT NOT NULL,          -- ok | failed | no_minute_data
    n_entries    INTEGER,
    n_resolved   INTEGER,
    built_at     TIMESTAMP NOT NULL DEFAULT now(),
    note         TEXT,
    PRIMARY KEY (ticker, entry_anchor)
);
