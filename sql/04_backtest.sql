-- Backtest results: one row per (ticker, trade_date, exit_date) trade.
-- Re-running the backtest script upserts / overwrites existing rows.

CREATE TABLE IF NOT EXISTS backtest_call_spread (
    id                      SERIAL PRIMARY KEY,

    -- Input fields from trade CSV
    ticker                  TEXT             NOT NULL,
    trade_date              DATE             NOT NULL,
    exit_date               DATE             NOT NULL,
    fired_systems           TEXT,
    spot_entry_open         DOUBLE PRECISION,    -- CSV open price (proxy for 9:35 spot)
    spot_exit_close         DOUBLE PRECISION,    -- CSV close price (reference only)

    -- Selected expiration (soonest on or after exit_date)
    expiration              DATE,

    -- Entry legs (09:35 on trade_date)
    long_strike             DOUBLE PRECISION,
    short_strike            DOUBLE PRECISION,
    long_entry_bid          DOUBLE PRECISION,
    long_entry_ask          DOUBLE PRECISION,
    long_entry_mid          DOUBLE PRECISION,
    long_entry_spread       DOUBLE PRECISION,
    short_entry_bid         DOUBLE PRECISION,
    short_entry_ask         DOUBLE PRECISION,
    short_entry_mid         DOUBLE PRECISION,
    short_entry_spread      DOUBLE PRECISION,
    net_entry_debit         DOUBLE PRECISION,    -- long_mid - short_mid (cost to open)

    -- Exit legs (15:30 on exit_date)
    long_exit_bid           DOUBLE PRECISION,
    long_exit_ask           DOUBLE PRECISION,
    long_exit_mid           DOUBLE PRECISION,
    long_exit_spread        DOUBLE PRECISION,
    short_exit_bid          DOUBLE PRECISION,
    short_exit_ask          DOUBLE PRECISION,
    short_exit_mid          DOUBLE PRECISION,
    short_exit_spread       DOUBLE PRECISION,
    net_exit_value          DOUBLE PRECISION,    -- long_mid - short_mid at exit

    -- Position sizing
    max_risk_per_contract   DOUBLE PRECISION,    -- net_entry_debit * 100
    max_profit_per_contract DOUBLE PRECISION,    -- (short - long - net_entry_debit) * 100
    qty                     INTEGER,
    capital_deployed        DOUBLE PRECISION,    -- net_entry_debit * qty * 100

    -- Results
    total_pnl               DOUBLE PRECISION,
    pnl_pct                 DOUBLE PRECISION,

    -- Outcome tracking
    status                  TEXT,                -- 'ok','no_expiration','no_entry_data',
                                                 -- 'insufficient_strikes','no_exit_data',
                                                 -- 'invalid_entry_debit','api_error'
    created_at              TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (ticker, trade_date, exit_date)
);
