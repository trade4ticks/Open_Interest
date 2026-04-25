-- =============================================================================
-- Helper views for AI Explorer / ad-hoc queries.
--
-- These views read from the filtered surface (option_oi_surface) so the AI
-- only sees meaningful nodes. Drop and recreate is fine — they hold no state.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- v_oi_surface_latest
-- The full filtered surface for the most recent trade_date per ticker.
-- This is the table the AI should look at first to "see" the current OI surface.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_oi_surface_latest AS
WITH latest AS (
    SELECT ticker, MAX(trade_date) AS trade_date
    FROM option_oi_surface
    GROUP BY ticker
)
SELECT s.*
FROM option_oi_surface s
JOIN latest l USING (ticker, trade_date)
ORDER BY ticker, expiration, strike, right;

-- ---------------------------------------------------------------------------
-- v_oi_top_nodes_latest
-- Top 25 OI nodes per ticker on the most recent trade_date, with rank.
-- "Where is the volume concentrated right now?"
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_oi_top_nodes_latest AS
WITH latest AS (
    SELECT ticker, MAX(trade_date) AS trade_date
    FROM option_oi_surface
    GROUP BY ticker
),
ranked AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY s.ticker
            ORDER BY s.open_interest DESC
        ) AS oi_rank
    FROM option_oi_surface s
    JOIN latest l USING (ticker, trade_date)
)
SELECT *
FROM ranked
WHERE oi_rank <= 25
ORDER BY ticker, oi_rank;

-- ---------------------------------------------------------------------------
-- v_oi_changes_daily
-- Per (ticker, expiration, strike, right): OI today vs prior available
-- trade_date for that contract. Helps spot accumulation / unwinds.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_oi_changes_daily AS
SELECT
    s.ticker,
    s.trade_date,
    s.expiration,
    s.dte,
    s.strike,
    s.right,
    s.open_interest                                                            AS oi_today,
    LAG(s.open_interest) OVER w                                                AS oi_prev,
    s.open_interest - LAG(s.open_interest) OVER w                              AS oi_change,
    CASE
        WHEN LAG(s.open_interest) OVER w > 0 THEN
            (s.open_interest - LAG(s.open_interest) OVER w)::DOUBLE PRECISION
            / LAG(s.open_interest) OVER w
        ELSE NULL
    END                                                                        AS oi_pct_change,
    s.spot_close,
    s.moneyness
FROM option_oi_surface s
WINDOW w AS (
    PARTITION BY s.ticker, s.expiration, s.strike, s.right
    ORDER BY s.trade_date
);

-- ---------------------------------------------------------------------------
-- v_oi_concentration
-- Per (ticker, trade_date, expiration): what fraction of OI is on calls vs
-- puts, and how concentrated is the surface? Useful for clustering analysis.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_oi_concentration AS
SELECT
    ticker,
    trade_date,
    expiration,
    dte,
    SUM(open_interest)                                                AS total_oi,
    SUM(CASE WHEN right = 'C' THEN open_interest ELSE 0 END)          AS call_oi,
    SUM(CASE WHEN right = 'P' THEN open_interest ELSE 0 END)          AS put_oi,
    COUNT(*)                                                          AS n_strikes,
    MIN(strike)                                                       AS min_strike,
    MAX(strike)                                                       AS max_strike,
    AVG(spot_close)                                                   AS spot_close,
    -- OI-weighted average strike
    SUM(strike * open_interest)::DOUBLE PRECISION
        / NULLIF(SUM(open_interest), 0)                               AS oi_weighted_strike
FROM option_oi_surface
GROUP BY ticker, trade_date, expiration, dte;

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

-- ---------------------------------------------------------------------------
-- v_pin_candidates
-- Strikes near spot for the front (nearest-DTE) expiration, ranked by OI.
-- "Where might this thing pin into expiry?"
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_pin_candidates AS
WITH front_expiry AS (
    SELECT ticker, trade_date, MIN(expiration) AS front_expiration
    FROM option_oi_surface
    WHERE dte >= 0
    GROUP BY ticker, trade_date
)
SELECT
    s.ticker,
    s.trade_date,
    s.expiration,
    s.dte,
    s.strike,
    s.right,
    s.open_interest,
    s.spot_close,
    s.moneyness,
    ROW_NUMBER() OVER (
        PARTITION BY s.ticker, s.trade_date
        ORDER BY s.open_interest DESC
    ) AS rank_in_front
FROM option_oi_surface s
JOIN front_expiry f
  ON  f.ticker     = s.ticker
  AND f.trade_date = s.trade_date
  AND f.front_expiration = s.expiration
WHERE ABS(s.moneyness) <= 0.05
ORDER BY s.ticker, s.trade_date, s.open_interest DESC;
