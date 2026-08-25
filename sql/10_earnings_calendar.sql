-- =============================================================================
-- Earnings calendar, from yfinance get_earnings_dates().
--
-- Two tables, because "this fund has no earnings" and "this fetch failed" must
-- not look alike. 28 of the 121 tickers are ETFs and legitimately return
-- nothing; without an explicit marker a nightly monitor would flag them as
-- broken every single night and the alert would be trained away.
--
--   earnings_calendar   one row per (ticker, earnings_date)
--   earnings_coverage   one row per ticker: did it have earnings, when was it
--                       last checked, and what happened
-- =============================================================================

CREATE TABLE IF NOT EXISTS earnings_calendar (
    ticker           TEXT NOT NULL,
    -- The ET calendar date. Yahoo returns a tz-aware timestamp; the date is
    -- what days_to_earnings counts to, the timestamp below keeps the rest.
    earnings_date    DATE NOT NULL,
    -- Full stamp, preserved because the HOUR carries the session convention:
    -- 16:00 is after the close, 06:00-09:00 is before the open. An after-close
    -- report on day D moves D+1's open; a before-open report on D moves D's.
    -- days_to_earnings does not use this yet, but discarding it would make
    -- that refinement a refetch rather than a recompute.
    earnings_ts      TIMESTAMPTZ,
    -- 'amc' (after market close), 'bmo' (before market open), 'unknown'.
    -- Derived from earnings_ts at write time.
    earnings_session TEXT,
    eps_estimate     DOUBLE PRECISION,
    reported_eps     DOUBLE PRECISION,
    surprise_pct     DOUBLE PRECISION,
    -- TRUE while Yahoo has no reported EPS for the date, i.e. it has not
    -- happened yet and the date is a projection that may move. This is why
    -- the upsert overwrites rather than skipping on conflict.
    is_estimated     BOOLEAN NOT NULL DEFAULT FALSE,
    -- When this row was last CONFIRMED by a fetch, not when it was created.
    -- A future date that stops appearing in Yahoo's response keeps its old
    -- fetched_at, which is how a stale projection is spotted.
    fetched_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, earnings_date)
);

-- The lookup days_to_earnings makes: next date on or after a trade_date.
CREATE INDEX IF NOT EXISTS ix_earnings_calendar_lookup
    ON earnings_calendar (ticker, earnings_date);


CREATE TABLE IF NOT EXISTS earnings_coverage (
    ticker             TEXT PRIMARY KEY,
    -- FALSE for a fund. The distinction that stops 28 ETFs reading as 28
    -- failures every night.
    has_earnings       BOOLEAN NOT NULL,
    n_dates            INTEGER NOT NULL DEFAULT 0,
    next_earnings_date DATE,
    -- 'ok' | 'no_earnings' | 'error'. A monitor alerts on 'error' only.
    last_status        TEXT NOT NULL,
    last_error         TEXT,
    last_fetched_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- What a nightly monitor should actually watch: genuine failures, and tickers
-- whose next date has passed with nothing behind it (Yahoo publishes only the
-- next confirmed date, so a missed week leaves the calendar silently empty).
CREATE OR REPLACE VIEW earnings_coverage_alert AS
SELECT ticker, last_status, last_error, next_earnings_date, last_fetched_at,
       CASE
           WHEN last_status = 'error'                       THEN 'fetch failed'
           WHEN has_earnings AND next_earnings_date IS NULL  THEN 'no future date'
           WHEN has_earnings AND next_earnings_date < CURRENT_DATE
                                                            THEN 'next date is in the past'
           WHEN last_fetched_at < now() - INTERVAL '3 days'  THEN 'stale'
       END AS issue
FROM earnings_coverage
WHERE last_status = 'error'
   OR (has_earnings AND (next_earnings_date IS NULL
                         OR next_earnings_date < CURRENT_DATE))
   OR last_fetched_at < now() - INTERVAL '3 days';
