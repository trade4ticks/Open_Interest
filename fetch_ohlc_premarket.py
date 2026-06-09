"""
fetch_ohlc_premarket.py — Fetch a premarket open-price proxy from ThetaData's
IV snapshot endpoint and write it to underlying_ohlc.open with provenance.

Used exclusively by the 7am early-morning pipeline run.  The 7am run needs a
current price so OI spot-referenced metrics are meaningful before the 9:30
regular session opens.

Price methodology
-----------------
Hit /v3/option/snapshot/greeks/implied_volatility for each ticker.  Every row
in the response carries the same `underlying_price` and `underlying_timestamp`
(a single per-call live snapshot, not per-contract); we read both fields off
the first row.

If the endpoint returns no rows or no usable underlying_price for a ticker,
write nothing — "no early signal" is the correct safe-failure mode.

Why ThetaData and not yfinance?  yfinance won't serve current-day premarket
1m bars before the regular open, which broke the prior implementation.  We
don't have a ThetaData regular-stock subscription, but this options endpoint
exposes the live underlying price as a side-effect and the values are
confirmed current during premarket.

Write contract
--------------
Dedicated upsert that touches ONLY open, open_source, and open_asof_ts.
Never overwrites high/low/close/volume.  The 9:35 MORNING run
(fetch_ohlc.py UPSERT_SQL) later overwrites open with the official 9:30
print and stamps open_source = 'daily_1d'.

Provenance: open_source = 'premarket_theta' — distinguishes this row from
the old 'premarket_1m' yfinance source and from the official 'daily_1d'.
"""
from __future__ import annotations

import logging
from datetime import date

from lib.thetadata import fetch_underlying_snapshot

log = logging.getLogger(__name__)

# Dedicated upsert: touches ONLY open + provenance.  All other columns are
# left exactly as they were (or absent if this is the first write of the day).
# The 9:35 daily-bar upsert will overwrite open + set open_source='daily_1d'.
PREMARKET_UPSERT_SQL = """
INSERT INTO underlying_ohlc (ticker, trade_date, open, open_source, open_asof_ts)
VALUES (%(ticker)s, %(trade_date)s, %(open)s, 'premarket_theta', %(open_asof_ts)s)
ON CONFLICT (ticker, trade_date) DO UPDATE SET
    open         = EXCLUDED.open,
    open_source  = EXCLUDED.open_source,
    open_asof_ts = EXCLUDED.open_asof_ts
"""


def fetch_and_write(conn, ticker: str, trade_date: date) -> bool:
    """Fetch the live ThetaData IV-snapshot underlying price for one ticker
    and upsert to underlying_ohlc.

    Returns True if a valid price was written, False if skipped (snapshot
    returned no usable price — write nothing rather than stale/null).
    """
    price, ts = fetch_underlying_snapshot(ticker)
    if price is None or ts is None:
        log.info("  %s: no usable IV-snapshot underlying — skipping", ticker)
        return False

    with conn.cursor() as cur:
        cur.execute(PREMARKET_UPSERT_SQL, {
            "ticker":       ticker,
            "trade_date":   trade_date,
            "open":         price,
            "open_asof_ts": ts,
        })
    log.info("  %s: wrote premarket open %.4f (snapshot @ %s ET)",
             ticker, price, ts.strftime("%H:%M:%S"))
    return True


def run(conn, tickers: list[str], trade_date: date) -> int:
    """Fetch + write premarket open for all tickers.  Per-ticker exceptions
    are caught and logged so a single bad ticker doesn't abort the rest.
    Returns count of tickers written.
    """
    written = 0
    for ticker in tickers:
        try:
            if fetch_and_write(conn, ticker, trade_date):
                written += 1
        except Exception as exc:
            log.warning("  %s: premarket fetch failed — %s", ticker, exc)
    conn.commit()
    log.info("Premarket open (ThetaData IV snapshot): %d/%d tickers written for %s",
             written, len(tickers), trade_date)
    return written
