"""
fetch_ohlc_premarket.py — Fetch a premarket open-price proxy for each ticker
and write it to underlying_ohlc.open (with provenance columns).

Used exclusively by the 7am early-morning pipeline run.  The 7am run needs
a current price to make OI spot-referenced metrics meaningful before the
9:30 regular session opens.  This module supplies that price.

Price methodology
-----------------
Fetch yfinance 1-minute bars with prepost=True, filter to the premarket
window (04:00–09:29 ET inclusive), take the Close of the last bar with
nonzero volume.  A bar with zero volume is assumed to be a yfinance
forward-fill artifact, not a real print.  Stale-by-10–20-min is
acceptable; spurious/forward-filled is not.

If no nonzero-volume premarket bar exists (ticker didn't trade premarket),
we write nothing for that ticker — no early signal today is the correct
safe-failure mode.

Write contract
--------------
Uses a dedicated minimal upsert that touches ONLY open, open_source, and
open_asof_ts.  Never overwrites high/low/close/volume.  The 9:35 MORNING
run (fetch_ohlc.py UPSERT_SQL) will later overwrite open with the official
9:30 print and stamp open_source = 'daily_1d'.

Timezone
--------
yfinance intraday bars may be returned in UTC or ET depending on version.
We always tz_convert to America/New_York before any between_time filter to
guarantee correctness regardless of yfinance version.
"""
from __future__ import annotations

import logging
import math
from datetime import date, datetime, timedelta

import yfinance as yf

from db import get_connection
from lib.market_hours import get_trading_days

log = logging.getLogger(__name__)

# Premarket window in ET (inclusive both ends for between_time).
# 09:29 is the last full premarket minute; 09:30 is the regular-session open.
_PM_START = "04:00"
_PM_END   = "09:29"

# Dedicated upsert: touches ONLY open + provenance.  All other columns are
# left exactly as they were (or absent if this is the first write of the day).
# The 9:35 daily-bar upsert will overwrite open + set open_source='daily_1d'.
PREMARKET_UPSERT_SQL = """
INSERT INTO underlying_ohlc (ticker, trade_date, open, open_source, open_asof_ts)
VALUES (%(ticker)s, %(trade_date)s, %(open)s, 'premarket_1m', %(open_asof_ts)s)
ON CONFLICT (ticker, trade_date) DO UPDATE SET
    open         = EXCLUDED.open,
    open_source  = EXCLUDED.open_source,
    open_asof_ts = EXCLUDED.open_asof_ts
"""


def fetch_premarket_open(
    ticker: str,
    trade_date: date,
) -> tuple[float | None, datetime | None]:
    """Fetch the premarket open-price proxy for one ticker on one date.

    Returns
    -------
    (price, bar_ts)  — last nonzero-volume premarket 1m bar's Close and its
                       ET-aware timestamp.
    (None, None)     — no usable premarket bar found; caller should skip write.
    """
    df = yf.Ticker(ticker).history(
        start=trade_date.isoformat(),
        end=(trade_date + timedelta(days=1)).isoformat(),
        interval="1m",
        prepost=True,
        auto_adjust=False,
    )
    if df.empty:
        log.debug("  %s: yfinance returned empty 1m frame", ticker)
        return None, None

    # Guarantee ET regardless of yfinance version (may return UTC or ET).
    df.index = df.index.tz_convert("America/New_York")

    # Restrict to premarket window.
    premarket = df.between_time(_PM_START, _PM_END)
    if premarket.empty:
        log.debug("  %s: no 1m bars in premarket window %s–%s ET",
                  ticker, _PM_START, _PM_END)
        return None, None

    # Filter to bars with nonzero volume — zero-volume bars are yfinance
    # forward-fills, not real prints.
    active = premarket[premarket["Volume"] > 0]
    if active.empty:
        log.debug("  %s: all premarket bars have zero volume", ticker)
        return None, None

    last_bar = active.iloc[-1]
    price = last_bar["Close"]

    if price is None or (isinstance(price, float) and not math.isfinite(price)):
        log.debug("  %s: last active premarket bar has non-finite Close", ticker)
        return None, None

    price    = float(price)
    bar_ts   = last_bar.name.to_pydatetime()   # tz-aware ET datetime

    log.debug("  %s: premarket proxy %.4f @ %s ET", ticker, price, bar_ts)
    return price, bar_ts


def fetch_and_write(conn, ticker: str, trade_date: date) -> bool:
    """Fetch premarket proxy and upsert to underlying_ohlc.  Returns True if
    a valid price was found and written, False if skipped (no premarket data).
    """
    price, bar_ts = fetch_premarket_open(ticker, trade_date)
    if price is None:
        log.info("  %s: no premarket data — skipping", ticker)
        return False

    with conn.cursor() as cur:
        cur.execute(PREMARKET_UPSERT_SQL, {
            "ticker":       ticker,
            "trade_date":   trade_date,
            "open":         price,
            "open_asof_ts": bar_ts,
        })
    log.info("  %s: wrote premarket open %.4f (bar @ %s ET)",
             ticker, price, bar_ts.strftime("%H:%M"))
    return True


def run(conn, tickers: list[str], trade_date: date) -> int:
    """Fetch + write premarket open for all tickers.  Returns count written."""
    written = 0
    for ticker in tickers:
        try:
            if fetch_and_write(conn, ticker, trade_date):
                written += 1
        except Exception as exc:
            log.warning("  %s: premarket fetch failed — %s", ticker, exc)
    conn.commit()
    log.info("Premarket open: %d/%d tickers written for %s",
             written, len(tickers), trade_date)
    return written
