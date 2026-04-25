"""
fetch_oi.py — Pull daily Open Interest for one or more tickers from
ThetaData and upsert into option_oi_raw, then rebuild option_oi_surface
for the affected dates.

Usage:
    python fetch_oi.py
    (prompts for tickers + date range)

Strategy
--------
For each ticker:
  1. Discover all expirations the terminal knows about (one call).
  2. Filter to expirations whose date is >= start of our window
     (so they were active during the requested range).
  3. For each expiration, fetch OI for [start, min(end, expiration)] in one
     call. Auto-splits on HTTP 570 inside the client.
  4. Bulk-upsert into option_oi_raw.
After all tickers are loaded, rebuild option_oi_surface for the touched
(ticker, trade_date) pairs by joining to underlying_ohlc and applying the
filter thresholds from .env.

Resume-safe: re-running over the same range just re-upserts identical rows.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

import psycopg2.extras
from tqdm import tqdm

from config import OI_MAX_DTE, OI_MAX_MONEYNESS, OI_MIN
from db import get_connection
from lib.market_hours import last_trading_day
from lib.thetadata import (
    TerminalServerError,
    TerminalTimeoutError,
    fetch_oi,
    list_expirations,
    test_connection,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MAX_WORKERS = 2

UPSERT_RAW_SQL = """
INSERT INTO option_oi_raw
    (ticker, trade_date, expiration, strike, right, open_interest)
VALUES %s
ON CONFLICT (ticker, trade_date, expiration, strike, right) DO UPDATE SET
    open_interest = EXCLUDED.open_interest
"""

# Rebuild the surface for one (ticker, trade_date) by deleting and reinserting.
# Filter: open_interest >= OI_MIN, dte between 0 and OI_MAX_DTE,
#         |strike/spot - 1| <= OI_MAX_MONEYNESS (skip rows with no spot).
REBUILD_SURFACE_SQL = """
DELETE FROM option_oi_surface
 WHERE ticker = %(ticker)s AND trade_date = %(trade_date)s;

INSERT INTO option_oi_surface
    (ticker, trade_date, expiration, dte, strike, right, open_interest,
     spot_close, moneyness)
SELECT
    r.ticker,
    r.trade_date,
    r.expiration,
    (r.expiration - r.trade_date)::INTEGER                  AS dte,
    r.strike,
    r.right,
    r.open_interest,
    o.close                                                 AS spot_close,
    (r.strike / o.close) - 1.0                              AS moneyness
FROM option_oi_raw r
JOIN underlying_ohlc o
  ON  o.ticker     = r.ticker
  AND o.trade_date = r.trade_date
WHERE r.ticker     = %(ticker)s
  AND r.trade_date = %(trade_date)s
  AND r.open_interest >= %(oi_min)s
  AND (r.expiration - r.trade_date) BETWEEN 0 AND %(max_dte)s
  AND ABS((r.strike / o.close) - 1.0) <= %(max_moneyness)s
  AND o.close IS NOT NULL AND o.close > 0;
"""


# --- Prompts ---------------------------------------------------------------

def prompt_tickers() -> list[str]:
    raw = input("Tickers (comma-separated, e.g. SPY,QQQ,AAPL): ").strip()
    out = [t.strip().upper() for t in raw.split(",") if t.strip()]
    if not out:
        raise SystemExit("No tickers entered.")
    return out


def prompt_date(label: str) -> date:
    while True:
        raw = input(f"{label} (YYYY-MM-DD): ").strip()
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            print("  Use YYYY-MM-DD (e.g. 2024-01-02)")


# --- Per-ticker pipeline ---------------------------------------------------

def _df_to_rows(ticker: str, expiration: date, df) -> list[tuple]:
    if df.empty:
        return []
    rows = []
    for r in df.itertuples(index=False):
        rows.append((
            ticker,
            r.trade_date,
            expiration,
            float(r.strike),
            r.right,
            int(r.open_interest),
        ))
    return rows


def _fetch_one_expiry(ticker: str, expiration: date, start: date, end: date):
    eff_end = min(end, expiration)
    if eff_end < start:
        return ticker, expiration, []
    df = fetch_oi(ticker, expiration, start, eff_end)
    return ticker, expiration, _df_to_rows(ticker, expiration, df)


def fetch_ticker(conn, ticker: str, start: date, end: date) -> set[date]:
    """Returns the set of trade_dates that received any rows."""
    log.info("Discovering expirations for %s ...", ticker)
    all_exps = list_expirations(ticker)
    exps = [e for e in all_exps if e >= start]
    if not exps:
        log.warning("  no expirations >= %s for %s", start, ticker)
        return set()
    log.info("  %d expirations to scan", len(exps))

    touched_dates: set[date] = set()
    failures: list[date]     = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_fetch_one_expiry, ticker, e, start, end): e
            for e in exps
        }
        with tqdm(total=len(futures), unit="exp", ncols=90, desc=f"  {ticker}") as bar:
            for fut in as_completed(futures):
                exp = futures[fut]
                try:
                    _, _, rows = fut.result()
                except (TerminalTimeoutError, TerminalServerError) as exc:
                    failures.append(exp)
                    log.warning("  TIMEOUT %s %s: %s", ticker, exp, exc)
                    bar.update(1)
                    continue
                except Exception as exc:
                    failures.append(exp)
                    log.warning("  FAIL    %s %s: %s", ticker, exp, exc)
                    bar.update(1)
                    continue

                if rows:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_values(
                            cur, UPSERT_RAW_SQL, rows, page_size=2000
                        )
                    conn.commit()
                    for r in rows:
                        touched_dates.add(r[1])
                    bar.set_postfix_str(f"{exp} ({len(rows)}r)")
                bar.update(1)

    if failures:
        log.warning("  %d expirations failed for %s: retry by re-running.",
                    len(failures), ticker)
    return touched_dates


# --- Surface rebuild -------------------------------------------------------

def rebuild_surface(conn, ticker: str, trade_dates: set[date]) -> int:
    if not trade_dates:
        return 0
    n = 0
    params_list = [
        {
            "ticker":         ticker,
            "trade_date":     d,
            "oi_min":         OI_MIN,
            "max_dte":        OI_MAX_DTE,
            "max_moneyness":  OI_MAX_MONEYNESS,
        }
        for d in sorted(trade_dates)
    ]
    with conn.cursor() as cur:
        for params in tqdm(params_list, ncols=90, unit="day",
                           desc=f"  rebuild {ticker}"):
            cur.execute(REBUILD_SURFACE_SQL, params)
            n += 1
    conn.commit()
    return n


# --- Main ------------------------------------------------------------------

def main() -> None:
    print("=== OI_Research — ThetaData OI fetch ===\n")
    tickers = prompt_tickers()
    start   = prompt_date("Start date")
    end     = prompt_date("End   date")
    if end < start:
        raise SystemExit("End date must be >= start date.")

    end = min(end, last_trading_day())
    if end < start:
        raise SystemExit("No completed trading days in the requested range.")

    print(f"\nFetching {len(tickers)} tickers from {start} → {end}")

    print("Checking ThetaData ...", end=" ", flush=True)
    if not test_connection():
        raise SystemExit("FAILED — terminal not reachable.")
    print("OK")

    with get_connection() as conn:
        for t in tickers:
            print(f"\n--- {t} ---")
            touched = fetch_ticker(conn, t, start, end)
            if touched:
                rebuild_surface(conn, t, touched)
                print(f"  surface rebuilt for {len(touched)} trade dates")
            else:
                print("  no data fetched")

    print("\nDone. Run build_features.py next to refresh daily_features.")


if __name__ == "__main__":
    main()
