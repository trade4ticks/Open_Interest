"""
fetch_oi_snapshot.py — Pull TODAY's OI snapshot for one or more tickers
from ThetaData and write to parquet at {OI_RAW_DIR}/{ticker}/{year}.parquet.

Use this in addition to fetch_oi.py: the history endpoint
(/v3/option/history/open_interest) typically lags by ~1 day, so today's
chain isn't there yet at 7am ET. The snapshot endpoint
(/v3/option/snapshot/open_interest) returns the current OI for every
contract in one call per ticker.

The trade_date stamped on every row is `last_trading_day(today)`:
- On a trading day, that's today.
- On a weekend / holiday, that's the most recent session (the OI hasn't
  changed since then).

Usage:
    python fetch_oi_snapshot.py
    (prompts for tickers — blank = all already in OI_RAW_DIR)
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import pandas as pd
from tqdm import tqdm

from lib.market_hours import last_trading_day
from lib.parquet_store import list_tickers, write_rows
from lib.thetadata import (
    TerminalServerError,
    TerminalTimeoutError,
    fetch_oi_snapshot,
    test_connection,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MAX_WORKERS = 6


# --- Prompts ---------------------------------------------------------------

def prompt_tickers() -> list[str]:
    raw = input(
        "Tickers (comma-separated; blank = all tickers in OI_RAW_DIR): "
    ).strip()
    if raw:
        return [t.strip().upper() for t in raw.split(",") if t.strip()]
    out = list_tickers()
    if not out:
        raise SystemExit(
            "No tickers entered and OI_RAW_DIR is empty — please specify."
        )
    return out


# --- Per-ticker pipeline ---------------------------------------------------

def fetch_ticker(ticker: str, trade_date: date | None = None) -> int:
    """
    Snapshot one ticker, write to parquet. Returns total rows written.
    `trade_date` defaults to last_trading_day(today).
    """
    td = trade_date or last_trading_day()
    df = fetch_oi_snapshot(ticker, td)
    if df.empty:
        log.warning("  no snapshot data for %s", ticker)
        return 0

    n_raw = len(df)
    df = (
        df.groupby(["trade_date", "expiration", "strike", "option_type"], as_index=False)
          ["open_interest"].sum()
    )
    n_dedup = len(df)
    if n_dedup < n_raw:
        log.warning("  DEDUP   %s snapshot: %d → %d rows (%d dupes summed)",
                    ticker, n_raw, n_dedup, n_raw - n_dedup)

    log.info("  %s: %d rows (snapshot for %s)", ticker, len(df), td)
    by_year = write_rows(ticker, df)
    for y, n in sorted(by_year.items()):
        log.info("    %s/%d.parquet → %d rows total", ticker, y, n)
    return len(df)


# --- Main ------------------------------------------------------------------

def main() -> None:
    print("=== OI_Research — ThetaData OI snapshot (today) ===\n")
    tickers = prompt_tickers()

    td = last_trading_day()
    print(f"\nFetching snapshot for {len(tickers)} tickers (trade_date = {td})")

    print("Checking ThetaData ...", end=" ", flush=True)
    if not test_connection():
        raise SystemExit("FAILED — terminal not reachable.")
    print("OK")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_ticker, t, td): t for t in tickers}
        with tqdm(total=len(futures), unit="tk", ncols=90,
                  desc="snapshot") as bar:
            for fut in as_completed(futures):
                t = futures[fut]
                try:
                    fut.result()
                except (TerminalTimeoutError, TerminalServerError) as exc:
                    log.warning("  TIMEOUT %s: %s", t, exc)
                except Exception as exc:
                    log.warning("  FAIL    %s: %s", t, exc)
                bar.update(1)

    print("\nDone.")


if __name__ == "__main__":
    main()
