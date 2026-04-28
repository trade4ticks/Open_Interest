"""
fetch_oi.py — Pull daily Open Interest for one or more tickers from
ThetaData and write to parquet at {OI_RAW_DIR}/{ticker}/{year}.parquet.

Usage:
    python fetch_oi.py
    (prompts for tickers + date range)

Strategy
--------
For each (ticker, trading_day) we make ONE HTTP call:

    /v3/option/history/open_interest?symbol=TICK&expiration=*&date=YYYYMMDD

That returns the entire OI chain (every strike of every active expiration)
for the ticker on that day. Rows are accumulated in memory per ticker, then
written to parquet split by year. The parquet writer dedupes on
(trade_date, expiration, strike, option_type) so re-running a date range is
safe and overwrites any prior values for the same contract on the same day.

The Postgres `option_oi_raw` and `option_oi_surface` tables are no longer
populated by this script — derived metrics in `daily_features` are now
computed directly from the parquet store by `build_features.py`.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

import pandas as pd
from tqdm import tqdm

from lib.market_hours import get_trading_days, last_trading_day
from lib.parquet_store import write_rows
from lib.thetadata import (
    TerminalServerError,
    TerminalTimeoutError,
    fetch_oi_day,
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

def _fetch_one_day(ticker: str, day: date) -> pd.DataFrame:
    """
    Return a DataFrame of OI rows for one (ticker, day) — empty on no data.
    Dedupes on (expiration, strike, option_type) by SUMMING open_interest;
    warns if dupes were seen (ThetaData has been observed to occasionally
    return repeated rows in a single response — see commit da8ddda).
    """
    df = fetch_oi_day(ticker, day)
    if df.empty:
        return df

    # Pin trade_date to the requested day (response carries it back, but we
    # already know what we asked for).
    df = df.copy()
    df["trade_date"] = day
    df = df[["trade_date", "expiration", "strike", "option_type", "open_interest"]]

    n_raw = len(df)
    df = (
        df.groupby(["trade_date", "expiration", "strike", "option_type"], as_index=False)
          ["open_interest"].sum()
    )
    n_dedup = len(df)
    if n_dedup < n_raw:
        log.warning("  DEDUP   %s %s: %d → %d rows (%d dupes summed)",
                    ticker, day, n_raw, n_dedup, n_raw - n_dedup)

    return df


def fetch_ticker(ticker: str, trading_days: list[date]) -> int:
    """
    Fetch every trading_day for one ticker in parallel, then write one
    parquet merge per year touched. Returns total rows written this run.
    """
    frames: list[pd.DataFrame] = []
    failures: list[date] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_one_day, ticker, d): d for d in trading_days}
        with tqdm(total=len(futures), unit="day", ncols=90,
                  desc=f"  {ticker} fetch") as bar:
            for fut in as_completed(futures):
                d = futures[fut]
                try:
                    df = fut.result()
                except (TerminalTimeoutError, TerminalServerError) as exc:
                    failures.append(d)
                    log.warning("  TIMEOUT %s %s: %s", ticker, d, exc)
                    bar.update(1)
                    continue
                except Exception as exc:
                    failures.append(d)
                    log.warning("  FAIL    %s %s: %s", ticker, d, exc)
                    bar.update(1)
                    continue

                if not df.empty:
                    frames.append(df)
                    bar.set_postfix_str(f"{d} ({len(df)}r)")
                bar.update(1)

    if failures:
        log.warning("  %d days failed for %s: re-run to retry.",
                    len(failures), ticker)

    if not frames:
        return 0

    combined = pd.concat(frames, ignore_index=True)
    log.info("  %s: merging %d rows into parquet ...", ticker, len(combined))
    by_year = write_rows(ticker, combined)
    for y, n in sorted(by_year.items()):
        log.info("    %s/%d.parquet → %d rows total", ticker, y, n)
    return len(combined)


# --- Main ------------------------------------------------------------------

def main() -> None:
    print("=== OI_Research — ThetaData OI fetch (parquet) ===\n")
    tickers = prompt_tickers()
    start   = prompt_date("Start date")
    end     = prompt_date("End   date")
    if end < start:
        raise SystemExit("End date must be >= start date.")

    end = min(end, last_trading_day())
    if end < start:
        raise SystemExit("No completed trading days in the requested range.")

    trading_days = get_trading_days(start, end)
    if not trading_days:
        raise SystemExit("No NYSE trading days in the requested range.")

    print(f"\nFetching {len(tickers)} tickers × {len(trading_days)} trading days "
          f"({start} → {end})")

    print("Checking ThetaData ...", end=" ", flush=True)
    if not test_connection():
        raise SystemExit("FAILED — terminal not reachable.")
    print("OK")

    for t in tickers:
        print(f"\n--- {t} ---")
        fetch_ticker(t, trading_days)

    print("\nDone. Run build_features.py next to refresh daily_features.")


if __name__ == "__main__":
    main()
