"""
export_raw_to_parquet.py — One-time migration:
    Postgres `option_oi_raw`  →  {OI_RAW_DIR}/{ticker}/{year}.parquet

Does NOT drop the Postgres table. After this finishes, validate the parquet
files (e.g. spot-check counts vs Postgres), then drop the table yourself:

    psql -d open_interest -c "DROP TABLE option_oi_raw;"

Usage:
    python export_raw_to_parquet.py
    (prompts for tickers — blank = all tickers in the table)
"""
from __future__ import annotations

import logging

import pandas as pd
from tqdm import tqdm

from db import get_connection
from lib.parquet_store import write_year

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

LIST_TICKERS_SQL = "SELECT DISTINCT ticker FROM option_oi_raw ORDER BY ticker"

LIST_YEARS_SQL = """
SELECT DISTINCT EXTRACT(YEAR FROM trade_date)::INTEGER AS year
FROM option_oi_raw
WHERE ticker = %(ticker)s
ORDER BY year
"""

LOAD_YEAR_SQL = """
SELECT trade_date, expiration, strike, option_type, open_interest
FROM option_oi_raw
WHERE ticker = %(ticker)s
  AND trade_date >= make_date(%(year)s, 1, 1)
  AND trade_date <  make_date(%(year)s + 1, 1, 1)
"""

POSTGRES_COUNT_SQL = """
SELECT COUNT(*)::BIGINT
FROM option_oi_raw
WHERE ticker = %(ticker)s
  AND trade_date >= make_date(%(year)s, 1, 1)
  AND trade_date <  make_date(%(year)s + 1, 1, 1)
"""


def prompt_tickers(conn) -> list[str]:
    raw = input("Tickers (comma-separated; blank = all in option_oi_raw): ").strip()
    if raw:
        return [t.strip().upper() for t in raw.split(",") if t.strip()]
    with conn.cursor() as cur:
        cur.execute(LIST_TICKERS_SQL)
        return [r[0] for r in cur.fetchall()]


def export_ticker(conn, ticker: str) -> None:
    with conn.cursor() as cur:
        cur.execute(LIST_YEARS_SQL, {"ticker": ticker})
        years = [r[0] for r in cur.fetchall()]
    if not years:
        log.info("  no rows for %s — skipping", ticker)
        return

    for y in tqdm(years, ncols=90, unit="yr", desc=f"  {ticker}"):
        df = pd.read_sql(LOAD_YEAR_SQL, conn, params={"ticker": ticker, "year": y})
        if df.empty:
            continue
        rows_written = write_year(ticker, y, df)
        # Cross-check parquet row count vs Postgres source.
        with conn.cursor() as cur:
            cur.execute(POSTGRES_COUNT_SQL, {"ticker": ticker, "year": y})
            pg_count = cur.fetchone()[0]
        if rows_written != pg_count:
            log.warning("  MISMATCH %s/%d.parquet: parquet=%d, postgres=%d",
                        ticker, y, rows_written, pg_count)
        else:
            log.info("  %s/%d.parquet ← %d rows (matches postgres)",
                     ticker, y, rows_written)


def main() -> None:
    print("=== OI_Research — Export option_oi_raw → parquet ===\n")
    with get_connection() as conn:
        tickers = prompt_tickers(conn)
        if not tickers:
            print("No tickers found in option_oi_raw — nothing to do.")
            return
        print(f"\nExporting: {', '.join(tickers)}\n")
        for t in tickers:
            print(f"--- {t} ---")
            export_ticker(conn, t)

    print("\nDone. Validate the parquet files, then if happy:")
    print("    psql -d open_interest -c \"DROP TABLE option_oi_raw;\"")


if __name__ == "__main__":
    main()
