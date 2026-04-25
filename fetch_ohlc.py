"""
fetch_ohlc.py — Pull daily OHLC for one or more tickers from yfinance and
upsert into underlying_ohlc.

Usage:
    python fetch_ohlc.py
    (prompts for tickers + date range)
"""
from __future__ import annotations

import logging
import math
from datetime import date, datetime, timedelta

import numpy as np
import psycopg2.extras
import yfinance as yf

from db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

UPSERT_SQL = """
INSERT INTO underlying_ohlc
    (ticker, trade_date, open, high, low, close, adj_close, volume, dividends, splits)
VALUES %s
ON CONFLICT (ticker, trade_date) DO UPDATE SET
    open      = EXCLUDED.open,
    high      = EXCLUDED.high,
    low       = EXCLUDED.low,
    close     = EXCLUDED.close,
    adj_close = EXCLUDED.adj_close,
    volume    = EXCLUDED.volume,
    dividends = EXCLUDED.dividends,
    splits    = EXCLUDED.splits
"""


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


def _safe(v):
    """
    Convert numpy scalars from yfinance to native Python types and map
    NaN/Inf → None. psycopg2 calls repr() on unregistered types, and
    NumPy>=2.0's repr(np.float64) is "np.float64(x)" — Postgres then
    parses 'np' as a schema and errors out.
    """
    if v is None:
        return None
    if isinstance(v, np.floating):
        f = float(v)
        return None if not math.isfinite(f) else f
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, float) and not math.isfinite(v):
        return None
    return v


def fetch_one(ticker: str, start: date, end: date) -> list[tuple]:
    """Returns rows ready for execute_values(). yfinance end is exclusive."""
    df = yf.Ticker(ticker).history(
        start=start.isoformat(),
        end=(end + timedelta(days=1)).isoformat(),  # yfinance end is exclusive
        auto_adjust=False,
        actions=True,
    )
    if df.empty:
        return []

    rows = []
    for idx, r in df.iterrows():
        d = idx.date() if hasattr(idx, "date") else idx
        rows.append((
            ticker,
            d,
            _safe(r.get("Open")),
            _safe(r.get("High")),
            _safe(r.get("Low")),
            _safe(r.get("Close")),
            _safe(r.get("Adj Close")),
            int(r["Volume"]) if _safe(r.get("Volume")) is not None else None,
            _safe(r.get("Dividends")),
            _safe(r.get("Stock Splits")),
        ))
    return rows


def main() -> None:
    print("=== OI_Research — Daily OHLC fetch ===\n")
    tickers = prompt_tickers()
    start   = prompt_date("Start date")
    end     = prompt_date("End   date")
    if end < start:
        raise SystemExit("End date must be >= start date.")

    print(f"\nFetching {len(tickers)} tickers from {start} → {end}")

    with get_connection() as conn:
        for t in tickers:
            log.info("yfinance: %s ...", t)
            rows = fetch_one(t, start, end)
            log.info("  %s: %d rows", t, len(rows))
            if not rows:
                continue
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, UPSERT_SQL, rows, page_size=500)
            conn.commit()

    print("\nDone.")


if __name__ == "__main__":
    main()
