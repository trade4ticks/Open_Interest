"""Parquet store: layout, atomic writes, and what is already on disk.

Parquet is the record. Postgres holds derived metrics only — no tick data
ever.

LAYOUT
    <SCALP_DATA_DIR>/raw/<SYMBOL>/<YYYY-MM-DD>.parquet

One file per symbol-day. That granularity is what makes `fetch.py` resumable
without any bookkeeping: the presence of the file IS the record that the day
was fetched, so a run that dies at symbol 300 skips the 299 already there on
the next attempt, and no separate manifest can drift out of agreement with the
disk.

WRITES ARE ATOMIC. Written to a temp file in the same directory and then
os.replace'd into position. A run killed mid-write leaves either the previous
file or none — never a truncated parquet that reads as a short session and
silently produces a metric for half a day.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from scalp import config


def symbol_dir(symbol: str) -> Path:
    return config.raw_dir() / symbol.upper()


def day_path(symbol: str, day: date) -> Path:
    return symbol_dir(symbol) / f"{day.isoformat()}.parquet"


def has_day(symbol: str, day: date) -> bool:
    p = day_path(symbol, day)
    return p.exists() and p.stat().st_size > 0


def write_day(symbol: str, day: date, df: pd.DataFrame) -> Path:
    """Atomically write one symbol-day. Returns the final path."""
    target = day_path(symbol, day)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".parquet.tmp")
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, tmp, compression=config.PARQUET_COMPRESSION)
    os.replace(tmp, target)
    return target


def read_day(symbol: str, day: date) -> pd.DataFrame:
    p = day_path(symbol, day)
    if not p.exists():
        return pd.DataFrame()
    return pq.read_table(p).to_pandas()


def stored_days(symbol: str) -> list[date]:
    d = symbol_dir(symbol)
    if not d.exists():
        return []
    out = []
    for f in d.glob("*.parquet"):
        try:
            out.append(date.fromisoformat(f.stem))
        except ValueError:
            continue
    return sorted(out)


def stored_symbols() -> list[str]:
    root = config.raw_dir()
    if not root.exists():
        return []
    return sorted(p.name for p in root.iterdir() if p.is_dir())


def missing_days(symbol: str, days: list[date]) -> list[date]:
    """Which of `days` are not on disk. This is the incremental check —
    fetch.py requests only the gap, so the first run is long and nightly runs
    are short."""
    return [d for d in days if not has_day(symbol, d)]


def store_bytes() -> int:
    root = config.raw_dir()
    if not root.exists():
        return 0
    return sum(f.stat().st_size for f in root.rglob("*.parquet"))


# --- trading days ------------------------------------------------------------

def _weekdays(start: date, end: date) -> list[date]:
    out, d = [], start
    while d <= end:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def trading_days(start: date, end: date) -> list[date]:
    """NYSE sessions in [start, end].

    Uses pandas_market_calendars when it is installed — it already is, for the
    options pipeline — and falls back to weekdays otherwise. The fallback is
    safe rather than merely convenient: a holiday returns no data and is
    skipped, so the only cost is one wasted request per holiday. Getting it
    right just avoids nine pointless requests a year per symbol.
    """
    try:
        import pandas_market_calendars as mcal
        cal = mcal.get_calendar("NYSE")
        sched = cal.schedule(start_date=start, end_date=end)
        return [d.date() for d in sched.index]
    except Exception:
        return _weekdays(start, end)
