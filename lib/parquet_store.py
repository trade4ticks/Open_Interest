"""
Parquet storage for raw OI rows.

Layout:  {OI_RAW_DIR}/{ticker}/{year}.parquet

Schema per file (ticker is implied by path; year is implied by path + trade_date):
    trade_date    DATE
    expiration    DATE
    strike        FLOAT64
    option_type   STRING ('C'/'P')
    open_interest INT64

Append behaviour: read existing file, concat new rows, dedupe on
(trade_date, expiration, strike, option_type) keeping the latest value, sort,
write atomically (write to .tmp then rename).
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import OI_RAW_DIR

DEDUPE_KEYS = ["trade_date", "expiration", "strike", "option_type"]

_SCHEMA = pa.schema([
    ("trade_date",    pa.date32()),
    ("expiration",    pa.date32()),
    ("strike",        pa.float64()),
    ("option_type",   pa.string()),
    ("open_interest", pa.int64()),
])


# --- Path helpers ----------------------------------------------------------

def ticker_dir(ticker: str) -> Path:
    return OI_RAW_DIR / ticker.upper()


def year_path(ticker: str, year: int) -> Path:
    return ticker_dir(ticker) / f"{year}.parquet"


def list_tickers() -> list[str]:
    """Tickers that have at least one parquet file under OI_RAW_DIR."""
    if not OI_RAW_DIR.exists():
        return []
    out = []
    for p in OI_RAW_DIR.iterdir():
        if p.is_dir() and any(p.glob("*.parquet")):
            out.append(p.name.upper())
    return sorted(out)


def list_years(ticker: str) -> list[int]:
    d = ticker_dir(ticker)
    if not d.exists():
        return []
    years = []
    for p in d.glob("*.parquet"):
        try:
            years.append(int(p.stem))
        except ValueError:
            continue
    return sorted(years)


# --- Read ------------------------------------------------------------------

def read_year(ticker: str, year: int) -> pd.DataFrame:
    p = year_path(ticker, year)
    if not p.exists():
        return pd.DataFrame(columns=DEDUPE_KEYS + ["open_interest"])
    return pd.read_parquet(p)


def read_range(ticker: str, start: date, end: date) -> pd.DataFrame:
    """All rows for `ticker` with start <= trade_date <= end."""
    frames = []
    for y in range(start.year, end.year + 1):
        df = read_year(ticker, y)
        if df.empty:
            continue
        mask = (df["trade_date"] >= start) & (df["trade_date"] <= end)
        frames.append(df.loc[mask])
    if not frames:
        return pd.DataFrame(columns=DEDUPE_KEYS + ["open_interest"])
    return pd.concat(frames, ignore_index=True)


def parquet_glob(ticker: str) -> str:
    """Glob string pointing at every year file for one ticker (for DuckDB)."""
    return str(ticker_dir(ticker) / "*.parquet")


# --- Write -----------------------------------------------------------------

def _coerce(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce dtypes to the canonical schema before writing."""
    out = df.copy()
    out["trade_date"]    = pd.to_datetime(out["trade_date"]).dt.date
    out["expiration"]    = pd.to_datetime(out["expiration"]).dt.date
    out["strike"]        = pd.to_numeric(out["strike"], errors="coerce").astype("float64")
    out["option_type"]   = out["option_type"].astype("string")
    out["open_interest"] = pd.to_numeric(out["open_interest"], errors="coerce").astype("int64")
    return out[["trade_date", "expiration", "strike", "option_type", "open_interest"]]


def _atomic_write(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, schema=_SCHEMA, preserve_index=False)
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(table, tmp, compression="snappy")
    tmp.replace(path)


def write_year(ticker: str, year: int, new_df: pd.DataFrame) -> int:
    """
    Merge new_df into {ticker}/{year}.parquet (creating the file if missing).

    Dedupes on (trade_date, expiration, strike, option_type), keeping the
    LATEST open_interest value (so a refetch overrides). Returns the total
    row count after merging.
    """
    if new_df.empty:
        return 0

    new_df = _coerce(new_df)
    new_df = new_df[new_df["trade_date"].apply(lambda d: d.year == year)]
    if new_df.empty:
        return 0

    existing = read_year(ticker, year)
    if existing.empty:
        merged = new_df
    else:
        merged = pd.concat([existing, new_df], ignore_index=True)

    merged = (
        merged
        .drop_duplicates(subset=DEDUPE_KEYS, keep="last")
        .sort_values(DEDUPE_KEYS)
        .reset_index(drop=True)
    )

    _atomic_write(year_path(ticker, year), merged)
    return len(merged)


def write_rows(ticker: str, df: pd.DataFrame) -> dict[int, int]:
    """
    Write rows spanning any number of years. Splits by year and merges into
    each year's file. Returns {year: row_count_after_merge}.
    """
    if df.empty:
        return {}
    df = _coerce(df)
    out: dict[int, int] = {}
    df["__year"] = df["trade_date"].apply(lambda d: d.year)
    for y, chunk in df.groupby("__year"):
        out[int(y)] = write_year(ticker, int(y), chunk.drop(columns="__year"))
    return out
