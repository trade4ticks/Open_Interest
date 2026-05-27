"""
Parquet storage for raw EOD greeks chain rows.

Layout:  {CHAIN_EOD_DIR}/{ticker}/{year}.parquet

Schema (10 columns):
    trade_date     DATE   — actual session the data is from (NOT shifted)
    source_session DATE   — = trade_date in this store; included for audit symmetry
                            and forward-compat with the future 15:45 endpoint
                            where the two may diverge
    feature_date   DATE   — next_trading_day(trade_date); join key for build_features
    expiration     DATE
    strike         FLOAT64 — raw, unadjusted (split adjustment applied at read
                             time in the chain_adj DuckDB view)
    option_type    STRING ('C' / 'P')
    volume         INT64
    implied_vol    FLOAT64
    delta          FLOAT64
    iv_error       FLOAT64 — solver residual; NULL if endpoint omits the field

Append behaviour: read existing file, concat new rows, dedupe on
(trade_date, expiration, strike, option_type) keeping the LATEST values
(so a refetch overrides), sort, write atomically (write to .tmp then rename).
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import CHAIN_EOD_DIR

DEDUPE_KEYS = ["trade_date", "expiration", "strike", "option_type"]

_SCHEMA = pa.schema([
    ("trade_date",     pa.date32()),
    ("source_session", pa.date32()),
    ("feature_date",   pa.date32()),
    ("expiration",     pa.date32()),
    ("strike",         pa.float64()),
    ("option_type",    pa.string()),
    ("volume",         pa.int64()),
    ("implied_vol",    pa.float64()),
    ("delta",          pa.float64()),
    ("iv_error",       pa.float64()),
])

COLUMNS = [f.name for f in _SCHEMA]


# --- Path helpers ----------------------------------------------------------

def ticker_dir(ticker: str) -> Path:
    return CHAIN_EOD_DIR / ticker.upper()


def year_path(ticker: str, year: int) -> Path:
    return ticker_dir(ticker) / f"{year}.parquet"


def list_tickers() -> list[str]:
    """Tickers that have at least one parquet file under CHAIN_EOD_DIR."""
    if not CHAIN_EOD_DIR.exists():
        return []
    out = []
    for p in CHAIN_EOD_DIR.iterdir():
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


def has_data(ticker: str) -> bool:
    """True if this ticker has any parquet file in the chain store."""
    d = ticker_dir(ticker)
    return d.exists() and any(d.glob("*.parquet"))


def parquet_glob(ticker: str) -> str:
    """Glob string pointing at every year file for one ticker (for DuckDB)."""
    return str(ticker_dir(ticker) / "*.parquet")


# --- Read ------------------------------------------------------------------

def read_year(ticker: str, year: int) -> pd.DataFrame:
    p = year_path(ticker, year)
    if not p.exists():
        return pd.DataFrame(columns=COLUMNS)
    return pd.read_parquet(p)


def loaded_dates(ticker: str) -> set:
    """Distinct trade_dates already present in this ticker's parquet store.
    Used by the resumable backfill to skip work that's already done."""
    out: set = set()
    if not ticker_dir(ticker).exists():
        return out
    for y in list_years(ticker):
        df = read_year(ticker, y)
        if df.empty:
            continue
        out.update(df["trade_date"].tolist())
    return out


# --- Write -----------------------------------------------------------------

def _coerce(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce dtypes to the canonical schema before writing."""
    out = df.copy()
    out["trade_date"]     = pd.to_datetime(out["trade_date"]).dt.date
    out["source_session"] = pd.to_datetime(out["source_session"]).dt.date
    out["feature_date"]   = pd.to_datetime(out["feature_date"]).dt.date
    out["expiration"]     = pd.to_datetime(out["expiration"]).dt.date
    out["strike"]         = pd.to_numeric(out["strike"], errors="coerce").astype("float64")
    out["option_type"]    = out["option_type"].astype("string")
    out["volume"]         = pd.to_numeric(out.get("volume", 0), errors="coerce").fillna(0).astype("int64")
    out["implied_vol"]    = pd.to_numeric(out["implied_vol"], errors="coerce").astype("float64")
    out["delta"]          = pd.to_numeric(out.get("delta"), errors="coerce").astype("float64")
    out["iv_error"]       = pd.to_numeric(out.get("iv_error"), errors="coerce").astype("float64")
    return out[COLUMNS]


def _atomic_write(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, schema=_SCHEMA, preserve_index=False)
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(table, tmp, compression="snappy")
    tmp.replace(path)


def write_year(ticker: str, year: int, new_df: pd.DataFrame) -> int:
    """
    Merge new_df into {ticker}/{year}.parquet (creating the file if missing).

    Dedupes on (trade_date, expiration, strike, option_type), keeping LATEST
    so a refetch overrides. Returns the total row count after merging.
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


def write_rows(ticker: str, df: pd.DataFrame) -> dict:
    """Write rows spanning any number of years. Splits by year and merges
    into each year's file. Returns {year: row_count_after_merge}."""
    if df.empty:
        return {}
    df = _coerce(df)
    out: dict = {}
    df["__year"] = df["trade_date"].apply(lambda d: d.year)
    for y, chunk in df.groupby("__year"):
        out[int(y)] = write_year(ticker, int(y), chunk.drop(columns="__year"))
    return out
