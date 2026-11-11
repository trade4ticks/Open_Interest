"""
Parquet storage for raw SPX / SPXW trade-based EOD option volume.

Layout:  {SPX_VOLUME_DIR}/{root}/{year}.parquet

Schema per file (root is implied by path, as ticker is in lib.parquet_store):
    trade_date    DATE    the session the volume was traded in (NOT shifted;
                          the build maps session T-1 onto row T)
    expiration    DATE
    strike        FLOAT64 raw — index options have no split adjustment
    option_type   STRING  ('C'/'P')
    volume        INT64   > 0 only; zero-volume contracts add nothing to any
                          volume aggregate and are not stored

Write semantics differ from lib.parquet_store on purpose: a write REPLACES
every trade_date it carries rather than merging contract by contract. A
re-fetched day therefore cannot keep a stale contract the vendor no longer
reports, which a keep-last merge would.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import SPX_VOLUME_DIR
from lib.parquet_schema import normalize_to_schema

KEYS = ["trade_date", "expiration", "strike", "option_type"]

_SCHEMA = pa.schema([
    ("trade_date",  pa.date32()),
    ("expiration",  pa.date32()),
    ("strike",      pa.float64()),
    ("option_type", pa.string()),
    ("volume",      pa.int64()),
])

COLUMNS = [f.name for f in _SCHEMA]


def root_dir(root: str) -> Path:
    return SPX_VOLUME_DIR / root.upper()


def year_path(root: str, year: int) -> Path:
    return root_dir(root) / f"{year}.parquet"


def read_year(root: str, year: int) -> pd.DataFrame:
    p = year_path(root, year)
    if not p.exists():
        return pd.DataFrame(columns=COLUMNS)
    return pd.read_parquet(p)


def _coerce(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["trade_date"]  = pd.to_datetime(out["trade_date"]).dt.date
    out["expiration"]  = pd.to_datetime(out["expiration"]).dt.date
    out["strike"]      = pd.to_numeric(out["strike"], errors="coerce").astype("float64")
    out["option_type"] = out["option_type"].astype("string")
    out["volume"]      = pd.to_numeric(out["volume"], errors="coerce").astype("int64")
    return out[COLUMNS]


def _atomic_write(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, schema=_SCHEMA, preserve_index=False)
    table = normalize_to_schema(table, _SCHEMA, where=f"spx_volume_eod/{path.name}")
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(table, tmp, compression="snappy")
    tmp.replace(path)


def write_year(root: str, year: int, new_df: pd.DataFrame) -> int:
    """Replace every trade_date in new_df within {root}/{year}.parquet.
    Returns the file's total row count after the write."""
    if new_df.empty:
        return 0
    new_df = _coerce(new_df)
    new_df = new_df[new_df["trade_date"].map(lambda d: d.year == year)]
    if new_df.empty:
        return 0
    if new_df.duplicated(subset=KEYS).any():
        raise ValueError(f"{root}/{year}: duplicate {KEYS} in rows to write")

    existing = read_year(root, year)
    if not existing.empty:
        existing["trade_date"] = pd.to_datetime(existing["trade_date"]).dt.date
        existing = existing[~existing["trade_date"].isin(set(new_df["trade_date"]))]
        merged = pd.concat([existing, new_df], ignore_index=True)
    else:
        merged = new_df

    merged = merged.sort_values(KEYS).reset_index(drop=True)
    _atomic_write(year_path(root, year), merged)
    return len(merged)


def write_rows(root: str, df: pd.DataFrame) -> dict[int, int]:
    """Write rows spanning any number of years. Returns {year: rows_after}."""
    if df.empty:
        return {}
    df = _coerce(df)
    years = df["trade_date"].map(lambda d: d.year)
    return {int(y): write_year(root, int(y), chunk)
            for y, chunk in df.groupby(years)}
