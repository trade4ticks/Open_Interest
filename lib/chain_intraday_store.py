"""
Parquet storage for full-day 5-minute intraday option-chain bars.

Layout:  {CHAIN_INTRADAY_DIR}/{ticker}/{year}.parquet

Sibling of lib/chain_snapshot_store.py, carrying the IDENTICAL 20-column
schema so the two stores are directly comparable — the intraday rows stamped
snapshot='0945' and '1545' should match the snapshots store row for row, which
is how the fetch-shape experiment gets validated.

The one structural difference is the key. The snapshots store has exactly two
rows per contract-day and keys on the '0945'/'1545' label; this store has ~78
rows per contract-day, one per 5-minute bar, and keys on the bar's own
timestamp:

    DEDUPE/SORT: (trade_date, timestamp, expiration, strike, option_type)

`snapshot` is still populated, as HHMM derived from each bar's timestamp
('0935', '0940', ... '1600'), so it stays a useful filter and keeps the
schema identical — but it is the timestamp, not the label, that identifies a
bar. Keying on the label alone would be equivalent here, but the timestamp is
the vendor's own value and cannot drift from the data it stamps.

Volume warning: ~78x the rows per session of the snapshots store. For a
wide chain like SPY that is ~1.1M rows per session, so a full year in one
file approaches 280M rows. The year-file layout (read-modify-rewrite on every
append) is chosen here for consistency with the sibling stores and is fine for
the bounded measurement runs this store exists for; it is NOT obviously the
right layout for a multi-year 5-minute store, and the (D) PARQUET WRITE GROWTH
section of the timing summary is what should decide that.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import CHAIN_INTRADAY_DIR

# Bar identity is the timestamp, not a fixed label.
DEDUPE_KEYS = ["trade_date", "timestamp", "expiration", "strike", "option_type"]

# trade_date leads so row-group date stats stay tight; timestamp second keeps
# a session's bars time-contiguous, so a time-of-day filter also skips groups.
SORT_KEYS = ["trade_date", "timestamp", "expiration", "strike", "option_type"]

ROW_GROUP_SIZE = 128_000

_SCHEMA = pa.schema([
    ("ticker",               pa.string()),
    ("trade_date",           pa.date32()),
    ("snapshot",             pa.string()),
    ("feature_date",         pa.date32()),
    ("timestamp",            pa.timestamp("ms")),
    ("expiration",           pa.date32()),
    ("strike",               pa.float64()),
    ("option_type",          pa.string()),
    ("bid",                  pa.float64()),
    ("ask",                  pa.float64()),
    ("delta",                pa.float64()),
    ("theta",                pa.float64()),
    ("vega",                 pa.float64()),
    ("rho",                  pa.float64()),
    ("epsilon",              pa.float64()),
    ("lambda",               pa.float64()),
    ("implied_vol",          pa.float64()),
    ("iv_error",             pa.float64()),
    ("underlying_timestamp", pa.timestamp("ms")),
    ("underlying_price",     pa.float64()),
])

COLUMNS = [f.name for f in _SCHEMA]

_FLOAT_COLS = [
    "strike", "bid", "ask", "delta", "theta", "vega", "rho", "epsilon",
    "lambda", "implied_vol", "iv_error", "underlying_price",
]
_DATE_COLS = ["trade_date", "feature_date", "expiration"]
_TS_COLS   = ["timestamp", "underlying_timestamp"]
_STR_COLS  = ["ticker", "snapshot", "option_type"]


# --- Path helpers ----------------------------------------------------------

def ticker_dir(ticker: str) -> Path:
    return CHAIN_INTRADAY_DIR / ticker.upper()


def year_path(ticker: str, year: int) -> Path:
    return ticker_dir(ticker) / f"{year}.parquet"


def list_tickers() -> list[str]:
    if not CHAIN_INTRADAY_DIR.exists():
        return []
    out = []
    for p in CHAIN_INTRADAY_DIR.iterdir():
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
    d = ticker_dir(ticker)
    return d.exists() and any(d.glob("*.parquet"))


def parquet_glob(ticker: str) -> str:
    return str(ticker_dir(ticker) / "*.parquet")


# --- Read ------------------------------------------------------------------

def read_year(ticker: str, year: int,
              columns: list[str] | None = None) -> pd.DataFrame:
    p = year_path(ticker, year)
    if not p.exists():
        return pd.DataFrame(columns=columns or COLUMNS)
    return pd.read_parquet(p, columns=columns)


def loaded_dates(ticker: str, years: set[int] | None = None) -> set[date]:
    """Distinct trade_dates already present for this ticker.

    Resumability is per SESSION here, not per (session, label): one interval
    call covers a whole session's bars, so a session is either fetched or not.

    Reads only the one key column — these files are very large and a full read
    would dominate startup. Pass `years` to restrict the scan.
    """
    out: set[date] = set()
    for y in list_years(ticker):
        if years is not None and y not in years:
            continue
        try:
            tbl = pq.read_table(year_path(ticker, y), columns=["trade_date"])
        except Exception:
            # A truncated or unreadable file must not make the caller think
            # those dates are loaded — treat as "nothing loaded" for that year.
            continue
        for td in tbl.column("trade_date").to_pylist():
            if td is not None:
                out.add(td)
    return out


# --- Write -----------------------------------------------------------------

def _col(df: pd.DataFrame, name: str) -> pd.Series:
    if name in df.columns:
        return df[name]
    return pd.Series([None] * len(df), index=df.index, dtype="object")


def _coerce(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)

    for c in _STR_COLS:
        out[c] = _col(df, c).astype("string")

    for c in _DATE_COLS:
        out[c] = pd.to_datetime(_col(df, c), errors="coerce").dt.date

    for c in _TS_COLS:
        # Floor to milliseconds so the ns -> ms cast in _atomic_write is exact.
        out[c] = pd.to_datetime(_col(df, c), errors="coerce").dt.floor("ms")

    for c in _FLOAT_COLS:
        out[c] = pd.to_numeric(_col(df, c), errors="coerce").astype("float64")

    return out[COLUMNS]


def _atomic_write(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, schema=_SCHEMA, preserve_index=False)
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(table, tmp, compression="snappy",
                   row_group_size=ROW_GROUP_SIZE, use_dictionary=True)
    tmp.replace(path)


def write_year(ticker: str, year: int, new_df: pd.DataFrame) -> int:
    """Merge new_df into {ticker}/{year}.parquet, dedupe keep-last, sort, write."""
    if new_df.empty:
        return 0

    new_df = _coerce(new_df)
    new_df = new_df[new_df["trade_date"].apply(
        lambda d: d is not None and not pd.isna(d) and d.year == year
    )]
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
        .sort_values(SORT_KEYS)
        .reset_index(drop=True)
    )

    _atomic_write(year_path(ticker, year), merged)
    return len(merged)


def write_rows(ticker: str, df: pd.DataFrame) -> dict:
    """Write rows spanning any number of years. Returns {year: rows_after_merge}."""
    if df.empty:
        return {}
    df = _coerce(df)
    df = df[df["trade_date"].notna()]
    if df.empty:
        return {}
    out: dict = {}
    df = df.assign(__year=df["trade_date"].apply(lambda d: d.year))
    for y, chunk in df.groupby("__year"):
        out[int(y)] = write_year(ticker, int(y), chunk.drop(columns="__year"))
    return out
