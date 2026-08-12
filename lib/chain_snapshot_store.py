"""
Parquet storage for twice-daily intraday option-chain snapshots.

Layout:  {CHAIN_SNAPSHOTS_DIR}/{ticker}/{year}.parquet

Sibling to lib/chain_store.py (the EOD greeks chain).  Deliberately separate:
this store is fed by /v3/option/history/greeks/first_order at 09:45 and 15:45,
carries bid/ask and the full first-order greek set, and keeps every row the
vendor returns (including zero-IV / no-quote contracts — an absent or wide
market is itself information).

Schema (20 columns):
    ticker              STRING  — redundant with the path; stored by request.
                                  Parquet dictionary-encodes it, so the on-disk
                                  cost is ~nil.
    trade_date          DATE    — session the snapshot is from (NOT shifted)
    snapshot            STRING  — '0945' / '1545'
    feature_date        DATE    — next_trading_day(trade_date); same function
                                  and therefore the same join key as chain_eod
    timestamp           TIMESTAMP — vendor value, verbatim, naive ET
    expiration          DATE
    strike              FLOAT64 — raw, unadjusted (split adjustment applied at
                                  read time, same convention as chain_eod)
    option_type         STRING  ('C' / 'P')
    bid, ask            FLOAT64
    delta, theta, vega, rho, epsilon, lambda   FLOAT64
    implied_vol         FLOAT64
    iv_error            FLOAT64
    underlying_timestamp TIMESTAMP — kept for now: it is the field that proves
                                  the underlying is aligned to the requested
                                  snapshot instant.  Drop once validated.
    underlying_price    FLOAT64

There is no `source_session` column.  chain_eod reserved one for "the future
15:45 endpoint where the two may diverge" — that is this store, and `snapshot`
now carries that information, so source_session would only duplicate
trade_date.  Map snapshot -> source_session at join time if a validation join
against chain_eod needs it.

Write behaviour: read the existing year file, concat, dedupe on
(trade_date, snapshot, expiration, strike, option_type) keeping the LATEST
values (so a refetch overrides), SORT, then write atomically (.tmp + rename).

The sort is load-bearing, not cosmetic.  Rows are sorted by trade_date first
so that each row group covers a narrow date range; combined with an explicit
ROW_GROUP_SIZE this gives tight per-row-group min/max statistics and therefore
effective date-range predicate pushdown within a single year file.  Sorting
alone would not achieve this — pyarrow's default row-group size is large
enough that a year could land as a handful of groups each spanning months.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import CHAIN_SNAPSHOTS_DIR

# Snapshot labels used across the store and the fetcher.
SNAPSHOT_LABELS = ("0945", "1545")

DEDUPE_KEYS = ["trade_date", "snapshot", "expiration", "strike", "option_type"]

# trade_date leads so row-group date stats are tight; snapshot second keeps a
# session's two snapshots contiguous, so filtering one snapshot also skips
# row groups.
SORT_KEYS = ["trade_date", "snapshot", "expiration", "strike", "option_type"]

# Small enough that a year file gets many row groups (tight date min/max),
# large enough to keep per-group overhead and compression sane.
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
    return CHAIN_SNAPSHOTS_DIR / ticker.upper()


def year_path(ticker: str, year: int) -> Path:
    return ticker_dir(ticker) / f"{year}.parquet"


def list_tickers() -> list[str]:
    """Tickers that have at least one parquet file under CHAIN_SNAPSHOTS_DIR."""
    if not CHAIN_SNAPSHOTS_DIR.exists():
        return []
    out = []
    for p in CHAIN_SNAPSHOTS_DIR.iterdir():
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
    """Glob string pointing at every year file for one ticker (for DuckDB)."""
    return str(ticker_dir(ticker) / "*.parquet")


# --- Read ------------------------------------------------------------------

def read_year(ticker: str, year: int,
              columns: list[str] | None = None) -> pd.DataFrame:
    p = year_path(ticker, year)
    if not p.exists():
        return pd.DataFrame(columns=columns or COLUMNS)
    return pd.read_parquet(p, columns=columns)


def loaded_keys(ticker: str, years: set[int] | None = None) -> set[tuple[date, str]]:
    """Distinct (trade_date, snapshot) pairs already present for this ticker.

    Used by the fetcher to skip work that's already done.  Reads only the two
    key columns — these year files are large and a full read would dominate
    startup.  Pass `years` to restrict the scan to the years the caller's date
    range actually touches.
    """
    out: set[tuple[date, str]] = set()
    for y in list_years(ticker):
        if years is not None and y not in years:
            continue
        try:
            tbl = pq.read_table(year_path(ticker, y),
                                columns=["trade_date", "snapshot"])
        except Exception:
            # A truncated or unreadable file must not make the caller think
            # those dates are loaded — treat as "nothing loaded" for that year.
            continue
        df = tbl.to_pandas().drop_duplicates()
        for td, sn in zip(df["trade_date"], df["snapshot"]):
            if td is not None and sn is not None:
                out.add((td, str(sn)))
    return out


# --- Write -----------------------------------------------------------------

def _col(df: pd.DataFrame, name: str) -> pd.Series:
    """Column `name`, or an all-null Series if the vendor omitted it."""
    if name in df.columns:
        return df[name]
    return pd.Series([None] * len(df), index=df.index, dtype="object")


def _coerce(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce to the canonical schema, tolerating missing vendor columns."""
    out = pd.DataFrame(index=df.index)

    for c in _STR_COLS:
        out[c] = _col(df, c).astype("string")

    for c in _DATE_COLS:
        out[c] = pd.to_datetime(_col(df, c), errors="coerce").dt.date

    for c in _TS_COLS:
        # Floor to milliseconds so the ns -> ms cast in _atomic_write is exact.
        # pyarrow casts safely by default and would raise on sub-ms precision.
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
    """
    Merge new_df into {ticker}/{year}.parquet (creating the file if missing).

    Dedupes on (trade_date, snapshot, expiration, strike, option_type) keeping
    LATEST so a refetch overrides, then sorts by SORT_KEYS before writing.
    Returns the total row count after merging.
    """
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
    """Write rows spanning any number of years. Splits by year and merges into
    each year's file. Returns {year: row_count_after_merge}."""
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
