"""
Parquet storage for twice-daily intraday option-chain snapshots.

Layout:  {CHAIN_SNAPSHOTS_DIR}/{ticker}/{YYYYMM}.parquet

--- Why monthly ------------------------------------------------------------

This store was {ticker}/{year}.parquet until the append path OOM-killed the
fetcher on a 15GB VPS. The append is read-modify-rewrite of a whole file, so
its peak memory is set by the file it merges into, not by the batch being
merged — and a year file only ever grows. A year frame in pandas is also
several times its on-disk size, because date32 comes back as an object array
of datetime.date (a pointer plus a ~32-byte object per cell, across three date
columns) where parquet stores 4 bytes. A few hundred MB on disk became
multiple GB of RSS, four copies live at the high-water mark.

Monthly divides that by ~12 and is where the problem stops: a ~15MB file is
~120MB of pandas, so the merge peaks in the hundreds of MB rather than the
gigabytes.

Not per-session, which lib/chain_intraday_store.py uses: that store's memory
bound comes from streaming one expiration at a time through a
pq.ParquetWriter, not from its file granularity, and it holds 78 snapshots per
session against this store's 2. Per-session here would mean ~212,000 files
against ~10,200 for monthly, for a problem already solved at monthly — and
chain_intraday made the same file-count trade itself, choosing session files
over {date}/{expiration} to keep a rolling window at 2,400 files rather than
84,000.

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

Write behaviour: read the existing month file, concat, dedupe on
(trade_date, snapshot, expiration, strike, option_type) keeping the LATEST
values (so a refetch overrides), SORT, then write atomically (.tmp + rename).

The sort is load-bearing, not cosmetic.  Rows are sorted by trade_date first
so that each row group covers a narrow date range; combined with an explicit
ROW_GROUP_SIZE this gives tight per-row-group min/max statistics and therefore
effective date-range predicate pushdown within a single file.  Sorting alone
would not achieve this — pyarrow's default row-group size is large enough that
a month could land as a single group spanning all of it.

That pushdown is now actually exercised: build_equity_surface.load_day passes
filters=[("trade_date", "==", day)]. It did not until the same change that
introduced this layout, which is why the store could carry a whole year per
file for as long as it did without anyone noticing the read cost.
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


# Month keys are the int YYYYMM, which is exactly the filename stem, sorts
# chronologically as an integer, and cannot be confused with a bare year the
# way a (year, month) tuple silently could during the migration.

def month_key(d: date) -> int:
    return d.year * 100 + d.month


def month_path(ticker: str, ym: int) -> Path:
    return ticker_dir(ticker) / f"{ym:06d}.parquet"


def months_between(start: date, end: date) -> set[int]:
    """Every month key touched by the inclusive range [start, end]."""
    out: set[int] = set()
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        out.add(y * 100 + m)
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def list_tickers() -> list[str]:
    """Tickers that have at least one parquet file under CHAIN_SNAPSHOTS_DIR."""
    if not CHAIN_SNAPSHOTS_DIR.exists():
        return []
    out = []
    for p in CHAIN_SNAPSHOTS_DIR.iterdir():
        if p.is_dir() and any(p.glob("*.parquet")):
            out.append(p.name.upper())
    return sorted(out)


def list_months(ticker: str) -> list[int]:
    """Month keys present on disk, ascending.

    A stem that is not a 6-digit YYYYMM is skipped rather than guessed at —
    which is what makes a leftover {YYYY}.parquet from before the monthly
    migration invisible here instead of being read as month 2024 of year 20.
    """
    d = ticker_dir(ticker)
    if not d.exists():
        return []
    months = []
    for p in d.glob("*.parquet"):
        stem = p.stem
        if len(stem) != 6 or not stem.isdigit():
            continue
        ym = int(stem)
        if not 1 <= ym % 100 <= 12:
            continue
        months.append(ym)
    return sorted(months)


def list_legacy_year_files(ticker: str) -> list[Path]:
    """Pre-migration {YYYY}.parquet files still present for this ticker.

    Exists so callers can SAY that unmigrated data is being ignored rather
    than silently returning less than the store holds. `list_months` skips
    these by construction; without this they would be invisible.
    """
    d = ticker_dir(ticker)
    if not d.exists():
        return []
    return sorted(p for p in d.glob("*.parquet")
                  if len(p.stem) == 4 and p.stem.isdigit())


def has_data(ticker: str) -> bool:
    d = ticker_dir(ticker)
    return d.exists() and any(d.glob("*.parquet"))


def parquet_glob(ticker: str) -> str:
    """Glob string pointing at every month file for one ticker (for DuckDB)."""
    return str(ticker_dir(ticker) / "*.parquet")


# --- Read ------------------------------------------------------------------

def read_month(ticker: str, ym: int,
               columns: list[str] | None = None) -> pd.DataFrame:
    p = month_path(ticker, ym)
    if not p.exists():
        return pd.DataFrame(columns=columns or COLUMNS)
    return pd.read_parquet(p, columns=columns)


def loaded_cells(ticker: str,
                 months: set[int] | None = None,
                 dates: set[date] | None = None) -> set[tuple[date, date, str]]:
    """(trade_date, expiration, snapshot) triples present in the store.

    This is the granularity at which point queries actually fail, so it is the
    granularity gap repair has to work at. `loaded_keys` below is coarser —
    (trade_date, snapshot) — which is why a plain re-run cannot see a hole
    inside a session that wrote *some* expirations.

    Reads three columns only. Pass `months` and/or `dates` to bound the scan.
    """
    out: set[tuple[date, date, str]] = set()
    for ym in list_months(ticker):
        if months is not None and ym not in months:
            continue
        try:
            tbl = pq.read_table(month_path(ticker, ym),
                                columns=["trade_date", "expiration", "snapshot"])
        except Exception:
            continue
        df = tbl.to_pandas().drop_duplicates()
        if dates is not None:
            df = df[df["trade_date"].isin(dates)]
        for td, exp, sn in zip(df["trade_date"], df["expiration"], df["snapshot"]):
            if td is not None and exp is not None and sn is not None:
                out.add((td, exp, str(sn)))
    return out


def loaded_keys(ticker: str, months: set[int] | None = None) -> set[tuple[date, str]]:
    """Distinct (trade_date, snapshot) pairs already present for this ticker.

    Used by the fetcher to skip work that's already done.  Reads only the two
    key columns — a full read would dominate startup.  Pass `months` to
    restrict the scan to the months the caller's date range actually touches;
    under the monthly layout that is a far tighter bound than the year set it
    replaced.
    """
    out: set[tuple[date, str]] = set()
    for ym in list_months(ticker):
        if months is not None and ym not in months:
            continue
        try:
            tbl = pq.read_table(month_path(ticker, ym),
                                columns=["trade_date", "snapshot"])
        except Exception:
            # A truncated or unreadable file must not make the caller think
            # those dates are loaded — treat as "nothing loaded" for that month.
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


def _atomic_write_table(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(table, tmp, compression="snappy",
                   row_group_size=ROW_GROUP_SIZE, use_dictionary=True)
    tmp.replace(path)


def _atomic_write(path: Path, df: pd.DataFrame) -> None:
    _atomic_write_table(
        path, pa.Table.from_pandas(df, schema=_SCHEMA, preserve_index=False))


def write_month(ticker: str, ym: int, new_df: pd.DataFrame,
                coerced: bool = False) -> int:
    """
    Merge new_df into {ticker}/{YYYYMM}.parquet (creating the file if missing).

    Dedupes on (trade_date, snapshot, expiration, strike, option_type) keeping
    LATEST so a refetch overrides, then sorts by SORT_KEYS before writing.
    Returns the total row count after merging.

    `coerced=True` promises new_df has already been through `_coerce`, which
    lets `write_rows` skip a second pass. `_coerce` is idempotent, so the flag
    is an optimisation only — it never changes the result, just the number of
    full-frame copies alive at once.

    --- Memory ---------------------------------------------------------------

    This is still read-modify-rewrite, so its cost is set by the file it merges
    into rather than by the batch — but the file is now a month, which is what
    bounds it. See the module docstring for why monthly and not yearly or
    per-session.

    Each rebinding below is deliberate. Written as one chained expression
    (concat -> drop_duplicates -> sort_values -> reset_index) pandas holds the
    intermediate of every stage alive simultaneously, because the chain keeps a
    reference to each; that is three whole-file frames plus both inputs at the
    high-water mark. Rebinding `merged` at each step lets the previous stage's
    frame be freed as soon as the next one is built, and dropping the inputs
    right after the concat has copied them removes two more. `ignore_index` on
    the sort folds what was a separate `reset_index` copy into it.

    Converting to Arrow before releasing the pandas frame — rather than after,
    as the original ordering did — keeps the fat pandas representation and the
    parquet serialisation buffers from being live at the same time.
    """
    if new_df.empty:
        return 0

    if not coerced:
        new_df = _coerce(new_df)
    keep = new_df["trade_date"].map(
        lambda d: (d is not None and not pd.isna(d)
                   and d.year * 100 + d.month == ym)
    ).astype(bool)
    if not keep.all():
        new_df = new_df[keep]
    del keep
    if new_df.empty:
        return 0

    existing = read_month(ticker, ym)
    if existing.empty:
        merged = new_df
        del existing
    else:
        merged = pd.concat([existing, new_df], ignore_index=True)
        # concat has already copied both inputs; holding them through the
        # dedupe and sort below is two whole frames of pure waste.
        del existing
    new_df = None

    merged = merged.drop_duplicates(subset=DEDUPE_KEYS, keep="last")
    merged = merged.sort_values(SORT_KEYS, ignore_index=True)

    n = len(merged)
    table = pa.Table.from_pandas(merged, schema=_SCHEMA, preserve_index=False)
    del merged
    _atomic_write_table(month_path(ticker, ym), table)
    return n


def write_rows(ticker: str, df: pd.DataFrame) -> dict:
    """Write rows spanning any number of months. Splits by month and merges
    into each month's file. Returns {YYYYMM: row_count_after_merge}."""
    if df.empty:
        return {}
    df = _coerce(df)
    df = df[df["trade_date"].notna()]
    if df.empty:
        return {}

    # Group on a derived Series rather than an assigned column: `assign` copies
    # the whole frame to add the key and `drop(columns=...)` copies it back, so
    # the old form paid two extra full-batch copies for a grouping key that
    # never needed to be in the frame at all.
    keys = df["trade_date"].map(month_key)
    out: dict = {}
    for ym, chunk in df.groupby(keys, sort=True):
        out[int(ym)] = write_month(ticker, int(ym), chunk, coerced=True)
    return out
