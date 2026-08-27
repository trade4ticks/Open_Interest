"""
Parquet storage for 1-minute equity bars (Polygon / Massive).

Layout:  {EQUITY_1MIN_DIR}/{ticker}/{year}.parquet
         {EQUITY_1MIN_DIR}/_manifest/{ticker}.parquet

Sibling of lib/chain_snapshot_store.py and lib/chain_intraday_store.py, and
follows their conventions deliberately: per-ticker directory, one file per
year, atomic .tmp+rename write, dedupe keep-last so a refetch overrides, and
an explicit ROW_GROUP_SIZE so row-group min/max statistics stay tight enough
for date-range predicate pushdown inside a year file.

--- Timestamps: naive ET, matching the sibling stores --------------------

chain_snapshots and chain_intraday both store `pa.timestamp("ms")` with NO
timezone, holding the vendor's naive-Eastern wall clock verbatim
(lib/chain_snapshot_store.py's schema comment says so explicitly). This store
matches that convention rather than introducing a second one.

That is safe here, not merely consistent. US DST transitions occur at 02:00
ET, and the extended-hours window this store covers is 04:00-20:00 ET — so no
bar can ever land in the skipped hour (spring forward) or the repeated hour
(fall back). Naive ET is unambiguous for every timestamp this store can hold.

Polygon nonetheless returns an unambiguous UTC millisecond epoch, and that raw
value is kept alongside as `ts_ms`. It costs ~1-2 bytes/row after delta+snappy
on a sorted column, makes the naive-ET column auditable against the vendor's
own value, and means the convention can be changed later without a refetch.

Storing the vendor's value verbatim is the same instinct the chain stores
follow; the difference is only that ThetaData hands over ET wall clock while
Polygon hands over an instant.

--- Schema ----------------------------------------------------------------

    ticker        STRING   — the CANONICAL (current) symbol. Redundant with the
                             path; dictionary-encoded, ~nil.
    source_symbol STRING   — the symbol actually REQUESTED for this bar. Equal
                             to ticker except across a rename: META bars from
                             before 2022-06-09 carry source_symbol='FB'. Keying
                             the store on the canonical symbol keeps one company
                             in one directory; keeping the requested symbol
                             makes the rename handling auditable after the fact
                             instead of a claim in a log file.
    trade_date    DATE     — ET session date; the resumability + audit key
    session       STRING   — 'premarket' / 'regular' / 'after' / 'other',
                             derived in ET against the EXCHANGE CALENDAR, so
                             13:00 early closes classify correctly
    timestamp     TIMESTAMP(ms) — bar open, naive ET (see above)
    ts_ms         INT64    — vendor's raw UTC epoch ms, verbatim
    open/high/low/close  FLOAT64
    volume        FLOAT64
    vwap          FLOAT64
    transactions  INT64

`trade_date` and `ts_ms` are additions to the columns requested; both earn
their place. trade_date is what the year-file layout partitions on and what
resumability and the completeness audit key on. ts_ms is the identity column —
a bar is uniquely a (ticker, instant), and dedupe keys on it.

--- The manifest, and why the store alone is not enough -------------------

The chain_snapshots fetcher derived resumability from the data itself: a
(trade_date, snapshot) present in the store meant "done". That broke because
requests failed at a FINER granularity (trade_date, expiration, snapshot), so
a session that wrote some expirations looked complete and was skipped forever.

Here a request is one (ticker, chunk) — a month. Derived-from-data
resumability would key on trade_date, which is again FINER than the request:
a chunk that half-wrote would leave some sessions present and look partly
done, and — worse — a ticker legitimately has no bars before it listed, so
"absent" cannot be distinguished from "failed" by looking at rows.

So the unit of a failed request is recorded explicitly, at exactly its own
granularity:

    (ticker, chunk_start, chunk_end) -> status in {ok, empty, failed}

`empty` is the case the data cannot express: the vendor genuinely has no bars
for that ticker-month (pre-listing, post-delisting). Recording it stops every
future run from refetching a permanently empty range forever.

WRITE ORDER IS LOAD-BEARING: bars are written and fsynced into the year file
BEFORE the manifest records the chunk. A crash between the two leaves the
chunk unrecorded, so the next run refetches it and the keep-last dedupe makes
that a no-op. The reverse order would record a chunk whose data never landed
and skip it forever — the exact bug this design exists to avoid.

The manifest is fast, exact-granularity bookkeeping; it is NOT the source of
truth for completeness. audit_equity_1min.py re-derives coverage from the bars
themselves and diffs against the calendar, which is what catches manifest
drift.

The manifest lives under _manifest/ rather than inside the ticker directory so
that parquet_glob()'s "*.parquet" — which DuckDB reads directly — can never
pick it up and union a foreign schema into a bar scan.
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import EQUITY_1MIN_DIR
from lib.parquet_schema import normalize_to_schema

SESSIONS = ("premarket", "regular", "after", "other")

# A bar IS its instant. trade_date leads only so the sort key and the dedupe
# key are the same list.
DEDUPE_KEYS = ["trade_date", "ts_ms"]
SORT_KEYS   = ["trade_date", "ts_ms"]

ROW_GROUP_SIZE = 128_000

_SCHEMA = pa.schema([
    ("ticker",        pa.string()),
    ("source_symbol", pa.string()),
    ("trade_date",    pa.date32()),
    ("session",       pa.string()),
    ("timestamp",     pa.timestamp("ms")),
    ("ts_ms",         pa.int64()),
    ("open",          pa.float64()),
    ("high",          pa.float64()),
    ("low",           pa.float64()),
    ("close",         pa.float64()),
    ("volume",        pa.float64()),
    ("vwap",          pa.float64()),
    ("transactions",  pa.int64()),
])

COLUMNS = [f.name for f in _SCHEMA]

_FLOAT_COLS = ["open", "high", "low", "close", "volume", "vwap"]
_INT_COLS   = ["ts_ms", "transactions"]
_DATE_COLS  = ["trade_date"]
_TS_COLS    = ["timestamp"]
_STR_COLS   = ["ticker", "source_symbol", "session"]

# --- Manifest schema --------------------------------------------------------

MANIFEST_OK     = "ok"
MANIFEST_EMPTY  = "empty"
MANIFEST_FAILED = "failed"

_MANIFEST_SCHEMA = pa.schema([
    ("ticker",      pa.string()),
    ("chunk_start", pa.date32()),
    ("chunk_end",   pa.date32()),
    ("status",      pa.string()),
    ("bars",        pa.int64()),
    ("sessions",    pa.int64()),
    ("fetched_at",  pa.timestamp("ms")),
    ("note",        pa.string()),
])
_MANIFEST_COLUMNS = [f.name for f in _MANIFEST_SCHEMA]


# --- Path helpers -----------------------------------------------------------

def ticker_dir(ticker: str) -> Path:
    return EQUITY_1MIN_DIR / ticker.upper()


def year_path(ticker: str, year: int) -> Path:
    return ticker_dir(ticker) / f"{year}.parquet"


def manifest_dir() -> Path:
    return EQUITY_1MIN_DIR / "_manifest"


def manifest_path(ticker: str) -> Path:
    return manifest_dir() / f"{ticker.upper()}.parquet"


def list_tickers() -> list[str]:
    """Tickers with at least one bar file. Underscore-prefixed directories are
    store bookkeeping (_manifest), never tickers."""
    if not EQUITY_1MIN_DIR.exists():
        return []
    out = []
    for p in EQUITY_1MIN_DIR.iterdir():
        if p.is_dir() and not p.name.startswith("_") and any(p.glob("*.parquet")):
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
    return bool(list_years(ticker))


def parquet_glob(ticker: str) -> str:
    return str(ticker_dir(ticker) / "*.parquet")


# --- Read -------------------------------------------------------------------

def read_year(ticker: str, year: int,
              columns: list[str] | None = None) -> pd.DataFrame:
    p = year_path(ticker, year)
    if not p.exists():
        return pd.DataFrame(columns=columns or COLUMNS)
    return pd.read_parquet(p, columns=columns)


def loaded_dates(ticker: str, years: set[int] | None = None) -> set:
    """Distinct trade_dates present for this ticker.

    Used by the AUDIT, not by the fetcher's skip logic — see the module
    docstring on why derived-from-data resumability is the wrong granularity
    here. Reads one column; pass `years` to bound the scan.
    """
    out: set = set()
    for y in list_years(ticker):
        if years is not None and y not in years:
            continue
        try:
            tbl = pq.read_table(year_path(ticker, y), columns=["trade_date"])
        except Exception:
            # A truncated or unreadable file must not read as "loaded".
            continue
        for td in tbl.column("trade_date").to_pylist():
            if td is not None:
                out.add(td)
    return out


def session_counts(ticker: str, years: set[int] | None = None) -> pd.DataFrame:
    """Per (trade_date, session) bar counts — the audit's input.

    Two columns off disk, aggregated in pandas. Reading whole year files would
    dominate an audit over 121 tickers.
    """
    frames = []
    for y in list_years(ticker):
        if years is not None and y not in years:
            continue
        try:
            tbl = pq.read_table(year_path(ticker, y),
                                columns=["trade_date", "session"])
        except Exception:
            continue
        frames.append(tbl.to_pandas())
    if not frames:
        return pd.DataFrame(columns=["trade_date", "session", "bars"])
    df = pd.concat(frames, ignore_index=True)
    return (df.groupby(["trade_date", "session"]).size()
              .reset_index(name="bars"))


# --- Write ------------------------------------------------------------------

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
        # Floor to ms so the ns -> ms cast in _atomic_write is exact; pyarrow
        # casts safely by default and raises on sub-ms precision.
        out[c] = pd.to_datetime(_col(df, c), errors="coerce").dt.floor("ms")

    for c in _FLOAT_COLS:
        out[c] = pd.to_numeric(_col(df, c), errors="coerce").astype("float64")

    for c in _INT_COLS:
        # Int64 (nullable) not int64: a single missing value would otherwise
        # promote the column to float and silently lose precision on ts_ms,
        # which is the dedupe key.
        out[c] = pd.to_numeric(_col(df, c), errors="coerce").astype("Int64")

    return out[COLUMNS]


def _atomic_write(path: Path, df: pd.DataFrame, schema: pa.Schema) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    # from_pandas's `schema=` is a request, not a guarantee: a pyarrow upgrade
    # once left it honoured for some string columns and not others. Applies to
    # both schemas this function is called with. See lib/parquet_schema.py.
    table = normalize_to_schema(table, schema, where=f"equity_1min/{path.name}")
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(table, tmp, compression="snappy",
                   row_group_size=ROW_GROUP_SIZE, use_dictionary=True)
    tmp.replace(path)


def write_year(ticker: str, year: int, new_df: pd.DataFrame) -> int:
    """Merge new_df into {ticker}/{year}.parquet, dedupe keep-last, sort, write.
    Returns total rows in the file after the merge."""
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

    _atomic_write(year_path(ticker, year), merged, _SCHEMA)
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


# --- Manifest ---------------------------------------------------------------

def read_manifest(ticker: str) -> pd.DataFrame:
    p = manifest_path(ticker)
    if not p.exists():
        return pd.DataFrame(columns=_MANIFEST_COLUMNS)
    try:
        return pd.read_parquet(p)
    except Exception:
        # An unreadable manifest must mean "nothing recorded", never "all
        # done" — the safe direction is refetching, which dedupe makes free.
        return pd.DataFrame(columns=_MANIFEST_COLUMNS)


def completed_chunks(ticker: str) -> set:
    """{(chunk_start, chunk_end)} the manifest records as ok or empty.

    `failed` chunks are deliberately NOT included: a recorded failure is a
    to-do, so a plain re-run retries it without needing --force.
    """
    mf = read_manifest(ticker)
    if mf.empty:
        return set()
    done = mf[mf["status"].isin([MANIFEST_OK, MANIFEST_EMPTY])]
    out = set()
    for a, b in zip(done["chunk_start"], done["chunk_end"]):
        if a is not None and b is not None:
            out.add((pd.Timestamp(a).date(), pd.Timestamp(b).date()))
    return out


def leading_empty_span(ticker: str) -> dict | None:
    """The run of `empty` chunks at the START of a ticker's fetched range.

    This is the ticker-rename detector of last resort. A rename makes the
    vendor return nothing for the pre-rename period under the new symbol, and
    every layer then behaves correctly while the data is wrong — see
    lib/polygon_symbols.py. A long leading run of `empty` chunks is what that
    looks like from the store's side, and it is indistinguishable from a
    genuine pre-listing gap WITHOUT reference data. So this reports the shape
    and leaves the judgement to a human with `list_date` in front of them.

    Returns None when the first chunk already has data (the common case).
    """
    mf = read_manifest(ticker)
    if mf.empty:
        return None
    mf = mf.sort_values("chunk_start").reset_index(drop=True)
    considered = mf[mf["status"].isin([MANIFEST_OK, MANIFEST_EMPTY])]
    if considered.empty:
        return None

    n_empty = 0
    first_data = None
    for _, r in considered.iterrows():
        if r["status"] == MANIFEST_EMPTY:
            n_empty += 1
        else:
            first_data = r["chunk_start"]
            break

    if n_empty == 0:
        return None
    return {
        "ticker": ticker.upper(),
        "empty_chunks": n_empty,
        "range_start": considered.iloc[0]["chunk_start"],
        "first_data_chunk": first_data,
        "all_empty": first_data is None,
    }


def first_data_date(ticker: str) -> date | None:
    """Earliest trade_date with at least one stored bar, or None."""
    dates = loaded_dates(ticker)
    return min(dates) if dates else None


def record_chunks(ticker: str, records: list[dict]) -> int:
    """Upsert chunk outcomes into the ticker's manifest, keyed on
    (chunk_start, chunk_end). Returns total manifest rows after the merge.

    MUST be called only AFTER the corresponding bars are durably written —
    see the module docstring on write ordering.
    """
    if not records:
        return 0
    new = pd.DataFrame(records)
    for c in _MANIFEST_COLUMNS:
        if c not in new.columns:
            new[c] = None
    new["ticker"] = ticker.upper()
    new["chunk_start"] = pd.to_datetime(new["chunk_start"], errors="coerce").dt.date
    new["chunk_end"]   = pd.to_datetime(new["chunk_end"], errors="coerce").dt.date
    new["status"]      = new["status"].astype("string")
    new["note"]        = new["note"].astype("string")
    new["bars"]        = pd.to_numeric(new["bars"], errors="coerce").astype("Int64")
    new["sessions"]    = pd.to_numeric(new["sessions"], errors="coerce").astype("Int64")
    new["fetched_at"]  = pd.to_datetime(new["fetched_at"], errors="coerce").dt.floor("ms")
    new = new[_MANIFEST_COLUMNS]

    existing = read_manifest(ticker)
    merged = pd.concat([existing, new], ignore_index=True) if not existing.empty else new
    merged = (merged
              .drop_duplicates(subset=["chunk_start", "chunk_end"], keep="last")
              .sort_values(["chunk_start"])
              .reset_index(drop=True))

    _atomic_write(manifest_path(ticker), merged, _MANIFEST_SCHEMA)
    return len(merged)
