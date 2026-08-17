"""
Parquet storage for full-day 5-minute intraday option-chain bars.

Layout:  {CHAIN_INTRADAY_DIR}/{ticker}/{YYYYMMDD}.parquet
         {CHAIN_INTRADAY_DIR}/{ticker}/{YYYYMMDD}.manifest.json

ONE FILE PER (ticker, session), written INCREMENTALLY — one row group per
expiration — so the whole session is never held in memory.

--- Why not a year file ----------------------------------------------------

The previous layout was {ticker}/{year}.parquet, whose append path was
read-modify-rewrite: coerce, read the year back, concat, dedupe, sort, convert
to Arrow. On a wide chain a session is ~1.1M rows (~167MB per copy), and that
path materialised eight-plus copies of it, on top of the caller accumulating
the session with pd.concat before handing it over. On a 7.8GB VPS with no
swap that was an OOM on SPY/QQQ-class tickers.

The fix mirrors the SPX intraday fetcher (Thetadata_Raw_SPX), which never OOMs
on comparably large chains for exactly one reason: it never holds more than
one expiration. Its leaf file is {date}/{expiration}, so its read-modify-
rewrite works on ~31K rows. This store keeps the file count low by writing one
file per ticker-session instead — a rolling 20-day window is ~2,400 files
rather than ~84,000, and expiring a day deletes ~120 files rather than ~4,200
— and gets the same memory bound from pq.ParquetWriter, which serialises each
table to disk on write_table and retains only footer metadata (per row group,
per column: min/max/null count, single-digit KB at ~35 row groups).

Peak per session is therefore one expiration (~31K rows, ~5MB), not the
session (~1.1M rows, ~167MB), regardless of how wide the chain is.

--- Atomicity and partial sessions -----------------------------------------

The writer targets {YYYYMMDD}.parquet.tmp and only renames to .parquet after
close() has written the footer. A killed process leaves a .tmp with NO footer
— unreadable as parquet, so it cannot be mistaken for data — and the session
is simply absent. loaded_dates() never sees it and the next run refetches the
day in full. A mid-session death can never leave a silently incomplete day.

That covers the process dying. It does not cover a session that completed with
some expirations failing, which would leave a file that looks loaded. Hence
the sidecar manifest recording enumerated / written / failed expirations.
`loaded_dates()` counts a session as loaded only when the manifest exists AND
records no failures, so a plain re-run retries an incomplete day without
--force. The manifest also makes a completeness audit a pure file read, with
no parquet I/O.

--- Keys -------------------------------------------------------------------

Within a session file trade_date is constant and each expiration occupies
exactly one row group, so:

    DEDUPE (within an expiration): (timestamp, expiration, strike, option_type)
    SORT   (within a row group):   (timestamp, strike, option_type)

There is no global dedupe pass over the file. Correctness comes from writing
each expiration exactly once; the per-expiration dedupe guards only against
the vendor repeating a bar inside one response. Cross-expiration duplicates
are structurally impossible because each expiration is written in exactly one
row group.

Date and expiration pruning for future consumers happens by path (one file per
session) and by row-group statistics (one expiration per group) rather than by
a whole-file sort.
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import CHAIN_INTRADAY_DIR

# Applied per expiration, before its row group is written.
DEDUPE_KEYS = ["timestamp", "expiration", "strike", "option_type"]
SORT_KEYS   = ["timestamp", "strike", "option_type"]

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


def session_path(ticker: str, sess: date) -> Path:
    return ticker_dir(ticker) / f"{sess:%Y%m%d}.parquet"


def manifest_path(ticker: str, sess: date) -> Path:
    return ticker_dir(ticker) / f"{sess:%Y%m%d}.manifest.json"


def list_tickers() -> list[str]:
    if not CHAIN_INTRADAY_DIR.exists():
        return []
    out = []
    for p in CHAIN_INTRADAY_DIR.iterdir():
        if p.is_dir() and any(p.glob("*.parquet")):
            out.append(p.name.upper())
    return sorted(out)


def has_data(ticker: str) -> bool:
    d = ticker_dir(ticker)
    return d.exists() and any(d.glob("*.parquet"))


def parquet_glob(ticker: str) -> str:
    """Glob covering every session file for one ticker (for DuckDB)."""
    return str(ticker_dir(ticker) / "*.parquet")


# --- Manifest --------------------------------------------------------------

def read_manifest(ticker: str, sess: date) -> dict | None:
    p = manifest_path(ticker, sess)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        # Unreadable manifest must not read as "complete".
        return None


def write_manifest(ticker: str, sess: date, enumerated: int,
                   written: list[date], failed: list[str], rows: int) -> None:
    payload = {
        "ticker": ticker.upper(),
        "trade_date": sess.isoformat(),
        "enumerated": enumerated,
        "written": [d.isoformat() for d in sorted(written)],
        "written_count": len(written),
        "failed": sorted(failed),
        "rows": rows,
        "written_at": datetime.now().isoformat(timespec="seconds"),
    }
    manifest_path(ticker, sess).write_text(
        json.dumps(payload, indent=2), encoding="utf-8")


def loaded_dates(ticker: str, years: set[int] | None = None) -> set[date]:
    """Sessions that are COMPLETE for this ticker.

    A pure directory + manifest read — no parquet I/O at all. Complete means:
    the session parquet exists, its manifest exists, and the manifest records
    no failed expirations. A session that died mid-write has no .parquet (only
    an orphaned .tmp) and a session that completed with failures has a
    manifest listing them, so both are re-attempted by a plain re-run.
    """
    d = ticker_dir(ticker)
    if not d.exists():
        return set()
    out: set[date] = set()
    for p in d.glob("*.parquet"):
        try:
            sess = datetime.strptime(p.stem, "%Y%m%d").date()
        except ValueError:
            continue
        if years is not None and sess.year not in years:
            continue
        man = read_manifest(ticker, sess)
        if man is None or man.get("failed"):
            continue
        out.add(sess)
    return out


def stored_expirations(ticker: str, sess: date) -> set[date]:
    """Expirations present in a stored session, from the manifest if possible.

    Falls back to a single-column parquet read when the manifest is absent
    (e.g. a session written before manifests existed).
    """
    man = read_manifest(ticker, sess)
    if man is not None and "written" in man:
        out = set()
        for s in man["written"]:
            try:
                out.add(datetime.strptime(s, "%Y-%m-%d").date())
            except ValueError:
                continue
        return out
    p = session_path(ticker, sess)
    if not p.exists():
        return set()
    try:
        tbl = pq.read_table(p, columns=["expiration"])
    except Exception:
        return set()
    return {d for d in tbl.column("expiration").to_pylist() if d is not None}


# --- Coercion --------------------------------------------------------------

def _col(df: pd.DataFrame, name: str) -> pd.Series:
    if name in df.columns:
        return df[name]
    return pd.Series([None] * len(df), index=df.index, dtype="object")


def coerce(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce one expiration's frame to the canonical schema.

    Public because the caller applies it per expiration — there is deliberately
    no second coercion inside the write path. The old year-file path called
    _coerce twice over a whole session, which was one of the full-size copies
    that made the write path unaffordable.
    """
    out = pd.DataFrame(index=df.index)

    for c in _STR_COLS:
        out[c] = _col(df, c).astype("string")

    for c in _DATE_COLS:
        out[c] = pd.to_datetime(_col(df, c), errors="coerce").dt.date

    for c in _TS_COLS:
        # Floor to ms so the ns -> ms cast below is exact; pyarrow casts safely
        # by default and would raise on sub-ms precision.
        out[c] = pd.to_datetime(_col(df, c), errors="coerce").dt.floor("ms")

    for c in _FLOAT_COLS:
        out[c] = pd.to_numeric(_col(df, c), errors="coerce").astype("float64")

    return out[COLUMNS]


# --- Streaming session writer ----------------------------------------------

class SessionWriter:
    """Streams one (ticker, session) file, one row group per expiration.

    Usage:

        with SessionWriter(ticker, sess) as w:
            for exp, frame in ...:
                w.write_expiration(exp, frame)
            w.finalize(enumerated=n, failed=[...])

    Holds ONE expiration at a time. The table handed to write_table is
    serialised to the sink and released on return; the writer retains only
    footer metadata. Nothing accumulates across expirations.

    Abandoning the context without finalize() (an exception, or the process
    being killed) leaves only the .tmp, which has no footer and is therefore
    not a readable parquet file — the session stays absent rather than
    appearing complete.
    """

    def __init__(self, ticker: str, sess: date):
        self.ticker = ticker.upper()
        self.sess = sess
        self.final_path = session_path(ticker, sess)
        self.tmp_path = self.final_path.with_suffix(".parquet.tmp")
        self._writer: pq.ParquetWriter | None = None
        self.rows = 0
        self.row_groups = 0
        self.written_expirations: list[date] = []
        # Largest per-expiration frame seen, so the memory bound this class
        # exists to enforce is observed rather than assumed.
        self.max_frame_bytes = 0
        self.max_frame_rows = 0
        self.max_frame_expiration: date | None = None

    def __enter__(self) -> "SessionWriter":
        self.tmp_path.parent.mkdir(parents=True, exist_ok=True)
        # A leftover .tmp from a previous killed run is garbage by definition.
        if self.tmp_path.exists():
            self.tmp_path.unlink()
        self._writer = pq.ParquetWriter(self.tmp_path, _SCHEMA,
                                        compression="snappy",
                                        use_dictionary=True)
        return self

    def write_expiration(self, exp: date, frame: pd.DataFrame) -> int:
        """Sort, dedupe, coerce and write ONE expiration as one row group.

        Returns rows written. The frame is released by the caller immediately
        after; nothing here retains a reference.
        """
        if frame is None or frame.empty:
            return 0

        # Measure before the copies, on the projected frame the caller holds.
        nbytes = int(frame.memory_usage(deep=True).sum())
        if nbytes > self.max_frame_bytes:
            self.max_frame_bytes = nbytes
            self.max_frame_rows = len(frame)
            self.max_frame_expiration = exp

        f = frame.drop_duplicates(subset=DEDUPE_KEYS, keep="last")
        f = f.sort_values(SORT_KEYS, kind="mergesort")
        table = pa.Table.from_pandas(coerce(f), schema=_SCHEMA,
                                     preserve_index=False)
        del f
        n = table.num_rows
        self._writer.write_table(table)
        del table

        self.rows += n
        self.row_groups += 1
        self.written_expirations.append(exp)
        return n

    def finalize(self, enumerated: int, failed: list[str]) -> int:
        """Close the file, publish it atomically, write the manifest.

        Returns rows written. A session with zero rows writes no file — an
        empty parquet would read as a complete-but-empty day.
        """
        if self._writer is None:
            return 0
        self._writer.close()
        self._writer = None

        if self.rows == 0:
            self.tmp_path.unlink(missing_ok=True)
            return 0

        os.replace(self.tmp_path, self.final_path)
        write_manifest(self.ticker, self.sess, enumerated,
                       self.written_expirations, failed, self.rows)
        return self.rows

    def abort(self) -> None:
        """Discard the in-progress file. Safe to call after finalize()."""
        if self._writer is not None:
            try:
                self._writer.close()
            except Exception:
                pass
            self._writer = None
            self.tmp_path.unlink(missing_ok=True)

    def __exit__(self, exc_type, exc, tb) -> bool:
        # finalize() clears _writer; reaching here with it set means the body
        # raised or returned early. Drop the .tmp so no half-file lingers.
        self.abort()
        return False
