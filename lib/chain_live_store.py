"""
Parquet storage for live 5-minute chain captures.

Layout:  {CHAIN_LIVE_DIR}/{TICKER}/{YYYYMMDD}/{HHMM}.parquet

ONE FILE PER CAPTURE CYCLE, complete the instant it appears.

--- Why this is a separate store from chain_intraday ------------------------

The two hold near-identical content and must not share a tree. Over a full
session, 78 live captures of a contract accumulate the same 78 five-minute
observations that the historical fetcher writes as one file. A reader that
found both shapes under one root would have no way to tell which writer
produced a given file, so they are separated at the root and by layout:

    chain_intraday  {TICKER}/{YYYYMMDD}.parquet          one file per session
                    written by fetch_chain_intraday.py after the close.
                    Authoritative and complete: every expiration the EOD
                    enumeration listed, every bar of the session.

    chain_live      {TICKER}/{YYYYMMDD}/{HHMM}.parquet   one file per cycle
                    written by fetch_live_surface.py as the session runs.
                    Available immediately, and incomplete by nature — a
                    failed or skipped cycle simply has no file, and the
                    snapshot endpoint's chain is whatever was listed at that
                    moment.

Prefer chain_intraday when it exists for a session; reach for chain_live when
the day has not closed yet, or to replay a fit against exactly the quotes the
live surface saw.

--- Completeness on write ---------------------------------------------------

chain_intraday streams one row group per expiration into a .tmp and renames
only at session end, which is why nothing can read the current day mid-
session. That is the right trade for a fetcher writing a whole session in one
pass; it is the wrong one here, where the entire point is that a cycle is
readable the moment it lands.

Each cycle is therefore its own file, written to .tmp in the SAME directory
and renamed immediately. The rename is atomic within a filesystem, so a
reader either does not see the file or sees it whole — never a partial one.
The .tmp exists for milliseconds rather than a whole session.

--- Schema ------------------------------------------------------------------

Identical to chain_intraday, imported rather than redeclared so the two
cannot drift. Existing readers work unchanged.

Rows are persisted PRE-clean_chain: everything clean_chain adds is derived
from these columns, so a change to the cleaning rules is replayable from what
is stored here. Persisting post-clean would freeze one version of those rules
into the archive.

--- Compression -------------------------------------------------------------

zstd level 3, not the snappy chain_intraday uses. Measured on a 13,400-row
SPY cycle: 0.506 MB against snappy's 0.655 MB (-22.8%) and slightly FASTER to
write (18.8ms vs 22.6ms), round-trip identical. The two stores face different
constraints — this one is disk-bound on a 34 GB volume and writes many small
files, where the compression penalty is worst. pyarrow reads any codec
transparently, so nothing downstream needs to know.
"""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import CHAIN_LIVE_DIR
from lib.chain_intraday_store import COLUMNS, SCHEMA, coerce
from lib.parquet_schema import normalize_to_schema

# Measured better than snappy on this store's shape; see module docstring.
COMPRESSION = "zstd"
COMPRESSION_LEVEL = 3


def ticker_dir(ticker: str) -> Path:
    return CHAIN_LIVE_DIR / ticker.upper()


def session_dir(ticker: str, trade_date: date) -> Path:
    return ticker_dir(ticker) / f"{trade_date:%Y%m%d}"


def cycle_path(ticker: str, trade_date: date, snapshot: str) -> Path:
    """One capture. `snapshot` is the 5-minute grid bucket, e.g. '1345'."""
    return session_dir(ticker, trade_date) / f"{snapshot}.parquet"


def list_tickers() -> list[str]:
    if not CHAIN_LIVE_DIR.exists():
        return []
    return sorted(p.name.upper() for p in CHAIN_LIVE_DIR.iterdir()
                  if p.is_dir() and any(p.glob("*/*.parquet")))


def list_sessions(ticker: str) -> list[date]:
    d = ticker_dir(ticker)
    if not d.exists():
        return []
    out = []
    for p in d.iterdir():
        if not p.is_dir() or not any(p.glob("*.parquet")):
            continue
        try:
            out.append(date(int(p.name[:4]), int(p.name[4:6]), int(p.name[6:8])))
        except (ValueError, IndexError):
            continue
    return sorted(out)


def list_cycles(ticker: str, trade_date: date) -> list[str]:
    """Grid buckets captured for a session, ascending. A pure listing — the
    gaps in it ARE the record of which cycles failed."""
    d = session_dir(ticker, trade_date)
    if not d.exists():
        return []
    return sorted(p.stem for p in d.glob("*.parquet"))


def parquet_glob(ticker: str, trade_date: date | None = None) -> str:
    """Glob for DuckDB. Whole ticker, or one session."""
    if trade_date is None:
        return str(ticker_dir(ticker) / "*" / "*.parquet")
    return str(session_dir(ticker, trade_date) / "*.parquet")


def write_cycle(ticker: str, trade_date: date, snapshot: str,
                frame: pd.DataFrame) -> tuple[Path, int]:
    """Persist one capture. Returns (path, bytes_written).

    Raises on failure — the CALLER decides that a parquet problem must not
    take the surface computation down with it. Swallowing it here would hide
    a store that had stopped accepting writes.
    """
    if frame is None or frame.empty:
        return cycle_path(ticker, trade_date, snapshot), 0

    path = cycle_path(ticker, trade_date, snapshot)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")

    table = pa.Table.from_pandas(coerce(frame), schema=SCHEMA,
                                 preserve_index=False)
    # from_pandas's `schema=` is a request, not a guarantee: a pyarrow upgrade
    # once left it honoured for some string columns and not others. This store
    # shares chain_intraday's 20-column SCHEMA, so a divergence here would also
    # be a divergence against that store. See lib/parquet_schema.py.
    table = normalize_to_schema(table, SCHEMA, where=f"chain_live/{path.name}")
    try:
        pq.write_table(table, tmp, compression=COMPRESSION,
                       compression_level=COMPRESSION_LEVEL,
                       use_dictionary=True)
        # Same directory, so same filesystem: the rename is atomic and a
        # reader sees the file whole or not at all.
        os.replace(tmp, path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
    return path, path.stat().st_size


def read_cycle(ticker: str, trade_date: date, snapshot: str) -> pd.DataFrame:
    p = cycle_path(ticker, trade_date, snapshot)
    if not p.exists():
        return pd.DataFrame(columns=COLUMNS)
    return pd.read_parquet(p)


def read_session(ticker: str, trade_date: date) -> pd.DataFrame:
    """Every captured cycle for one session, concatenated in time order."""
    frames = []
    for snap in list_cycles(ticker, trade_date):
        f = read_cycle(ticker, trade_date, snap)
        if not f.empty:
            frames.append(f)
    if not frames:
        return pd.DataFrame(columns=COLUMNS)
    return pd.concat(frames, ignore_index=True)
