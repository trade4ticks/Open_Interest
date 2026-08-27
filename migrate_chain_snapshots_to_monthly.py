"""
migrate_chain_snapshots_to_monthly.py — one-time layout change for the
chain_snapshots store.

    {CHAIN_SNAPSHOTS_DIR}/{TICKER}/{YYYY}.parquet
      ->
    {CHAIN_SNAPSHOTS_DIR}/{TICKER}/{YYYYMM}.parquet

Why: the store's append is read-modify-rewrite of a whole file, so its peak
memory is set by the file it merges into rather than by the batch being
merged.  A year file only grows, and a year frame in pandas is several times
its on-disk size (date32 comes back as an object array of datetime.date), so
the fetcher was reaching ~5GB RSS and being OOM-killed on a 15GB VPS with no
swap.  Monthly divides that by ~12.  See lib/chain_snapshot_store.py.

--- How it works -----------------------------------------------------------

One streaming pass per (ticker, year), in Arrow, never pandas:

    pq.ParquetFile(year).iter_batches(batch_size=ROW_GROUP_SIZE)
      -> route each batch to its month
      -> pq.ParquetWriter per month, writing to {YYYYMM}.parquet.tmp
      -> close, VERIFY, rename all, then delete the year file

Rows are already sorted by trade_date, so batches arrive month-contiguous and
at most two month writers are active at a boundary.  Peak memory is one row
group (~128K rows), NOT one month — the migration cannot reproduce the OOM it
exists to fix.

--- Safety -----------------------------------------------------------------

The year file is deleted LAST, only after every month file for that year has
been written, verified and renamed.  Every earlier failure mode leaves the
year file intact and the run re-doable:

  * died mid-write        -> orphan .tmp files, year file intact
  * verification failed   -> .tmp files removed, year file intact, exit 1
  * died mid-rename       -> some months final, some .tmp, year file intact

On re-run, ANY .tmp present for a year means that year is incomplete: its
.tmp files AND any already-renamed month files for that year are removed and
the year is redone from the (still present) year file.  A year whose months
are all final with no .tmp and which verifies is treated as already migrated,
and only the year file deletion is repeated.

Peak extra disk is therefore one year file (~200MB), not a second copy of the
store.

--- Verification -----------------------------------------------------------

--verify stats (default)
    Row counts must match exactly, per month and in total.  Then, per column,
    the aggregated min / max / null_count from the parquet FOOTERS of the
    month files must equal those of the year file.  Footer-only, so this is
    essentially free, and it catches truncation, row loss, misrouting and
    column-level corruption.

--verify deep
    Everything above, plus a re-read of the five dedupe-key columns from BOTH
    sides, compared row-for-row.  Order is meaningful here — the migration is
    a pure copy that preserves it — so this is the strictest check available
    and needs no sort or hash table.

    Cost: roughly doubles a ticker-year's read time, and holds both sides'
    key columns in Arrow at once — on the order of several hundred MB for the
    largest ticker-year, against ~128K rows for --verify stats.  Still far
    below the peak this migration exists to remove, but it is the one mode
    whose memory scales with the file.  Worth running on the first ticker you
    inspect; stats is the sensible default for the remaining 560.

Verification failure is FATAL for that ticker-year: .tmp files are removed,
the year file is left alone, and the run stops unless --keep-going.

--- Column-type normalisation ----------------------------------------------

Some files in this store carry ticker / snapshot / option_type as
`large_string` where the store's _SCHEMA says `string`.  Same values, wider
offsets (int64 vs int32).  It matters because all three are part of
DEDUPE_KEYS, and any Arrow-level operation across a mixed pair fails on the
type rather than the data — which is how it was found.

By default this script casts those to the store's types on the way through
and reports every file it corrected.  The cast is lossless (see
_SAFE_NORMALIZATIONS), and doing it here is what makes the store internally
consistent afterwards rather than leaving one year permanently divergent.
--no-normalize restores the strict behaviour of halting instead.

NOTE that this fixes the DATA, not the CAUSE.  No writer in this repository
produces large_string: every store module declares pa.string() and passes
schema=_SCHEMA to from_pandas, and lib/chain_snapshot_store now enforces the
canonical types at its single write choke point (normalize_to_schema).  So
whatever wrote those files either bypassed the store module or ran under a
different pandas/pyarrow than the one that wrote its neighbours.

The parquet footer records both.  To identify it:

    python - <<'PY'
    import pyarrow.parquet as pq
    md = pq.ParquetFile("<store>/AAL/2026.parquet").metadata
    print("created_by:", md.created_by)
    kv = md.metadata or {}
    for k, v in kv.items():
        print(k.decode(), "=", v.decode()[:400])
    PY

`created_by` names the writing library and version (e.g. "parquet-cpp-arrow
version 14.0.2" for pyarrow, or a DuckDB string).  A `pandas` key means it
went through pa.Table.from_pandas and carries the pandas version that did it.
Compare against a known-good neighbour such as AAL/2025.parquet: whichever of
the two fields differs is the answer.

Usage:
    python migrate_chain_snapshots_to_monthly.py --dry-run
    python migrate_chain_snapshots_to_monthly.py --dry-run --tickers SPY

    # do one and look at it before committing to the rest
    python migrate_chain_snapshots_to_monthly.py --tickers SPY --years 2024 \
        --verify deep --keep-year-files

    python migrate_chain_snapshots_to_monthly.py --tickers SPY --verify deep
    python migrate_chain_snapshots_to_monthly.py            # everything

Single-threaded on purpose: it is restartable per (ticker, year), so a slow
estimate costs little while a concurrency bug on an irreplaceable store costs
a great deal.
"""
from __future__ import annotations

import argparse
import logging
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from config import CHAIN_SNAPSHOTS_DIR
from lib.chain_fetch_common import log_path, setup_file_logging
from lib.chain_snapshot_store import (
    DEDUPE_KEYS,
    ROW_GROUP_SIZE,
    _SCHEMA,
    month_path,
    ticker_dir,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("migrate_monthly")


# --- Discovery --------------------------------------------------------------

def year_files(ticker: str) -> list[tuple[int, Path]]:
    """(year, path) for every {YYYY}.parquet under this ticker, ascending."""
    d = ticker_dir(ticker)
    if not d.exists():
        return []
    out = []
    for p in sorted(d.glob("*.parquet")):
        if len(p.stem) == 4 and p.stem.isdigit():
            out.append((int(p.stem), p))
    return out


def all_tickers() -> list[str]:
    if not CHAIN_SNAPSHOTS_DIR.exists():
        return []
    return sorted(p.name.upper() for p in CHAIN_SNAPSHOTS_DIR.iterdir()
                  if p.is_dir())


def month_files_of_year(ticker: str, year: int) -> list[Path]:
    d = ticker_dir(ticker)
    if not d.exists():
        return []
    return sorted(p for p in d.glob(f"{year}??.parquet")
                  if len(p.stem) == 6 and p.stem.isdigit()
                  and 1 <= int(p.stem) % 100 <= 12)


def tmp_files_of_year(ticker: str, year: int) -> list[Path]:
    d = ticker_dir(ticker)
    if not d.exists():
        return []
    return sorted(d.glob(f"{year}??.parquet.tmp"))


# --- Footer statistics ------------------------------------------------------

def footer_profile(paths: list[Path]) -> dict:
    """Aggregate num_rows and per-column min/max/null_count from footers only.

    Reads no data pages.  Columns whose type carries no statistics are
    reported as None and compared as such, so a parquet build that stops
    emitting stats for a column degrades to "not checked" rather than to a
    false mismatch.
    """
    total_rows = 0
    cols: dict[str, dict] = {}
    for p in paths:
        md = pq.ParquetFile(p).metadata
        total_rows += md.num_rows
        for rg in range(md.num_row_groups):
            group = md.row_group(rg)
            for c in range(group.num_columns):
                col = group.column(c)
                name = col.path_in_schema
                acc = cols.setdefault(
                    name, {"min": None, "max": None, "nulls": 0,
                           "has_stats": True})
                st = col.statistics
                if st is None or not st.has_min_max:
                    acc["has_stats"] = False
                    continue
                acc["nulls"] += (st.null_count or 0)
                lo, hi = st.min, st.max
                acc["min"] = lo if acc["min"] is None else min(acc["min"], lo)
                acc["max"] = hi if acc["max"] is None else max(acc["max"], hi)
    return {"rows": total_rows, "cols": cols}


def compare_profiles(src: dict, dst: dict) -> list[str]:
    """Human-readable differences; empty list means they agree."""
    diffs = []
    if src["rows"] != dst["rows"]:
        diffs.append(f"row count {src['rows']:,} -> {dst['rows']:,}")
    names = set(src["cols"]) | set(dst["cols"])
    for name in sorted(names):
        a, b = src["cols"].get(name), dst["cols"].get(name)
        if a is None or b is None:
            diffs.append(f"column {name} present on only one side")
            continue
        if not (a["has_stats"] and b["has_stats"]):
            continue        # no statistics on one side: not checked, not failed
        for field in ("min", "max", "nulls"):
            if a[field] != b[field]:
                diffs.append(f"{name}.{field}: {a[field]!r} -> {b[field]!r}")
    return diffs


def deep_key_match(year_file: Path, month_paths: list[Path]) -> list[str]:
    """Compare the five dedupe-key columns row-for-row, in order.

    Order is meaningful: the migration preserves it, so an in-order comparison
    is both the strictest available check and the cheapest way to run it — no
    sort, no hash table.  Reads five columns instead of twenty.

    Where a column was normalised (large_string -> string), the two sides have
    different Arrow types and `.equals()` would report a difference that is
    only a difference of offset width.  The source is cast to the destination's
    types first so the comparison is about VALUES.

    That cast is the same one the migration performed, so on its own this
    would not catch a bug inside the cast.  Two other things do: the string key
    columns are additionally compared as Python string SETS, which is
    independent of offset width entirely and cheap here because all three are
    low-cardinality (one ticker, two snapshots, two option types); and the
    footer min/max comparison runs over parquet's BYTE_ARRAY statistics, which
    are the same physical form on both sides.
    """
    src = pq.read_table(year_file, columns=DEDUPE_KEYS)
    dst = pa.concat_tables(
        [pq.read_table(p, columns=DEDUPE_KEYS) for p in month_paths])
    if src.num_rows != dst.num_rows:
        return [f"deep: row count {src.num_rows:,} -> {dst.num_rows:,}"]

    diffs = []
    # Offset-width-independent check on the string keys, before any cast.
    for name in DEDUPE_KEYS:
        if not pa.types.is_string(dst.field(name).type) and \
           not pa.types.is_large_string(dst.field(name).type):
            continue
        a = sorted(v for v in pc.unique(src.column(name)).to_pylist()
                   if v is not None)
        b = sorted(v for v in pc.unique(dst.column(name)).to_pylist()
                   if v is not None)
        if a != b:
            diffs.append(f"deep: {name} distinct values {a[:5]} -> {b[:5]}")
    if diffs:
        return diffs

    if not src.schema.equals(dst.schema):
        src = src.cast(dst.schema)
    for name in DEDUPE_KEYS:
        if not src.column(name).equals(dst.column(name)):
            diffs.append(f"deep: column {name} differs")
    return diffs


# --- Planning ---------------------------------------------------------------

def plan_year(ticker: str, year: int, p: Path) -> dict:
    """What migrating this file would produce. Footer-only, writes nothing."""
    pf = pq.ParquetFile(p)
    md = pf.metadata
    per_month: dict[int, int] = {}
    lo = hi = None
    # trade_date row-group statistics give the month spread without reading a
    # single data page, because the file is written sorted by trade_date.
    idx = md.schema.names.index("trade_date")
    for rg in range(md.num_row_groups):
        group = md.row_group(rg)
        st = group.column(idx).statistics
        if st is None or not st.has_min_max:
            per_month.clear()
            break
        a, b = st.min, st.max
        if isinstance(a, datetime):
            a, b = a.date(), b.date()
        lo = a if lo is None else min(lo, a)
        hi = b if hi is None else max(hi, b)
        # A row group can straddle a month end; attribute it to its own months
        # proportionally is not worth it, so this is an ESTIMATE and labelled
        # as one in the report. Exact counts come from the real pass.
        km, kn = a.year * 100 + a.month, b.year * 100 + b.month
        span = max(1, kn - km + 1)
        for k in range(km, kn + 1):
            if 1 <= k % 100 <= 12:
                per_month[k] = per_month.get(k, 0) + group.num_rows // span
    return {
        "ticker": ticker, "year": year, "path": p,
        "bytes": p.stat().st_size,
        "rows": md.num_rows,
        "row_groups": md.num_row_groups,
        "months": sorted(per_month),
        "rows_by_month": per_month,
        "date_min": lo, "date_max": hi,
    }


# --- Schema normalisation ---------------------------------------------------

# Type differences this script will silently correct on the way through.
# Each pair is (source type, store type) and each is a pure change of physical
# representation with an identical value domain:
#
#   large_string / string   differ only in the offset width, int64 vs int32.
#                           The bytes are unchanged; nothing can be lost
#                           unless a single array exceeds 2GB of character
#                           data, and this script writes row groups of
#                           ROW_GROUP_SIZE rows of short symbols and labels.
#   large_binary / binary   same, for completeness.
#
# Everything else — a differing timestamp unit, a float32 where the store has
# float64, a date64 where it has date32 — is NOT here. Those can round, shift
# or silently truncate, and a data migration is the last place to be guessing
# about that. They halt the run instead.
_SAFE_NORMALIZATIONS = {
    (pa.large_string(), pa.string()),
    (pa.string(), pa.large_string()),
    (pa.large_binary(), pa.binary()),
    (pa.binary(), pa.large_binary()),
}


def resolve_target_schema(src_schema: pa.Schema):
    """(target_schema, normalized_names, unsafe_names) for one source file.

    The target is the store's canonical types carrying the SOURCE's metadata,
    so a normalised file still reads back with the same pandas dtypes as its
    already-correct neighbours.
    """
    normalized, unsafe = [], []
    for f in _SCHEMA:
        src_type = src_schema.field(f.name).type
        if src_type == f.type:
            continue
        if (src_type, f.type) in _SAFE_NORMALIZATIONS:
            normalized.append(f.name)
        else:
            unsafe.append(f.name)
    # Only call with_metadata when there is metadata to carry: passing None is
    # not documented to be a no-op and its behaviour has varied by version.
    target = _SCHEMA
    if src_schema.metadata:
        target = _SCHEMA.with_metadata(src_schema.metadata)
    return target, normalized, unsafe


# --- Migration --------------------------------------------------------------

class _MonthWriters:
    """Open pq.ParquetWriter per month, targeting .tmp paths.

    The writers are built on the SOURCE file's Arrow schema, not on the
    store's bare _SCHEMA. The field names and types are identical — that is
    checked before we get here — but the source carries pandas metadata in its
    footer, written by the original pa.Table.from_pandas call. Dropping it
    would make pd.read_parquet hand back object dtype for ticker / snapshot /
    option_type where a freshly-written file gives StringDtype, so migrated
    and new files would read back differently. Harmless in every use we have
    today, and exactly the kind of thing that is miserable to find later.
    """

    def __init__(self, ticker: str, schema: pa.Schema) -> None:
        self.ticker = ticker
        self.schema = schema
        self.writers: dict[int, pq.ParquetWriter] = {}
        self.paths: dict[int, Path] = {}
        self.rows: dict[int, int] = {}

    def write(self, ym: int, table: pa.Table) -> None:
        w = self.writers.get(ym)
        if w is None:
            final = month_path(self.ticker, ym)
            final.parent.mkdir(parents=True, exist_ok=True)
            tmp = final.with_suffix(final.suffix + ".tmp")
            w = pq.ParquetWriter(tmp, self.schema, compression="snappy",
                                 use_dictionary=True)
            self.writers[ym] = w
            self.paths[ym] = final
            self.rows[ym] = 0
        w.write_table(table)
        self.rows[ym] += table.num_rows

    def close(self) -> None:
        for w in self.writers.values():
            try:
                w.close()
            except Exception:
                pass

    def tmp_paths(self) -> list[Path]:
        return [p.with_suffix(p.suffix + ".tmp") for p in self.paths.values()]

    def discard(self) -> None:
        self.close()
        for p in self.tmp_paths():
            p.unlink(missing_ok=True)


def _month_keys(col) -> list[int]:
    """YYYYMM per row, computed in Arrow rather than in Python.

    `col.to_pylist()` on a date column would build a datetime.date object per
    row — 128K of them per batch, on a script whose entire purpose is to not
    allocate. pc.year / pc.month stay in Arrow's C++ and the only Python
    objects built are the ints this returns, which is what the run-boundary
    scan below needs anyway. A null date yields 0, which the caller treats as
    unroutable.
    """
    y = pc.year(col)
    m = pc.month(col)
    keys = pc.add(pc.multiply(pc.cast(y, pa.int32()), 100),
                  pc.cast(m, pa.int32()))
    return [0 if k is None else k for k in keys.to_pylist()]


def migrate_year(ticker: str, year: int, p: Path, verify: str,
                 keep_year_files: bool, no_normalize: bool = False) -> dict:
    """Split one year file into month files. Returns a result dict.

    Raises nothing on a data problem — it reports `ok: False` and leaves the
    year file untouched, so the caller decides whether to stop.
    """
    t0 = time.monotonic()

    # Any .tmp for this year means a previous attempt died partway. Clear both
    # the .tmp files AND any month files it had already renamed, then redo from
    # the year file, which is still authoritative because it is deleted last.
    stale_tmp = tmp_files_of_year(ticker, year)
    if stale_tmp:
        existing = month_files_of_year(ticker, year)
        log.warning("  %s %d: %d orphan .tmp from an interrupted attempt — "
                    "removing those and %d already-renamed month file(s), "
                    "redoing the year", ticker, year, len(stale_tmp),
                    len(existing))
        for q in stale_tmp + existing:
            q.unlink(missing_ok=True)

    pf = pq.ParquetFile(p)
    src_rows = pf.metadata.num_rows
    src_schema = pf.schema_arrow

    # Column NAMES must match: a file with different columns is not what this
    # script is for, and there is no safe guess to make.
    if list(src_schema.names) != list(_SCHEMA.names):
        log.error("  %s %d: schema mismatch — file has %d column(s) %s, store "
                  "schema has %d. Not migrating this file.",
                  ticker, year, len(src_schema.names),
                  list(src_schema.names)[:6], len(_SCHEMA.names))
        return {"ok": False, "ticker": ticker, "year": year,
                "reason": "schema names differ from the store schema"}

    target_schema, normalized, unsafe = resolve_target_schema(src_schema)
    if unsafe:
        log.error("  %s %d: column type(s) differ from the store schema in a "
                  "way this script will not cast: %s. Not migrating.",
                  ticker, year,
                  ", ".join(f"{n}: {src_schema.field(n).type} -> "
                            f"{_SCHEMA.field(n).type}" for n in unsafe[:6]))
        return {"ok": False, "ticker": ticker, "year": year,
                "reason": f"unsafe type difference: {', '.join(unsafe[:3])}"}
    if normalized:
        if no_normalize:
            log.error("  %s %d: column type(s) differ (%s) and --no-normalize "
                      "is set. Not migrating.", ticker, year,
                      ", ".join(normalized))
            return {"ok": False, "ticker": ticker, "year": year,
                    "reason": f"types differ, normalization disabled: "
                              f"{', '.join(normalized[:3])}"}
        log.warning("  %s %d: NORMALIZING %s to the store schema — %s",
                    ticker, year, ", ".join(normalized),
                    ", ".join(f"{n}: {src_schema.field(n).type} -> "
                              f"{_SCHEMA.field(n).type}" for n in normalized[:5]))

    writers = _MonthWriters(ticker, target_schema)

    try:
        for batch in pf.iter_batches(batch_size=ROW_GROUP_SIZE):
            tbl = pa.Table.from_batches([batch], schema=src_schema)
            if normalized:
                tbl = tbl.cast(target_schema)
            keys = _month_keys(tbl.column("trade_date"))
            # Sorted by trade_date, so a batch is one month or two at a
            # boundary. Slice rather than filter: contiguous runs mean no
            # boolean mask and no row-level scatter.
            start = 0
            for i in range(1, len(keys) + 1):
                if i == len(keys) or keys[i] != keys[start]:
                    ym = keys[start]
                    if ym:
                        writers.write(ym, tbl.slice(start, i - start))
                    else:
                        log.error("  %s %d: %d row(s) with an unusable "
                                  "trade_date — cannot route to a month",
                                  ticker, year, i - start)
                        writers.discard()
                        return {"ok": False, "ticker": ticker, "year": year,
                                "reason": "unroutable trade_date"}
                    start = i
        writers.close()
    except Exception as exc:                                  # noqa: BLE001
        log.error("  %s %d: FAILED during split — %s", ticker, year, exc,
                  exc_info=True)
        writers.discard()
        return {"ok": False, "ticker": ticker, "year": year,
                "reason": f"{type(exc).__name__}: {exc}"}

    if not writers.paths:
        log.error("  %s %d: produced no month files from %d rows",
                  ticker, year, src_rows)
        writers.discard()
        return {"ok": False, "ticker": ticker, "year": year,
                "reason": "no output"}

    # --- verify, on the .tmp files, BEFORE anything is renamed or deleted ---
    tmps = [writers.paths[k].with_suffix(writers.paths[k].suffix + ".tmp")
            for k in sorted(writers.paths)]
    written_rows = sum(writers.rows.values())
    diffs: list[str] = []
    if written_rows != src_rows:
        diffs.append(f"rows written {written_rows:,} != source {src_rows:,}")
    if not diffs:
        diffs = compare_profiles(footer_profile([p]), footer_profile(tmps))
    if not diffs and verify == "deep":
        diffs = deep_key_match(p, tmps)

    if diffs:
        log.error("  %s %d: VERIFICATION FAILED — year file left untouched, "
                  ".tmp removed. Differences: %s", ticker, year,
                  "; ".join(diffs[:6]))
        writers.discard()
        return {"ok": False, "ticker": ticker, "year": year,
                "reason": "verification failed: " + "; ".join(diffs[:3])}

    # --- commit: rename every month, THEN drop the year file ---------------
    # Release the source handle first. Unlinking an open file is legal on
    # Linux and would leave the inode alive until this process exits, which
    # across 560 files would quietly hold the whole store's worth of freed
    # space and defeat the "one year file at a time" disk bound.
    try:
        pf.close()
    except Exception:
        pass

    for k in sorted(writers.paths):
        final = writers.paths[k]
        final.with_suffix(final.suffix + ".tmp").replace(final)

    if keep_year_files:
        log.info("  %s %d: --keep-year-files, %s left in place",
                 ticker, year, p.name)
    else:
        p.unlink()

    secs = time.monotonic() - t0
    log.info("  %s %d: %d rows -> %d month file(s) in %.1fs%s%s",
             ticker, year, src_rows, len(writers.paths), secs,
             f" [normalized {', '.join(normalized)}]" if normalized else "",
             " (year file kept)" if keep_year_files else "")
    return {"ok": True, "ticker": ticker, "year": year, "rows": src_rows,
            "months": len(writers.paths), "secs": secs,
            "normalized": normalized,
            "bytes": p.stat().st_size if keep_year_files else 0}


# --- Main -------------------------------------------------------------------

def human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} TB"


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Migrate chain_snapshots from {YYYY} to {YYYYMM} files.")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would be done; write and delete nothing")
    ap.add_argument("--tickers", help="comma-separated subset")
    ap.add_argument("--years", help="comma-separated subset, e.g. 2024,2025")
    ap.add_argument("--verify", choices=["stats", "deep"], default="stats",
                    help=("stats (default): row counts plus per-column "
                          "min/max/null_count from parquet footers, free. "
                          "deep: also re-reads the five dedupe-key columns "
                          "and compares them row-for-row."))
    ap.add_argument("--keep-year-files", action="store_true",
                    help=("write the month files but do NOT delete the year "
                          "file. For inspecting the result before committing "
                          "to the rest. NOTE: the fetcher refuses to run "
                          "while a year file is present, so clear them before "
                          "resuming normal operation."))
    ap.add_argument("--no-normalize", action="store_true",
                    help=("halt on any column-type difference instead of "
                          "casting the losslessly-castable ones "
                          "(large_string <-> string) to the store schema"))
    ap.add_argument("--keep-going", action="store_true",
                    help="continue after a ticker-year fails (default: stop)")
    args = ap.parse_args()

    log_file = setup_file_logging("migrate_chain_snapshots_to_monthly")
    print("=== chain_snapshots: {YYYY}.parquet -> {YYYYMM}.parquet ===")
    print(f"Store: {CHAIN_SNAPSHOTS_DIR}")
    print(f"Log:   {log_file}\n")

    if not CHAIN_SNAPSHOTS_DIR.exists():
        raise SystemExit(f"Store does not exist: {CHAIN_SNAPSHOTS_DIR}")

    tickers = ([t.strip().upper() for t in args.tickers.split(",") if t.strip()]
               if args.tickers else all_tickers())
    if not tickers:
        raise SystemExit(f"No ticker directories under {CHAIN_SNAPSHOTS_DIR}")
    want_years = ({int(y) for y in args.years.split(",") if y.strip()}
                  if args.years else None)

    units: list[tuple[str, int, Path]] = []
    for t in tickers:
        for y, p in year_files(t):
            if want_years is None or y in want_years:
                units.append((t, y, p))

    if not units:
        print("Nothing to migrate — no {YYYY}.parquet files matched.")
        print("(If you expected some, the store may already be monthly.)")
        return 0

    total_bytes = sum(p.stat().st_size for _, _, p in units)
    print(f"{len(units)} ticker-year file(s) across "
          f"{len({t for t, _, _ in units})} ticker(s), "
          f"{human_bytes(total_bytes)}\n")

    # --- dry run --------------------------------------------------------
    if args.dry_run:
        print(f"{'ticker':<8}{'year':>6}{'size':>12}{'rows':>14}"
              f"{'groups':>8}{'months':>8}   date range")
        out_files = 0
        norm_units: list[tuple[str, int, list, pa.Schema]] = []
        unsafe_units: list[tuple[str, int, list]] = []
        for t, y, p in units:
            pl = plan_year(t, y, p)
            out_files += len(pl["months"])
            rng = (f"{pl['date_min']} .. {pl['date_max']}"
                   if pl["date_min"] else "?")
            flag = ""
            src_schema = pq.ParquetFile(p).schema_arrow
            if list(src_schema.names) != list(_SCHEMA.names):
                flag = "  SCHEMA NAMES DIFFER — would not migrate"
                unsafe_units.append((t, y, ["column names"]))
            else:
                _, norm, unsafe = resolve_target_schema(src_schema)
                if unsafe:
                    flag = f"  UNSAFE TYPES {','.join(unsafe)} — would not migrate"
                    unsafe_units.append((t, y, unsafe))
                elif norm:
                    flag = f"  normalize {','.join(norm)}"
                    norm_units.append((t, y, norm, src_schema))
            print(f"{t:<8}{y:>6}{human_bytes(pl['bytes']):>12}"
                  f"{pl['rows']:>14,}{pl['row_groups']:>8}"
                  f"{len(pl['months']):>8}   {rng}{flag}")
            existing = month_files_of_year(t, y)
            if existing:
                print(f"         WARNING: {len(existing)} month file(s) for "
                      f"{y} already exist and WOULD BE REPLACED")

        if norm_units:
            print(f"\n{len(norm_units)} file(s) carry non-canonical column "
                  f"types that would be normalized to the store schema:")
            for t, y, cols, sf in norm_units[:5]:
                print(f"  {t} {y}: " + ", ".join(
                    f"{c} {sf.field(c).type} -> {_SCHEMA.field(c).type}"
                    for c in cols))
            if len(norm_units) > 5:
                print(f"  ... and {len(norm_units) - 5} more")
            print("  These casts change representation only, never values. "
                  "--no-normalize to halt on them instead.")
        if unsafe_units:
            print(f"\n{len(unsafe_units)} file(s) WOULD NOT MIGRATE:")
            for t, y, cols in unsafe_units[:10]:
                print(f"  {t} {y}: {', '.join(cols)}")

        print(f"\nWould read  {human_bytes(total_bytes)} across {len(units)} "
              f"file(s)")
        print(f"Would write {out_files} month file(s) of comparable total size")
        print(f"Would then delete {len(units)} year file(s)"
              + (" — except --keep-year-files is set"
                 if args.keep_year_files else ""))
        print(f"Peak extra disk: one year file at a time, "
              f"{human_bytes(max(p.stat().st_size for _, _, p in units))}")
        print("\nMonth counts are ESTIMATES from row-group statistics; the "
              "real pass counts exactly.")
        print("Nothing was written. Re-run without --dry-run to migrate.")
        return 0

    # --- real run -------------------------------------------------------
    free = shutil.disk_usage(CHAIN_SNAPSHOTS_DIR).free
    need = max(p.stat().st_size for _, _, p in units) * 2
    print(f"Free disk: {human_bytes(free)} | needed at peak: "
          f"{human_bytes(need)} (one year file plus its months)")
    if free < need:
        raise SystemExit("Not enough free space for the largest ticker-year.")
    print(f"Verification: {args.verify}\n")

    ok, failed = [], []
    t_run = time.monotonic()
    done_bytes = 0
    for i, (t, y, p) in enumerate(units, 1):
        size = p.stat().st_size
        log.info("[%d/%d] %s %d (%s)", i, len(units), t, y, human_bytes(size))
        res = migrate_year(t, y, p, args.verify, args.keep_year_files,
                           no_normalize=args.no_normalize)
        if res["ok"]:
            ok.append(res)
            done_bytes += size
            elapsed = time.monotonic() - t_run
            rate = done_bytes / elapsed if elapsed > 0 else 0
            remaining = total_bytes - done_bytes
            if rate > 0:
                log.info("       %s done, ~%.0f min remaining at %s/s",
                         human_bytes(done_bytes), remaining / rate / 60,
                         human_bytes(rate))
        else:
            failed.append(res)
            if not args.keep_going:
                print(f"\nSTOPPED at {t} {y}: {res['reason']}")
                print("The year file was NOT deleted and no .tmp remains. "
                      "Fix, then re-run — completed ticker-years are skipped "
                      "because their year files are gone.")
                print(f"Log: {log_path()}")
                return 1

    print(f"\n{len(ok)} ticker-year(s) migrated, {len(failed)} failed, "
          f"in {(time.monotonic() - t_run) / 60:.1f} min")
    normed = [r for r in ok if r.get("normalized")]
    if normed:
        print(f"{len(normed)} file(s) had column types normalized to the "
              f"store schema (representation only, values unchanged):")
        for r in normed[:10]:
            print(f"  {r['ticker']} {r['year']}: "
                  f"{', '.join(r['normalized'])}")
        if len(normed) > 10:
            print(f"  ... and {len(normed) - 10} more")
        print("  The store is now internally consistent, but whatever WROTE "
              "those types will do it again —")
        print("  see the note at the top of this file on finding it.")
    if failed:
        for r in failed[:10]:
            print(f"  FAILED {r['ticker']} {r['year']}: {r['reason']}")
    print(f"Log: {log_path()}")
    if args.keep_year_files:
        print("\n--keep-year-files was set: the year files are still there, "
              "and fetch_chain_snapshots.py will refuse to run until they "
              "are gone. Inspect, then re-run without the flag.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
