"""
fetch_equity_1min.py — 1-minute equity bars from Polygon.io / Massive.

Backfills the OI-store ticker universe to 2019-01-01, including extended
hours, into data/equity_1min/{ticker}/{year}.parquet.

Why the aggregates API and not flat files: flat files are one file per session
containing all ~11,000 US tickers. This universe is ~121 of them, so flat
files would move ~90x the needed bytes. The aggregates endpoint takes one
ticker per request, which is the right shape.

SCOPE: fetch-and-store only. No metric is repointed, nothing is wired into
cron. Those are separate, deliberate steps.

--- Request shape ---------------------------------------------------------

    /v2/aggs/ticker/{ticker}/range/1/minute/{from}/{to}
        adjusted=false   store as-traded; the project applies ONE universal
                         split adjustment at read time (see the data
                         dictionary) and that stays the single source of truth
        limit=50000      sent explicitly — the vendor DEFAULT IS 5000, which
                         would truncate every chunk to ~5 sessions
        sort=asc

--- Chunk size: MONTHLY, not quarterly ------------------------------------

`limit` caps base aggregates at 50,000. With extended hours a session yields up
to 960 bars (04:00-20:00 ET):

    quarter (max 64 sessions in this range) x 960 = 61,440   OVER the cap
    month   (max 23 sessions in this range) x 960 = 22,080   2.3x headroom

The vendor's own guidance is to "limit minute/hourly requests to one-month
timeframes per query to avoid gaps". Quarterly chunking would silently
truncate the densest tickers. lib/polygon.py additionally raises
TruncatedResponseError if a response ever fills the limit, and this fetcher
splits that chunk in half and retries rather than storing a short read.

--- Resumability ----------------------------------------------------------

Keyed on (ticker, chunk_start, chunk_end) — exactly the unit that a request,
and therefore a failure, is. This is the lesson from fetch_chain_snapshots.py,
where resumability keyed on (date, label) while failures happened at
(date, expiration, label), so partial sessions looked complete and were
skipped forever.

Deriving it from stored rows would repeat that mistake in a new shape:
trade_date is FINER than a chunk, so a half-written month would look partly
done — and a ticker with no bars before it listed is indistinguishable from a
failed fetch by looking at rows alone. So chunk outcomes are recorded
explicitly in a manifest, including `empty` for ranges the vendor genuinely
has no data for. See lib/equity_1min_store.py for the write-ordering contract.

Usage:
    python fetch_equity_1min.py --dry-run
    python fetch_equity_1min.py --dry-run --probe        (one real request to
                                                          calibrate estimates)
    python fetch_equity_1min.py --tickers AAPL,SPY --start 20190101 --end 20261231
    python fetch_equity_1min.py --repair
    python fetch_equity_1min.py --force
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import date, datetime, timedelta

import pandas as pd
from tqdm import tqdm

from config import EQUITY_1MIN_DIR, CHAIN_EOD_DIR
from lib.chain_fetch_common import (
    TIMING,
    log_path,
    preflight_store,
    print_timing_summary,
    prompt_date,
    prompt_tickers,
    set_local_busy,
    setup_file_logging,
    start_sampler,
    start_watchdog,
    stop_background_threads,
    track,
)
from lib.market_hours import ET, get_trading_days, last_trading_day, session_bounds


def list_oi_tickers() -> list:
    """The OI-store universe, imported lazily.

    lib.parquet_store pulls in pyarrow, which a machine that only ever runs
    --dry-run does not need. Keeping this import out of module scope is what
    lets the dry run work anywhere — including a dev box with no store mounted.
    """
    from lib.parquet_store import list_tickers
    return list_tickers()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_START = date(2019, 1, 1)

# Chunks accumulated in memory before a parquet write + manifest record.
# Lower = less lost to an interrupt; higher = fewer year-file rewrites.
DEFAULT_BATCH_CHUNKS = 12

# Theoretical max bars in an extended-hours session: 04:00-20:00 ET.
MAX_BARS_PER_SESSION = 960

# Vendor fields we know about. Anything else is reported once per run so a
# vendor schema change surfaces instead of being silently dropped.
_KNOWN_VENDOR_FIELDS = {"t", "o", "h", "l", "c", "v", "vw", "n", "otc"}
_UNKNOWN_WARNED: set = set()


# --- Chunking ---------------------------------------------------------------

def month_chunks(start: date, end: date) -> list:
    """Split [start, end] into calendar-month windows, clipped to the range.

    Months, not quarters — see the module docstring on the 50,000 cap.
    Months with no trading days are dropped rather than fetched.
    """
    out: list = []
    cur = date(start.year, start.month, 1)
    while cur <= end:
        if cur.month == 12:
            nxt = date(cur.year + 1, 1, 1)
        else:
            nxt = date(cur.year, cur.month + 1, 1)
        c_start = max(cur, start)
        c_end = min(nxt - timedelta(days=1), end)
        if c_start <= c_end and get_trading_days(c_start, c_end):
            out.append((c_start, c_end))
        cur = nxt
    return out


# --- Projection -------------------------------------------------------------

def _warn_unknown_fields(raw: pd.DataFrame) -> None:
    unknown = set(raw.columns) - _KNOWN_VENDOR_FIELDS - _UNKNOWN_WARNED
    if unknown:
        _UNKNOWN_WARNED.update(unknown)
        log.warning("aggregates response carries field(s) not in the stored "
                    "schema: %s — they are NOT being stored. Update "
                    "lib/equity_1min_store.py if you want them.",
                    ", ".join(sorted(unknown)))


def _project(results: list, ticker: str, bounds: dict,
             source_symbol: str | None = None) -> pd.DataFrame:
    """Vendor `results` -> the stored schema.

    Field mapping (Polygon aggregates, documented + confirmed against a live
    response):
        t  -> ts_ms (UTC epoch ms) -> timestamp (naive ET) + trade_date
        o,h,l,c -> open, high, low, close
        v  -> volume
        vw -> vwap
        n  -> transactions

    `session` is derived in ET against the EXCHANGE CALENDAR, not a hardcoded
    09:30/16:00: early closes (13:00 on the day after Thanksgiving, Christmas
    Eve, July 3rd) would otherwise misclassify afternoon bars as 'regular' on
    days the market was already shut.

    trade_date is the ET calendar date of the bar. It is derived from the
    timestamp rather than the requested range because a chunk spans many
    sessions and only the bar itself knows which one it belongs to.
    """
    if not results:
        return pd.DataFrame()

    raw = pd.DataFrame(results)
    _warn_unknown_fields(raw)

    if "t" not in raw.columns:
        log.warning("  %s: response has no 't' field — dropping %d rows",
                    ticker, len(raw))
        return pd.DataFrame()

    ts_ms = pd.to_numeric(raw["t"], errors="coerce").astype("Int64")

    # UTC instant -> ET wall clock, then drop the tz. Safe for this data: US
    # DST transitions happen at 02:00 ET and this store only ever holds
    # 04:00-20:00 ET bars, so no timestamp can land in the skipped or repeated
    # hour. See lib/equity_1min_store.py.
    ts_utc = pd.to_datetime(ts_ms.astype("float64"), unit="ms", utc=True,
                            errors="coerce")
    ts_et = ts_utc.dt.tz_convert(ET).dt.tz_localize(None)

    trade_date = ts_et.dt.date

    # Vectorised session classification against per-session real bounds.
    opens = trade_date.map(lambda d: bounds.get(d, (None, None))[0])
    closes = trade_date.map(lambda d: bounds.get(d, (None, None))[1])
    opens = pd.to_datetime(pd.Series(list(opens), index=raw.index), errors="coerce")
    closes = pd.to_datetime(pd.Series(list(closes), index=raw.index), errors="coerce")

    session = pd.Series("other", index=raw.index, dtype="object")
    known = opens.notna() & closes.notna() & ts_et.notna()
    session[known & (ts_et < opens)] = "premarket"
    session[known & (ts_et >= opens) & (ts_et < closes)] = "regular"
    session[known & (ts_et >= closes)] = "after"

    out = pd.DataFrame({
        "ticker":        ticker.upper(),
        # Provenance: which symbol this bar was actually requested under. Equal
        # to ticker except across a rename (META bars pre-2022-06-09 -> 'FB').
        "source_symbol": (source_symbol or ticker).upper(),
        "trade_date":   trade_date,
        "session":      session,
        "timestamp":    ts_et,
        "ts_ms":        ts_ms,
        "open":         pd.to_numeric(raw.get("o"), errors="coerce"),
        "high":         pd.to_numeric(raw.get("h"), errors="coerce"),
        "low":          pd.to_numeric(raw.get("l"), errors="coerce"),
        "close":        pd.to_numeric(raw.get("c"), errors="coerce"),
        "volume":       pd.to_numeric(raw.get("v"), errors="coerce"),
        "vwap":         pd.to_numeric(raw.get("vw"), errors="coerce"),
        "transactions": pd.to_numeric(raw.get("n"), errors="coerce"),
    })

    return out.dropna(subset=["trade_date", "ts_ms"])


# --- Per-ticker fetch -------------------------------------------------------

def fetch_ticker(ticker: str, chunks: list, bounds: dict,
                 batch_chunks: int = DEFAULT_BATCH_CHUNKS,
                 force: bool = False, repair: bool = False,
                 debug_response: bool = False,
                 symbols=None) -> int:
    """Fetch every chunk for one ticker. Returns rows fetched and merged.

    `symbols` is a SymbolHistory (lib/polygon_symbols.py). A chunk that
    straddles a rename is split at the boundary and each part requested under
    the symbol that actually traded then — transparently, so the manifest still
    keys on the whole month and resumability granularity is unchanged.
    """
    from lib.equity_1min_store import (
        MANIFEST_EMPTY, MANIFEST_FAILED, MANIFEST_OK,
        completed_chunks, record_chunks, write_rows, year_path,
    )
    from lib.polygon import (
        PolygonError, TruncatedResponseError, fetch_aggs_minute,
        max_connections,
    )

    t_lk = time.monotonic()
    set_local_busy(True)
    done = set() if force else completed_chunks(ticker)
    set_local_busy(False)
    TIMING.loaded_keys += time.monotonic() - t_lk

    # `repair` and a plain re-run differ only in what they consider done:
    # repair re-examines chunks the manifest calls `empty` as well, because an
    # empty result that was actually a silent vendor hiccup is invisible
    # otherwise. `ok` chunks are still skipped — use --force for those.
    if repair:
        from lib.equity_1min_store import read_manifest
        mf = read_manifest(ticker)
        if not mf.empty:
            ok_only = mf[mf["status"] == MANIFEST_OK]
            done = {(pd.Timestamp(a).date(), pd.Timestamp(b).date())
                    for a, b in zip(ok_only["chunk_start"], ok_only["chunk_end"])}
        else:
            done = set()

    todo = [c for c in chunks if c not in done]
    if not todo:
        log.info("  %s: all %d chunk(s) already recorded — skip",
                 ticker, len(chunks))
        return 0

    log.info("  %s: %d/%d chunk(s) to fetch", ticker, len(todo), len(chunks))

    fetched = 0

    def _fetch_span(symbol: str, a: date, b: date) -> list:
        """One request under one symbol, splitting on truncation."""
        try:
            return fetch_aggs_minute(symbol, a, b)
        except TruncatedResponseError:
            # Should not happen with monthly chunks; if the vendor ever returns
            # denser data than 960 bars/session, halve and retry rather than
            # accept a short read as if it were complete.
            log.error("  %s %s..%s: TRUNCATED at the 50k cap — splitting",
                      symbol, a, b)
            mid = a + timedelta(days=(b - a).days // 2)
            res: list = []
            for x, y in ((a, mid), (mid + timedelta(days=1), b)):
                if x <= y and get_trading_days(x, y):
                    res.extend(fetch_aggs_minute(symbol, x, y))
            return res

    def _fetch_one(c_start: date, c_end: date):
        """One chunk, as (elapsed, [(source_symbol, results), ...]).

        Returns a LIST of per-symbol results rather than one flat list so the
        projection can stamp each bar with the symbol it was actually fetched
        under. A month straddling a rename is the only case with >1 entry.
        """
        t0 = time.monotonic()
        spans = (symbols.split_range(c_start, c_end) if symbols is not None
                 else [(c_start, c_end, ticker)])
        with track(f"{ticker} {c_start}..{c_end}", kind="query"):
            out = []
            for a, b, sym in spans:
                if not get_trading_days(a, b):
                    continue
                if sym.upper() != ticker.upper():
                    log.info("  %s %s..%s: requesting under former symbol %s",
                             ticker, a, b, sym)
                out.append((sym, _fetch_span(sym, a, b)))
        return time.monotonic() - t0, out

    for i in range(0, len(todo), batch_chunks):
        batch = todo[i:i + batch_chunks]
        frames: list = []
        records: list = []
        failures: list = []
        busy_seconds = 0.0
        n_ok = n_empty = 0
        batch_t0 = time.monotonic()

        with ThreadPoolExecutor(max_workers=max_connections()) as pool:
            pending = {pool.submit(_fetch_one, a, b): (a, b) for a, b in batch}
            while pending:
                t_wait = time.monotonic()
                completed, _ = wait(pending, return_when=FIRST_COMPLETED)
                TIMING.fanout_blocked += time.monotonic() - t_wait

                t_local = time.monotonic()
                set_local_busy(True)
                for fut in completed:
                    c_start, c_end = pending.pop(fut)
                    try:
                        elapsed, per_symbol = fut.result()
                        busy_seconds += elapsed
                        TIMING.query_secs += elapsed
                        TIMING.query_count += 1
                        parts = []
                        for sym, results in per_symbol:
                            if debug_response and results:
                                log.info("  DEBUG first bar (%s as %s %s): %s",
                                         ticker, sym, c_start, results[0])
                            part = _project(results, ticker, bounds,
                                            source_symbol=sym)
                            if not part.empty:
                                parts.append(part)
                        projected = (pd.concat(parts, ignore_index=True)
                                     if parts else pd.DataFrame())
                    except Exception as exc:
                        failures.append((c_start, c_end,
                                         f"{type(exc).__name__}: {exc}"))
                        records.append({
                            "chunk_start": c_start, "chunk_end": c_end,
                            "status": MANIFEST_FAILED, "bars": 0, "sessions": 0,
                            "fetched_at": datetime.now(),
                            "note": f"{type(exc).__name__}: {exc}"[:300],
                        })
                        continue

                    if projected.empty:
                        # Genuinely no bars: pre-listing, post-delisting, or a
                        # ticker the plan does not cover. Recorded so future
                        # runs stop refetching a permanently empty range.
                        n_empty += 1
                        records.append({
                            "chunk_start": c_start, "chunk_end": c_end,
                            "status": MANIFEST_EMPTY, "bars": 0, "sessions": 0,
                            "fetched_at": datetime.now(), "note": "",
                        })
                    else:
                        n_ok += 1
                        frames.append(projected)
                        records.append({
                            "chunk_start": c_start, "chunk_end": c_end,
                            "status": MANIFEST_OK,
                            "bars": len(projected),
                            "sessions": projected["trade_date"].nunique(),
                            "fetched_at": datetime.now(), "note": "",
                        })
                set_local_busy(False)
                TIMING.local_compute += time.monotonic() - t_local

        batch_wall = time.monotonic() - batch_t0
        TIMING.fanout_wall += batch_wall
        log.info("  %s batch %d-%d: %d chunks in %.1fs | %d ok, %d empty, "
                 "%d failed | measured concurrency %.2f of %d",
                 ticker, i + 1, i + len(batch), len(batch), batch_wall,
                 n_ok, n_empty, len(failures),
                 (busy_seconds / batch_wall) if batch_wall > 0 else 0.0,
                 max_connections())

        if failures:
            log.warning("  %s: %d chunk(s) FAILED and are recorded as such — "
                        "a plain re-run retries them. First few: %s",
                        ticker, len(failures),
                        "; ".join(f"{a}..{b}: {m[:60]}" for a, b, m in failures[:3]))

        # WRITE ORDER IS LOAD-BEARING: bars first, manifest second. A crash
        # between them leaves the chunk unrecorded, so the next run refetches
        # and the keep-last dedupe makes that a no-op. The reverse order would
        # mark a chunk done whose data never landed.
        if frames:
            combined = pd.concat(frames, ignore_index=True)
            write_t0 = time.monotonic()
            set_local_busy(True)
            try:
                by_year = write_rows(ticker, combined)
            except Exception as exc:
                log.error("  %s: PARQUET WRITE FAILED — %s", ticker, exc,
                          exc_info=True)
                raise SystemExit(
                    f"FATAL: parquet write failed for {ticker}: {exc}\n"
                    f"Store dir: {EQUITY_1MIN_DIR}\n"
                    "Aborting rather than continuing to fetch with nothing "
                    "stored."
                )
            set_local_busy(False)
            write_secs = time.monotonic() - write_t0
            TIMING.parquet_write += write_secs
            TIMING.writes.append((write_secs, len(combined)))
            fetched += len(combined)
            for y, n in sorted(by_year.items()):
                p = year_path(ticker, y)
                size_mb = (p.stat().st_size / 1e6) if p.exists() else 0.0
                log.info("    WROTE %s -> %d rows total, %.1f MB", p, n, size_mb)
            log.info("    write took %.1fs for %d new rows",
                     write_secs, len(combined))

        if records:
            set_local_busy(True)
            record_chunks(ticker, records)
            set_local_busy(False)

    return fetched


# --- Leading-empty report ---------------------------------------------------

def report_leading_empty(tickers: list, symbol_map: dict | None = None,
                         out_csv: str = "equity_1min_leading_empty.csv") -> list:
    """Every ticker whose fetched range STARTS with empty chunks, and the first
    date it does have data.

    This is the independent check on ticker renames, and it runs whether or not
    the symbol-history lookup worked. That independence is the point: the
    rename failure mode is one where the fetcher, the manifest and the audit
    all behave correctly and the data is still wrong, so the only useful check
    is one that does not share their assumptions. It reports a shape and asks a
    human to compare it against a real listing date.

    `list_date` is pulled from the vendor's reference endpoint where available,
    which turns most rows into a one-glance verdict: a leading gap that ends at
    list_date is explained; one that does not is a rename or a hole.
    """
    from lib.equity_1min_store import first_data_date, leading_empty_span

    rows: list = []
    for t in tickers:
        span = leading_empty_span(t)
        if span is None:
            continue
        fd = first_data_date(t)
        rows.append({
            "ticker": t,
            "empty_chunks_at_start": span["empty_chunks"],
            "requested_from": str(span["range_start"]),
            "first_data_date": str(fd) if fd else "",
            "all_empty": span["all_empty"],
            "symbol_history": (symbol_map.get(t).describe()
                               if symbol_map and symbol_map.get(t) else ""),
            "list_date": "",
            "explained_by_list_date": "",
        })

    if not rows:
        print("\nLeading-empty report: no ticker starts with an empty stretch.")
        return rows

    # Reference lookup only for the tickers that need explaining.
    try:
        from lib.polygon import fetch_ticker_details
        for r in rows:
            det = fetch_ticker_details(r["ticker"])
            ld = str(det.get("list_date") or "")
            r["list_date"] = ld
            if ld and r["first_data_date"]:
                # Explained when data starts within a week of the listing date.
                try:
                    d_list = datetime.strptime(ld, "%Y-%m-%d").date()
                    d_first = datetime.strptime(r["first_data_date"],
                                                "%Y-%m-%d").date()
                    r["explained_by_list_date"] = (
                        "yes" if abs((d_first - d_list).days) <= 7 else "NO")
                except ValueError:
                    pass
    except Exception as exc:
        log.warning("could not enrich leading-empty report with list_date: %s",
                    exc)

    import csv as _csv
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = _csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    unexplained = [r for r in rows if r["explained_by_list_date"] == "NO"]
    print(f"\n{'=' * 64}")
    print(f"LEADING-EMPTY REPORT — {len(rows)} ticker(s) start with a gap")
    print(f"{'=' * 64}")
    print(f"  {'ticker':<8}{'empty':>6}  {'first data':<12}{'list_date':<12}"
          f"{'explained':<10}symbol history")
    for r in sorted(rows, key=lambda x: -x["empty_chunks_at_start"]):
        print(f"  {r['ticker']:<8}{r['empty_chunks_at_start']:>6}  "
              f"{r['first_data_date']:<12}{r['list_date']:<12}"
              f"{r['explained_by_list_date']:<10}{r['symbol_history'][:40]}")
    if unexplained:
        print(f"\n  {len(unexplained)} ticker(s) NOT explained by list_date — "
              f"these are the likely renames or real holes:")
        print("    " + ", ".join(r["ticker"] for r in unexplained))
    print(f"\n  CSV: {out_csv}")
    print("  Eyeball these against real listing dates before trusting the "
          "store.")
    return rows


# --- Dry run ----------------------------------------------------------------

def dry_run(tickers: list, chunks: list, sessions: list,
            connections: int, probe: bool) -> None:
    """Report request count, storage footprint and runtime BEFORE committing.

    Estimates are arithmetic by default. --probe issues ONE real request to
    calibrate bars/session and bytes/bar from live data, which turns the
    footprint from a guess into a measurement.
    """
    n_tickers = len(tickers)
    n_chunks = len(chunks)
    n_requests = n_tickers * n_chunks
    n_sessions = len(sessions)

    bars_per_session = None
    bytes_per_bar = None
    secs_per_request = None

    if probe:
        from lib.polygon import fetch_aggs_minute
        probe_ticker = "AAPL" if "AAPL" in tickers else tickers[0]
        probe_chunk = chunks[len(chunks) // 2]
        print(f"\nProbing {probe_ticker} {probe_chunk[0]}..{probe_chunk[1]} "
              f"(1 request) ...")
        t0 = time.monotonic()
        results = fetch_aggs_minute(probe_ticker, probe_chunk[0], probe_chunk[1])
        secs_per_request = time.monotonic() - t0
        probe_sessions = len(get_trading_days(probe_chunk[0], probe_chunk[1]))
        if results and probe_sessions:
            bars_per_session = len(results) / probe_sessions
            bounds = session_bounds(probe_chunk[0], probe_chunk[1])
            proj = _project(results, probe_ticker, bounds)
            by_sess = proj["session"].value_counts().to_dict()
            print(f"  {len(results):,} bars over {probe_sessions} sessions "
                  f"= {bars_per_session:.0f} bars/session, "
                  f"{secs_per_request:.2f}s")
            print(f"  session mix: " + ", ".join(
                f"{k}={v:,}" for k, v in sorted(by_sess.items())))
            # Measured on-disk cost, written to a scratch file and removed.
            try:
                import tempfile, os
                from lib.equity_1min_store import _SCHEMA, _coerce, _atomic_write
                from pathlib import Path
                with tempfile.TemporaryDirectory() as td:
                    p = Path(td) / "probe.parquet"
                    _atomic_write(p, _coerce(proj), _SCHEMA)
                    bytes_per_bar = p.stat().st_size / max(1, len(proj))
                print(f"  measured {bytes_per_bar:.1f} bytes/bar on disk "
                      f"(parquet + snappy)")
            except Exception as exc:
                print(f"  (could not measure bytes/bar: {exc})")

    # Fallbacks when not probing. Deliberately a RANGE, because bars/session
    # varies by an order of magnitude across the universe: a liquid large cap
    # trades most extended-hours minutes, a thin one trades almost none, and
    # only minutes that actually traded produce a bar.
    bps_low, bps_mid, bps_high = 390, 700, MAX_BARS_PER_SESSION
    if bars_per_session is not None:
        bps_mid = bars_per_session
    bpb = bytes_per_bar if bytes_per_bar is not None else 42.0
    spr = secs_per_request if secs_per_request is not None else 1.2

    def fmt_bytes(n: float) -> str:
        return f"{n / 1e9:.1f} GB" if n >= 1e9 else f"{n / 1e6:.0f} MB"

    print("\n" + "=" * 64)
    print("DRY RUN — nothing fetched, nothing written")
    print("=" * 64)
    print(f"  tickers                        {n_tickers:>12,}")
    print(f"  trading sessions in range      {n_sessions:>12,}")
    print(f"  monthly chunks per ticker      {n_chunks:>12,}")
    print(f"  REQUESTS                       {n_requests:>12,}")
    print(f"  bars/request cap (50,000)      "
          f"{'OK' if n_sessions and bps_high * 23 < 50000 else 'CHECK':>12}"
          f"   (max {bps_high * 23:,} bars in a 23-session month)")

    print("\n  ROWS AND STORAGE")
    for label, bps in (("regular hours only", bps_low),
                       ("expected (mixed universe)", bps_mid),
                       ("theoretical max", bps_high)):
        rows = n_tickers * n_sessions * bps
        print(f"    {label:<28}{rows:>15,.0f} rows   "
              f"{fmt_bytes(rows * bpb):>10}")
    print(f"    bytes/bar assumed            {bpb:>15.1f}"
          f"{'  (MEASURED)' if bytes_per_bar else '  (estimated)'}")
    rows_mid = n_tickers * n_sessions * bps_mid
    print(f"    per ticker                   "
          f"{rows_mid / n_tickers:>15,.0f} rows   "
          f"{fmt_bytes(rows_mid / n_tickers * bpb):>10}")
    print(f"    per ticker-year (~8 yrs)     "
          f"{rows_mid / n_tickers / 8:>15,.0f} rows   "
          f"{fmt_bytes(rows_mid / n_tickers / 8 * bpb):>10}")

    print("\n  RUNTIME")
    print(f"    seconds/request assumed      {spr:>15.2f}"
          f"{'  (MEASURED)' if secs_per_request else '  (estimated)'}")
    total_request_secs = n_requests * spr
    print(f"    total request time           {total_request_secs:>15,.0f}s "
          f"({total_request_secs / 3600:.1f}h of worker time)")
    for c in sorted({4, 8, 16, connections}):
        wall = total_request_secs / c
        rps = c / spr
        flag = "  <-- default" if c == connections else ""
        print(f"    at {c:>2} connections            "
              f"{wall / 60:>15,.0f} min   ~{rps:.1f} req/s{flag}")
    print("    (plus parquet merge time, which grows through each year file)")

    print("\n  RATE LIMIT HEADROOM")
    print(f"    vendor guidance                    < 100 req/s")
    print(f"    at {connections} connections                 "
          f"~{connections / spr:.1f} req/s "
          f"({100.0 * (connections / spr) / 100:.0f}% of guidance)")
    print("=" * 64)
    print("\nRun without --dry-run to start. Resumable at (ticker, month):")
    print("  an interrupted run re-enters where it stopped.")


# --- Main -------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Fetch 1-minute equity bars from Polygon.io / Massive.")
    ap.add_argument("--dry-run", action="store_true",
                    help="report request count, storage footprint and runtime, "
                         "then exit without fetching or writing")
    ap.add_argument("--probe", action="store_true",
                    help="with --dry-run: issue ONE real request to calibrate "
                         "bars/session, bytes/bar and latency from live data")
    ap.add_argument("--force", action="store_true",
                    help="refetch every chunk in range, ignoring the manifest. "
                         "Dedupe keeps the newest row, so this never duplicates.")
    ap.add_argument("--repair", action="store_true",
                    help="refetch chunks the manifest records as `empty` or "
                         "`failed` (but not `ok`) — recovers a range that came "
                         "back empty because of a transient vendor hiccup "
                         "rather than a genuine absence of data")
    ap.add_argument("--batch-chunks", type=int, default=DEFAULT_BATCH_CHUNKS,
                    help=f"chunks accumulated per parquet write "
                         f"(default {DEFAULT_BATCH_CHUNKS})")
    ap.add_argument("--connections", type=int, default=None,
                    help="concurrent HTTP connections (default 8). Vendor "
                         "guidance is to stay under 100 req/s overall.")
    ap.add_argument("--splice-renames", default=None,
                    help="comma-separated tickers whose PRE-RENAME history may "
                         "be fetched under the former symbol, or 'all'. "
                         "DEFAULT IS NONE: a ticker_change covers rebrands "
                         "(FB->META, safe to splice) as well as de-SPACs "
                         "(CCIV->LCID) and bankruptcy relistings (HTZ), where "
                         "the former symbol is a different economic entity and "
                         "splicing would contaminate the training window. "
                         "Renames are always DISCOVERED and reported; this "
                         "flag only controls which are USED.")
    ap.add_argument("--symbol-history-out",
                    default="equity_1min_symbol_changes.csv",
                    help="CSV of discovered symbol changes and their "
                         "classification")
    ap.add_argument("--no-symbol-history", action="store_true",
                    help="skip the ticker-rename lookup. A renamed ticker will "
                         "then return empty for its whole pre-rename history "
                         "and every check downstream will accept it — only use "
                         "this if the reference endpoint is unavailable.")
    ap.add_argument("--debug-response", action="store_true",
                    help="log the first bar of the first non-empty response")
    ap.add_argument("--tickers", help="comma-separated; skips the prompt")
    ap.add_argument("--start", help="YYYYMMDD (default 20190101)")
    ap.add_argument("--end", help="YYYYMMDD (default last completed session)")
    args = ap.parse_args()

    if args.repair and args.force:
        raise SystemExit("--repair and --force are mutually exclusive: "
                         "--force refetches everything, --repair refetches "
                         "only what is missing or empty.")

    from lib.polygon import (
        POLYGON_TIMING, TOTAL_TIMEOUT, describe_http_config,
        describe_retry_policy, max_connections, reset_timing,
        set_max_connections, test_connection,
    )
    if args.connections is not None:
        set_max_connections(args.connections)

    log_file = setup_file_logging("fetch_equity_1min")
    print("=== Open_Interest — 1-minute equity bars (Polygon / Massive) ===")
    print(f"Log: {log_file}\n")
    log.info("argv: %s", " ".join(sys.argv[1:]))

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    elif args.dry_run:
        # A dry run must work on a machine with no store mounted, so fall back
        # to the documented universe size rather than failing.
        try:
            tickers = list_oi_tickers()
        except Exception as exc:
            log.warning("could not read the OI-store universe (%s)", exc)
            tickers = []
        if not tickers:
            raise SystemExit(
                "No tickers: the OI store is unreadable from here. Pass "
                "--tickers explicitly (e.g. --tickers $(cat universe.txt)) or "
                "run the dry run on the VPS."
            )
    else:
        tickers = prompt_tickers(list_oi_tickers)

    # Prompted when not passed, same as fetch_chain_snapshots.py and
    # fetch_chain_intraday.py. Defaults are offered and accepted on a blank
    # line, because this backfill's canonical range is the whole history —
    # but the range is always shown and confirmed rather than assumed, since
    # it is what determines an 11,000-request run.
    start = (datetime.strptime(args.start, "%Y%m%d").date()
             if args.start else prompt_date("Fetch start date", DEFAULT_START))
    end = (datetime.strptime(args.end, "%Y%m%d").date()
           if args.end else prompt_date("Fetch end   date", last_trading_day()))
    if end < start:
        raise SystemExit("End date must be >= start date.")
    end = min(end, last_trading_day())

    sessions = get_trading_days(start, end)
    if not sessions:
        raise SystemExit("No NYSE trading days in the requested range.")
    chunks = month_chunks(start, end)

    print(f"{len(tickers)} tickers x {len(sessions):,} sessions "
          f"({start} -> {end})")
    print(f"{len(chunks)} monthly chunk(s) per ticker, "
          f"{max_connections()} connections")
    print("adjusted=false (as-traded), limit=50000, extended hours included\n")

    if args.dry_run:
        if args.probe:
            print("Checking Polygon ...", end=" ", flush=True)
            if not test_connection():
                raise SystemExit("FAILED — check POLYGON_API_KEY in .env.")
            print("OK")
        dry_run(tickers, chunks, sessions, max_connections(), args.probe)
        return 0

    run_t0 = time.monotonic()
    reset_timing()
    start_sampler()

    preflight_store(EQUITY_1MIN_DIR, CHAIN_EOD_DIR)

    print("Checking Polygon ...", end=" ", flush=True)
    if not test_connection():
        raise SystemExit("FAILED — check POLYGON_API_KEY in .env.")
    print("OK")
    print(f"HTTP: {describe_http_config()}")
    print(f"Retry: {describe_retry_policy()}\n")

    # One calendar read for the whole range, shared by every ticker — the
    # per-session bounds are ticker-independent and rebuilding them per ticker
    # would call the calendar 121 times for identical output.
    bounds = session_bounds(start, end)

    # Symbol history BEFORE any bar is requested. A rename is the one defect
    # that every downstream check accepts as legitimate (see
    # lib/polygon_symbols.py), so it has to be resolved up front rather than
    # detected afterwards. 121 cheap reference calls against 11,132 data calls.
    symbol_map: dict = {}
    if not args.no_symbol_history:
        from lib.polygon_symbols import build_all, report_symbol_histories
        splice_allow = {s.strip().upper()
                        for s in (args.splice_renames or "").split(",")
                        if s.strip()}
        print("Resolving symbol history ...", end=" ", flush=True)
        symbol_map = build_all(tickers, max_workers=max_connections(),
                               splice_allow=splice_allow)
        renamed = {t: h for t, h in symbol_map.items() if h.renamed}
        print(f"OK ({len(renamed)} of {len(tickers)} ticker(s) have a "
              f"symbol change)")
        if renamed:
            report_symbol_histories(symbol_map,
                                    out_csv=args.symbol_history_out)
            for t, h in sorted(renamed.items()):
                log.info("symbol history %s: %s (%s, spliced=%s)",
                         t, h.describe(), h.classification, h.splice)
        else:
            print("  (none found — if you expect one, check that the "
                  "vX/reference events endpoint is entitled on this plan)")
    else:
        print("Symbol history DISABLED — renamed tickers will silently return "
              "empty for their pre-rename history.")

    TIMING.startup = time.monotonic() - run_t0
    start_watchdog(hard_cap=TOTAL_TIMEOUT)

    total = 0
    failed_tickers: list = []
    with tqdm(total=len(tickers), unit="tk", ncols=90, desc="equity_1min") as bar:
        for t in tickers:
            try:
                total += fetch_ticker(t, chunks, bounds,
                                      batch_chunks=args.batch_chunks,
                                      force=args.force, repair=args.repair,
                                      debug_response=args.debug_response,
                                      symbols=symbol_map.get(t))
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                log.error("  FAIL %s: %s", t, exc, exc_info=True)
                failed_tickers.append(t)
            bar.update(1)

    stop_background_threads()

    print(f"\n{total:,} rows fetched and merged into {EQUITY_1MIN_DIR}")
    print("Fetch-and-store only — no metrics repointed, no cron wired.")
    print(f"Log written to {log_path()}")

    print_timing_summary(time.monotonic() - run_t0,
                         query_label="chunk requests",
                         timing=POLYGON_TIMING,
                         retry_policy=describe_retry_policy(),
                         connections=max_connections())

    report_leading_empty(tickers, symbol_map)

    if failed_tickers:
        print(f"\n{len(failed_tickers)} ticker(s) FAILED: "
              f"{', '.join(failed_tickers[:10])}"
              f"{' ...' if len(failed_tickers) > 10 else ''}")

    if total == 0 and TIMING.query_count == 0 and not failed_tickers:
        print("\nNothing to do — every chunk was already recorded. "
              "Use --force to refetch or --repair to retry empties.")
        return 0

    if total == 0:
        raise SystemExit(
            "\nFAILED: no rows were stored.\n"
            f"Store dir: {EQUITY_1MIN_DIR}\n"
            "Check the per-batch counter lines above — they say whether the "
            "vendor returned no bars, or whether the requests failed."
        )
    print("\nNext: python audit_equity_1min.py")
    if failed_tickers:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
