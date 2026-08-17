"""
fetch_chain_snapshots.py — Pure I/O fetcher for twice-daily intraday option
chain snapshots (09:45:00 and 15:45:00 ET).

Source: ThetaData /v3/option/history/greeks/first_order — bid/ask, the full
first-order greek set, implied_vol, iv_error, and the underlying price at the
snapshot instant.  09:45 and 15:45 are used deliberately in place of 09:30 and
16:00, which are unreliable.

Writes to data/chain_snapshots/{ticker}/{year}.parquet — a sibling of
data/chain_eod/, created automatically on first write.

SCOPE: fetch-and-store only.  This script does not repoint any metric
calculation and is not wired into cron.  Those are separate deliberate steps,
to be taken after validating this data against chain_eod on overlapping dates.

By design:
  * No aggregation, no metric math, no Postgres writes.  Parquet is the only
    output.
  * No row filtering.  Every row the vendor returns is kept, including
    zero-IV / no-quote contracts — an absent or wide market is itself
    information for the IV-page and backtest use cases.
  * Fixed typed schema, but any vendor field NOT in the schema is reported
    once per run (see _warn_unknown_fields) so vendor changes surface instead
    of vanishing silently.

--- Expiration enumeration (the completeness guarantee) --------------------

The first_order endpoint requires a specific expiration — it rejects
expiration=*.  So the fetcher must know which expirations existed on each
session.  It enumerates them from /v3/option/history/eod with expiration=* for
the SAME single session it is about to fetch.

That source is chosen over /v3/option/list/expirations and over the OI parquet
store because it is keyed per-date (no historical over-fetch waste), it
reports what the exchange actually listed rather than what traded, and it is
one consistent source for backfill and live alike.  Every ticker here lists
weeklies, so a source keyed off traded activity — the OI store drops zero-OI
rows, and this script would run before the OI fetch anyway — would
systematically miss a brand-new weekly on its listing day.  That would be a
guaranteed weekly hole on every ticker.

Enumeration is per session (option (b)), which makes coverage exact rather
than merely safe:

  1. The enumeration window IS the fetch window — one session — so the set
     fetched for a date is precisely what was listed on that date.  A weekly
     first listed on day N is enumerated on day N.  There is no path where one
     date's list gets applied to another date, and no dead (expiration,
     session) pairs to absorb.
  2. The enumerated set is fetched in full.  No DTE cap, no intersection
     against another source, no row filtering.
  3. The single narrowing — discarding expirations before the session date —
     is provably a no-op, since a contract listed on D expires on or after D.
     It only guards against a stale enumeration row costing a round trip.

--- Request shape -----------------------------------------------------------

Every request is a point query in BOTH dimensions:
    start_time == end_time == 09:45:00 / 15:45:00   (one instant, not a bar)
    start_date == end_date == the session            (one session, not a range)

The date dimension matters as much as the time dimension.  An earlier version
issued one call per (expiration, snapshot) spanning up to 30 calendar days,
which made the terminal compute greeks across ~21 sessions of a full-width
chain per request.  That produced 570s and 60s timeouts, whose halving-and-
retry cascade then dominated the runtime.  Total rows retrieved are identical
either way; only the per-request size differs, and the terminal is markedly
superlinear in it.

--- Connections -----------------------------------------------------------

The ThetaData subscription allows 4 concurrent connections.  The cap is
enforced two ways so no loop structure can exceed it: tickers are processed
sequentially with a single 4-worker pool covering only the expiration x
snapshot fan-out (enumeration runs on the main thread between fan-outs), and
lib/thetadata.py holds a BoundedSemaphore(4) around every snapshot request.

Usage:
    python fetch_chain_snapshots.py
        (prompts for tickers + date range)

    python fetch_chain_snapshots.py --force
        (refetch dates already present in the store)

    python fetch_chain_snapshots.py --batch-days 5
        (write more often — lower peak memory, less lost to an interrupt)
"""
from __future__ import annotations

import argparse
import logging
import queue
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import date, datetime

import pandas as pd
from tqdm import tqdm

from config import CHAIN_EOD_DIR, CHAIN_SNAPSHOTS_DIR
from lib.chain_fetch_common import (
    TIMING,
    chunk_range,
    ParquetWriterThread,
    log_path,
    preflight_store,
    print_timing_summary,
    prompt_date,
    prompt_tickers,
    set_local_busy,
    start_sampler,
    start_watchdog,
    setup_file_logging,
    stop_background_threads,
    to_date_series,
    track,
)
from lib.chain_snapshot_store import (
    SNAPSHOT_LABELS,
    loaded_cells,
    loaded_keys,
    write_rows,
    year_path,
)
from lib.market_hours import get_trading_days, last_trading_day, next_trading_day
from lib.parquet_store import list_tickers as list_oi_tickers
from lib.thetadata import (
    SNAPSHOT_TOTAL_TIMEOUT,
    TerminalServerError,
    TerminalTimeoutError,
    describe_http_config,
    describe_retry_policy,
    enumerate_expirations_eod,
    max_connections,
    set_max_connections,
    fetch_first_order_raw,
    reset_snapshot_timing,
    test_connection,
)

# label -> the time-of-day string sent to the endpoint.
# These are point-in-time instants, not bars: start_time == end_time returns
# the chain state at exactly that moment.
SNAPSHOT_TIMES = {
    "0945": "09:45:00",
    "1545": "15:45:00",
}

# The store owns the labels; a mismatch here would silently skip a snapshot.
assert set(SNAPSHOT_TIMES) == set(SNAPSHOT_LABELS), \
    "SNAPSHOT_TIMES must cover exactly lib.chain_snapshot_store.SNAPSHOT_LABELS"

# Sessions per WRITE batch. Requests are always single-session point queries,
# so this no longer affects request size or response time — only how many
# sessions are accumulated in memory before a parquet write, and therefore how
# much work an interrupted run discards. Lower it to reduce peak memory on
# wide chains; raise it to write less often.
DEFAULT_BATCH_DAYS = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Vendor fields we know about. Anything outside this set is reported once per
# run so a vendor schema change surfaces instead of being silently dropped.
_KNOWN_VENDOR_FIELDS = {
    "symbol", "root", "ticker", "date", "timestamp", "expiration", "strike",
    "right", "option_type", "bid", "ask", "delta", "theta", "vega", "rho",
    "epsilon", "lambda", "implied_vol", "iv", "iv_error",
    "underlying_timestamp", "underlying_price",
}
_UNKNOWN_WARNED: set[str] = set()
_MISSING_WARNED: set[str] = set()
_WARN_LOCK = threading.Lock()

# Fields we expect to store; a persistent absence is worth knowing about.
_EXPECTED_VENDOR_FIELDS = {
    "bid", "ask", "delta", "theta", "vega", "rho", "epsilon", "lambda",
    "implied_vol", "iv_error", "underlying_timestamp", "underlying_price",
}



# --- Projection ------------------------------------------------------------

def _warn_unknown_fields(raw: pd.DataFrame) -> None:
    """Report unexpected / absent vendor fields once per run.

    Projection now runs on pool threads, so the once-per-run sets are guarded:
    without the lock a check-then-update race would emit the same warning from
    several workers, which reads like several distinct schema changes.
    """
    cols = set(raw.columns)

    with _WARN_LOCK:
        _warn_unknown_fields_locked(cols)


def _warn_unknown_fields_locked(cols: set) -> None:
    unknown = cols - _KNOWN_VENDOR_FIELDS - _UNKNOWN_WARNED
    if unknown:
        _UNKNOWN_WARNED.update(unknown)
        log.warning(
            "first_order response carries field(s) not in the stored schema: "
            "%s — they are NOT being stored. Update lib/chain_snapshot_store.py "
            "if you want them.", ", ".join(sorted(unknown))
        )

    missing = _EXPECTED_VENDOR_FIELDS - cols - _MISSING_WARNED
    if missing:
        _MISSING_WARNED.update(missing)
        log.warning(
            "first_order response is missing expected field(s): %s — "
            "storing as NULL.", ", ".join(sorted(missing))
        )



def _project(raw: pd.DataFrame, ticker: str, snapshot: str,
             session: date) -> pd.DataFrame:
    """Project the vendor's raw first_order frame into the stored schema.

    Field mapping, confirmed against a real response rather than inferred:

        vendor                  stored
        ------                  ------
        timestamp            -> timestamp   (verbatim, naive ET)
                             -> trade_date  (date part; see below)
        expiration           -> expiration
        strike               -> strike
        right                -> option_type ('C'/'P')
        bid, ask             -> bid, ask
        delta, theta, vega,
        rho, epsilon, lambda -> same names
        implied_vol          -> implied_vol
        iv_error             -> iv_error
        underlying_price     -> underlying_price
        underlying_timestamp -> underlying_timestamp
        symbol               -> (unused; `ticker` comes from the fetch loop,
                                 which is authoritative for the request)

    Plus three derived columns: ticker, snapshot ('0945'/'1545'), and
    feature_date = next_trading_day(trade_date).  16 vendor fields + 4 derived
    = the 20-column store schema.

    The response carries NO `date` field, so trade_date is derived from
    `timestamp`.  That makes one field load-bearing for the store's primary
    key, so `session` — the date we requested, which is known and correct —
    is used as a fallback wherever the timestamp will not parse.  Without it
    a timestamp format change would silently drop every row.

    No row filtering beyond dropping rows that lack an identifier the store is
    keyed on (trade_date, expiration, strike, option_type).  Zero-IV and
    no-quote contracts are kept deliberately.
    """
    if raw.empty:
        return raw

    _warn_unknown_fields(raw)

    if "expiration" not in raw.columns:
        log.warning("  %s %s: response has no 'expiration' field — dropping "
                    "%d rows", ticker, snapshot, len(raw))
        return pd.DataFrame()

    ts = pd.to_datetime(raw["timestamp"], errors="coerce") \
        if "timestamp" in raw.columns else pd.Series(pd.NaT, index=raw.index)

    # trade_date: prefer an explicit vendor date field, then the date part of
    # the row timestamp, then the session we asked for. The last fallback is
    # what stops a timestamp-format change from silently emptying the store.
    if "date" in raw.columns:
        td = to_date_series(raw["date"]).dt.date
    else:
        td = ts.dt.date
    n_fallback = int(td.isna().sum())
    if n_fallback:
        td = td.fillna(session)
        log.warning("  %s %s @%s: %d/%d rows had no parseable timestamp — "
                    "trade_date fell back to the requested session",
                    ticker, session, snapshot, n_fallback, len(raw))

    # option_type: vendor uses 'right' (C/P or CALL/PUT); project convention
    # is option_type with 'C'/'P'.
    src_otype = raw.get("option_type")
    if src_otype is None:
        src_otype = raw.get("right")
    if src_otype is None:
        log.warning("  %s %s: response has neither 'right' nor 'option_type' "
                    "— dropping %d rows", ticker, snapshot, len(raw))
        return pd.DataFrame()
    otype = src_otype.astype(str).str.strip().str.upper().map(
        lambda s: "C" if s in ("CALL", "C") else ("P" if s in ("PUT", "P") else None)
    )

    # feature_date once per unique session, not per row — the per-row .apply()
    # calls pandas_market_calendars thousands of times on a dense chain and
    # holds the GIL, which also serialises the worker pool.
    fd_map = {d: next_trading_day(d) for d in td.dropna().unique()}

    out = pd.DataFrame({
        "ticker":               ticker.upper(),
        "trade_date":           td,
        "snapshot":             snapshot,
        "feature_date":         td.map(fd_map),
        "timestamp":            ts,
        "expiration":           to_date_series(raw["expiration"]).dt.date,
        "strike":               pd.to_numeric(raw.get("strike"), errors="coerce"),
        "option_type":          otype,
        "bid":                  pd.to_numeric(raw.get("bid"), errors="coerce"),
        "ask":                  pd.to_numeric(raw.get("ask"), errors="coerce"),
        "delta":                pd.to_numeric(raw.get("delta"), errors="coerce"),
        "theta":                pd.to_numeric(raw.get("theta"), errors="coerce"),
        "vega":                 pd.to_numeric(raw.get("vega"), errors="coerce"),
        "rho":                  pd.to_numeric(raw.get("rho"), errors="coerce"),
        "epsilon":              pd.to_numeric(raw.get("epsilon"), errors="coerce"),
        "lambda":               pd.to_numeric(raw.get("lambda"), errors="coerce"),
        "implied_vol":          pd.to_numeric(
                                    raw["implied_vol"] if "implied_vol" in raw.columns
                                    else raw.get("iv"), errors="coerce"),
        "iv_error":             pd.to_numeric(raw.get("iv_error"), errors="coerce"),
        "underlying_timestamp": pd.to_datetime(raw.get("underlying_timestamp"),
                                               errors="coerce"),
        "underlying_price":     pd.to_numeric(raw.get("underlying_price"),
                                              errors="coerce"),
    })

    return out.dropna(subset=["trade_date", "expiration", "strike", "option_type"])


# --- Per-ticker fetch ------------------------------------------------------

# Worker threads per allowed connection. Above 1.0 so a projecting worker does
# not hold a slot that could be issuing a request; the semaphore still caps
# actual network concurrency, so this cannot exceed the vendor's limit.
POOL_OVERSUBSCRIBE = 2


def pool_size() -> int:
    return max(max_connections() * POOL_OVERSUBSCRIBE, max_connections() + 1)


def fetch_ticker(ticker: str, batches: list[tuple[date, date]],
                 writer: ParquetWriterThread,
                 force: bool = False, debug_response: bool = False,
                 repair: bool = False) -> int:
    """Enumerate + fetch every session in every batch for one ticker.

    Every ThetaData request issued here is a point query — one expiration,
    one session, one instant. `batches` group sessions for WRITE batching
    only; they no longer affect request size.

    Returns rows fetched and merged (before dedupe against what was already
    on disk), so a refetch reports the rows it re-sent, not net new rows.

    Writes once per batch, AFTER that batch's fan-out has completed, so the
    (trade_date, snapshot) "loaded" flag stays honest: a run interrupted
    mid-batch leaves nothing for that batch, and a rerun redoes it in full
    rather than skipping a partial gap.
    """
    years = {y for (a, b) in batches for y in range(a.year, b.year + 1)}
    t_lk = time.monotonic()
    set_local_busy(True)
    if repair:
        # Repair works at (session, expiration, label) — the granularity at
        # which point queries actually fail. The coarse (session, label) key
        # cannot see a hole inside a session that wrote some expirations.
        already = set()
        present_cells = loaded_cells(ticker, years)
    else:
        already = set() if force else loaded_keys(ticker, years)
        present_cells = set()
    set_local_busy(False)
    TIMING.loaded_keys += time.monotonic() - t_lk

    fetched = 0
    for w_start, w_end in batches:
        sessions = get_trading_days(w_start, w_end)
        if not sessions:
            continue

        # In repair mode every session is re-enumerated: the whole point is to
        # discover cells the store is missing, which by definition are not
        # visible from what it already holds.
        todo = sessions if repair else [
            d for d in sessions
            if any((d, s) not in already for s in SNAPSHOT_LABELS)
        ]
        if not todo:
            log.info("  %s %s..%s: all %d sessions loaded — skip",
                     ticker, w_start, w_end, len(sessions))
            continue

        # --- enumerate + fetch, pipelined in ONE pool -----------------------
        # Enumerating per session (option (b)) rather than per window makes the
        # completeness invariant exact rather than merely safe: the enumeration
        # window IS the fetch window, one session wide, so the fetched set is
        # precisely what existed that day. No dead (expiration, session) pairs.
        #
        # Enumeration and fetching share a single 4-slot pool with NO barrier
        # between them: the moment one session's expirations come back, its
        # point queries are submitted, while other sessions are still
        # enumerating. A barrier here would idle up to 3 connections for the
        # whole enumeration phase (and all 4 but one when a batch holds a
        # single session). The cap still holds — one pool, 4 workers, both
        # kinds of request drawing from the same slots.
        exp_by_session: dict[date, list[date]] = {}
        enum_failures: list[tuple[date, str]] = []
        frames: list[pd.DataFrame] = []
        failures: list[tuple[date, date, str, str]] = []
        # Every path a row can take is counted, so "nothing was stored" always
        # resolves to a specific stage rather than an absence of log lines.
        n_ok = n_empty = n_projected_away = 0
        raw_rows = proj_rows = 0
        n_queries = 0
        n_repair_targets = 0
        first_raw_cols: list[str] | None = None
        # Sum of per-request wall time. Divided by batch wall time this gives
        # measured average concurrency — the direct check on whether the 4
        # allowed connections are actually being kept busy.
        busy_seconds = 0.0

        # track() times the whole task body in a finally block and accrues it
        # to TIMING.task_secs, so tasks that RAISE are counted. The `elapsed`
        # returned below is accrued on the main thread and is reached only on
        # success — kept for run-to-run comparability, but see (B2) in the
        # summary for the authoritative worker-side number.
        def _enum_one(sess: date) -> tuple[float, set[date]]:
            t0 = time.monotonic()
            with track(f"enum {ticker} {sess}", kind="enum"):
                out = enumerate_expirations_eod(ticker, sess, sess)
            return time.monotonic() - t0, out

        def _fetch_one(sess: date, exp: date, snap: str):
            """Fetch AND project, on the pool thread.

            Projection used to run on the main thread while it collected
            futures, which stalled the fan-out for ~19% of the run. Doing it
            here overlaps it with other requests. The pool is sized above the
            connection cap (see below) so a projecting worker does not occupy
            a slot that could be fetching.

            Returns (elapsed, raw_rows, raw_cols, projected). raw_cols is
            carried out only so the main thread can report the first response's
            shape without keeping the raw frame alive.
            """
            t0 = time.monotonic()
            with track(f"{ticker} exp={exp} {sess}@{snap}", kind="query"):
                raw = fetch_first_order_raw(ticker, exp, sess, SNAPSHOT_TIMES[snap])
            elapsed = time.monotonic() - t0
            if raw.empty:
                return elapsed, 0, None, None
            cols = list(raw.columns)
            first_row = raw.iloc[0].to_dict() if debug_response else None
            projected = _project(raw, ticker, snap, sess)
            return elapsed, len(raw), (cols, first_row), projected

        batch_t0 = time.monotonic()
        # Pool is deliberately WIDER than the connection cap. The semaphore in
        # lib/thetadata is what actually bounds in-flight HTTP, so extra
        # workers cannot exceed it — they exist so that a worker busy
        # projecting a response is not occupying a slot that could be
        # fetching. With pool == cap, every projection directly cost a
        # connection.
        with ThreadPoolExecutor(max_workers=pool_size()) as pool:
            # kind, session, expiration, snapshot
            pending: dict = {
                pool.submit(_enum_one, d): ("enum", d, None, None) for d in todo
            }
            while pending:
                t_wait = time.monotonic()
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                TIMING.fanout_blocked += time.monotonic() - t_wait

                t_local = time.monotonic()
                set_local_busy(True)
                for fut in done:
                    kind, sess, exp, snap = pending.pop(fut)

                    if kind == "enum":
                        try:
                            elapsed, raw_exps = fut.result()
                        except Exception as exc:
                            enum_failures.append(
                                (sess, f"{type(exc).__name__}: {exc}"))
                            continue
                        busy_seconds += elapsed
                        TIMING.enum_secs += elapsed
                        TIMING.enum_count += 1
                        # exp >= sess is a no-op safety net: a contract listed
                        # on sess necessarily expires on or after sess.
                        exps = sorted(e for e in raw_exps if e >= sess)
                        if not exps:
                            continue
                        exp_by_session[sess] = exps
                        # Submit this session's queries immediately — no wait
                        # for the other sessions to finish enumerating.
                        for e in exps:
                            for s in SNAPSHOT_LABELS:
                                if repair:
                                    # Refetch ONLY cells absent from the store.
                                    if (sess, e, s) in present_cells:
                                        continue
                                    n_repair_targets += 1
                                elif (sess, s) in already:
                                    continue
                                n_queries += 1
                                nf = pool.submit(_fetch_one, sess, e, s)
                                pending[nf] = ("fetch", sess, e, s)
                        continue

                    try:
                        # Projection already happened on the pool thread; a
                        # malformed response still surfaces here as a unit
                        # failure, because the worker raised inside its future.
                        elapsed, n_raw, shape, projected = fut.result()
                        busy_seconds += elapsed
                        TIMING.query_secs += elapsed
                        TIMING.query_count += 1
                        # None means the worker saw an empty response. Keyed on
                        # `projected` alone rather than on n_raw too: the two
                        # always agree today, but a disagreement would reach
                        # len(projected) below, which is past the except blocks
                        # and would take the whole ticker down instead of one
                        # unit.
                        if projected is None:
                            n_empty += 1
                            continue
                        raw_rows += n_raw
                        if first_raw_cols is None and shape is not None:
                            first_raw_cols, first_row = shape
                            if debug_response:
                                log.info("  DEBUG first non-empty response "
                                         "(%s exp=%s %s @%s): columns=%s",
                                         ticker, exp, sess, snap, first_raw_cols)
                                log.info("  DEBUG first row: %s", first_row)
                    except (TerminalTimeoutError, TerminalServerError) as exc:
                        failures.append((sess, exp, snap, f"{type(exc).__name__}: {exc}"))
                        continue
                    except Exception as exc:
                        failures.append((sess, exp, snap, f"{type(exc).__name__}: {exc}"))
                        continue
                    proj_rows += len(projected)
                    if projected.empty:
                        n_projected_away += 1
                    else:
                        n_ok += 1
                        frames.append(projected)

                set_local_busy(False)
                TIMING.local_compute += time.monotonic() - t_local

        batch_wall = time.monotonic() - batch_t0
        TIMING.fanout_wall += batch_wall
        n_requests = n_queries + len(todo)
        log.info("  %s %s..%s: %d sessions, %d point queries, %d requests in "
                 "%.1fs | measured concurrency %.2f of %d "
                 "(%.2fs avg per request)",
                 ticker, w_start, w_end, len(exp_by_session), n_queries,
                 n_requests, batch_wall,
                 (busy_seconds / batch_wall) if batch_wall > 0 else 0.0,
                 max_connections(),
                 (busy_seconds / n_requests) if n_requests else 0.0)

        if enum_failures:
            # These sessions are simply not written, so they stay unloaded and
            # a plain rerun retries them — no --force needed.
            log.warning("  %s %s..%s: enumeration FAILED for %d session(s) — "
                        "not written, rerun to retry. First few: %s",
                        ticker, w_start, w_end, len(enum_failures),
                        "; ".join(f"{d}: {m[:60]}" for d, m in enum_failures[:3]))

        if failures:
            log.warning("  %s %s..%s: %d/%d point queries FAILED — those "
                        "(session, expiration, snapshot) cells are missing. "
                        "Rerun with --force to retry. First few: %s",
                        ticker, w_start, w_end, len(failures), n_queries,
                        "; ".join(f"{d}/{e}/{s}: {m[:60]}"
                                  for d, e, s, m in failures[:3]))

        log.info("  %s %s..%s: %d queries -> %d with rows, %d empty, "
                 "%d projected-away, %d failed | %d raw rows -> %d projected",
                 ticker, w_start, w_end, n_queries, n_ok, n_empty,
                 n_projected_away, len(failures), raw_rows, proj_rows)

        # The one case that would otherwise look identical to "no data exists":
        # the vendor DID return rows and the projection discarded all of them.
        if repair:
            # A gap that refetches to nothing is not a failure — it means the
            # vendor has no data for that (expiration, label) and the store is
            # already as complete as it can be. Separating the two is the
            # whole point: a second repair run should show recovered=0 and the
            # same still-empty count, which is what "complete" looks like.
            log.info("  %s %s..%s: REPAIR — %d missing cell(s) found, "
                     "%d recovered with rows, %d returned no data (vendor has "
                     "none), %d failed",
                     ticker, w_start, w_end, n_repair_targets, n_ok, n_empty,
                     len(failures))

        if raw_rows > 0 and proj_rows == 0:
            log.error("  %s %s..%s: %d raw rows returned but ALL were dropped "
                      "in projection — rows are dropped only when trade_date, "
                      "expiration, strike or option_type cannot be parsed. "
                      "Vendor columns were: %s",
                      ticker, w_start, w_end, raw_rows, first_raw_cols)

        if not frames:
            log.warning("  %s %s..%s: NOTHING WRITTEN — no rows survived "
                        "(see the counts above for which stage lost them)",
                        ticker, w_start, w_end)
            continue

        combined = pd.concat(frames, ignore_index=True)
        # Hand off and move on. The next batch's enumeration and point queries
        # start immediately; the merge-and-rewrite happens behind them. Blocks
        # only if the writer is still busy with the previous batch, which is
        # recorded as writer_wait so backpressure is measured rather than
        # guessed at.
        writer.submit(ticker, combined, f"{ticker} {w_start}..{w_end}")
        fetched += len(combined)

    return fetched


# --- Main ------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fetch twice-daily (09:45 / 15:45) option chain snapshots."
    )
    ap.add_argument("--force", action="store_true",
                    help=("refetch every (date, snapshot) cell in range. "
                          "Overwrites in place — dedupe keeps the newest row, "
                          "so re-running never duplicates."))
    ap.add_argument("--repair", action="store_true",
                    help=("gap repair: re-enumerate every session, diff "
                          "against the (session, expiration, snapshot) cells "
                          "actually stored, and refetch ONLY what is missing. "
                          "This is the only mode that heals point-query "
                          "failures inside an otherwise-written session."))
    ap.add_argument("--batch-days", type=int, default=DEFAULT_BATCH_DAYS,
                    help=("calendar days accumulated per parquet write "
                          f"(default {DEFAULT_BATCH_DAYS}). Does not affect "
                          "request size — every request is a point query."))
    ap.add_argument("--connections", type=int, default=None,
                    help=("concurrent ThetaData connections (default 4). "
                          "Vendor guidance: this should MATCH the Theta "
                          "Terminal's HTTP_CONCURRENCY setting."))
    ap.add_argument("--write-queue", type=int, default=2,
                    help=("parquet batches that may sit queued for the writer "
                          "thread (default 2). Bounded on purpose: the queue "
                          "holds whole batch frames, so a large value trades "
                          "memory for the ability to hide a slow writer. If "
                          "the summary shows a large 'main thread blocked on "
                          "queue', raising this only defers the problem."))
    ap.add_argument("--debug-response", action="store_true",
                    help="dump columns + first row of the first non-empty "
                         "response per batch (diagnosing empty stores)")
    ap.add_argument("--tickers", help="comma-separated; skips the prompt")
    ap.add_argument("--start", help="YYYYMMDD; skips the prompt")
    ap.add_argument("--end", help="YYYYMMDD; skips the prompt")
    args = ap.parse_args()

    # Must happen before any request is in flight — it rebuilds the semaphore.
    if args.connections is not None:
        set_max_connections(args.connections)
    if args.repair and args.force:
        raise SystemExit("--repair and --force are mutually exclusive: "
                         "--force refetches everything, --repair refetches "
                         "only what is missing.")

    log_file = setup_file_logging("fetch_chain_snapshots")

    print("=== Open_Interest — intraday chain snapshots (09:45 / 15:45) ===")
    print(f"Log: {log_file}\n")
    log.info("argv: %s", " ".join(sys.argv[1:]))
    log.info("connections=%d | retry: %s",
             max_connections(), describe_retry_policy())

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = prompt_tickers(list_oi_tickers)

    start = (datetime.strptime(args.start, "%Y%m%d").date()
             if args.start else prompt_date("Fetch start date"))
    end = (datetime.strptime(args.end, "%Y%m%d").date()
           if args.end else prompt_date("Fetch end   date"))
    if end < start:
        raise SystemExit("End date must be >= start date.")

    end = min(end, last_trading_day())
    if end < start:
        raise SystemExit("No completed trading days in the requested range.")

    sessions = get_trading_days(start, end)
    if not sessions:
        raise SystemExit("No NYSE trading days in the requested range.")

    batches = chunk_range(start, end, args.batch_days)

    print(f"\n{len(tickers)} tickers x {len(sessions)} sessions "
          f"({start} -> {end})")
    print(f"{len(batches)} write batch(es) of <= {args.batch_days} calendar days, "
          f"{max_connections()} concurrent connections"
          f"{', --force (ignoring loaded cells)' if args.force else ''}")
    print("Every request is a point query: one expiration, one session, "
          "one instant.")
    print("Note: the last session is included even if today's 15:45 snapshot "
          "has not happened yet — it simply returns no data.\n")

    run_t0 = time.monotonic()
    reset_snapshot_timing()
    start_sampler()

    preflight_store(CHAIN_SNAPSHOTS_DIR, CHAIN_EOD_DIR)

    print("Checking ThetaData ...", end=" ", flush=True)
    if not test_connection():
        raise SystemExit("FAILED — terminal not reachable.")
    print("OK")
    print(f"Per-request caps: connect 10s, read 45s, hard total "
          f"{SNAPSHOT_TOTAL_TIMEOUT}s. Watchdog reports stalls every 30s.")
    print(f"HTTP: {describe_http_config()}\n")

    TIMING.startup = time.monotonic() - run_t0
    start_watchdog()

    writer = ParquetWriterThread(write_rows, year_path,
                                 CHAIN_SNAPSHOTS_DIR,
                                 maxsize=args.write_queue)
    writer.start()

    # Tickers are sequential on purpose: the connection budget is spent inside
    # each chunk's fan-out, so ticker-level parallelism would buy nothing and
    # could only risk exceeding the cap.
    total = 0
    failed_tickers: list[str] = []
    with tqdm(total=len(tickers), unit="tk", ncols=90, desc="snapshots") as bar:
        for t in tickers:
            try:
                total += fetch_ticker(t, batches, writer, force=args.force,
                                      debug_response=args.debug_response,
                                      repair=args.repair)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                log.error("  FAIL %s: %s", t, exc, exc_info=True)
                failed_tickers.append(t)
            bar.update(1)

    # Drain BEFORE the summary and before reporting row counts: queued batches
    # are fetched but not yet on disk, and a run that reported success while
    # the tail of its data was still in memory would be lying in the one place
    # it matters. close() also re-raises a writer failure the loop never saw.
    print("\nFlushing pending parquet writes ...", end=" ", flush=True)
    flush_t0 = time.monotonic()
    writer.close()
    print(f"OK ({time.monotonic() - flush_t0:.1f}s)")

    stop_background_threads()

    print(f"\n{total:,} rows fetched and merged into {CHAIN_SNAPSHOTS_DIR}")
    print("Fetch-and-store only — no metrics repointed, no cron wired.")
    print(f"Log written to {log_path()}")

    print_timing_summary(time.monotonic() - run_t0)

    if failed_tickers:
        print(f"\n{len(failed_tickers)} ticker(s) FAILED: "
              f"{', '.join(failed_tickers[:10])}"
              f"{' ...' if len(failed_tickers) > 10 else ''}")

    # A run that attempted no requests at all had nothing to do (everything
    # already loaded) — that is a legitimate no-op, not a failure.
    attempted = TIMING.enum_count + TIMING.query_count
    if total == 0 and attempted == 0 and not failed_tickers:
        print("\nNothing to do — every (date, snapshot) cell was already "
              "loaded. Use --force to refetch.")
        return

    # A run that DID work but stored nothing must not look like a success.
    if total == 0:
        raise SystemExit(
            "\nFAILED: no rows were stored.\n"
            f"Store dir: {CHAIN_SNAPSHOTS_DIR}\n"
            "Check the per-batch counter lines above — they say whether the "
            "vendor returned no rows (all queries empty), whether rows were "
            "dropped in projection, or whether the queries failed.\n"
            "Re-run one ticker with --debug-response to dump the first "
            "non-empty response."
        )
    if failed_tickers:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
