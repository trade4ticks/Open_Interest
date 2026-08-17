"""
fetch_chain_intraday.py — full-day 5-minute intraday option chain bars.

Sister script to fetch_chain_snapshots.py. Same vendor endpoint
(/v3/option/history/greeks/first_order), same 20-column schema, same shared
infrastructure (lib/chain_fetch_common.py) — the ONE difference is the request
shape:

    snapshots:  TWO point queries per (session, expiration)
                start_time == end_time == 09:45:00, then 15:45:00
                -> 2 bars per contract-day

    intraday:   ONE interval call per (session, expiration)
                start_time=09:35:00, end_time=16:00:00, interval=5m
                -> ~78 bars per contract-day

PURPOSE: this exists to measure whether one wide interval call is cheaper
PER BAR than the point-query approach, and therefore whether a 5-minute store
is feasible across many tickers or only a small subset. The timing summary
(sections A/B/C/D) is the deliverable; the parquet output is a by-product.

09:30 is skipped deliberately — that bar is always zero. The window is
09:35..16:00 inclusive, which at 5m is 78 bars (77 if the endpoint treats
end_time as exclusive).

SCOPE: fetch-and-store only. No metric calculation is repointed, nothing is
wired into cron.

--- Bar identity ------------------------------------------------------------

Unlike snapshots, the time label is not known a priori — each of the ~78 bars
carries its own timestamp. So:

  * `snapshot` is populated per bar as HHMM from that bar's timestamp
    ('0935', '0940', ... '1600'), keeping the schema identical to the
    snapshots store and making the '0945'/'1545' rows directly comparable
    against it for validation.
  * the dedupe/sort key is (trade_date, timestamp, expiration, strike,
    option_type), so all ~78 bars per contract-day are retained rather than
    collapsed.
  * a row whose timestamp will not parse is DROPPED, with a warning. This
    differs from snapshots, which falls back to the requested session — there
    the label is known independently, here it is not, and an unidentifiable
    bar would compare equal to its siblings under drop_duplicates and
    silently collapse a contract's whole day into one row. Failing loudly
    beats losing 77 bars quietly.

--- Volume ------------------------------------------------------------------

~78x the rows per session of the snapshots store. SPY is ~14.3k contracts, so
~1.1M rows per session, roughly 300-500MB in memory before the write. Hence
--batch-days 1 by default: one session per parquet write.

Usage:
    python fetch_chain_intraday.py
    python fetch_chain_intraday.py --tickers SPY,AAPL --start 20260803 --end 20260805
    python fetch_chain_intraday.py --force --debug-response
"""
from __future__ import annotations

import argparse
import logging
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import date, datetime

import numpy as np
import pandas as pd
from tqdm import tqdm

from config import CHAIN_INTRADAY_DIR, CHAIN_SNAPSHOTS_DIR
from lib.chain_fetch_common import (
    TIMING,
    ParquetWriterThread,
    chunk_range,
    preflight_store,
    print_timing_summary,
    prompt_date,
    prompt_tickers,
    set_local_busy,
    start_sampler,
    start_watchdog,
    stop_background_threads,
    to_date_series,
    track,
)
from lib.chain_intraday_store import loaded_dates, write_rows, year_path
from lib.market_hours import get_trading_days, last_trading_day, next_trading_day
from lib.parquet_store import list_tickers as list_oi_tickers
from lib.thetadata import (
    SNAPSHOT_TOTAL_TIMEOUT,
    TerminalServerError,
    TerminalTimeoutError,
    enumerate_expirations_eod,
    fetch_first_order_window,
    max_connections,
    reset_snapshot_timing,
    set_max_connections,
    test_connection,
)

# 09:30 omitted on purpose — that bar is always zero.
INTRADAY_START_TIME = "09:35:00"
INTRADAY_END_TIME   = "16:00:00"
INTRADAY_INTERVAL   = "5m"

# One session per parquet write. At ~78 bars per contract-day a wide chain is
# ~1.1M rows per session; two sessions would be ~1GB resident before the write.
DEFAULT_BATCH_DAYS = 1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_KNOWN_VENDOR_FIELDS = {
    "symbol", "root", "ticker", "date", "timestamp", "expiration", "strike",
    "right", "option_type", "bid", "ask", "delta", "theta", "vega", "rho",
    "epsilon", "lambda", "implied_vol", "iv", "iv_error",
    "underlying_timestamp", "underlying_price",
}
_EXPECTED_VENDOR_FIELDS = {
    "bid", "ask", "delta", "theta", "vega", "rho", "epsilon", "lambda",
    "implied_vol", "iv_error", "underlying_timestamp", "underlying_price",
}
_UNKNOWN_WARNED: set[str] = set()
_MISSING_WARNED: set[str] = set()


# --- Projection ------------------------------------------------------------

def _warn_unknown_fields(raw: pd.DataFrame) -> None:
    cols = set(raw.columns)
    unknown = cols - _KNOWN_VENDOR_FIELDS - _UNKNOWN_WARNED
    if unknown:
        _UNKNOWN_WARNED.update(unknown)
        log.warning("first_order response carries field(s) not in the stored "
                    "schema: %s — they are NOT being stored.",
                    ", ".join(sorted(unknown)))
    missing = _EXPECTED_VENDOR_FIELDS - cols - _MISSING_WARNED
    if missing:
        _MISSING_WARNED.update(missing)
        log.warning("first_order response is missing expected field(s): %s — "
                    "storing as NULL.", ", ".join(sorted(missing)))


def _project(raw: pd.DataFrame, ticker: str, session: date) -> pd.DataFrame:
    """Project a full-day 5-minute response into the stored schema.

    Field mapping is identical to fetch_chain_snapshots._project (16 vendor
    fields 1:1, right -> option_type 'C'/'P', plus ticker / snapshot /
    feature_date / trade_date derived). The difference is that `snapshot` and
    `trade_date` both come from each bar's own timestamp rather than from the
    caller's fixed label.
    """
    if raw.empty:
        return raw

    _warn_unknown_fields(raw)

    if "expiration" not in raw.columns:
        log.warning("  %s %s: response has no 'expiration' field — dropping "
                    "%d rows", ticker, session, len(raw))
        return pd.DataFrame()
    if "timestamp" not in raw.columns:
        log.error("  %s %s: response has no 'timestamp' field — cannot "
                  "identify bars, dropping %d rows", ticker, session, len(raw))
        return pd.DataFrame()

    ts = pd.to_datetime(raw["timestamp"], errors="coerce")

    # A bar with an unparseable timestamp cannot be identified. Dropping is
    # deliberate: keeping it would let drop_duplicates collapse a contract's
    # entire day, since the key would match across every such row.
    n_bad = int(ts.isna().sum())
    if n_bad:
        log.warning("  %s %s: %d/%d rows had an unparseable timestamp — "
                    "DROPPED (cannot identify which bar they are)",
                    ticker, session, n_bad, len(raw))

    # --- factorize-and-take -------------------------------------------------
    # A session's response has ~1.1M rows but only ~78 distinct timestamps and
    # ~40 distinct expirations. Every derived column below used to be computed
    # per ROW in Python: ts.dt.strftime('%H%M') alone measured 4.8s per 2M rows
    # (~49s at a 20M-row session) and was 65% of projection, because strftime
    # over datetime64 is a Python-level loop.
    #
    # factorize gives integer codes plus the small unique set; the Python work
    # then runs 78 times instead of 1.1M, and a numpy take fans it back out.
    # Measured 6.1x on the whole function, with byte-identical output.
    #
    # This is also why moving projection onto pool threads does NOT fix it on
    # its own: those per-row loops hold the GIL, so four workers projecting
    # measured 0.91x — marginally slower than one. The work has to get cheaper
    # before parallelism can do anything with it.
    codes, uniq = pd.factorize(ts, use_na_sentinel=True)
    have = codes >= 0
    safe = np.where(have, codes, 0)

    hhmm_u = np.array([pd.Timestamp(u).strftime("%H%M") for u in uniq],
                      dtype=object)
    snapshot = pd.Series(np.where(have, hhmm_u[safe], None), index=raw.index)

    # trade_date from the bar's own timestamp; prefer an explicit vendor date
    # column if one ever appears.
    if "date" in raw.columns:
        td = to_date_series(raw["date"]).dt.date
        distinct_days = pd.unique(td.dropna())
        fd_map = {d: next_trading_day(d) for d in distinct_days}
        fd = td.map(fd_map)
    else:
        day_u = np.array([pd.Timestamp(u).date() for u in uniq], dtype=object)
        td = pd.Series(np.where(have, day_u[safe], None), index=raw.index)
        # next_trading_day is pandas_market_calendars and slow; call it once
        # per distinct DAY (typically one), not once per distinct bar.
        fd_map = {d: next_trading_day(d) for d in pd.unique(day_u)}
        fd_u = np.array([fd_map[d] for d in day_u], dtype=object)
        fd = pd.Series(np.where(have, fd_u[safe], None), index=raw.index)

    src_otype = raw.get("option_type")
    if src_otype is None:
        src_otype = raw.get("right")
    if src_otype is None:
        log.warning("  %s %s: response has neither 'right' nor 'option_type' "
                    "— dropping %d rows", ticker, session, len(raw))
        return pd.DataFrame()
    # Two distinct values; np.where beats a per-row lambda by ~10x.
    r_up = src_otype.astype("string").str.strip().str.upper()
    otype = pd.Series(
        np.where(r_up.isin(("C", "CALL")), "C",
                 np.where(r_up.isin(("P", "PUT")), "P", None)),
        index=raw.index)

    e_codes, e_uniq = pd.factorize(raw["expiration"], use_na_sentinel=True)
    e_have = e_codes >= 0
    e_safe = np.where(e_have, e_codes, 0)
    e_parsed = to_date_series(pd.Series(e_uniq))
    exp_u = np.array([d.date() if pd.notna(d) else None for d in e_parsed],
                     dtype=object)
    expiration = pd.Series(np.where(e_have, exp_u[e_safe], None),
                           index=raw.index)

    out = pd.DataFrame({
        "ticker":               ticker.upper(),
        "trade_date":           td,
        # HHMM from each bar's own timestamp: '0935', '0940', ... '1600'.
        "snapshot":             snapshot,
        "feature_date":         fd,
        "timestamp":            ts,
        "expiration":           expiration,
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

    # timestamp joins the required set here — it IS the bar's identity.
    return out.dropna(subset=["trade_date", "timestamp", "expiration",
                              "strike", "option_type"])


# --- Per-ticker fetch ------------------------------------------------------

def fetch_ticker(ticker: str, batches: list[tuple[date, date]],
                 writer: ParquetWriterThread,
                 force: bool = False, debug_response: bool = False) -> int:
    """Enumerate + fetch every session in every batch for one ticker.

    Identical control flow to fetch_chain_snapshots.fetch_ticker — per-session
    enumeration and fetching pipelined through ONE 4-slot pool with no barrier
    — except each expiration costs a single interval call instead of two point
    queries.
    """
    years = {y for (a, b) in batches for y in range(a.year, b.year + 1)}
    t_lk = time.monotonic()
    set_local_busy(True)
    already = set() if force else loaded_dates(ticker, years)
    set_local_busy(False)
    TIMING.loaded_keys += time.monotonic() - t_lk

    fetched = 0
    for w_start, w_end in batches:
        sessions = get_trading_days(w_start, w_end)
        if not sessions:
            continue

        todo = [d for d in sessions if d not in already]
        if not todo:
            log.info("  %s %s..%s: all %d sessions loaded — skip",
                     ticker, w_start, w_end, len(sessions))
            continue

        exp_by_session: dict[date, list[date]] = {}
        enum_failures: list[tuple[date, str]] = []
        frames: list[pd.DataFrame] = []
        failures: list[tuple[date, date, str]] = []
        n_ok = n_empty = n_projected_away = 0
        raw_rows = proj_rows = 0
        n_queries = 0
        first_raw_cols: list[str] | None = None
        busy_seconds = 0.0

        def _enum_one(sess: date) -> tuple[float, set[date]]:
            t0 = time.monotonic()
            with track(f"enum {ticker} {sess}"):
                out = enumerate_expirations_eod(ticker, sess, sess)
            return time.monotonic() - t0, out

        def _fetch_one(sess: date, exp: date):
            """Fetch AND project, on the pool thread.

            Worth less here than the vectorisation above — projection is
            largely GIL-bound, so four workers projecting measured 0.91x on
            the pre-vectorised code. It is still right: the vectorised version
            spends more of its time in numpy, which does release the GIL, and
            it takes the work off the thread that is also collecting futures.

            Returns (elapsed, raw_rows, shape, projected); shape carries the
            first response's columns out so the main thread can report them
            without holding the raw frame alive.
            """
            t0 = time.monotonic()
            with track(f"{ticker} exp={exp} {sess} full-day"):
                raw = fetch_first_order_window(
                    ticker, exp, sess,
                    INTRADAY_START_TIME, INTRADAY_END_TIME, INTRADAY_INTERVAL,
                )
            elapsed = time.monotonic() - t0
            if raw.empty:
                return elapsed, 0, None, None
            shape = None
            if debug_response:
                shape = (list(raw.columns), raw.iloc[0].to_dict(),
                         raw["timestamp"].nunique()
                         if "timestamp" in raw.columns else None)
            else:
                shape = (list(raw.columns), None, None)
            return elapsed, len(raw), shape, _project(raw, ticker, sess)

        batch_t0 = time.monotonic()
        # max_connections() is CALLED, not imported as a value. Importing
        # SNAPSHOT_MAX_CONNECTIONS bound the module default (4) at import time,
        # so set_max_connections could rebuild the semaphore to 8 and this pool
        # would still be 4 — the run would sit at 4 and --connections would
        # look like it did nothing.
        with ThreadPoolExecutor(max_workers=max_connections()) as pool:
            pending: dict = {
                pool.submit(_enum_one, d): ("enum", d, None) for d in todo
            }
            while pending:
                t_wait = time.monotonic()
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                TIMING.fanout_blocked += time.monotonic() - t_wait

                t_local = time.monotonic()
                set_local_busy(True)
                for fut in done:
                    kind, sess, exp = pending.pop(fut)

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
                        exps = sorted(e for e in raw_exps if e >= sess)
                        if not exps:
                            continue
                        exp_by_session[sess] = exps
                        for e in exps:
                            n_queries += 1
                            nf = pool.submit(_fetch_one, sess, e)
                            pending[nf] = ("fetch", sess, e)
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
                        # `projected` alone: a disagreement with n_raw would
                        # otherwise reach len(projected) below, which is past
                        # the except blocks and would take the whole ticker
                        # down instead of one unit.
                        if projected is None:
                            n_empty += 1
                            continue
                        raw_rows += n_raw
                        if first_raw_cols is None and shape is not None:
                            first_raw_cols, first_row, n_distinct_ts = shape
                            if debug_response:
                                log.info("  DEBUG first non-empty response "
                                         "(%s exp=%s %s): %d rows, columns=%s",
                                         ticker, exp, sess, n_raw,
                                         first_raw_cols)
                                log.info("  DEBUG first row: %s", first_row)
                                if n_distinct_ts is not None:
                                    log.info("  DEBUG distinct timestamps in "
                                             "this response: %d", n_distinct_ts)
                    except (TerminalTimeoutError, TerminalServerError) as exc:
                        failures.append((sess, exp, f"{type(exc).__name__}: {exc}"))
                        continue
                    except Exception as exc:
                        failures.append((sess, exp, f"{type(exc).__name__}: {exc}"))
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
        log.info("  %s %s..%s: %d sessions, %d interval calls, %d requests in "
                 "%.1fs | measured concurrency %.2f of %d (%.2fs avg/request)",
                 ticker, w_start, w_end, len(exp_by_session), n_queries,
                 n_requests, batch_wall,
                 (busy_seconds / batch_wall) if batch_wall > 0 else 0.0,
                 max_connections(),
                 (busy_seconds / n_requests) if n_requests else 0.0)

        if enum_failures:
            log.warning("  %s %s..%s: enumeration FAILED for %d session(s) — "
                        "not written, rerun to retry. First few: %s",
                        ticker, w_start, w_end, len(enum_failures),
                        "; ".join(f"{d}: {m[:60]}" for d, m in enum_failures[:3]))

        if failures:
            log.warning("  %s %s..%s: %d/%d interval calls FAILED — those "
                        "(session, expiration) days are missing. Rerun with "
                        "--force to retry. First few: %s",
                        ticker, w_start, w_end, len(failures), n_queries,
                        "; ".join(f"{d}/{e}: {m[:60]}" for d, e, m in failures[:3]))

        # Bars-per-call is the headline number for the experiment.
        bars_per_call = (raw_rows / n_ok) if n_ok else 0.0
        log.info("  %s %s..%s: %d calls -> %d with rows, %d empty, "
                 "%d projected-away, %d failed | %d raw rows -> %d projected "
                 "| %.0f rows/call",
                 ticker, w_start, w_end, n_queries, n_ok, n_empty,
                 n_projected_away, len(failures), raw_rows, proj_rows,
                 bars_per_call)

        if raw_rows > 0 and proj_rows == 0:
            log.error("  %s %s..%s: %d raw rows returned but ALL were dropped "
                      "in projection. Vendor columns were: %s",
                      ticker, w_start, w_end, raw_rows, first_raw_cols)

        if not frames:
            log.warning("  %s %s..%s: NOTHING WRITTEN — no rows survived "
                        "(see the counts above)", ticker, w_start, w_end)
            continue

        combined = pd.concat(frames, ignore_index=True)
        # Hand off and move on: the next session's enumeration and interval
        # calls start immediately while this frame is merged and rewritten.
        # Blocks only if the writer is still busy, recorded as writer_wait.
        writer.submit(ticker, combined, f"{ticker} {w_start}..{w_end}")
        fetched += len(combined)

    return fetched


# --- Main ------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fetch full-day 5-minute intraday option chain bars."
    )
    ap.add_argument("--force", action="store_true",
                    help="refetch sessions already in the store")
    ap.add_argument("--batch-days", type=int, default=DEFAULT_BATCH_DAYS,
                    help=("calendar days accumulated per parquet write "
                          f"(default {DEFAULT_BATCH_DAYS}). Does not affect "
                          "request size — every request is one session."))
    ap.add_argument("--write-queue", type=int, default=1,
                    help=("parquet batches that may sit queued for the writer "
                          "thread (default 1). Lower than the snapshots "
                          "default because a batch here is a whole session of "
                          "5-minute bars — hundreds of MB per frame."))
    ap.add_argument("--connections", type=int, default=None,
                    help=("concurrent ThetaData connections (default 4). "
                          "Vendor guidance: this should MATCH the Theta "
                          "Terminal's HTTP_CONCURRENCY setting — exceeding it "
                          "is documented to cause timeouts rather than clean "
                          "rejections. Note this also raises peak memory: "
                          "each in-flight response is a full session of "
                          "5-minute bars for one expiration."))
    ap.add_argument("--debug-response", action="store_true",
                    help="dump columns, first row and distinct-timestamp count "
                         "of the first non-empty response per batch")
    ap.add_argument("--tickers", help="comma-separated; skips the prompt")
    ap.add_argument("--start", help="YYYYMMDD; skips the prompt")
    ap.add_argument("--end", help="YYYYMMDD; skips the prompt")
    args = ap.parse_args()

    # Must happen before any request is in flight — it rebuilds the semaphore.
    if args.connections is not None:
        set_max_connections(args.connections)

    print("=== Open_Interest — intraday 5-minute chain bars "
          f"({INTRADAY_START_TIME[:5]}–{INTRADAY_END_TIME[:5]}) ===\n")

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

    run_t0 = time.monotonic()
    reset_snapshot_timing()
    start_sampler()

    print(f"{len(tickers)} tickers x {len(sessions)} sessions "
          f"({start} -> {end})")
    print(f"{len(batches)} write batch(es) of <= {args.batch_days} calendar "
          f"day(s), {max_connections()} concurrent connections"
          f"{', --force' if args.force else ''}")
    print(f"Request shape: ONE interval call per (session, expiration), "
          f"{INTRADAY_START_TIME}..{INTRADAY_END_TIME} @ {INTRADAY_INTERVAL} "
          f"(~78 bars/contract-day)\n")

    preflight_store(CHAIN_INTRADAY_DIR, CHAIN_SNAPSHOTS_DIR)

    print("Checking ThetaData ...", end=" ", flush=True)
    if not test_connection():
        raise SystemExit("FAILED — terminal not reachable.")
    print("OK")
    print(f"Per-request caps: connect 10s, read 45s, hard total "
          f"{SNAPSHOT_TOTAL_TIMEOUT}s. Watchdog reports stalls every 30s.\n")

    TIMING.startup = time.monotonic() - run_t0
    start_watchdog()

    # maxsize defaults to 1 here, not 2 as in snapshots: a batch frame is a
    # whole session of 5-minute bars (~1.1M rows for a wide chain), so each
    # queued frame is hundreds of MB. One in flight plus one being written is
    # already the memory that --batch-days 1 exists to bound.
    writer = ParquetWriterThread(write_rows, year_path, CHAIN_INTRADAY_DIR,
                                 maxsize=args.write_queue)
    writer.start()

    total = 0
    failed_tickers: list[str] = []
    with tqdm(total=len(tickers), unit="tk", ncols=90, desc="intraday") as bar:
        for t in tickers:
            try:
                total += fetch_ticker(t, batches, writer, force=args.force,
                                      debug_response=args.debug_response)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                log.error("  FAIL %s: %s", t, exc, exc_info=True)
                failed_tickers.append(t)
            bar.update(1)

    # Drain BEFORE the summary and the row count: queued batches are fetched
    # but not yet on disk, and a run reporting success with its tail still in
    # memory would be lying where it matters most. close() also re-raises a
    # writer failure the loop never saw.
    print("\nFlushing pending parquet writes ...", end=" ", flush=True)
    flush_t0 = time.monotonic()
    writer.close()
    print(f"OK ({time.monotonic() - flush_t0:.1f}s)")

    stop_background_threads()

    print(f"\n{total:,} rows fetched and merged into {CHAIN_INTRADAY_DIR}")
    print("Fetch-and-store only — no metrics repointed, no cron wired.")

    if failed_tickers:
        print(f"\n{len(failed_tickers)} ticker(s) FAILED: "
              f"{', '.join(failed_tickers[:10])}"
              f"{' ...' if len(failed_tickers) > 10 else ''}")

    print_timing_summary(time.monotonic() - run_t0,
                         query_label="interval calls")

    # Headline for the experiment: cost per stored bar, comparable directly
    # against the snapshots run's own per-bar figure.
    if TIMING.query_count and total:
        print(f"\nEXPERIMENT: {TIMING.query_count:,} interval calls -> "
              f"{total:,} stored bars")
        print(f"  {total / TIMING.query_count:>10.1f} bars per call")
        print(f"  {TIMING.query_secs / total * 1000:>10.3f} ms of request time "
              f"per stored bar")
        print("  Compare against the snapshots run: 2 calls per "
              "(session, expiration) for 2 bars.")

    attempted = TIMING.enum_count + TIMING.query_count
    if total == 0 and attempted == 0 and not failed_tickers:
        print("\nNothing to do — every session was already loaded. "
              "Use --force to refetch.")
        return
    if total == 0:
        raise SystemExit(
            "\nFAILED: no rows were stored.\n"
            f"Store dir: {CHAIN_INTRADAY_DIR}\n"
            "Check the per-batch counter lines above."
        )
    if failed_tickers:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
