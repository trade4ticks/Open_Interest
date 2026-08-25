"""
fetch_live_surface.py — live intraday surface, every 5 minutes.

Captures the current chain for every ticker, fits the surface and writes to
Postgres so a dashboard can read current skew across the universe during the
session.

The raw frame is ALSO archived to parquet (lib/chain_live_store.py), one file
per capture cycle, written pre-clean_chain. Not for the dashboard — for
re-fits. Three fixes to the fit are queued and each needs re-surfacing;
refetching a 5-minute session per-expiration is ~4,100 requests against a 2-3
connection budget, hours per ticker-day. Without the archive a fit change
simply cannot be applied to live-captured data. Postgres remains the primary
output: an archive failure is logged and the cycle continues.

    /v3/option/snapshot/greeks/first_order  with  expiration=*

One request per ticker for the whole chain. The wildcard works on the SNAPSHOT
variant and is rejected by the HISTORY one, which is why fetch_chain_intraday
enumerates expirations — that script is unchanged and remains the historical
backfill path.

--- Why this is a separate script -----------------------------------------

fetch_chain_intraday finalizes one parquet per ticker-session and nothing can
read that file while it runs, so it cannot serve a live path. This script
writes one COMPLETE file per cycle instead, into a separate store — so a
capture is readable the moment it lands, and the two writers never share a
directory. See lib/chain_live_store.py for why that separation matters.

--- Why fetching happens in the PARENT ------------------------------------

The connection cap in lib/thetadata is a threading.BoundedSemaphore, which
bounds one PROCESS. Five worker processes each fetching would hold five
semaphores and draw up to five connections against a pool of three that is
shared with the portfolio dashboard and other crons.

So the split is not arbitrary: the parent fetches on a 3-thread pool, workers
only clean and fit. Fetch and fit overlap, and the connection cap is enforced
where it can actually be enforced.

--- Grid bucket vs capture time -------------------------------------------

A capture lands at 13:47:30, not 13:45:00. Both are recorded:

    snapshot     the 5-minute grid bucket, '1345'. Stays part of the primary
                 key, so the dashboard and cross-ticker joins are unaffected
                 and a later exact rebuild upserts over the live row.
    captured_at  the true instant.
    source       'live' here; 'exact' when rebuilt from the historical record.

Usage:
    python fetch_live_surface.py                  # one cycle, guarded
    python fetch_live_surface.py --force          # ignore the market window
    python fetch_live_surface.py --tickers SPY,AAPL --workers 2

Cron (every 5 minutes during the session; flock stops a slow cycle overlapping
the next, which would double the connection draw):

    TZ=America/New_York */5 9-16 * * 1-5 flock -n /tmp/live_surface.lock \\
        /Open_Interest/.venv/bin/python /Open_Interest/fetch_live_surface.py \\
        >> /Open_Interest/logs/live_surface.log 2>&1
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from concurrent.futures import (FIRST_COMPLETED, ProcessPoolExecutor,
                                ThreadPoolExecutor, as_completed, wait)
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path

import pandas as pd

from lib.chain_fetch_common import (close_file_logging, log_path,
                                    setup_file_logging)
from lib.market_hours import ET, get_trading_days, session_bounds

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("live_surface")

# The 5-minute grid the buckets snap to. 09:30 is excluded deliberately — the
# opening print is unreliable, matching fetch_chain_intraday's window.
GRID_START = dtime(9, 35)
GRID_MINUTES = 5

# Fetch concurrency. Hard cap: the ThetaData pool is shared with the portfolio
# dashboard and other crons.
DEFAULT_CONNECTIONS = 3
# Fit concurrency. Below core count on purpose — Postgres runs in Docker on
# the same 8 cores and the fit is the only real CPU consumer.
DEFAULT_WORKERS = 5
# Raw frames allowed in flight. Fetch (~25s for the universe) outruns fit
# (~51s), so without a bound every frame would be resident at once: 121 x
# ~20MB is 2.4GB against a box that has OOM-killed at 3.2GB.
DEFAULT_MAX_INFLIGHT = 12

# Progress cadence. Frequent enough that a stall names the ticker it stopped
# near, sparse enough not to write 121 lines every five minutes.
PROGRESS_EVERY = 20

SOURCE_LIVE = "live"
LOCK_PATH = Path(os.environ.get("LIVE_SURFACE_LOCK", "/tmp/live_surface.lock"))


# --- Window -----------------------------------------------------------------

def flush_log() -> None:
    """Push buffered log records to disk mid-cycle.

    The run log was completely empty after six minutes of the deadlocked run,
    so the failure left no evidence and needed py-spy to find. Handlers are
    flushed explicitly rather than trusted to line-buffer, because a stall that
    produces no output is the one case where the log matters most.
    """
    for h in logging.getLogger().handlers:
        try:
            h.flush()
        except Exception:                                     # noqa: BLE001
            pass
    close_file_logging()


def market_window(now_et: datetime | None = None) -> tuple:
    """(open_now, reason). Session close comes from the exchange calendar, so
    a 13:00 half-day stops at 13:00 rather than running to a hardcoded 16:00."""
    now_et = now_et or datetime.now(ET).replace(tzinfo=None)
    today = now_et.date()
    if not get_trading_days(today, today):
        return False, f"{today} is not an NYSE trading day"
    bounds = session_bounds(today, today).get(today)
    if not bounds:
        return False, f"no session bounds for {today}"
    close = bounds[1]
    start = datetime.combine(today, GRID_START)
    if now_et < start:
        return False, f"before {GRID_START:%H:%M} ET (now {now_et:%H:%M})"
    if now_et > close:
        return False, f"after the {close:%H:%M} close (now {now_et:%H:%M})"
    return True, f"{now_et:%H:%M} ET, session closes {close:%H:%M}"


def grid_bucket(ts: datetime) -> str:
    """The 5-minute slot a capture belongs to. 13:47:30 -> '1345'."""
    return f"{ts.hour:02d}{ts.minute - (ts.minute % GRID_MINUTES):02d}"


# --- Lock -------------------------------------------------------------------

def under_flock(path: Path) -> bool:
    """True when an inherited descriptor already refers to `path`.

    That is the signature of having been launched by flock(1): it opens the
    lock file, takes the lock on THAT open file description, then execs us with
    the descriptor still open and still held for our whole lifetime.

    Detecting it matters because flock(2) locks are per-open-file-description,
    not per-process. A second open() of the same path inside this process is a
    DIFFERENT description, so locking it conflicts with the one our own parent
    holds — and fails every time, from the very first run.
    """
    try:
        target = path.resolve()
        for fd in Path("/proc/self/fd").iterdir():
            if int(fd.name) <= 2:
                continue
            try:
                if fd.resolve() == target:
                    return True
            except OSError:
                continue
    except Exception:                                         # noqa: BLE001
        pass                       # no /proc (Windows dev box) — assume not
    return False


def acquire_lock(path: Path):
    """Take an in-process lock. Only for runs NOT wrapped in flock(1).

    Returns the handle to keep open, False if another holder has it, or None
    when locking is unavailable or unnecessary.

    THE CRON'S flock(1) IS AUTHORITATIVE. This is opt-in via --lock and exists
    only for a manual run outside cron. Two reasons flock(1) is the better
    layer, not merely the incumbent:

      * it covers the ENTIRE process lifetime, including interpreter startup
        and the pandas/scipy/pyarrow imports. Those take a second or more, and
        an in-process lock cannot protect that window — two cron firings could
        both get past exec and into imports before either one locked.
      * it is external to the process, so no exception path inside can drop it.

    Running both is what caused every cycle of 2026-08-24 to be skipped: this
    function's open() created a second file description on the same path, and
    locking it conflicted with the one flock(1) already held on our behalf.
    """
    if under_flock(path):
        log.info("already under flock(1) (%s) — skipping the in-process lock; "
                 "the cron wrapper is the authoritative guard", path)
        return None
    try:
        import fcntl
    except ImportError:
        log.debug("no fcntl on this platform; relying on the cron's flock")
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    # "a", not "w": a failed acquisition must not truncate the file or touch
    # its mtime. Under the old "w" every skipped attempt rewrote the lock file,
    # which is why it read 0 bytes with an mtime tracking the last SKIP rather
    # than the last successful run — forensics that pointed away from the real
    # cause.
    fh = open(path, "a")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()                 # releases nothing we hold; just tidies up
        return False
    # Only now is it ours to write. A held lock should say who holds it.
    try:
        fh.seek(0)
        fh.truncate()
        fh.write(str(os.getpid()) + chr(10))
        fh.flush()
    except OSError:
        pass
    return fh


# --- Worker -----------------------------------------------------------------

def project(raw: pd.DataFrame, ticker: str, trade_date, snapshot: str
            ) -> pd.DataFrame:
    """Vendor snapshot frame -> the 20-column schema clean_chain expects.

    `right` arrives as 'CALL'/'PUT'. clean_chain tolerates both spellings but
    surface_fit does NOT — solve_forward_rate and build_smile_points compare
    against exactly 'C' and 'P', so a passthrough would silently yield zero
    calls, zero puts, and no smile at all. Mapped here.
    """
    if raw.empty:
        return pd.DataFrame()
    right = raw.get("right", raw.get("option_type"))
    otype = (right.astype("string").str.strip().str.upper()
             .map(lambda s: "C" if s in ("C", "CALL")
                  else ("P" if s in ("P", "PUT") else None)))
    out = pd.DataFrame({
        "ticker": ticker,
        "trade_date": trade_date,
        "snapshot": snapshot,
        "feature_date": trade_date,
        "timestamp": pd.to_datetime(raw["timestamp"], errors="coerce"),
        "expiration": pd.to_datetime(raw["expiration"].astype("string"),
                                     errors="coerce").dt.date,
        "strike": pd.to_numeric(raw.get("strike"), errors="coerce"),
        "option_type": otype,
    })
    for c in ("bid", "ask", "delta", "theta", "vega", "rho", "epsilon",
              "lambda", "implied_vol", "iv_error", "underlying_price"):
        out[c] = pd.to_numeric(raw.get(c), errors="coerce")
    out["underlying_timestamp"] = pd.to_datetime(
        raw.get("underlying_timestamp"), errors="coerce")
    return out.dropna(subset=["expiration", "strike", "option_type"])


def fit_one(args) -> dict:
    """Clean and fit one ticker's chain, in a WORKER PROCESS. No database.

    Returns row lists only. build_snapshot also hands back SmileFit objects
    holding scipy splines; pickling ~32 of those per ticker back to the parent
    would cost more than the fit did.
    """
    from lib.clean_chain import clean_chain
    from lib.surface_fit import build_snapshot

    ticker, raw, trade_date, snapshot, captured_at, persist = args
    out = {"ticker": ticker, "snapshot": snapshot, "captured_at": captured_at,
           "trade_date": trade_date, "surface": [], "atm": [],
           "diagnostics": [], "error": None, "n_rows": 0, "n_exp": 0,
           "persist_error": None, "persist_bytes": 0, "persist_secs": 0.0}
    try:
        df = project(raw, ticker, trade_date, snapshot)
        if df.empty:
            out["error"] = "projection produced no rows"
            return out
        out["n_rows"] = len(df)
        out["n_exp"] = int(df["expiration"].nunique())

        # Persist BEFORE clean_chain. Everything clean_chain adds is derived
        # from these columns, so archiving pre-clean keeps a change to the
        # cleaning rules replayable; archiving post-clean would freeze one
        # version of those rules into the store.
        #
        # The Postgres rows are the primary output, so a parquet failure is
        # recorded and returned for the parent to log — never raised. Losing
        # an archive copy must not cost a surface.
        if persist:
            import time as _time
            try:
                from lib.chain_live_store import write_cycle
                _t0 = _time.perf_counter()
                _, nbytes = write_cycle(ticker, trade_date, snapshot, df)
                out["persist_secs"] = _time.perf_counter() - _t0
                out["persist_bytes"] = nbytes
            except Exception as exc:                          # noqa: BLE001
                out["persist_error"] = f"{type(exc).__name__}: {exc}"
        res = build_snapshot(clean_chain(df), ticker, trade_date, snapshot)
        for key in ("surface", "atm", "diagnostics"):
            rows = res[key]
            if key != "diagnostics":
                for r in rows:
                    r["captured_at"] = captured_at
                    r["source"] = SOURCE_LIVE
            out[key] = rows
    except Exception as exc:                                  # noqa: BLE001
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


# --- Cycle ------------------------------------------------------------------

def new_totals(skipped: str = "") -> dict:
    """The totals shape, defined once.

    run_cycle builds one of these and every early exit in run() returns one
    too, so `rc, totals = run(args)` unpacks whatever happened. run() used to
    return a bare int when the market window was closed, which crashed the
    pipeline on every cron firing before 09:35 and after 16:00.

    `skipped` is the reason no capture was attempted, empty when one was.
    """
    return {"tickers": 0, "surface": 0, "atm": 0, "diagnostics": 0,
            "failed": [], "captures": [],
            "persist_bytes": 0, "persist_secs": 0.0, "persist_failed": [],
            # (ticker, trade_date, snapshot) for rows that reached Postgres.
            # The pipeline scopes its metrics pass to exactly these: captures
            # holds every ticker that was FETCHED, including ones whose fit or
            # write later failed, and computing metrics for those would read a
            # surface that is not there.
            "written": [],
            "skipped": skipped}


def run_cycle(conn, tickers: list, connections: int, workers: int,
              max_inflight: int, persist: bool = True) -> dict:
    """One capture pass over the universe."""
    from lib.surface_store import write_snapshot
    from lib.thetadata import fetch_first_order_snapshot, set_max_connections

    set_max_connections(connections)
    totals = new_totals()

    def fetch(tk: str):
        t0 = time.monotonic()
        raw = fetch_first_order_snapshot(tk)
        captured_at = datetime.now(ET).replace(tzinfo=None)
        return tk, raw, captured_at, time.monotonic() - t0

    # Bounds raw frames resident at once; released as each fit result drains.
    import threading
    slots = threading.Semaphore(max_inflight)

    def fetch_guarded(tk: str):
        slots.acquire()
        try:
            return fetch(tk)
        except Exception:
            slots.release()
            raise

    # ONE loop over both kinds of future, not two sequential as_completed
    # passes. The permit a fetch takes is released when its FIT completes, so
    # draining fits in a second loop that only starts after every fetch has
    # been consumed is a guaranteed deadlock: fetch number max_inflight + 1
    # blocks in slots.acquire() waiting for a release that the not-yet-running
    # second loop is the only thing able to perform. It stalled at ticker 12-14
    # with the default of 12, and any run shorter than the bound completed
    # fine, which is why 3- and 8-ticker tests never showed it.
    #
    # Interleaving means a fit result drains, and its permit frees, while
    # fetches are still queued.
    with ProcessPoolExecutor(max_workers=workers) as fitpool,          ThreadPoolExecutor(max_workers=connections) as fetchpool:
        pending = {fetchpool.submit(fetch_guarded, tk): ("fetch", tk)
                   for tk in tickers}
        n_fetched = n_fitted = 0
        n_total = len(tickers)

        while pending:
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for fut in done:
                kind, tk = pending.pop(fut)

                if kind == "fetch":
                    n_fetched += 1
                    try:
                        tk, raw, captured_at, secs = fut.result()
                    except Exception as exc:                  # noqa: BLE001
                        # fetch_guarded released its own permit before
                        # re-raising, so the parent must NOT release here —
                        # doing so would over-count the semaphore and let the
                        # resident-frame bound drift upward over a run.
                        log.warning("  %s: fetch FAILED — %s: %s", tk,
                                    type(exc).__name__, exc)
                        totals["failed"].append(f"{tk} (fetch)")
                        continue
                    if raw is None or raw.empty:
                        log.warning("  %s: empty chain", tk)
                        totals["failed"].append(f"{tk} (empty)")
                        slots.release()
                        continue

                    # trade_date from the VENDOR's own quote stamp, not the
                    # wall clock. Outside the session the snapshot endpoint
                    # returns the previous close, and stamping that with
                    # today's date would file stale quotes under a session that
                    # has not happened.
                    vend = pd.to_datetime(raw["timestamp"],
                                          errors="coerce").max()
                    trade_date = (vend.date() if pd.notna(vend)
                                  else captured_at.date())
                    if trade_date != captured_at.date():
                        log.warning("  %s: STALE — vendor stamp is %s but "
                                    "today is %s. Quotes are from a previous "
                                    "session; the bucket label is wall-clock "
                                    "and will not match.",
                                    tk, trade_date, captured_at.date())
                    snapshot = grid_bucket(captured_at)
                    totals["captures"].append((tk, captured_at, secs,
                                               trade_date))

                    f = fitpool.submit(fit_one,
                                       (tk, raw, trade_date, snapshot,
                                        captured_at, persist))
                    pending[f] = ("fit", tk)
                    del raw    # drop the parent's reference; pickle is queued
                    continue

                # --- fit completion: this is where the permit comes back ----
                n_fitted += 1
                slots.release()
                try:
                    got = fut.result()
                except Exception as exc:                      # noqa: BLE001
                    log.error("  %s: worker died — %s: %s", tk,
                              type(exc).__name__, exc)
                    totals["failed"].append(f"{tk} (worker)")
                    continue

                # Archive accounting BEFORE the fit check: a cycle whose fit
                # failed may still have persisted its raw frame, and that copy
                # is exactly what a later re-fit needs.
                totals["persist_bytes"] += got.get("persist_bytes", 0)
                totals["persist_secs"] += got.get("persist_secs", 0.0)
                if got.get("persist_error"):
                    # Logged, never fatal — Postgres is the primary output.
                    log.warning("  %s: parquet archive FAILED — %s", tk,
                                got["persist_error"])
                    totals["persist_failed"].append(tk)

                if got["error"]:
                    log.warning("  %s: %s", tk, got["error"])
                    totals["failed"].append(f"{tk} (fit)")
                    continue
                try:
                    written = write_snapshot(conn, got, got["trade_date"])
                except Exception as exc:                      # noqa: BLE001
                    conn.rollback()
                    log.error("  %s: WRITE FAILED — %s: %s", tk,
                              type(exc).__name__, exc)
                    totals["failed"].append(f"{tk} (write)")
                    continue
                for k in ("surface", "atm", "diagnostics"):
                    totals[k] += written[k]
                totals["tickers"] += 1
                totals["written"].append((tk, got["trade_date"],
                                          got["snapshot"]))

            # Progress, flushed as it happens. The log was empty after six
            # minutes of the deadlocked run because the parent logged nothing
            # until its result loop — which never ran — so a hang produced no
            # evidence at all and needed py-spy to see. A periodic line means
            # the next stall shows where it stopped and how far it got.
            if n_fetched and (n_fetched % PROGRESS_EVERY == 0
                              or not pending):
                log.info("  progress: fetched %d/%d, fitted %d/%d, "
                         "%d future(s) outstanding",
                         n_fetched, n_total, n_fitted, n_total, len(pending))
                flush_log()
    return totals


# --- Main -------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Live intraday surface capture.")
    ap.add_argument("--tickers", help="comma-separated; default = all in the "
                                      "snapshots store")
    ap.add_argument("--connections", type=int, default=DEFAULT_CONNECTIONS,
                    help=f"ThetaData connections (default "
                         f"{DEFAULT_CONNECTIONS}). The pool is SHARED with the "
                         f"portfolio dashboard and other crons — raising this "
                         f"takes connections from them.")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                    help=f"fit processes (default {DEFAULT_WORKERS}, below "
                         f"core count because Postgres shares the cores)")
    ap.add_argument("--max-inflight", type=int, default=DEFAULT_MAX_INFLIGHT,
                    help="raw frames resident at once (memory bound)")
    ap.add_argument("--no-persist", action="store_true",
                    help=("skip the parquet archive. The Postgres rows are "
                          "unaffected; only the ability to re-fit this cycle "
                          "later is given up."))
    ap.add_argument("--force", action="store_true",
                    help="run outside the market window. The snapshot endpoint "
                         "returns the PREVIOUS CLOSE when the market is shut, "
                         "so this captures stale quotes — trade_date is taken "
                         "from the vendor stamp so they are at least filed "
                         "under the right session.")
    # Default OFF. The cron wraps this in flock(1), which is the authoritative
    # guard; taking a second lock inside the process conflicts with it and
    # skips every cycle. A bare manual run therefore just runs, which is what
    # you want at 5-minute resolution when a missed cycle is unrecoverable.
    ap.add_argument("--lock", action="store_true",
                    help=("take an in-process lock. NOT needed under the cron, "
                          "which already wraps the command in flock(1). Use "
                          "only for a manual run that must not overlap "
                          "another."))
    ap.add_argument("--no-lock", action="store_true",
                    help=argparse.SUPPRESS)   # deprecated no-op; kept so an
                                              # existing manual command or
                                              # cron entry does not break
    return ap


def run(args) -> tuple:
    """One capture cycle. Returns (exit_code, totals).

    Split from main() so run_live_pipeline.py can drive a capture and
    then read totals["written"] — the (ticker, trade_date, snapshot)
    triples that actually reached Postgres. A pipeline cannot derive
    the bucket from the wall clock instead: a 72s capture can straddle
    a 5-minute boundary, and the label the rows were written under is
    the only correct scope for the metrics pass.
    """

    log_file = setup_file_logging("fetch_live_surface")
    now_et = datetime.now(ET).replace(tzinfo=None)
    open_now, reason = market_window(now_et)
    if not open_now and not args.force:
        log.info("outside the market window — %s", reason)
        return 0, new_totals(skipped=f"outside the market window — {reason}")

    lock = None
    if args.no_lock:
        log.info("--no-lock is now the default and has no effect; the "
                 "in-process lock is opt-in via --lock")
    if args.lock and not args.no_lock:
        lock = acquire_lock(LOCK_PATH)
        if lock is False:
            log.warning("previous cycle still running (%s held) — skipping. "
                        "A cycle is never retried: 'now' has moved.", LOCK_PATH)
            return 0, new_totals(skipped="previous cycle still running")

    print("=== live intraday surface ===")
    print(f"Log: {log_file}")
    print(f"window: {reason}{'  (FORCED)' if not open_now else ''}")

    from db import get_connection
    tickers = ([t.strip().upper() for t in args.tickers.split(",") if t.strip()]
               if args.tickers else None)
    if tickers is None:
        from build_equity_surface import list_tickers
        from lib.surface_config import SOURCE_SNAPSHOTS
        tickers = list_tickers(SOURCE_SNAPSHOTS)
    if not tickers:
        raise SystemExit("no tickers — pass --tickers")

    workers = max(1, min(args.workers, len(tickers)))
    print(f"{len(tickers)} ticker(s), {args.connections} connection(s), "
          f"{workers} worker(s), grid bucket {grid_bucket(now_et)}\n")

    t0 = time.monotonic()
    with get_connection() as conn:
        totals = run_cycle(conn, tickers, args.connections, workers,
                           args.max_inflight, persist=not args.no_persist)
    wall = time.monotonic() - t0

    caps = totals["captures"]
    print(f"\n{totals['tickers']}/{len(tickers)} ticker(s) in {wall:.0f}s")
    print(f"  equity_surface  {totals['surface']:>8,} rows")
    print(f"  equity_atm      {totals['atm']:>8,} rows")
    if not args.no_persist:
        mb = totals["persist_bytes"] / 1e6
        # persist_secs is summed ACROSS worker processes, so it overlaps the
        # fit rather than adding to the cycle. Reported as the serial total
        # with the per-cycle share the parent actually waited for.
        print(f"  parquet archive {mb:>8.1f} MB  "
              f"({totals['persist_secs']:.1f}s across {workers} worker(s) "
              f"~= {totals['persist_secs'] / max(workers, 1):.1f}s of wall)")
        if totals["persist_failed"]:
            print(f"  archive FAILED  {len(totals['persist_failed'])} ticker(s): "
                  f"{', '.join(totals['persist_failed'][:8])}")
    if caps:
        stamps = sorted(c[1] for c in caps)
        spread = (stamps[-1] - stamps[0]).total_seconds()
        buckets = sorted({grid_bucket(c[1]) for c in caps})
        # A widening spread is the first sign the cycle is falling behind, and
        # a cycle straddling two buckets means captures landed under different
        # grid labels — the point at which the 5-minute grid stops holding.
        print(f"  captured_at     {stamps[0]:%H:%M:%S} .. {stamps[-1]:%H:%M:%S}"
              f"   spread {spread:.0f}s")
        log.info("capture spread %.0fs across %d ticker(s), bucket(s): %s",
                 spread, len(caps), ", ".join(buckets))
        if len(buckets) > 1:
            log.warning("cycle straddled %d grid buckets (%s) — captures "
                        "landed under different labels", len(buckets),
                        ", ".join(buckets))
        if spread > 60:
            log.warning("capture spread %.0fs is wide; cross-ticker "
                        "comparisons within this bucket are that misaligned",
                        spread)
    if wall > 240:
        log.warning("cycle took %.0fs against a 300s window — close to "
                    "overlapping the next", wall)
    if totals["failed"]:
        print(f"\n  {len(totals['failed'])} failure(s): "
              f"{', '.join(totals['failed'][:10])}"
              f"{' ...' if len(totals['failed']) > 10 else ''}")
        print("  Not retried — a retry captures a different instant under the "
              "old label.")
    print(f"\nLog: {log_path()}")
    if lock:
        lock.close()
    return (0 if totals["tickers"] else 1), totals


def main() -> int:
    rc, _ = run(build_parser().parse_args())
    return rc


if __name__ == "__main__":
    sys.exit(main())
