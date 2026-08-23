"""
fetch_live_surface.py — live intraday surface, every 5 minutes.

Captures the current chain for every ticker, fits the surface and writes to
Postgres so a dashboard can read current skew across the universe during the
session. Writes NOTHING to disk: fetch, clean, fit, write, discard.

    /v3/option/snapshot/greeks/first_order  with  expiration=*

One request per ticker for the whole chain. The wildcard works on the SNAPSHOT
variant and is rejected by the HISTORY one, which is why fetch_chain_intraday
enumerates expirations — that script is unchanged and remains the historical
backfill path.

--- Why this is a separate script -----------------------------------------

fetch_chain_intraday finalizes one parquet per ticker-session and nothing can
read that file while it runs, so it cannot serve a live path. Going through
disk is what creates finalization, partial-session semantics and the
fetcher/reader handoff. None of that exists here because nothing is written.

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

from lib.chain_fetch_common import log_path, setup_file_logging
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

SOURCE_LIVE = "live"
LOCK_PATH = Path(os.environ.get("LIVE_SURFACE_LOCK", "/tmp/live_surface.lock"))


# --- Window -----------------------------------------------------------------

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

def acquire_lock(path: Path):
    """Refuse to start if a previous cycle is still running.

    Belt and braces with the cron's flock: a cycle that overruns 5 minutes
    would otherwise double the connection draw against a shared pool. Returns
    the handle to keep open, or None if the platform has no flock (Windows dev
    box) — the VPS is Linux, where this is the real guard.
    """
    try:
        import fcntl
    except ImportError:
        log.debug("no fcntl on this platform; relying on the cron's flock")
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(path, "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        return False
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

    ticker, raw, trade_date, snapshot, captured_at = args
    out = {"ticker": ticker, "snapshot": snapshot, "captured_at": captured_at,
           "trade_date": trade_date, "surface": [], "atm": [],
           "diagnostics": [], "error": None, "n_rows": 0, "n_exp": 0}
    try:
        df = project(raw, ticker, trade_date, snapshot)
        if df.empty:
            out["error"] = "projection produced no rows"
            return out
        out["n_rows"] = len(df)
        out["n_exp"] = int(df["expiration"].nunique())
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

def run_cycle(conn, tickers: list, connections: int, workers: int,
              max_inflight: int) -> dict:
    """One capture pass over the universe."""
    from lib.surface_store import write_snapshot
    from lib.thetadata import fetch_first_order_snapshot, set_max_connections

    set_max_connections(connections)
    totals = {"tickers": 0, "surface": 0, "atm": 0, "diagnostics": 0,
              "failed": [], "captures": []}

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

    with ProcessPoolExecutor(max_workers=workers) as fitpool, \
         ThreadPoolExecutor(max_workers=connections) as fetchpool:
        fetch_futs = {fetchpool.submit(fetch_guarded, tk): tk for tk in tickers}
        fit_futs = {}

        for fut in as_completed(fetch_futs):
            tk = fetch_futs[fut]
            try:
                tk, raw, captured_at, secs = fut.result()
            except Exception as exc:                          # noqa: BLE001
                log.warning("  %s: fetch FAILED — %s: %s", tk,
                            type(exc).__name__, exc)
                totals["failed"].append(f"{tk} (fetch)")
                continue
            if raw is None or raw.empty:
                log.warning("  %s: empty chain", tk)
                totals["failed"].append(f"{tk} (empty)")
                slots.release()
                continue

            # trade_date from the VENDOR's own quote stamp, not the wall clock.
            # Outside the session the snapshot endpoint returns the previous
            # close, and stamping that with today's date would file stale
            # quotes under a session that has not happened.
            vend = pd.to_datetime(raw["timestamp"], errors="coerce").max()
            trade_date = (vend.date() if pd.notna(vend) else captured_at.date())
            if trade_date != captured_at.date():
                # The endpoint served a previous session. Filing it under the
                # vendor's date keeps it honest, but the grid bucket comes from
                # the wall clock, so the pair only agrees during a live
                # session. Loud, because in normal operation it cannot happen.
                log.warning("  %s: STALE — vendor stamp is %s but today is %s. "
                            "Quotes are from a previous session; the bucket "
                            "label is wall-clock and will not match.",
                            tk, trade_date, captured_at.date())
            snapshot = grid_bucket(captured_at)
            totals["captures"].append((tk, captured_at, secs, trade_date))

            f = fitpool.submit(fit_one,
                               (tk, raw, trade_date, snapshot, captured_at))
            fit_futs[f] = tk
            del raw          # drop the parent's reference; the pickle is queued

        for fut in as_completed(fit_futs):
            tk = fit_futs[fut]
            slots.release()
            try:
                got = fut.result()
            except Exception as exc:                          # noqa: BLE001
                log.error("  %s: worker died — %s: %s", tk,
                          type(exc).__name__, exc)
                totals["failed"].append(f"{tk} (worker)")
                continue
            if got["error"]:
                log.warning("  %s: %s", tk, got["error"])
                totals["failed"].append(f"{tk} (fit)")
                continue
            try:
                written = write_snapshot(conn, got, got["trade_date"])
            except Exception as exc:                          # noqa: BLE001
                conn.rollback()
                log.error("  %s: WRITE FAILED — %s: %s", tk,
                          type(exc).__name__, exc)
                totals["failed"].append(f"{tk} (write)")
                continue
            for k in ("surface", "atm", "diagnostics"):
                totals[k] += written[k]
            totals["tickers"] += 1
    return totals


# --- Main -------------------------------------------------------------------

def main() -> int:
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
    ap.add_argument("--force", action="store_true",
                    help="run outside the market window. The snapshot endpoint "
                         "returns the PREVIOUS CLOSE when the market is shut, "
                         "so this captures stale quotes — trade_date is taken "
                         "from the vendor stamp so they are at least filed "
                         "under the right session.")
    ap.add_argument("--no-lock", action="store_true",
                    help="skip the lock file (for a manual one-off)")
    args = ap.parse_args()

    log_file = setup_file_logging("fetch_live_surface")
    now_et = datetime.now(ET).replace(tzinfo=None)
    open_now, reason = market_window(now_et)
    if not open_now and not args.force:
        log.info("outside the market window — %s", reason)
        return 0

    lock = None
    if not args.no_lock:
        lock = acquire_lock(LOCK_PATH)
        if lock is False:
            log.warning("previous cycle still running (%s held) — skipping. "
                        "A cycle is never retried: 'now' has moved.", LOCK_PATH)
            return 0

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
                           args.max_inflight)
    wall = time.monotonic() - t0

    caps = totals["captures"]
    print(f"\n{totals['tickers']}/{len(tickers)} ticker(s) in {wall:.0f}s")
    print(f"  equity_surface  {totals['surface']:>8,} rows")
    print(f"  equity_atm      {totals['atm']:>8,} rows")
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
    return 0 if totals["tickers"] else 1


if __name__ == "__main__":
    sys.exit(main())
