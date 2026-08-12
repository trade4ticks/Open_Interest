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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

import pandas as pd
from tqdm import tqdm

from lib.chain_snapshot_store import SNAPSHOT_LABELS, loaded_keys, write_rows
from lib.market_hours import get_trading_days, last_trading_day, next_trading_day
from lib.parquet_store import list_tickers as list_oi_tickers
from lib.thetadata import (
    SNAPSHOT_MAX_CONNECTIONS,
    TerminalServerError,
    TerminalTimeoutError,
    enumerate_expirations_eod,
    fetch_first_order_raw,
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

# Fields we expect to store; a persistent absence is worth knowing about.
_EXPECTED_VENDOR_FIELDS = {
    "bid", "ask", "delta", "theta", "vega", "rho", "epsilon", "lambda",
    "implied_vol", "iv_error", "underlying_timestamp", "underlying_price",
}


# --- Prompts ---------------------------------------------------------------

def _prompt_tickers() -> list[str]:
    raw = input(
        "Tickers (comma-separated; blank = all tickers in OI store): "
    ).strip()
    if raw:
        return [t.strip().upper() for t in raw.split(",") if t.strip()]
    out = list_oi_tickers()
    if not out:
        raise SystemExit(
            "No tickers entered and OI store is empty — please specify."
        )
    return out


def _prompt_date(label: str) -> date:
    while True:
        raw = input(f"{label} (YYYYMMDD): ").strip()
        try:
            return datetime.strptime(raw, "%Y%m%d").date()
        except ValueError:
            print("  Use YYYYMMDD (e.g. 20240102)")


# --- Date chunking ---------------------------------------------------------

def chunk_range(start: date, end: date, max_days: int) -> list[tuple[date, date]]:
    """Split [start, end] into inclusive calendar-day windows of <= max_days.

    These are WRITE batches, not request windows — every request the fetcher
    issues covers a single session.  A batch with no trading days is harmless:
    it yields no sessions and is skipped.
    """
    if max_days < 1:
        raise ValueError("max_days must be >= 1")
    out: list[tuple[date, date]] = []
    cur = start
    while cur <= end:
        w_end = min(cur + timedelta(days=max_days - 1), end)
        out.append((cur, w_end))
        cur = w_end + timedelta(days=1)
    return out


# --- Projection ------------------------------------------------------------

def _warn_unknown_fields(raw: pd.DataFrame) -> None:
    """Report unexpected / absent vendor fields once per run."""
    cols = set(raw.columns)

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


def _to_date_series(s: pd.Series) -> pd.Series:
    """Parse a vendor date column that may be 'YYYY-MM-DD', 'YYYYMMDD', or the
    integer 20241104.  astype(str) first — pd.to_datetime on a raw int would
    read it as a nanosecond epoch."""
    return pd.to_datetime(s.astype("string"), errors="coerce")


def _project(raw: pd.DataFrame, ticker: str, snapshot: str) -> pd.DataFrame:
    """Project the vendor's raw first_order frame into the stored schema.

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

    # trade_date: prefer the vendor's own date field; fall back to the date
    # component of the row timestamp.
    if "date" in raw.columns:
        td = _to_date_series(raw["date"]).dt.date
    else:
        td = ts.dt.date

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
        "expiration":           _to_date_series(raw["expiration"]).dt.date,
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

def fetch_ticker(ticker: str, batches: list[tuple[date, date]],
                 force: bool = False) -> int:
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
    already = set() if force else loaded_keys(ticker, years)

    fetched = 0
    for w_start, w_end in batches:
        sessions = get_trading_days(w_start, w_end)
        if not sessions:
            continue

        todo = [d for d in sessions
                if any((d, s) not in already for s in SNAPSHOT_LABELS)]
        if not todo:
            log.info("  %s %s..%s: all %d sessions loaded — skip",
                     ticker, w_start, w_end, len(sessions))
            continue

        # --- phase 1: enumerate, one session at a time ----------------------
        # Enumerating per session (option (b)) rather than per window makes the
        # completeness invariant exact rather than merely safe: the enumeration
        # window IS the fetch window, one session wide, so the fetched set is
        # precisely what existed that day. No dead (expiration, session) pairs.
        exp_by_session: dict[date, list[date]] = {}
        enum_failures: list[tuple[date, str]] = []

        with ThreadPoolExecutor(max_workers=SNAPSHOT_MAX_CONNECTIONS) as pool:
            futs = {pool.submit(enumerate_expirations_eod, ticker, d, d): d
                    for d in todo}
            for fut in as_completed(futs):
                d = futs[fut]
                try:
                    # exp >= d is a no-op safety net: a contract listed on d
                    # necessarily expires on or after d.
                    exps = sorted(e for e in fut.result() if e >= d)
                except Exception as exc:
                    enum_failures.append((d, f"{type(exc).__name__}: {exc}"))
                    continue
                if exps:
                    exp_by_session[d] = exps

        if enum_failures:
            # These sessions are simply not written, so they stay unloaded and
            # a plain rerun retries them — no --force needed.
            log.warning("  %s %s..%s: enumeration FAILED for %d session(s) — "
                        "not written, rerun to retry. First few: %s",
                        ticker, w_start, w_end, len(enum_failures),
                        "; ".join(f"{d}: {m[:60]}" for d, m in enum_failures[:3]))

        if not exp_by_session:
            log.info("  %s %s..%s: no expirations listed", ticker, w_start, w_end)
            continue

        # --- phase 2: point-query fetch, <= 4 in flight ---------------------
        units = [(d, e, s)
                 for d, exps in sorted(exp_by_session.items())
                 for e in exps
                 for s in SNAPSHOT_LABELS
                 if (d, s) not in already]

        log.info("  %s %s..%s: %d sessions, %d point queries",
                 ticker, w_start, w_end, len(exp_by_session), len(units))

        frames: list[pd.DataFrame] = []
        failures: list[tuple[date, date, str, str]] = []

        def _fetch_one(sess: date, exp: date, snap: str) -> pd.DataFrame:
            return fetch_first_order_raw(
                ticker, exp, sess, SNAPSHOT_TIMES[snap],
            )

        with ThreadPoolExecutor(max_workers=SNAPSHOT_MAX_CONNECTIONS) as pool:
            futs = {pool.submit(_fetch_one, d, e, s): (d, e, s)
                    for d, e, s in units}
            for fut in as_completed(futs):
                sess, exp, snap = futs[fut]
                try:
                    raw = fut.result()
                    if raw.empty:
                        continue
                    # Projection is inside the try so one malformed response
                    # is recorded as a unit failure instead of aborting the
                    # whole ticker.
                    projected = _project(raw, ticker, snap)
                except (TerminalTimeoutError, TerminalServerError) as exc:
                    failures.append((sess, exp, snap, f"{type(exc).__name__}: {exc}"))
                    continue
                except Exception as exc:
                    failures.append((sess, exp, snap, f"{type(exc).__name__}: {exc}"))
                    continue
                if not projected.empty:
                    frames.append(projected)

        if failures:
            log.warning("  %s %s..%s: %d/%d point queries FAILED — those "
                        "(session, expiration, snapshot) cells are missing. "
                        "Rerun with --force to retry. First few: %s",
                        ticker, w_start, w_end, len(failures), len(units),
                        "; ".join(f"{d}/{e}/{s}: {m[:60]}"
                                  for d, e, s, m in failures[:3]))

        if not frames:
            log.info("  %s %s..%s: no rows produced", ticker, w_start, w_end)
            continue

        combined = pd.concat(frames, ignore_index=True)
        by_year = write_rows(ticker, combined)
        fetched += len(combined)
        for y, n in sorted(by_year.items()):
            log.info("    %s/%d.parquet -> %d rows total", ticker, y, n)

    return fetched


# --- Main ------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fetch twice-daily (09:45 / 15:45) option chain snapshots."
    )
    ap.add_argument("--force", action="store_true",
                    help="refetch (date, snapshot) cells already in the store")
    ap.add_argument("--batch-days", type=int, default=DEFAULT_BATCH_DAYS,
                    help=("calendar days accumulated per parquet write "
                          f"(default {DEFAULT_BATCH_DAYS}). Does not affect "
                          "request size — every request is a point query."))
    ap.add_argument("--tickers", help="comma-separated; skips the prompt")
    ap.add_argument("--start", help="YYYYMMDD; skips the prompt")
    ap.add_argument("--end", help="YYYYMMDD; skips the prompt")
    args = ap.parse_args()

    print("=== Open_Interest — intraday chain snapshots (09:45 / 15:45) ===\n")

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = _prompt_tickers()

    start = (datetime.strptime(args.start, "%Y%m%d").date()
             if args.start else _prompt_date("Fetch start date"))
    end = (datetime.strptime(args.end, "%Y%m%d").date()
           if args.end else _prompt_date("Fetch end   date"))
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
          f"{SNAPSHOT_MAX_CONNECTIONS} concurrent connections"
          f"{', --force (ignoring loaded cells)' if args.force else ''}")
    print("Every request is a point query: one expiration, one session, "
          "one instant.")
    print("Note: the last session is included even if today's 15:45 snapshot "
          "has not happened yet — it simply returns no data.\n")

    print("Checking ThetaData ...", end=" ", flush=True)
    if not test_connection():
        raise SystemExit("FAILED — terminal not reachable.")
    print("OK\n")

    # Tickers are sequential on purpose: the 4-connection budget is spent
    # inside each chunk's fan-out, so ticker-level parallelism would buy
    # nothing and could only risk exceeding the cap.
    total = 0
    with tqdm(total=len(tickers), unit="tk", ncols=90, desc="snapshots") as bar:
        for t in tickers:
            try:
                total += fetch_ticker(t, batches, force=args.force)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                log.warning("  FAIL %s: %s", t, exc)
            bar.update(1)

    print(f"\nDone. {total:,} rows fetched and merged into data/chain_snapshots/.")
    print("Fetch-and-store only — no metrics repointed, no cron wired.")


if __name__ == "__main__":
    main()
