"""
build_equity_surface.py — stage 3 of the chain pipeline.

    fetch (exists) -> clean (lib/clean_chain.py) -> INTERPOLATE (this) -> metrics

Resamples irregular option chains onto a fixed (tenor x delta) grid and writes
to Postgres. Expiries land where the exchange lists them and strikes where
liquidity is; this makes any (ticker, date, snapshot, dte, put_delta) a direct
lookup.

Every DataFrame read here goes through clean_chain() BEFORE anything else —
that module adds mid_price, spread, moneyness, gamma, dte and the flag_*
columns this stage filters on. clean_chain does no file I/O, so reading the
parquet is this script's job.

Sources (roots resolve from config.py, set in .env — do not assume a literal
path here, both stores have already moved once):
    snapshots (default)  {CHAIN_SNAPSHOTS_DIR}/<TICKER>/<YYYYMM>.parquet
    intraday             {CHAIN_INTRADAY_DIR}/<TICKER>/<YYYYMMDD>.parquet

Both share one 20-column schema, so there is no per-store branching beyond
locating the files. A single file holds many expirations AND many snapshots,
so it is split by (snapshot, expiration) before processing.

Usage:
    python build_equity_surface.py init-db
    python build_equity_surface.py batch --start 20260601 --end 20260630
    python build_equity_surface.py batch --start 20260601 --end 20260630 --tickers AAPL,MSFT
    python build_equity_surface.py incremental
    python build_equity_surface.py intraday --source intraday
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from lib.chain_fetch_common import log_path, setup_file_logging
from lib.clean_chain import clean_chain
from lib.market_hours import get_trading_days, last_trading_day
from lib.surface_config import SOURCE_INTRADAY, SOURCE_SNAPSHOTS
from lib.surface_fit import build_snapshot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("build_equity_surface")


# --- Source location --------------------------------------------------------

def _store_dir(source: str) -> Path:
    from config import CHAIN_INTRADAY_DIR, CHAIN_SNAPSHOTS_DIR
    return Path(CHAIN_INTRADAY_DIR if source == SOURCE_INTRADAY
                else CHAIN_SNAPSHOTS_DIR)


def list_tickers(source: str) -> list:
    root = _store_dir(source)
    if not root.exists():
        return []
    return sorted(p.name.upper() for p in root.iterdir()
                  if p.is_dir() and not p.name.startswith("_")
                  and any(p.glob("*.parquet")))


def files_for(ticker: str, source: str, day: date) -> list:
    """Parquet file(s) that could hold `day` for this ticker.

    The two stores are laid out differently — one file per session for
    intraday, one per month for snapshots — so this is the only place the
    difference shows up.
    """
    d = _store_dir(source) / ticker.upper()
    if not d.exists():
        return []
    if source == SOURCE_INTRADAY:
        p = d / f"{day:%Y%m%d}.parquet"
        return [p] if p.exists() else []
    p = d / f"{day:%Y%m}.parquet"
    return [p] if p.exists() else []


def _read_day(p: Path, day: date) -> pd.DataFrame:
    """One file's rows for `day`, pushed down to the parquet reader.

    The snapshots store holds a whole year per file and is written sorted by
    trade_date with an explicit ROW_GROUP_SIZE, precisely so a single date
    touches only the row groups whose min/max bracket it. Nothing asked for
    that until now: this read was `pd.read_parquet(p)`, which materialised
    ~250 sessions to keep one, once per day in the loop — so a 160-session
    batch read the same year file 160 times and discarded ~99.6% of each.

    The filter is on trade_date only, so BOTH snapshots of the day still come
    through; splitting by (snapshot, expiration) happens downstream.

    Falls back to the whole-file read if the reader rejects the predicate. A
    surface build must not fail over an optimisation, and the caller's pandas
    filter is still there to narrow whatever comes back.
    """
    try:
        return pd.read_parquet(p, filters=[("trade_date", "==", day)])
    except Exception as exc:                                  # noqa: BLE001
        log.debug("  %s: predicate pushdown unavailable (%s) — reading whole "
                  "file", p.name, type(exc).__name__)
        return pd.read_parquet(p)


def load_day(ticker: str, source: str, day: date) -> pd.DataFrame:
    """Raw rows for one ticker-day, already narrowed to that trade_date."""
    frames = []
    for p in files_for(ticker, source, day):
        try:
            frames.append(_read_day(p, day))
        except Exception as exc:                              # noqa: BLE001
            log.warning("  %s: unreadable (%s) — %s", p.name,
                        type(exc).__name__, exc)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if df.empty or "trade_date" not in df.columns:
        return pd.DataFrame()
    # Kept even though the pushdown above already narrows: the fallback path
    # returns the whole file, and pruning is only as exact as the reader makes
    # it. This is the line that guarantees the contract in the docstring.
    td = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    return df[td == day]


# --- Processing -------------------------------------------------------------

def expected_cost(ticker: str, source: str, days: list) -> int:
    """Cheap proxy for how long a ticker will take: bytes on disk.

    Used to order work largest-first. Fit time tracks the number of quotes,
    which tracks file size closely enough — SPY is ~3x T on both — and a
    stat() costs nothing, whereas counting rows would mean reading every
    file twice.

    Falls back to 0 when nothing is on disk, which sorts those units last;
    they return immediately anyway.
    """
    total = 0
    seen = set()
    for day in days:
        for p in files_for(ticker, source, day):
            if p in seen:            # snapshots keeps a whole month in one file
                continue
            seen.add(p)
            try:
                total += p.stat().st_size
            except OSError:
                pass
    return total


def _grid_instant(day, snapshot: str):
    """The exact instant a grid label denotes: '1345' on `day` -> 13:45:00."""
    from datetime import datetime, time as _t
    try:
        return datetime.combine(day, _t(int(snapshot[:2]), int(snapshot[2:4])))
    except (ValueError, IndexError):
        return None


def fit_ticker(args) -> dict:
    """One ticker's fits, computed in a WORKER PROCESS. No database.

    The parent owns every write, for three reasons: partition creation would
    otherwise race between workers, one connection is cheaper than N, and a
    worker that dies cannot leave a half-written transaction behind.

    Returns only the three row lists. `build_snapshot` also hands back its
    SmileFit objects, each holding a scipy spline — pickling ~35 of those per
    snapshot back to the parent would cost more than the fit did, and nothing
    downstream of the write needs them.

    Log records do not cross the process boundary, so messages are collected
    and returned for the parent to emit into the run log. A worker logging to
    its own stderr would be lost the moment the terminal scrolled.
    """
    ticker, source, days, skip_map = args
    out = {"ticker": ticker, "units": [], "messages": [], "error": None}
    try:
        for day in days:
            raw = load_day(ticker, source, day)
            if raw.empty:
                continue

            # FILTER BEFORE CLEANING. clean_chain is the expensive part and it
            # scales with rows, so cleaning 78 snapshots to use one is ~5x the
            # cost of the fit it feeds. The skip set is known here, so the
            # frame is narrowed first — this is what makes an intraday re-run
            # cost one snapshot instead of a whole session.
            skip = skip_map.get(day)
            if skip:
                keep = ~raw["snapshot"].astype("string").isin(skip)
                raw = raw[keep.to_numpy(dtype=bool)]
                if raw.empty:
                    continue

            cleaned = clean_chain(raw)
            for snapshot, sub in cleaned.groupby("snapshot", sort=True):
                snap = str(snapshot)
                try:
                    res = build_snapshot(sub, ticker, day, snap)
                except Exception as exc:                      # noqa: BLE001
                    out["messages"].append(
                        f"{ticker} {day} {snap}: build FAILED — "
                        f"{type(exc).__name__}: {exc}")
                    continue
                # Stamp the exact counterpart of what fetch_live_surface
                # writes. A live row sits in grid slot '1345' with
                # source='live' and an approximate captured_at; this upserts
                # the SAME key with the grid instant and source='exact', so
                # the tables converge with no migration and no correction step.
                exact_at = _grid_instant(day, snap)
                for r in res["surface"] + res["atm"]:
                    r["captured_at"] = exact_at
                    r["source"] = "exact"
                out["units"].append({
                    "day": day, "snapshot": snap,
                    "surface": res["surface"], "atm": res["atm"],
                    "diagnostics": res["diagnostics"],
                })
    except Exception as exc:                                  # noqa: BLE001
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def _write_unit(conn, ticker: str, unit: dict, totals: dict) -> None:
    from lib.surface_store import write_snapshot
    try:
        written = write_snapshot(conn, unit, unit["day"])
    except Exception as exc:                                  # noqa: BLE001
        conn.rollback()
        log.error("  %s %s %s: WRITE FAILED — %s: %s", ticker, unit["day"],
                  unit["snapshot"], type(exc).__name__, exc)
        log.debug("  traceback", exc_info=True)
        return
    for k in ("surface", "atm", "diagnostics"):
        totals[k] += written[k]
    totals["snapshots"] += 1


def _drain(conn, got: dict, totals: dict) -> None:
    """Log a worker's messages and write its rows, in the parent."""
    for m in got.get("messages", []):
        log.warning("  %s", m)
    if got.get("error"):
        log.error("  FAIL %s: %s", got["ticker"], got["error"])
    for unit in got.get("units", []):
        _write_unit(conn, got["ticker"], unit, totals)


def default_workers() -> int:
    import os
    return max(1, (os.cpu_count() or 2) - 1)


def run_days(conn, tickers: list, source: str, days: list,
             intraday_resume: bool = False, workers: int = 1) -> dict:
    """Fit every (ticker, day) and write the results.

    Tickers are independent and the fit is CPU-bound, so the work is spread
    across processes — threads would not help, the smile fit holds the GIL.
    The unit is a TICKER rather than a (ticker, day) pair so that a multi-day
    batch reads each ticker's file once per day inside one worker instead of
    scattering the same file across several.

    Units are dispatched largest-first. ProcessPoolExecutor queues FIFO, so
    submitting the heaviest work first is longest-processing-time scheduling:
    with SPY ~3x T, naive ordering leaves workers idle at the tail waiting for
    one straggler to finish.
    """
    from lib.surface_store import completed_snapshots

    grand = {"surface": 0, "atm": 0, "diagnostics": 0, "snapshots": 0}

    # Skip sets are computed HERE, in the parent, because they need the
    # database and the workers deliberately have no connection.
    skip_by_ticker = {}
    if intraday_resume:
        for tk in tickers:
            skip_by_ticker[tk] = {d: set(completed_snapshots(conn, tk, d))
                                  for d in days}

    work = [(tk, source, days, skip_by_ticker.get(tk, {})) for tk in tickers]
    work.sort(key=lambda a: expected_cost(a[0], a[1], a[2]), reverse=True)
    if work:
        heaviest = work[0][0]
        log.info("dispatch order: heaviest first (%s ... %s)",
                 heaviest, work[-1][0])

    if workers <= 1:
        with tqdm(total=len(work), unit="tk", ncols=90, desc="surface") as bar:
            for args in work:
                _drain(conn, fit_ticker(args), grand)
                bar.update(1)
        return grand

    from concurrent.futures import ProcessPoolExecutor, as_completed
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fit_ticker, a): a[0] for a in work}
        with tqdm(total=len(futures), unit="tk", ncols=90,
                  desc=f"surface x{workers}") as bar:
            for fut in as_completed(futures):
                tk = futures[fut]
                try:
                    got = fut.result()
                except Exception as exc:                      # noqa: BLE001
                    log.error("  FAIL %s: worker died — %s: %s", tk,
                              type(exc).__name__, exc)
                    log.debug("  traceback", exc_info=True)
                    bar.update(1)
                    continue
                _drain(conn, got, grand)
                bar.update(1)
    return grand


# --- CLI --------------------------------------------------------------------

def _parse_day(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Interpolate equity option chains onto a fixed grid.")
    ap.add_argument("command",
                    choices=["init-db", "batch", "incremental", "intraday"])
    ap.add_argument("--start", help="YYYYMMDD (batch)")
    ap.add_argument("--end", help="YYYYMMDD (batch)")
    ap.add_argument("--tickers", help="comma-separated; default = all in store")
    ap.add_argument("--source", choices=[SOURCE_SNAPSHOTS, SOURCE_INTRADAY],
                    default=SOURCE_SNAPSHOTS,
                    help="which chain store to read (default snapshots)")
    ap.add_argument("--workers", type=int, default=None,
                    help="worker processes for the fit (default cpu_count()-1). "
                         "1 runs in-process, which is the one to use when "
                         "debugging: a traceback from a worker arrives as a "
                         "pickled string, not a live frame.")
    args = ap.parse_args()

    log_file = setup_file_logging("build_equity_surface")
    print("=== Open_Interest — equity surface interpolation ===")
    print(f"Log: {log_file}")
    log.info("argv: %s", " ".join(sys.argv[1:]))

    from db import get_connection
    from lib.surface_store import init_db, max_processed_date

    with get_connection() as conn:
        if args.command == "init-db":
            init_db(conn)
            print("Schema applied (idempotent).")
            return 0

        tickers = ([t.strip().upper() for t in args.tickers.split(",")
                    if t.strip()] if args.tickers else list_tickers(args.source))
        if not tickers:
            raise SystemExit(
                f"No tickers found under {_store_dir(args.source)}. "
                f"Pass --tickers, or check --source.")

        # The snapshots store is {TICKER}/{YYYYMM}.parquet. A ticker still
        # holding pre-migration {YYYY}.parquet files would make files_for()
        # return nothing for every day, and this run would report "no rows"
        # per ticker and exit 0 — a silent no-op that looks like a completed
        # build. Refuse instead, the same way the fetcher does.
        if args.source == SOURCE_SNAPSHOTS:
            from lib.chain_snapshot_store import list_legacy_year_files
            stale = sorted(t for t in tickers if list_legacy_year_files(t))
            if stale:
                raise SystemExit(
                    f"\n{len(stale)} ticker(s) still hold pre-migration "
                    f"{{YYYY}}.parquet files: {', '.join(stale[:10])}"
                    f"{' ...' if len(stale) > 10 else ''}\n"
                    "The snapshots store is now {TICKER}/{YYYYMM}.parquet and "
                    "year files are not read, so this run would find no rows "
                    "and silently do nothing.\n"
                    "  python migrate_chain_snapshots_to_monthly.py --dry-run")

        if args.command == "batch":
            if not (args.start and args.end):
                raise SystemExit("batch needs --start and --end")
            days = get_trading_days(_parse_day(args.start), _parse_day(args.end))
        elif args.command == "incremental":
            last = max_processed_date(conn)
            start = (last + timedelta(days=1)) if last else None
            if start is None:
                raise SystemExit(
                    "diagnostics is empty, so there is nothing to resume from. "
                    "Run `batch --start ... --end ...` for the first load.")
            end = last_trading_day()
            days = [d for d in get_trading_days(start, end)]
            print(f"Resuming after {last}")
        else:                                     # intraday
            days = [last_trading_day()]

        if not days:
            print("No trading days to process.")
            return 0

        print(f"{len(tickers)} ticker(s) x {len(days)} day(s), "
              f"source={args.source}")
        print(f"{days[0]} .. {days[-1]}\n")

        workers = args.workers if args.workers is not None else default_workers()
        workers = max(1, min(workers, len(tickers)))
        print(f"workers: {workers}"
              f"{' (serial)' if workers == 1 else ''}\n")

        t0 = time.monotonic()
        totals = run_days(conn, tickers, args.source, days,
                          intraday_resume=(args.command == "intraday"),
                          workers=workers)

    print(f"\n{totals['snapshots']:,} snapshot(s) processed in "
          f"{time.monotonic() - t0:.0f}s")
    print(f"  equity_surface              {totals['surface']:>10,} rows")
    print(f"  equity_atm                  {totals['atm']:>10,} rows")
    print(f"  equity_surface_diagnostics  {totals['diagnostics']:>10,} rows")
    print(f"\nLog: {log_path()}")
    if totals["diagnostics"] == 0:
        print("\nNothing was written. Check that the source store has files "
              "for these dates.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
