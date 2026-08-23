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

Sources:
    snapshots (default)  /data/chain_snapshots/<TICKER>/<YYYY>.parquet
    intraday             /mnt/trading_volume_3/chain_intraday/<TICKER>/<YYYYMMDD>.parquet

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
    intraday, one per year for snapshots — so this is the only place the
    difference shows up.
    """
    d = _store_dir(source) / ticker.upper()
    if not d.exists():
        return []
    if source == SOURCE_INTRADAY:
        p = d / f"{day:%Y%m%d}.parquet"
        return [p] if p.exists() else []
    p = d / f"{day:%Y}.parquet"
    return [p] if p.exists() else []


def load_day(ticker: str, source: str, day: date) -> pd.DataFrame:
    """Raw rows for one ticker-day, already narrowed to that trade_date."""
    frames = []
    for p in files_for(ticker, source, day):
        try:
            frames.append(pd.read_parquet(p))
        except Exception as exc:                              # noqa: BLE001
            log.warning("  %s: unreadable (%s) — %s", p.name,
                        type(exc).__name__, exc)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if df.empty or "trade_date" not in df.columns:
        return pd.DataFrame()
    td = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    return df[td == day]


# --- Processing -------------------------------------------------------------

def process_ticker_day(conn, ticker: str, source: str, day: date,
                       skip_snapshots=None) -> dict:
    """Every snapshot for one ticker-day. Returns written row counts."""
    from lib.surface_store import write_snapshot

    raw = load_day(ticker, source, day)
    totals = {"surface": 0, "atm": 0, "diagnostics": 0, "snapshots": 0}
    if raw.empty:
        return totals

    # THE clean_chain CALL. Everything below reads columns it adds.
    cleaned = clean_chain(raw)

    for snapshot, sub in cleaned.groupby("snapshot", sort=True):
        snap = str(snapshot)
        if skip_snapshots and snap in skip_snapshots:
            continue
        try:
            result = build_snapshot(sub, ticker, day, snap)
            written = write_snapshot(conn, result, day)
        except Exception as exc:                              # noqa: BLE001
            conn.rollback()
            log.error("  %s %s %s: FAILED — %s: %s", ticker, day, snap,
                      type(exc).__name__, exc)
            log.debug("  traceback", exc_info=True)
            continue
        for k in ("surface", "atm", "diagnostics"):
            totals[k] += written[k]
        totals["snapshots"] += 1
    return totals


def run_days(conn, tickers: list, source: str, days: list,
             intraday_resume: bool = False) -> dict:
    from lib.surface_store import completed_snapshots

    grand = {"surface": 0, "atm": 0, "diagnostics": 0, "snapshots": 0}
    with tqdm(total=len(tickers) * len(days), unit="tk-day", ncols=90,
              desc="surface") as bar:
        for day in days:
            for tk in tickers:
                skip = None
                if intraday_resume:
                    # Compare what diagnostics already holds against what is on
                    # disk, so a re-run mid-session only does the new bars.
                    skip = set(completed_snapshots(conn, tk, day))
                try:
                    got = process_ticker_day(conn, tk, source, day, skip)
                except Exception as exc:                      # noqa: BLE001
                    conn.rollback()
                    log.error("  FAIL %s %s: %s: %s", tk, day,
                              type(exc).__name__, exc)
                    log.debug("  traceback", exc_info=True)
                    bar.update(1)
                    continue
                for k in grand:
                    grand[k] += got[k]
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

        t0 = time.monotonic()
        totals = run_days(conn, tickers, args.source, days,
                          intraday_resume=(args.command == "intraday"))

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
