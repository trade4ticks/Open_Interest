"""Fetch trade_quote to parquet. Resumable, incremental, space-aware.

    python -m scalp.fetch --start 2026-08-17 --end 2026-08-28
    python -m scalp.fetch --start 2026-08-28 --end 2026-08-28        # nightly
    python -m scalp.fetch --start ... --end ... --plan               # no writes

RESUMABLE WITHOUT BOOKKEEPING. One parquet file per symbol-day, and the
presence of the file IS the record that the day was fetched. A run that dies
at symbol 300 skips the 299 already on disk next time, and there is no
manifest that can drift out of agreement with what is actually there.

INCREMENTAL BY DEFAULT. Only missing symbol-days are requested. First run
long, nightly runs short — s3 measured 5.2 minutes for the 10-day backfill at
concurrency 4 and 39 seconds for a nightly session.

SPACE IS CHECKED BEFORE ANYTHING IS WRITTEN. Block 3 has ~22 GB against a
3.1 GB backfill and ~310 MB per session, which is roughly 60 sessions of
headroom, and neither of the other volumes has a comfortable amount free. This
refuses to start rather than filling the disk halfway through.

IT WILL NEVER DELETE ANYTHING TO MAKE ROOM. Pruning is prune.py, run by hand.
An automatic deleter racing an automatic writer is how a store loses days
nobody noticed were gone.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

from scalp import config, db, store, thetadata as td

log = logging.getLogger(__name__)


def plan_units(symbols: list[str], days: list[date]) -> list[tuple[str, date]]:
    """Every (symbol, day) not already on disk."""
    units = []
    for sym in symbols:
        for d in store.missing_days(sym, days):
            units.append((sym, d))
    return units


def fetch_one(symbol: str, day: date) -> tuple[str, date, int, str]:
    """Fetch and write one symbol-day. Returns (symbol, day, rows, status)."""
    try:
        raw = td.trade_quote(symbol, day, day)
    except td.NoDataError:
        return symbol, day, 0, "nodata"
    except Exception as exc:
        return symbol, day, 0, f"error:{type(exc).__name__}:{exc}"

    df = raw.frame()
    if df.empty:
        # A holiday or a symbol that did not trade. Deliberately NOT written
        # as an empty file: an empty parquet would satisfy the resume check
        # and permanently mask a day that a later re-run could have filled.
        return symbol, day, 0, "empty"
    store.write_day(symbol, day, df)
    return symbol, day, len(df), "ok"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--symbols", default=None,
                    help="comma-separated override; default is the universe")
    ap.add_argument("--workers", type=int, default=None,
                    help=f"default: the connection cap ({td.max_connections()})")
    ap.add_argument("--plan", action="store_true",
                    help="print what would be fetched and stop")
    ap.add_argument("--yes", action="store_true",
                    help="required for runs above --confirm-threshold units")
    ap.add_argument("--confirm-threshold", type=int, default=2000)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s  %(levelname)-7s %(message)s",
                        datefmt="%H:%M:%S")

    if not config.VENUE_POLICY_VERIFIED:
        raise SystemExit(
            "config.VENUE_POLICY_VERIFIED is False. Refusing to run a bulk "
            "pull against an unverified venue policy — run "
            "scalp/step0/s1_venue_check.py first."
        )

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    days = store.trading_days(start, end)
    if not days:
        raise SystemExit(f"no trading sessions between {start} and {end}")

    symbols = ([s.strip().upper() for s in args.symbols.split(",") if s.strip()]
               if args.symbols else db.universe_symbols())
    if not symbols:
        raise SystemExit("empty universe — run scalp.update_universe first")

    units = plan_units(symbols, days)
    projected = config.projected_write_gb(len(symbols), len(days))
    data_root = config.data_dir()            # validates the mount before use
    free = config.free_space_gb()

    print()
    print(f"store          : {data_root}")
    print(f"symbols        : {len(symbols):,}")
    print(f"sessions       : {len(days)}  ({days[0]} .. {days[-1]})")
    print(f"already on disk: {len(symbols) * len(days) - len(units):,} symbol-days")
    print(f"to fetch       : {len(units):,} symbol-days")
    print(f"projected write: {projected:.2f} GB "
          f"(at {config.ESTIMATED_MB_PER_SYMBOL_DAY} MB/symbol-day)")
    print(f"free on volume : {free:.2f} GB "
          f"(margin {config.FREE_SPACE_MARGIN_GB:.1f} GB)")
    print(f"store size now : {store.store_bytes() / 1024**3:.2f} GB")

    if not units:
        print()
        print("Nothing to fetch — every symbol-day is already on disk.")
        return

    if projected + config.FREE_SPACE_MARGIN_GB > free:
        raise SystemExit(
            f"\nREFUSING TO START: projected {projected:.2f} GB plus a "
            f"{config.FREE_SPACE_MARGIN_GB:.1f} GB margin exceeds the "
            f"{free:.2f} GB free.\n"
            f"Run `python -m scalp.prune --older-than "
            f"{config.RAW_RETENTION_DAYS}` to see what could be freed, or "
            f"narrow the date range."
        )

    if args.plan:
        print()
        print("--plan: nothing fetched.")
        return

    if len(units) > args.confirm_threshold and not args.yes:
        raise SystemExit(
            f"\n{len(units):,} symbol-days is above the "
            f"{args.confirm_threshold:,} confirmation threshold. Re-run with "
            "--yes if that is intended."
        )

    workers = args.workers or td.max_connections()
    if workers > td.max_connections():
        log.warning("workers=%d exceeds the connection cap of %d; the vendor "
                    "documents timeouts rather than clean rejections above it",
                    workers, td.max_connections())

    t0 = time.monotonic()
    done = rows_total = failures = empties = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_one, s, d): (s, d) for s, d in units}
        for fut in as_completed(futures):
            sym, day, rows, status = fut.result()
            done += 1
            if status == "ok":
                rows_total += rows
            elif status in ("empty", "nodata"):
                empties += 1
            else:
                failures += 1
                log.warning("  %s %s: %s", sym, day, status)
            if done % 50 == 0 or done == len(units):
                elapsed = time.monotonic() - t0
                rate = done / max(elapsed, 1e-9)
                eta = (len(units) - done) / max(rate, 1e-9)
                log.info("  %d/%d  %.1f units/s  eta %.0fs  %s rows",
                         done, len(units), rate, eta, f"{rows_total:,}")

    elapsed = time.monotonic() - t0
    print()
    print(f"fetched {done - failures - empties:,} symbol-days, "
          f"{rows_total:,} rows in {elapsed/60:.1f} min")
    print(f"empty/no-data: {empties:,}   failures: {failures:,}")
    print(f"store size now: {store.store_bytes() / 1024**3:.2f} GB")
    print(f"free on volume: {config.free_space_gb():.2f} GB")
    if failures:
        print()
        print("Failures are safe to re-run: the units that succeeded are on "
              "disk and will be skipped.")
        sys.exit(1)


if __name__ == "__main__":
    main()
