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
from datetime import date, datetime, timezone

from scalp import config, db, schema, store, thetadata as td

log = logging.getLogger(__name__)


def plan_units(symbols: list[str], days: list[date]) -> list[tuple[str, date]]:
    """Every (symbol, day) not already on disk."""
    units = []
    for sym in symbols:
        for d in store.missing_days(sym, days):
            units.append((sym, d))
    return units


def exchange_code_count(df) -> int:
    """Distinct exchange codes in a symbol-day, or -1 if unreadable.

    THE TAPE-COMPLETENESS CHECK. A consolidated US equity tape carries prints
    from many venues: Aug 28 showed 20 codes. The same fetch without
    venue=utp_cta showed 5 — Nasdaq exchange plus Nasdaq TRF and essentially
    nothing else.

    So the code count says directly whether the tape is consolidated, and
    unlike a share-volume comparison it needs no per-symbol reference figure.
    """
    col = schema.find(df, schema.CAND_EXCHANGE, "exchange", required=False)
    if col is None:
        return -1
    return int(df[col].nunique(dropna=True))


def fetch_one(symbol: str, day: date,
              min_codes: int = config.MIN_EXCHANGE_CODES
              ) -> tuple[str, date, int, str]:
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

    codes = exchange_code_count(df)
    if 0 <= codes < min_codes:
        # NOT WRITTEN, deliberately. A thin file on disk satisfies the resume
        # check and is then indistinguishable from a good one, so every later
        # run skips it and every metric computed from it is wrong in a way
        # that reads as merely surprising. Leaving it absent means a re-run
        # picks it up.
        return symbol, day, len(df), f"thin_tape:{codes}"

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
    ap.add_argument("--allow-today", action="store_true",
                    help="fetch the current trading date. Off by default: the "
                         "consolidated tape keeps filling after the close, so "
                         "a same-day pull can be short of prints that arrive "
                         "later, even with venue=utp_cta.")
    ap.add_argument("--min-exchange-codes", type=int,
                    default=config.MIN_EXCHANGE_CODES,
                    help="refuse a symbol-day with fewer distinct exchange "
                         "codes than this. A consolidated tape shows ~20; a "
                         "Nasdaq-only one shows ~5. 0 disables the check.")
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

    # SAME-DAY DATA IS NOT SETTLED. The venue default falls back to the
    # real-time Nasdaq feed on a recent date, and the vendor's consolidated
    # tape continues to fill in after the close. venue=utp_cta fixes the first
    # problem and not the second, so a same-day pull can still be short of
    # prints that arrive later.
    today = td.today_et()
    if today in days and not args.allow_today:
        days = [d for d in days if d != today]
        log.warning("skipping %s: same-day data is not settled. The tape "
                    "continues to fill after the close, so a pull today can "
                    "be short of prints that arrive later. Use --allow-today "
                    "to override, and re-fetch the day afterwards.", today)
        if not days:
            raise SystemExit(
                f"{today} was the only session in range and same-day fetching "
                f"is off. Re-run tomorrow, or pass --allow-today knowing the "
                f"tape may be incomplete.")

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
    thin: list[tuple[str, date, str]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_one, s, d, args.min_exchange_codes):
                   (s, d) for s, d in units}
        for fut in as_completed(futures):
            sym, day, rows, status = fut.result()
            done += 1
            if status == "ok":
                rows_total += rows
            elif status in ("empty", "nodata"):
                empties += 1
            elif status.startswith("thin_tape"):
                thin.append((sym, day, status))
                log.warning("  %s %s: %s codes — NOT WRITTEN. The tape looks "
                            "Nasdaq-only rather than consolidated.",
                            sym, day, status.split(":")[1])
            else:
                failures += 1
                log.warning("  %s %s: %s", sym, day, status)
            if done % 50 == 0 or done == len(units):
                elapsed = time.monotonic() - t0
                rate = done / max(elapsed, 1e-9)
                eta = (len(units) - done) / max(rate, 1e-9)
                log.info("  %d/%d  %.1f units/s  eta %.0fs  %s rows",
                         done, len(units), rate, eta, f"{rows_total:,}")

    # One row per run per date. The counts were accumulated in memory and
    # printed at the end, so a bad run left no record once the terminal
    # scrolled — and the thin-tape count from the run that went wrong is
    # exactly what is worth looking back at.
    try:
        run_ts = datetime.now(timezone.utc)
        per_date: dict[date, dict[str, int]] = {}
        for sym, day in units:
            per_date.setdefault(day, {"ok": 0, "thin": 0, "empty": 0,
                                      "failed": 0})
        for sym, day, status in thin:
            per_date[day]["thin"] += 1
        for day, counts in per_date.items():
            counts["ok"] = sum(
                1 for s2, d2 in units if d2 == day) - counts["thin"]
        for day, counts in sorted(per_date.items()):
            db.write_fetch_run(run_ts, day, counts["ok"], counts["thin"],
                               counts["empty"], counts["failed"])
    except Exception as exc:
        log.warning("could not record the run in fetch_runs: %s", exc)

    elapsed = time.monotonic() - t0
    print()
    print(f"fetched {done - failures - empties:,} symbol-days, "
          f"{rows_total:,} rows in {elapsed/60:.1f} min")
    print(f"empty/no-data: {empties:,}   failures: {failures:,}   "
          f"thin tape: {len(thin):,}")
    if thin:
        print()
        print(f"{len(thin)} symbol-day(s) returned fewer than "
              f"{args.min_exchange_codes} exchange codes and were NOT written.")
        print("A consolidated tape shows ~20; a Nasdaq-only one shows ~5. The")
        print("usual cause is the venue parameter not reaching the request, or")
        print("a same-day pull against an unsettled tape. Check")
        print("config.VENUE_BY_ENDPOINT before re-running.")
        for sym, day, status in thin[:10]:
            print(f"    {sym} {day}  {status}")
    print(f"store size now: {store.store_bytes() / 1024**3:.2f} GB")
    print(f"free on volume: {config.free_space_gb():.2f} GB")
    if failures or thin:
        print()
        print("Safe to re-run: the units that succeeded are on disk and will "
              "be skipped. Nothing thin was written, so a re-run retries it.")
        sys.exit(1)


if __name__ == "__main__":
    main()
