"""Read parquet, compute metrics, write Postgres.

    python -m scalp.compute --start 2026-08-28 --end 2026-08-28
    python -m scalp.compute --start ... --end ... --symbols FDX,LLY --print

Two granularities from the same pull, per the brief:

  * DAILY — one row per symbol-day. Drives the ranking and the dashboard.
  * 15-MINUTE — stored for later. Mornings substantially outperform
    afternoons in the realised results and that wants investigating.

Both come from `metrics.compute_window`, called with different bounds. There
is no separate daily code path, which is what keeps the future intraday
re-rank honest: it will call the same function with the last 30 minutes.

NOTHING IS FILTERED HERE. A metrics row is written for every symbol that can
be computed, whether or not it passes any threshold. Thresholds live in
config.DEFAULT_FILTERS and are applied at read time by rank.py and the
dashboard. That way changing one is a page refresh rather than a recompute,
and the rows for names that failed are kept — they are the only data that can
say whether a threshold was set correctly. A pipeline that filters can never
answer "what did the excluded ones look like?".

A PROVENANCE ROW IS WRITTEN ALONGSIDE EACH METRICS ROW: what was dropped, by
what rule. Condition-code exclusions broken out per code, crossed and locked
quotes, records lost to same-instant collapsing, minutes trimmed at the
auction edges, and the resulting share of raw tape retained.

IT IS IDEMPOTENT. Every write upserts on its primary key — daily_metrics on
(trade_date, symbol, metric), intraday_metrics on (…, bucket_start, metric),
provenance on (…, item). Re-running after a partial failure overwrites the
rows it already wrote and never doubles them, so truncating first is not
required.

The one thing an upsert cannot do is remove a metric that no longer exists.
Rename or delete one and its old rows persist, still keyed and still readable.
`--replace` deletes each symbol-day's rows before writing, for use after that
kind of change.

Postgres holds derived metrics only. No tick data ever.
"""
from __future__ import annotations

import argparse
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date

import pandas as pd

from scalp import config, db, metrics, schema, store

log = logging.getLogger(__name__)


# Linux only: ask the kernel to signal this process when its PARENT dies.
PR_SET_PDEATHSIG = 1


def _worker_init() -> None:
    """Make the kernel kill this worker when the parent process dies.

    A killed compute run left six orphaned workers holding 5.9 GB two days
    later. atexit handlers and signal handlers cannot help there: the parent
    was killed with -9, which is not catchable and runs no cleanup.

    prctl(PR_SET_PDEATHSIG, SIGKILL) is handled by the KERNEL, not the
    parent, so it fires however the parent died — including -9. It is set in
    the child, inherited from nothing, and survives for the life of the
    process.

    Non-Linux and any failure are ignored: this is a safety net, and a pool
    that refuses to start because prctl is unavailable would be worse than
    one that occasionally leaks on a platform this never runs on.

    See tests/test_worker_reaping.py, which kills a parent with -9 and
    asserts the workers are gone.
    """
    try:
        import ctypes
        import signal as _signal
        libc = ctypes.CDLL("libc.so.6", use_errno=True)
        libc.prctl(PR_SET_PDEATHSIG, _signal.SIGKILL, 0, 0, 0)
    except Exception:
        pass

    # A parent that died between fork and prctl leaves this child already
    # orphaned, and the signal will never come. Check once.
    try:
        import os as _os
        if _os.getppid() == 1:
            _os._exit(0)
    except Exception:
        pass


def _compute_unit(unit: tuple[str, date, bool]):
    """Worker entry point. Module-level so it pickles.

    Returns (daily, buckets, provenance, error) rather than raising: an
    exception crossing a process boundary loses its traceback and can fail to
    pickle at all, which turns one bad symbol-day into a dead pool.
    """
    symbol, day, with_intraday = unit
    try:
        daily, buckets, prov = compute_symbol_day(
            symbol, day, with_intraday=with_intraday)
        return daily, buckets, prov, None
    except Exception as exc:
        return None, [], {}, f"{type(exc).__name__}: {exc}"


def session_bounds(day: date) -> tuple[pd.Timestamp, pd.Timestamp]:
    """RTH for one session, as naive timestamps matching the vendor's."""
    return (pd.Timestamp(f"{day.isoformat()} {config.RTH_START}"),
            pd.Timestamp(f"{day.isoformat()} {config.RTH_END}"))


def prepare(df: pd.DataFrame, day: date) -> tuple[pd.DataFrame, metrics.Columns]:
    """Resolve columns, parse timestamps, and flag condition codes — ONCE.

    All three are properties of the row, not of the window it falls in.
    Deriving the condition flags per window meant a `.apply(axis=1)` over
    seven columns, 27 times per symbol-day, and profiling put that one call at
    134.8s of 159.7s cumulative. Doing it once here and carrying the result as
    boolean columns is the whole fix.
    """
    cols = metrics.Columns(
        time=schema.find(df, schema.CAND_TRADE_TIME, "trade timestamp"),
        price=schema.find(df, schema.CAND_PRICE, "trade price"),
        size=schema.find(df, schema.CAND_SIZE, "trade size"),
        bid=schema.find(df, schema.CAND_BID, "bid"),
        ask=schema.find(df, schema.CAND_ASK, "ask"),
        exchange=schema.find(df, schema.CAND_EXCHANGE, "exchange",
                             required=False),
        condition_cols=schema.trade_condition_columns(df),
    )
    out = df.copy()
    out[cols.time] = schema.parse_times(out[cols.time], session_date=day)
    metrics.attach_condition_flags(out, cols.condition_cols)
    return out, cols


def compute_symbol_day(symbol: str, day: date, *, with_intraday: bool = True
                       ) -> tuple[dict | None, list[dict], dict]:
    """Returns (daily metrics, 15-minute rows, provenance).

    NOTHING IS FILTERED HERE. A row is written for every symbol that can be
    computed, whether or not it passes any threshold. Thresholds live in
    config.DEFAULT_FILTERS and are applied at read time by rank.py and the
    dashboard, so changing one is a page refresh rather than a recompute — and
    so the rows for names that failed are kept, since they are the only data
    that can say whether a threshold was set right.
    """
    df = store.read_day(symbol, day)
    if df.empty:
        return None, [], {}
    df, cols = prepare(df, day)
    start, end = session_bounds(day)

    daily = metrics.compute_window(df, cols, start, end)
    daily["symbol_day_rows"] = len(df)
    prov = daily.pop("_provenance", {})

    # --no-intraday SKIPS THE COMPUTATION, not just the write. It previously
    # only guarded the INSERT, so compute_buckets still ran 26 windows per
    # symbol-day and the result was discarded — the same pattern as the bucket
    # provenance. Measured 0.46 symbol-days/s without the flag against 0.48
    # with it, when the profiler had the daily row at 2.90s and the 26
    # intraday rows at 5.40s.
    #
    # It has to be a parameter rather than a module-level flag because this
    # runs in a worker PROCESS, which never sees argparse's result.
    buckets = []
    if with_intraday:
        buckets = metrics.compute_buckets(df, cols, start, end,
                                          config.INTRADAY_BUCKET_MINUTES)
    return daily, buckets, prov


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--symbols", default=None,
                    help="comma-separated; default is everything on disk")
    ap.add_argument("--no-intraday", action="store_true",
                    help="skip the 15-minute rows")
    ap.add_argument("--replace", action="store_true",
                    help="delete each symbol-day's stored rows before writing. "
                         "Re-running is already idempotent — the writes upsert "
                         "on (date, symbol, metric) — but an upsert cannot "
                         "remove a metric that no longer exists. Use this "
                         "after renaming or deleting one.")
    ap.add_argument("--workers", type=int, default=None,
                    help=f"process pool size (default {config.COMPUTE_WORKERS}; "
                         "deliberately below the core count, since the "
                         "ThetaData terminal and Postgres share this box). "
                         "1 runs in-process, which is what to use when "
                         "debugging a traceback.")
    ap.add_argument("--print", dest="do_print", action="store_true",
                    help="print the daily metrics instead of writing Postgres")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    # An upsert cannot remove what no longer exists, which is the whole point
    # of --replace; combining it with --no-intraday would delete the intraday
    # rows and then not rewrite them, silently emptying the table.
    if args.replace and args.no_intraday:
        raise SystemExit(
            "--replace with --no-intraday would delete each symbol-day's "
            "intraday rows and not rewrite them. Run them separately, or drop "
            "--no-intraday."
        )

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s  %(levelname)-7s %(message)s",
                        datefmt="%H:%M:%S")

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    days = store.trading_days(start, end)
    symbols = ([s.strip().upper() for s in args.symbols.split(",") if s.strip()]
               if args.symbols else store.stored_symbols())
    if not symbols:
        raise SystemExit("nothing in the store — run scalp.fetch first")

    with_intraday = not args.no_intraday

    if not args.do_print:
        db.init_schema()

        # Daily partitions have to exist before the first INSERT routes to
        # them. Cheap and idempotent, so created up front for every day in
        # range rather than lazily per write.
        if with_intraday:
            for day in days:
                db.ensure_intraday_partition(day)

        # THE RIGHT FILESYSTEM. Postgres lives on the root disk; the parquet
        # store is on block 3. config.free_space_gb() defaults to the data
        # dir, so a check using it would have passed while root filled to
        # 100% — which is what happened. db.postgres_free_space_gb() asks the
        # server for its own data directory instead, so it cannot drift.
        free = db.postgres_free_space_gb()
        projected = config.projected_intraday_write_gb(
            len(symbols), len(days), replace=args.replace)
        if free is not None:
            print(f"postgres data dir : {db.postgres_data_directory()}")
            print(f"free there        : {free:.2f} GB")
            print(f"projected write   : {projected:.3f} GB"
                  f"{'  (x2 transient for --replace)' if args.replace else ''}")
            if projected + config.PG_FREE_SPACE_MARGIN_GB > free:
                raise SystemExit(
                    f"\nREFUSING TO START: projected {projected:.2f} GB plus a "
                    f"{config.PG_FREE_SPACE_MARGIN_GB:.1f} GB margin exceeds "
                    f"the {free:.2f} GB free on the Postgres filesystem.\n"
                    f"Run `python -m scalp.prune --intraday` to drop partitions "
                    f"past retention, or narrow the date range."
                )
        else:
            log.warning("could not read free space for the Postgres data "
                        "directory (remote server?) — skipping the disk check")

    units = [(symbol, day, with_intraday) for day in days for symbol in symbols
             if store.has_day(symbol, day)]
    skipped = len(days) * len(symbols) - len(units)
    if not units:
        raise SystemExit("no symbol-days on disk for that range")

    workers = args.workers or config.COMPUTE_WORKERS
    log.info("%d symbol-days on disk, %d skipped (no parquet), %d worker(s)",
             len(units), skipped, workers)
    if workers > (os.cpu_count() or 1):
        log.warning("workers=%d exceeds the %d cores on this box; the "
                    "ThetaData terminal and Postgres run here too",
                    workers, os.cpu_count())

    rows_written = 0
    computed = failed = 0
    printed: list[dict] = []
    months_touched: set[date] = set()
    days_written: set[date] = set()
    t0 = time.monotonic()

    def handle(symbol: str, day: date, payload) -> None:
        """Write one finished unit. Runs in the PARENT — one DB connection."""
        nonlocal rows_written, computed, failed
        daily, buckets, prov, err = payload
        if err:
            failed += 1
            log.warning("  %s %s: %s", symbol, day, err)
            return
        if daily is None:
            return
        computed += 1
        if args.do_print:
            printed.append({"symbol": symbol, "trade_date": day, **daily})
            if prov:
                log.info("  %s provenance: %.1f%% of the tape retained, "
                         "%d records lost to collapsing, %.0f min trimmed",
                         symbol,
                         100 * prov.get("trade_retained_share", float("nan")),
                         int(prov.get("records_lost_to_collapse", 0)),
                         prov.get("auction_minutes_trimmed", 0.0))
            return
        if args.replace:
            db.delete_symbol_day(day, symbol)
        rows_written += db.write_daily_metrics(day, symbol, daily)
        rows_written += db.write_provenance(day, symbol, prov)
        if with_intraday:
            rows_written += db.write_intraday_metrics(day, symbol, buckets)
            months_touched.add(db.month_start(day))
        days_written.add(day)

    def progress(done: int) -> None:
        if done % 25 and done != len(units):
            return
        elapsed = time.monotonic() - t0
        rate = done / max(elapsed, 1e-9)
        log.info("  %d/%d  %.2f symbol-days/s  eta %.0fs  (%d failed)",
                 done, len(units), rate,
                 (len(units) - done) / max(rate, 1e-9), failed)

    if workers <= 1:
        for i, unit in enumerate(units, 1):
            handle(unit[0], unit[1], _compute_unit(unit))
            progress(i)
    else:
        # Metric computation is CPU-bound, so this is processes rather than
        # threads — the GIL would serialise a thread pool exactly where the
        # time is being spent.
        with ProcessPoolExecutor(max_workers=workers,
                                 initializer=_worker_init) as pool:
            futures = {pool.submit(_compute_unit, u): u for u in units}
            for i, fut in enumerate(as_completed(futures), 1):
                symbol, day = futures[fut][0], futures[fut][1]
                try:
                    payload = fut.result()
                except Exception as exc:          # worker died outright
                    payload = (None, [], {}, f"{type(exc).__name__}: {exc}")
                handle(symbol, day, payload)
                progress(i)

    elapsed = time.monotonic() - t0
    log.info("%d computed, %d skipped, %d failed in %.1f min (%.2f s/symbol-day)",
             computed, skipped, failed, elapsed / 60,
             elapsed / max(computed, 1))

    # --- monthly rollup -----------------------------------------------------
    # Written on every run, from day one. This is the ONLY irreversible
    # decision in the intraday design: intraday_metrics is kept 14 days, so a
    # month not aggregated while its raw parquet is still inside
    # RAW_RETENTION_DAYS can never be reconstructed. Added a year from now it
    # would start empty.
    if months_touched and not args.do_print:
        for month in sorted(months_touched):
            n = db.upsert_intraday_monthly(month, month)
            log.info("intraday_monthly %s: %d (symbol, bucket) row(s)",
                     month, n)

    # --- vacuum after --replace ---------------------------------------------
    # PLAIN VACUUM, never FULL. A --replace writes the new tuples before the
    # old ones are reclaimable, so the day's partition carries roughly twice
    # its own size in dead tuples afterwards. Plain VACUUM marks that space
    # reusable in place and needs no extra room; FULL rewrites the table and
    # needs free space equal to it, which is exactly what was unavailable when
    # the disk filled.
    if args.replace and days_written and not args.do_print:
        for day in sorted(days_written):
            name = db.partition_name(day)
            try:
                db.vacuum(name)
            except Exception as exc:
                log.warning("VACUUM %s failed: %s", name, exc)
                continue
            stats = db.table_stats(name)
            if stats:
                log.info("%s: %s live, %s dead, last_autovacuum=%s",
                         name, stats.get("n_live_tup"), stats.get("n_dead_tup"),
                         stats.get("last_autovacuum"))
        db.vacuum("daily_metrics")
        stats = db.table_stats("daily_metrics")
        if stats:
            log.info("daily_metrics: %s live, %s dead, last_autovacuum=%s",
                     stats.get("n_live_tup"), stats.get("n_dead_tup"),
                     stats.get("last_autovacuum"))

    if args.do_print and printed:
        frame = pd.DataFrame(printed)
        keep = [c for c in frame.columns
                if not c.endswith("__buckets") and c not in
                ("window_start", "window_end")]
        with pd.option_context("display.max_columns", None,
                               "display.width", 250,
                               "display.float_format", "{:,.3f}".format):
            print(frame[keep].to_string(index=False))
        return

    print()
    print(f"computed {computed} symbol-days, wrote {rows_written:,} metric rows")
    if failed:
        print(f"{failed} symbol-days failed — see the warnings above")


if __name__ == "__main__":
    main()
