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


def _compute_unit(unit: tuple[str, date]):
    """Worker entry point. Module-level so it pickles.

    Returns (daily, buckets, provenance, error) rather than raising: an
    exception crossing a process boundary loses its traceback and can fail to
    pickle at all, which turns one bad symbol-day into a dead pool.
    """
    symbol, day = unit
    try:
        daily, buckets, prov = compute_symbol_day(symbol, day)
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


def compute_symbol_day(symbol: str, day: date
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
    # compute_buckets passes with_provenance=False. Provenance is a symbol-day
    # fact and the daily row carries it; the bucket copies were built and then
    # thrown away here, at a cost of one pass per excluded code per bucket.
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

    if not args.do_print:
        db.init_schema()

    units = [(symbol, day) for day in days for symbol in symbols
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
        if not args.no_intraday:
            rows_written += db.write_intraday_metrics(day, symbol, buckets)

    def progress(done: int) -> None:
        if done % 25 and done != len(units):
            return
        elapsed = time.monotonic() - t0
        rate = done / max(elapsed, 1e-9)
        log.info("  %d/%d  %.2f symbol-days/s  eta %.0fs  (%d failed)",
                 done, len(units), rate,
                 (len(units) - done) / max(rate, 1e-9), failed)

    if workers <= 1:
        for i, (symbol, day) in enumerate(units, 1):
            handle(symbol, day, _compute_unit((symbol, day)))
            progress(i)
    else:
        # Metric computation is CPU-bound, so this is processes rather than
        # threads — the GIL would serialise a thread pool exactly where the
        # time is being spent.
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_compute_unit, u): u for u in units}
            for i, fut in enumerate(as_completed(futures), 1):
                symbol, day = futures[fut]
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
