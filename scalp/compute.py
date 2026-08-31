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

Postgres holds derived metrics only. No tick data ever.
"""
from __future__ import annotations

import argparse
import logging
from datetime import date

import pandas as pd

from scalp import config, db, metrics, schema, store

log = logging.getLogger(__name__)


def session_bounds(day: date) -> tuple[pd.Timestamp, pd.Timestamp]:
    """RTH for one session, as naive timestamps matching the vendor's."""
    return (pd.Timestamp(f"{day.isoformat()} {config.RTH_START}"),
            pd.Timestamp(f"{day.isoformat()} {config.RTH_END}"))


def prepare(df: pd.DataFrame, day: date) -> tuple[pd.DataFrame, metrics.Columns]:
    """Resolve columns and parse timestamps once per symbol-day."""
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
    return out, cols


def compute_symbol_day(symbol: str, day: date) -> tuple[dict | None, list[dict]]:
    df = store.read_day(symbol, day)
    if df.empty:
        return None, []
    df, cols = prepare(df, day)
    start, end = session_bounds(day)

    daily = metrics.compute_window(df, cols, start, end)
    daily["symbol_day_rows"] = len(df)
    buckets = metrics.compute_buckets(df, cols, start, end,
                                      config.INTRADAY_BUCKET_MINUTES)
    return daily, buckets


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--symbols", default=None,
                    help="comma-separated; default is everything on disk")
    ap.add_argument("--no-intraday", action="store_true",
                    help="skip the 15-minute rows")
    ap.add_argument("--print", dest="do_print", action="store_true",
                    help="print the daily metrics instead of writing Postgres")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

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

    rows_written = 0
    computed = skipped = failed = 0
    printed: list[dict] = []

    for day in days:
        for symbol in symbols:
            if not store.has_day(symbol, day):
                skipped += 1
                continue
            try:
                daily, buckets = compute_symbol_day(symbol, day)
            except Exception as exc:
                failed += 1
                log.warning("  %s %s: %s: %s", symbol, day,
                            type(exc).__name__, exc)
                continue
            if daily is None:
                skipped += 1
                continue
            computed += 1

            if args.do_print:
                printed.append({"symbol": symbol, "trade_date": day, **daily})
                continue

            rows_written += db.write_daily_metrics(day, symbol, daily)
            if not args.no_intraday:
                rows_written += db.write_intraday_metrics(day, symbol, buckets)

        log.info("%s: %d computed, %d skipped, %d failed",
                 day, computed, skipped, failed)

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
