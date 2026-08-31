"""Step 0.3 — how long does a 544-symbol backfill actually take?

Times a 10-day multi-day trade_quote pull for one symbol. Multiplied by 544
that gives the initial-fetch runtime, and tells us whether concurrency is
needed at all or whether a plain serial loop is fine.

Also answers a second question worth knowing before fetch.py is written: is
one 10-day call cheaper than ten 1-day calls? The endpoint supports multi-day
up to a month. If the multi-day call is meaningfully faster it shapes the fetch
loop; if it is slower or fails with 570, the loop iterates days.

    python -m scalp.step0.s3_multiday_timing
    python -m scalp.step0.s3_multiday_timing --symbol LLY --days 10 --compare-serial

WHAT A BAD RESULT LOOKS LIKE
  * Serial 544-symbol estimate over ~4 hours -> a nightly incremental run is
    still fine (one day, not ten), but the FIRST backfill needs concurrency.
  * HTTP 570 on the 10-day call -> the window must be split; note the row count
    at which it broke, that is the real cap regardless of the documented month.
  * Multi-day slower than day-by-day -> fetch.py iterates days, which also
    makes resumability finer-grained. Not a bad outcome, just a design input.
  * Memory blowing up on one response -> the fetch loop must stream to parquet
    per day rather than holding a frame per symbol.
"""
from __future__ import annotations

import argparse
import time
from datetime import timedelta

import pandas as pd

from scalp import config, thetadata as td
from scalp.step0 import _common as c


UNIVERSE_SIZE = 544


def _fmt_duration(seconds: float) -> str:
    if seconds < 90:
        return f"{seconds:.0f}s"
    if seconds < 5400:
        return f"{seconds / 60:.1f} min"
    return f"{seconds / 3600:.2f} hours"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--symbol", default=config.VENUE_CHECK_SYMBOL)
    ap.add_argument("--end-date", default=config.VENUE_CHECK_DATE,
                    help="last day of the window (YYYY-MM-DD)")
    ap.add_argument("--days", type=int, default=10,
                    help="calendar days back from --end-date")
    ap.add_argument("--compare-serial", action="store_true",
                    help="also pull the same window one day at a time")
    ap.add_argument("--universe-size", type=int, default=UNIVERSE_SIZE)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    c.setup_logging(args.verbose)
    end = td.parse_date(args.end_date)
    start = end - timedelta(days=args.days - 1)

    c.banner(f"STEP 0.3 — {args.symbol} trade_quote, {start} .. {end}")
    c.env_summary()
    print()
    print(f"window        : {args.days} calendar days")
    print(f"universe      : {args.universe_size} symbols")

    # --- one multi-day call -------------------------------------------------
    c.section("A. single multi-day call")
    try:
        raw = td.trade_quote(args.symbol, start, end, total_timeout=1800)
    except td.LargeRequestError as exc:
        print(f"  HTTP 570 — response too large for a {args.days}-day window.")
        print(f"  body: {exc.body[:300]}")
        print()
        print("  FINDING: the documented one-month cap is not reachable for a")
        print("  liquid name. fetch.py must split the window. Rerun with a")
        print("  smaller --days to find where it breaks.")
        return
    except Exception as exc:
        c.report_error(exc, "multi-day pull")
        c.die("Multi-day pull failed — see above.")

    c.describe_response(raw)
    t0 = time.monotonic()
    df = raw.frame()
    parse_secs = time.monotonic() - t0
    multi_total = raw.seconds + parse_secs

    print(f"csv -> arrow  : {parse_secs:.2f} s")
    print(f"rows          : {len(df):,}")
    print(f"total         : {multi_total:.2f} s")

    if df.empty:
        c.die("Empty response over a 10-day window — rerun with a window "
              "that certainly contains regular sessions.")

    # Sessions actually present tells us the real per-symbol-day cost, since
    # a calendar window contains weekends and holidays.
    time_col = c.find_column(df, ["date"] + c.CAND_TRADE_TIME,
                             "date/time", required=False)
    sessions = None
    if time_col:
        col = df[time_col]
        if time_col.lower() == "date":
            sessions = col.nunique()
        else:
            parsed = pd.to_datetime(col, errors="coerce")
            if not parsed.isna().all():
                sessions = parsed.dt.date.nunique()
    if sessions:
        print(f"sessions      : {sessions} (of {args.days} calendar days)")
        print(f"per symbol-day: {multi_total / sessions:.2f} s, "
              f"{len(df) / sessions:,.0f} rows")

    sizes = c.measure_parquet(df, f"{args.symbol}_{start}_{end}_trade_quote")
    c.report_parquet_sizes(sizes, n_rows=len(df), symbol_days=sessions or args.days)

    # --- day-by-day comparison ---------------------------------------------
    serial_total = None
    if args.compare_serial:
        c.section("B. same window, one call per day")
        day = start
        serial_total = 0.0
        serial_rows = 0
        n_calls = 0
        while day <= end:
            try:
                r = td.trade_quote(args.symbol, day, day, total_timeout=900)
            except td.NoDataError:
                print(f"  {day}: no data (weekend/holiday)")
                day += timedelta(days=1)
                continue
            except Exception as exc:
                c.report_error(exc, f"day {day}")
                day += timedelta(days=1)
                continue
            t0 = time.monotonic()
            d = r.frame()
            elapsed = r.seconds + (time.monotonic() - t0)
            serial_total += elapsed
            serial_rows += len(d)
            n_calls += 1
            print(f"  {day}: {len(d):>9,} rows  {elapsed:>6.2f}s  "
                  f"{c.fmt_bytes(r.nbytes)}")
            day += timedelta(days=1)

        print()
        print(f"day-by-day total : {serial_total:.2f} s over {n_calls} calls")
        print(f"multi-day total  : {multi_total:.2f} s over 1 call")
        print(f"rows match       : {serial_rows:,} vs {len(df):,} "
              f"({'YES' if serial_rows == len(df) else 'NO — INVESTIGATE'})")
        if serial_rows != len(df):
            print()
            print("  Row counts differing between one 10-day call and ten")
            print("  1-day calls is a real finding, not rounding. One of the")
            print("  two is dropping data and it must be understood before")
            print("  either shape is used for a backfill.")

    # --- extrapolation ------------------------------------------------------
    c.banner("EXTRAPOLATION")
    n = args.universe_size
    per_symbol = multi_total
    print(f"Basis: {per_symbol:.2f} s per symbol for a {args.days}-day window.")
    print()
    print(f"{'concurrency':<14s} {'initial backfill (' + str(args.days) + 'd)':>28s}")
    print("-" * 46)
    for conc in (1, 2, 4, 8):
        # Above the terminal's HTTP_CONCURRENCY the vendor documents timeouts
        # rather than clean rejections, so anything past the cap is a
        # hypothetical, not a plan.
        note = "" if conc <= td.max_connections() else "  (exceeds connection cap)"
        print(f"{conc:<14d} {_fmt_duration(per_symbol * n / conc):>28s}{note}")

    if sessions:
        nightly = per_symbol / sessions
        print()
        print(f"Nightly incremental (1 session, {n} symbols):")
        for conc in (1, 2, 4):
            print(f"  concurrency {conc}: {_fmt_duration(nightly * n / conc)}")
        print()
        print("The nightly run is the one that has to finish before morning.")
        print("The initial backfill can take all night and be resumed.")

    print()
    print(f"Connection cap is {td.max_connections()} (vendor guidance: match the")
    print("terminal's HTTP_CONCURRENCY, default 4). Exceeding it is documented")
    print("to cause timeouts rather than clean rejections.")


if __name__ == "__main__":
    main()
