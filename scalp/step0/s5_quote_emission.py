"""Step 0.5 — are quote records emitted on every update, or only on change?

This decides how time-weighting is implemented, and it also decides whether
"quote updates per minute" is a real flicker measure or an artifact of the
vendor's emission policy.

  * If records are EVENT-DRIVEN (emitted only when something changes), each
    record persists until the next one and duration weighting is simply the
    gap to the following record. Updates per minute is then a genuine measure
    of book activity.

  * If records are PERIODIC SAMPLES (emitted on a clock regardless of change),
    consecutive records will frequently be identical, gaps will be regular, and
    updates per minute measures the sampling rate, not the book. Duration
    weighting still works but the flicker metric has to be rebuilt on observed
    changes instead of on record count.

A third case matters for this strategy specifically: records that change only
in SIZE, with the bid and ask prices unmoved. Those are exactly the events an
unstable-bid-against-a-still-offer looks like, and whether the feed reports
them determines whether the separate bid-side and ask-side noise metrics can
be built at all.

    python -m scalp.step0.s5_quote_emission
    python -m scalp.step0.s5_quote_emission --symbol LITE --start-time 10:00:00 --end-time 10:30:00

WHAT A BAD RESULT LOOKS LIKE
  * >30% of consecutive records fully identical -> periodic sampling. Rebuild
    the flicker metric on observed changes and say so in the definitions.
  * Gaps tightly clustered at one value -> same conclusion, more decisively.
  * No bid_size/ask_size columns -> the separate bid-side/ask-side noise
    metrics cannot distinguish a size change from a price change, and the
    asymmetry the strategy cares about is only partly observable.
  * interval=tick rejected -> sub-second work is impossible on this
    subscription; the 5s noise horizon is the floor and that needs saying
    before the metric set is finalised.
"""
from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

from scalp import config, thetadata as td
from scalp.step0 import _common as c


def _resolve(df: pd.DataFrame):
    c.section("column resolution")
    bid = c.find_column(df, ["bid", "bid_price"], "bid")
    ask = c.find_column(df, ["ask", "ask_price"], "ask")
    bsz = c.find_column(df, ["bid_size", "bidsize"], "bid size", required=False)
    asz = c.find_column(df, ["ask_size", "asksize"], "ask size", required=False)
    tim = c.find_column(df, ["ms_of_day", "timestamp", "time", "datetime"],
                        "timestamp", required=False)
    return bid, ask, bsz, asz, tim


def _gaps_ms(df: pd.DataFrame, tim: str) -> pd.Series | None:
    """Inter-record gaps in milliseconds, or None if the column can't be read."""
    col = df[tim]
    if pd.api.types.is_numeric_dtype(col):
        lo, hi = col.min(), col.max()
        if 0 <= lo and hi <= 86_400_000:          # ms since ET midnight
            return col.diff().dropna()
        return None
    parsed = pd.to_datetime(col, errors="coerce")
    if parsed.isna().all():
        return None
    return parsed.diff().dt.total_seconds().mul(1000).dropna()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--symbol", default=config.VENUE_CHECK_SYMBOL)
    ap.add_argument("--date", default=config.VENUE_CHECK_DATE)
    ap.add_argument("--start-time", default="10:00:00")
    ap.add_argument("--end-time", default="10:30:00")
    ap.add_argument("--interval", default="tick")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    c.setup_logging(args.verbose)
    c.banner(f"STEP 0.5 — QUOTE EMISSION ({args.symbol} {args.date} "
             f"{args.start_time}-{args.end_time} @{args.interval})")
    c.env_summary()
    print()
    print("Bounded to a 30-minute window by default: a full day of tick quotes")
    print("on a liquid name is the largest response either project produces,")
    print("and the emission question is answerable from any busy half hour.")
    print()
    print("Single-day request — sub-1m intervals are single-day only.")

    try:
        raw = td.quote(args.symbol, args.date, args.date,
                       interval=args.interval,
                       start_time=args.start_time, end_time=args.end_time,
                       total_timeout=900)
    except td.BadRequestError as exc:
        print(f"\nHTTP 400: {exc.body[:300]}")
        print()
        print("If this rejected interval=tick, sub-second work is not available")
        print("on this subscription and the 5s noise horizon becomes the floor.")
        print("Rerun with --interval 1m to confirm the endpoint itself works.")
        return
    except Exception as exc:
        c.report_error(exc, "quote pull")
        c.die("Quote pull failed — see above.")

    c.describe_response(raw)
    df = raw.frame()
    if df.empty:
        c.die("Empty response — try a different window or date.")

    c.describe_frame(df, sample=5)
    bid, ask, bsz, asz, tim = _resolve(df)

    b = pd.to_numeric(df[bid], errors="coerce")
    a = pd.to_numeric(df[ask], errors="coerce")

    price_same = (b.diff() == 0) & (a.diff() == 0)
    n = len(df) - 1
    if n <= 0:
        c.die("Fewer than two records — nothing to compare.")

    c.section("consecutive-record comparison")
    print(f"records                       : {len(df):,}")
    print(f"BBO price unchanged           : {int(price_same.sum()):,} "
          f"({100 * price_same.sum() / n:.1f}%)")

    if bsz and asz:
        bs = pd.to_numeric(df[bsz], errors="coerce")
        as_ = pd.to_numeric(df[asz], errors="coerce")
        size_same = (bs.diff() == 0) & (as_.diff() == 0)
        fully_same = price_same & size_same
        size_only = price_same & ~size_same
        print(f"size unchanged                : {int(size_same.sum()):,} "
              f"({100 * size_same.sum() / n:.1f}%)")
        print(f"FULLY identical to previous   : {int(fully_same.sum()):,} "
              f"({100 * fully_same.sum() / n:.1f}%)")
        print(f"size changed, price unchanged : {int(size_only.sum()):,} "
              f"({100 * size_only.sum() / n:.1f}%)")

        # The asymmetry the strategy is built around.
        bid_moved = b.diff() != 0
        ask_moved = a.diff() != 0
        print()
        print(f"bid moved, ask still          : "
              f"{int((bid_moved & ~ask_moved).sum()):,} "
              f"({100 * (bid_moved & ~ask_moved).sum() / n:.1f}%)")
        print(f"ask moved, bid still          : "
              f"{int((ask_moved & ~bid_moved).sum()):,} "
              f"({100 * (ask_moved & ~bid_moved).sum() / n:.1f}%)")
        print(f"both moved                    : "
              f"{int((bid_moved & ask_moved).sum()):,} "
              f"({100 * (bid_moved & ask_moved).sum() / n:.1f}%)")
        print()
        print("One-sided moves dominating is the case the midpoint destroys by")
        print("construction — it is the reason bid-side and ask-side noise are")
        print("computed separately rather than folded into a mid.")
        fully_pct = 100 * fully_same.sum() / n
    else:
        print()
        print("No size columns — cannot distinguish a pure duplicate from a")
        print("size-only change. The bid-side/ask-side asymmetry is only")
        print("partly observable on this feed.")
        fully_pct = 100 * price_same.sum() / n

    # --- gap structure ------------------------------------------------------
    if tim:
        gaps = _gaps_ms(df, tim)
        if gaps is not None and len(gaps):
            c.section("inter-record gaps (ms)")
            qs = [0, 10, 25, 50, 75, 90, 99, 100]
            vals = np.percentile(gaps, qs)
            for q, v in zip(qs, vals):
                print(f"  p{q:<3d} : {v:>12,.1f} ms")
            print(f"  mean : {gaps.mean():>12,.1f} ms")
            zero = int((gaps == 0).sum())
            print(f"  identical timestamps : {zero:,} "
                  f"({100 * zero / len(gaps):.1f}%)")
            spread = vals[-2] / max(vals[1], 1e-9)     # p99 / p10
            print()
            print(f"  p99/p10 ratio : {spread:,.1f}")
            print("  A ratio near 1 means a fixed clock (periodic sampling).")
            print("  A wide ratio means event-driven emission.")
            if zero:
                print()
                print("  Identical timestamps mean multiple records share an")
                print("  instant. Duration weighting must collapse those to a")
                print("  single observation, or they get zero weight and the")
                print("  time-weighted midpoint silently ignores them.")
        else:
            print()
            print(f"Could not read gaps from {tim!r} — reporting record counts only.")

    # --- verdict ------------------------------------------------------------
    c.banner("VERDICT")
    if fully_pct > 30:
        print(f"{fully_pct:.1f}% of records repeat the previous one exactly.")
        print("This looks like PERIODIC SAMPLING, not change-only emission.")
        print()
        print("Consequences:")
        print("  * 'quote updates per minute' measures the sampling rate, not")
        print("    book activity. Rebuild the flicker metric on OBSERVED")
        print("    CHANGES — consecutive records that actually differ.")
        print("  * Time-weighting still works: each record persists to the")
        print("    next. No forward-fill is needed, because there are no gaps.")
    else:
        print(f"Only {fully_pct:.1f}% of records repeat the previous one exactly.")
        print("This looks like EVENT-DRIVEN emission — a record is written when")
        print("something changes.")
        print()
        print("Consequences:")
        print("  * Time-weighting needs FORWARD-FILL: each quote is in force")
        print("    until the next record, so its weight is the gap to the")
        print("    following record, not a uniform 1.")
        print("  * 'quote updates per minute' is a genuine book-activity")
        print("    measure and can be used as the flicker metric directly.")
    print()
    print("Either way, the 10-second noise buckets are built by weighting each")
    print("quote by its own duration inside the bucket — which is what makes a")
    print("40-share bid appearing and vanishing average out instead of moving")
    print("the reported midpoint five cents.")


if __name__ == "__main__":
    main()
