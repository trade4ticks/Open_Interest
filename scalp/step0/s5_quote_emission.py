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

HOW TO READ IT — GAP STRUCTURE OUTRANKS REPEAT RATE.

The first run of this script got the verdict wrong, and the mistake is worth
keeping written down. It saw 80.1% of consecutive records identical and
concluded periodic sampling — while its own gap distribution said the
opposite: p50 1 ms, p90 448 ms, max 21,523 ms, mean 175 ms. A sampler produces
near-constant gaps. Four orders of magnitude between median and maximum is not
a clock, and no repeat rate outweighs that.

THE VENUE HYPOTHESIS WAS TESTED AND MOSTLY REJECTED. The proposed explanation
was that the comparison omitted `bid_exchange` and `ask_exchange`, so a
different venue taking over at the same price and size would look like a
duplicate. Measured, venue turnover accounts for only 1.7 points: 80.1% ->
78.4%. So 78.4% of records are identical on price, size AND venue, and that
remains unexplained.

The verdict is unaffected — it rests on the gap structure, not on the repeats.

Most likely remaining cause: the NBBO is recomputed on every participant's
quote update, not only when the best changes. A venue behind the inside
adjusting its quote fires a record while the NBBO is unchanged. This endpoint
returns the NBBO, so that cause is not visible in the columns available and
cannot be confirmed from this data.

If that is right, the raw record count measures TOTAL QUOTE TRAFFIC across all
venues rather than inside-market instability. Still book activity, still
possibly useful — but a different quantity from the flicker metric as
intended, which is why both are computed (config.FLICKER_VARIANTS) and
calibration decides.

This decides whether the flicker metric survives:

  * EVENT-DRIVEN -> quote-records-per-minute IS book activity. Use the count
    directly. Rebuilding it on "observed changes" would discard the
    venue-turnover events that make a book re-form, which is the behaviour the
    metric exists to capture.
  * PERIODIC -> the count measures the sampling rate and is meaningless as a
    flicker metric; rebuild it on genuine changes including venue.

WHAT A BAD RESULT LOOKS LIKE
  * Gaps tightly clustered at one value -> genuinely periodic. Rebuild the
    flicker metric and say so in the definitions.
  * No bid_exchange/ask_exchange columns -> venue turnover cannot be separated
    from true duplication and the identical rate is an upper bound, not a
    measurement. Say that rather than reporting the number as if it were one.
  * No bid_size/ask_size columns -> the separate bid-side/ask-side noise
    metrics cannot distinguish a size change from a price change.
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
    bid = c.find_column(df, c.CAND_BID, "bid")
    ask = c.find_column(df, c.CAND_ASK, "ask")
    bsz = c.find_column(df, c.CAND_BID_SIZE, "bid size", required=False)
    asz = c.find_column(df, c.CAND_ASK_SIZE, "ask size", required=False)
    tim = c.find_column(df, c.CAND_QUOTE_TIME, "timestamp", required=False)
    bex = c.find_column(df, c.CAND_BID_EXCH, "bid exchange", required=False)
    aex = c.find_column(df, c.CAND_ASK_EXCH, "ask exchange", required=False)
    return bid, ask, bsz, asz, tim, bex, aex


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
    bid, ask, bsz, asz, tim, bex, aex = _resolve(df)

    b = pd.to_numeric(df[bid], errors="coerce")
    a = pd.to_numeric(df[ask], errors="coerce")

    price_same = (b.diff() == 0) & (a.diff() == 0)
    n = len(df) - 1
    if n <= 0:
        c.die("Fewer than two records — nothing to compare.")

    # Progressive comparison. Each level adds one more field to the identity
    # test, so the drop between levels says WHY a record that looked like a
    # duplicate is actually new information.
    c.section("consecutive-record comparison, progressively stricter")
    print(f"records                            : {len(df):,}")
    print()
    print(f"{'identical on':<36s} {'count':>9s} {'rate':>8s}")
    print("-" * 56)
    print(f"{'price (bid, ask)':<36s} {int(price_same.sum()):>9,} "
          f"{100 * price_same.sum() / n:>7.1f}%")

    same = price_same
    size_same = None
    if bsz and asz:
        bs = pd.to_numeric(df[bsz], errors="coerce")
        as_ = pd.to_numeric(df[asz], errors="coerce")
        size_same = (bs.diff() == 0) & (as_.diff() == 0)
        same = price_same & size_same
        print(f"{'+ size (bid_size, ask_size)':<36s} {int(same.sum()):>9,} "
              f"{100 * same.sum() / n:>7.1f}%")

    venue_same = None
    if bex and aex:
        # THE FIELD THE FIRST RUN MISSED. NBBO records fire when any
        # participant updates. A different venue taking over the same price
        # and size is a genuinely new record in which every previously
        # compared field is unchanged — it looks like a duplicate and is not.
        venue_same = (df[bex].eq(df[bex].shift())
                      & df[aex].eq(df[aex].shift()))
        same_with_venue = same & venue_same
        print(f"{'+ venue (bid_exchange, ask_exchange)':<36s} "
              f"{int(same_with_venue.sum()):>9,} "
              f"{100 * same_with_venue.sum() / n:>7.1f}%")
        drop = 100 * (same.sum() - same_with_venue.sum()) / n
        print()
        print(f"Adding venue drops the identical rate by {drop:.1f} points.")
        print("Those records differ ONLY in which participant is posting the")
        print("best price — real book events that the earlier comparison")
        print("counted as duplicates.")
        same = same_with_venue
    else:
        print()
        print("No bid_exchange/ask_exchange columns — venue turnover cannot be")
        print("separated from true duplication, and the identical rate below is")
        print("an UPPER BOUND, not a measurement.")

    identical_pct = 100 * same.sum() / n

    if size_same is not None:
        size_only = price_same & ~size_same
        print()
        print(f"size changed, price unchanged      : {int(size_only.sum()):,} "
              f"({100 * size_only.sum() / n:.1f}%)")

    # The asymmetry the strategy is built around.
    bid_moved = b.diff() != 0
    ask_moved = a.diff() != 0
    both = int((bid_moved & ask_moved).sum())
    bid_only = int((bid_moved & ~ask_moved).sum())
    ask_only = int((ask_moved & ~bid_moved).sum())
    c.section("one-sided vs two-sided repricing")
    print(f"bid moved, ask still          : {bid_only:,} "
          f"({100 * bid_only / n:.1f}%)")
    print(f"ask moved, bid still          : {ask_only:,} "
          f"({100 * ask_only / n:.1f}%)")
    print(f"both moved                    : {both:,} "
          f"({100 * both / n:.2f}%)")
    if both and (bid_only + ask_only):
        print(f"one-sided : two-sided         : "
              f"{(bid_only + ask_only) / both:,.0f} : 1")
    print()
    print("Two-sided repricing being rare is the empirical justification for")
    print("computing bid-side and ask-side noise separately. If the mid only")
    print("ever moves because one side twitched, a mid-based noise number")
    print("averages away the asymmetry that defines the trade.")

    # --- gap structure ------------------------------------------------------
    # This is the DECISIVE evidence, and it outranks the repeat rate. Periodic
    # sampling means a clock, and a clock produces near-constant gaps. Nothing
    # about a repeated payload can outweigh a gap distribution that spans four
    # orders of magnitude.
    gap_ratio = None
    gap_p50 = gap_max = None
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

            gap_p50, gap_max = vals[3], vals[-1]
            gap_ratio = vals[-2] / max(vals[3], 1e-9)      # p99 / p50
            print()
            print(f"  p99/p50 ratio : {gap_ratio:,.1f}")
            print(f"  max/median    : {gap_max / max(gap_p50, 1e-9):,.1f}")
            print("  Near 1 means a fixed clock (periodic sampling).")
            print("  Orders of magnitude means event-driven emission.")
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
    print(f"identical-record rate (strictest test) : {identical_pct:.1f}%")
    if gap_ratio is not None:
        print(f"gap p99/p50                            : {gap_ratio:,.1f}")
        print(f"gap median / max                       : "
              f"{gap_p50:,.0f} ms / {gap_max:,.0f} ms")
    print()

    if gap_ratio is not None and gap_ratio > 10:
        print("EVENT-DRIVEN emission. A record is written when something")
        print("changes; there is no clock.")
        print()
        print("The gap distribution decides this, not the repeat rate. A")
        print("sampler produces near-constant gaps, and a median of "
              f"{gap_p50:,.0f} ms against a maximum of {gap_max:,.0f} ms is not")
        print("a clock by any reading.")
        if identical_pct > 30:
            print()
            print(f"The {identical_pct:.1f}% identical rate is therefore NOT")
            print("evidence of sampling. Those records carry a real event whose")
            print("visible fields happen to match — most often venue turnover,")
            print("where a different participant takes over at the same price")
            print("and size.")
        print()
        print("Consequences:")
        print("  * 'quote updates per minute' IS book activity — the flicker")
        print("    metric measured directly. Use the record count. Do NOT")
        print("    rebuild it on 'observed changes': that would discard exactly")
        print("    the venue-turnover events that make a book re-form, which is")
        print("    the behaviour the metric is meant to capture.")
        print("  * Time-weighting needs FORWARD-FILL: each quote is in force")
        print("    until the next record, so its weight is the gap to the")
        print("    following record, not a uniform 1.")
    elif gap_ratio is not None:
        print("PERIODIC SAMPLING — gaps are near-constant, which means a clock.")
        print()
        print("Consequences:")
        print("  * 'quote updates per minute' measures the sampling rate, not")
        print("    book activity. Rebuild the flicker metric on OBSERVED")
        print("    CHANGES — consecutive records that genuinely differ,")
        print("    including venue.")
        print("  * Time-weighting still works: each record persists to the next.")
    else:
        print("Gap structure unreadable, so emission mode is UNDETERMINED.")
        print("Do not infer it from the identical rate alone — that is the")
        print("mistake this section exists to prevent.")

    print()
    print("Either way, the 10-second noise buckets are built by weighting each")
    print("quote by its own duration inside the bucket — which is what makes a")
    print("40-share bid appearing and vanishing average out instead of moving")
    print("the reported midpoint five cents.")


if __name__ == "__main__":
    main()
