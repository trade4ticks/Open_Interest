"""Step 0.2 — one symbol, one day of trade_quote: shape, time and size.

Answers four things the storage and runtime plan depend on:

  * How many rows is a liquid symbol-day?
  * What columns does the vendor ACTUALLY return? (Not what the docs say.)
  * Wall clock for one symbol-day — the unit the 544-symbol loop multiplies.
  * Parquet size on disk, zstd vs snappy.

The brief's storage estimate (~1-1.5 MB per symbol-day, 544 x 10 days ~ 5-8 GB)
is explicitly unverified. This verifies it.

    python -m scalp.step0.s2_one_day
    python -m scalp.step0.s2_one_day --symbol LLY --date 2026-08-28

WHAT A BAD RESULT LOOKS LIKE
  * No bid/ask columns          -> this is not trade-paired NBBO and the whole
                                   spread metric set has no source. Stop.
  * Wall clock over ~60s        -> 544 symbols serially is over 9 hours. s3
                                   decides whether concurrency is needed, but
                                   this is the first warning.
  * Parquet far above 1.5 MB    -> re-plan storage before any backfill runs;
                                   at 544 x 10 days the multiplier is 5,440.
  * Rows only from one exchange -> go back to s1; the venue question is not
                                   actually settled.
"""
from __future__ import annotations

import argparse

import pandas as pd

from scalp import config, thetadata as td
from scalp.step0 import _common as c


BID_CANDIDATES = c.CAND_BID
ASK_CANDIDATES = c.CAND_ASK
PRICE_CANDIDATES = c.CAND_TRADE_PRICE


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--symbol", default=config.VENUE_CHECK_SYMBOL)
    ap.add_argument("--date", default=config.VENUE_CHECK_DATE)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    c.setup_logging(args.verbose)
    c.banner(f"STEP 0.2 — {args.symbol} trade_quote, one day ({args.date})")
    c.env_summary()

    raw = td.trade_quote(args.symbol, args.date, args.date, total_timeout=900)
    c.describe_response(raw)

    t0 = pd.Timestamp.now()
    df = raw.frame()
    parse_secs = (pd.Timestamp.now() - t0).total_seconds()
    print(f"csv -> arrow  : {parse_secs:.2f} s")
    print(f"total         : {raw.seconds + parse_secs:.2f} s per symbol-day")
    print()
    print(f"Serial extrapolation, 544 symbols: "
          f"{(raw.seconds + parse_secs) * 544 / 60:.1f} minutes")

    if df.empty:
        c.die("Empty response. Rerun with --date on a different regular "
              "session before drawing any conclusion.")

    c.describe_frame(df, sample=5)

    # --- does this frame actually support the metric set? -------------------
    c.section("required fields for the metric set")
    bid = c.find_column(df, BID_CANDIDATES, "NBBO bid", required=False)
    ask = c.find_column(df, ASK_CANDIDATES, "NBBO ask", required=False)
    price = c.find_column(df, PRICE_CANDIDATES, "trade price", required=False)
    size = c.find_column(df, c.CAND_TRADE_SIZE, "trade size", required=False)
    bid_size = c.find_column(df, c.CAND_BID_SIZE, "bid size", required=False)
    ask_size = c.find_column(df, c.CAND_ASK_SIZE, "ask size", required=False)

    if not (bid and ask):
        c.die("No bid/ask columns in the response. This endpoint is not "
              "returning trade-paired NBBO, and every spread and midpoint "
              "metric in the design has no source. Stop and re-scope before "
              "writing anything else.")

    if bid and ask:
        b = pd.to_numeric(df[bid], errors="coerce")
        a = pd.to_numeric(df[ask], errors="coerce")
        spread = a - b
        mid = (a + b) / 2
        valid = spread.notna() & (mid > 0)
        c.section("sanity: quoted spread as returned")
        print(f"rows with usable bid/ask : {int(valid.sum()):,} of {len(df):,}")
        print(f"crossed/locked (ask<=bid): {int((spread[valid] <= 0).sum()):,}")
        print(f"spread cents  median     : {spread[valid].median() * 100:.2f}")
        print(f"spread cents  mean       : {spread[valid].mean() * 100:.2f}")
        sp_bps = (spread[valid] / mid[valid]) * 10_000
        print(f"spread bps    median     : {sp_bps.median():.2f}")
        print(f"spread bps    mean       : {sp_bps.mean():.2f}")
        print()
        print("This is an unweighted per-trade spread — NOT the time-weighted")
        print("number the ranking uses. It is here only to confirm the values")
        print("are plausible. A median far below 5 cents on a name known to")
        print("quote wider means the fields are being read wrongly.")

    if price and size:
        p = pd.to_numeric(df[price], errors="coerce")
        s = pd.to_numeric(df[size], errors="coerce")
        c.section("sanity: trade tape")
        print(f"total shares             : {int(s.fillna(0).sum()):,}")
        print(f"trades                   : {len(df):,}")
        print(f"median trade size        : {s.median():,.0f}")
        print(f"price range              : {p.min():.2f} .. {p.max():.2f}")

    if bid_size and ask_size:
        print()
        print("bid_size/ask_size present — depth is available for the "
              "flicker metrics without a second endpoint.")

    # --- storage ------------------------------------------------------------
    sizes = c.measure_parquet(df, f"{args.symbol}_{args.date}_trade_quote")
    c.report_parquet_sizes(sizes, n_rows=len(df), symbol_days=1)
    print()
    print(f"Files written under {config.STEP0_DIR} — safe to delete.")


if __name__ == "__main__":
    main()
