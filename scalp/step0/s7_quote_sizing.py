"""Step 0.7 — how big is a full-day quote-tick pull, and what does 1s cost?

Two questions, one probe, because they have to be answered together: the
interval decision changes the size by an order of magnitude and may change the
metrics by nothing.

  1. SIZE. Full-day `history/quote` at interval=tick for four symbols on one
     session. Rows, wire bytes, and parquet-on-disk at zstd per symbol-day,
     extrapolated to 544 symbols x QUOTE_LOOKBACK_DAYS.

     Any figure extrapolated from a 30-minute sample is unreliable — quote
     traffic is not uniform across the session, and the open and close carry
     far more of it than a mid-morning half hour. This measures whole days.

     LLY is in the set deliberately: it had ~3x FDX's trade count, so it
     should bound the heavy end rather than the typical one. DLTR and LITE
     span the other end of the realised results.

  2. RESOLUTION. interval=1s against interval=tick on one symbol-day. Size
     ratio, AND the flicker metrics computed both ways so the cost is visible
     rather than assumed.

     What to watch: at 1s the record count collapses to at most 86,400 a day
     by construction, so `quote_records_per_min` becomes a property of the
     sampling interval and stops measuring anything about the book. The
     question is whether `nbbo_changes_per_min` and the bid/ask change counts
     survive — those are the cancellation-and-repost signal. If they hold up,
     1s is the obvious choice. If they collapse too, tick is worth the space.

    python -m scalp.step0.s7_quote_sizing
    python -m scalp.step0.s7_quote_sizing --symbols FDX,LLY --date 2026-08-28

WHAT THE OUTCOMES MEAN
  * tick total comfortably under the free space -> pull tick, retain it.
  * tick large but 1s loses little -> pull 1s for the full universe.
  * both large, or 1s destroys the change counts -> compute-and-discard is
    worth building: pull per symbol, compute, write metrics to Postgres,
    delete the raw parquet. Peak disk is one symbol-day and raw quote ticks
    have no use beyond producing these metrics.

This writes only to STEP0_DIR (scratch, disposable). It does not touch the
production store.
"""
from __future__ import annotations

import argparse
import time

import pandas as pd

from scalp import config, metrics, schema, thetadata as td
from scalp.step0 import _common as c


UNIVERSE_SIZE = 544


def _pull(symbol: str, day: str, interval: str):
    print()
    print(f"[{symbol} {day} interval={interval}]")
    t0 = time.monotonic()
    try:
        raw = td.quote(symbol, day, day, interval=interval,
                       start_time=config.RTH_START, end_time=config.RTH_END,
                       total_timeout=1800)
    except td.LargeRequestError as exc:
        print(f"  HTTP 570 — response too large for a full session at "
              f"interval={interval}")
        print(f"  body: {exc.body[:200]}")
        print("  FINDING: a full-day tick pull does not fit in one request.")
        print("  The fetch layer would have to split by time window.")
        return None, None
    except Exception as exc:
        c.report_error(exc, f"{symbol} {interval}")
        return None, None

    df = raw.frame()
    parse_s = time.monotonic() - t0 - raw.seconds
    print(f"  {len(df):,} rows | {raw.seconds:.1f}s http | {parse_s:.1f}s parse "
          f"| {c.fmt_bytes(raw.nbytes)} wire")
    return raw, df


def _flicker(df: pd.DataFrame, day: str) -> dict:
    """Flicker metrics over the RTH window, using the production code path."""
    if df.empty:
        return {}
    tcol = schema.find(df, schema.CAND_TRADE_TIME + ["quote_timestamp"],
                       "quote timestamp")
    bid = schema.find(df, schema.CAND_BID, "bid")
    ask = schema.find(df, schema.CAND_ASK, "ask")

    work = df.copy()
    work[tcol] = schema.parse_times(work[tcol], session_date=day)
    start = pd.Timestamp(f"{day} {config.RTH_START}")
    end = pd.Timestamp(f"{day} {config.RTH_END}")

    window = metrics.slice_window(work, tcol, start, end)
    collapsed = metrics.collapse_to_distinct_instants(window, tcol)
    return metrics.flicker_metrics(window, collapsed, bid=bid, ask=ask,
                                   time_col=tcol, start=start, end=end)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--symbols", default="FDX,LLY,LITE,DLTR")
    ap.add_argument("--date", default=config.VENUE_CHECK_DATE)
    ap.add_argument("--compare-symbol", default=None,
                    help="symbol for the tick-vs-1s comparison "
                         "(default: the first of --symbols)")
    ap.add_argument("--universe-size", type=int, default=UNIVERSE_SIZE)
    ap.add_argument("--days", type=int, default=config.QUOTE_LOOKBACK_DAYS)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    c.setup_logging(args.verbose)
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    c.banner(f"STEP 0.7 — QUOTE-STREAM SIZING ({args.date})")
    c.env_summary()
    print()
    print(f"symbols        : {', '.join(symbols)}")
    print(f"extrapolate to : {args.universe_size} symbols x {args.days} days")
    print(f"free on volume : see below (production store is not touched)")

    # --- 1. full-day tick pulls --------------------------------------------
    rows = []
    for sym in symbols:
        raw, df = _pull(sym, args.date, "tick")
        if raw is None or df is None or df.empty:
            continue
        sizes = c.measure_parquet(df, f"quote_tick_{sym}_{args.date}",
                                  compressions=("zstd",))
        rows.append({
            "symbol": sym,
            "rows": len(df),
            "wire_mb": raw.nbytes / 1e6,
            "parquet_mb": sizes["zstd"] / 1e6,
            "seconds": raw.seconds,
        })

    if not rows:
        c.die("No symbol returned data — nothing to size.")

    tbl = pd.DataFrame(rows)
    c.section("full-day quote ticks, per symbol-day")
    with pd.option_context("display.float_format", "{:,.2f}".format):
        print(tbl.to_string(index=False))

    mean_mb = tbl["parquet_mb"].mean()
    max_mb = tbl["parquet_mb"].max()
    heavy = tbl.loc[tbl["parquet_mb"].idxmax(), "symbol"]

    c.section("extrapolation")
    n = args.universe_size * args.days
    print(f"symbol-days                 : {n:,}")
    print(f"at the MEAN  ({mean_mb:6.2f} MB) : "
          f"{mean_mb * n / 1024:7.2f} GB")
    print(f"at the MAX   ({max_mb:6.2f} MB) : "
          f"{max_mb * n / 1024:7.2f} GB   (every name as heavy as {heavy})")
    print()
    print("The mean is the honest estimate and the max is the ceiling. The")
    print("truth sits between: quote traffic is heavily skewed, so a")
    print("mean-based figure will understate a universe that contains many")
    print("liquid names.")
    print()
    print(f"free on block 3 now         : {config.free_space_gb():7.2f} GB")

    # --- 2. tick vs 1s ------------------------------------------------------
    cmp_symbol = (args.compare_symbol or symbols[0]).upper()
    c.banner(f"RESOLUTION: interval=tick vs interval=1s ({cmp_symbol})")

    results = {}
    for interval in ("tick", "1s"):
        raw, df = _pull(cmp_symbol, args.date, interval)
        if raw is None or df is None or df.empty:
            continue
        sizes = c.measure_parquet(df, f"quote_{interval}_{cmp_symbol}_{args.date}",
                                  compressions=("zstd",))
        try:
            flick = _flicker(df, args.date)
        except Exception as exc:
            c.report_error(exc, f"flicker {interval}")
            flick = {}
        results[interval] = {
            "rows": len(df),
            "parquet_mb": sizes["zstd"] / 1e6,
            "flicker": flick,
        }

    if len(results) == 2:
        t, s = results["tick"], results["1s"]
        c.section("size")
        print(f"{'':<14s} {'rows':>12s} {'parquet MB':>12s}")
        print(f"{'tick':<14s} {t['rows']:>12,} {t['parquet_mb']:>12.2f}")
        print(f"{'1s':<14s} {s['rows']:>12,} {s['parquet_mb']:>12.2f}")
        if s["parquet_mb"] > 0:
            print()
            print(f"1s is {t['parquet_mb'] / s['parquet_mb']:.1f}x smaller "
                  f"({t['rows'] / max(s['rows'], 1):.1f}x fewer rows)")
            print(f"full universe at 1s: "
                  f"{s['parquet_mb'] * n / 1024:.2f} GB "
                  f"(scaling this one symbol; the per-symbol table above is "
                  f"the better basis for tick)")

        c.section("what the resolution costs — flicker metrics both ways")
        keys = ["quote_records_per_min", "nbbo_changes_per_min",
                "bid_changes_per_min", "ask_changes_per_min",
                "two_sided_change_share", "same_instant_share"]
        print(f"{'metric':<28s} {'tick':>14s} {'1s':>14s} {'retained':>10s}")
        print("-" * 70)
        for k in keys:
            tv = t["flicker"].get(k, float("nan"))
            sv = s["flicker"].get(k, float("nan"))
            ratio = (sv / tv) if (tv and pd.notna(tv) and pd.notna(sv)
                                  and tv != 0) else float("nan")
            print(f"{k:<28s} {tv:>14,.2f} {sv:>14,.2f} {ratio:>9.1%}")

        print()
        print("HOW TO READ THIS")
        print("  quote_records_per_min collapsing at 1s is EXPECTED and is not")
        print("  a reason to reject 1s — one record per second is what the")
        print("  interval means. It does mean that variant stops measuring the")
        print("  book and starts measuring the sampler, so it would be dropped")
        print("  rather than stored misleadingly.")
        print()
        print("  nbbo_changes_per_min and the bid/ask change counts are the")
        print("  real question. Those are the cancellation-and-repost signal.")
        print("  If they retain most of their value at 1s, 1s is the obvious")
        print("  choice. If they collapse alongside the record count, the")
        print("  signal only exists at tick resolution and the space is the")
        print("  price of having it at all.")

    # --- 3. does compute-and-discard become necessary? ---------------------
    c.banner("DOES COMPUTE-AND-DISCARD BECOME NECESSARY?")
    free = config.free_space_gb()
    tick_gb = mean_mb * n / 1024
    print(f"projected (tick, mean basis) : {tick_gb:.2f} GB")
    print(f"free on block 3              : {free:.2f} GB")
    print()
    if tick_gb + config.FREE_SPACE_MARGIN_GB > free:
        print("The tick pull does NOT fit alongside the existing store.")
        print("Options, in the order worth considering:")
        print("  1. interval=1s, if the change counts above survived it")
        print("  2. compute-and-discard at tick resolution — pull per symbol,")
        print("     compute, write metrics, delete the parquet. Peak disk is")
        print("     one symbol-day, and raw quote ticks have no use beyond")
        print("     producing these metrics. Re-pull on a formula change.")
        print("  3. free space on block 3")
        print()
        print("NOT an option: a smaller symbol list. Flicker is an input to")
        print("the ranking, so measuring it only on names a flicker-blind")
        print("ranking already selected cannot tell us what the filter should")
        print("have been.")
    else:
        print("The tick pull fits with the margin intact. Retain it — raw")
        print("ticks cost a re-pull to recover and nothing to keep.")
    print()
    print(f"Scratch parquet written under {config.STEP0_DIR} — safe to delete.")


if __name__ == "__main__":
    main()
