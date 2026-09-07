"""
Where does quiet_session's time go, and on which symbols?

    python -m scalp.profile_quiet --date 2026-08-28
    python -m scalp.profile_quiet --date 2026-08-28 --top 8 --bottom 8
    python -m scalp.profile_quiet --date 2026-08-28 --symbols AAPL,SPY,DDS
    python -m scalp.profile_quiet --date 2026-08-28 --all --csv /tmp/q.csv

WHY THIS EXISTS SEPARATELY FROM profile_compute
-----------------------------------------------
A recompute ran five workers at 100% CPU for 32 minutes and wrote one
symbol-day, while profile_compute reported 5.89s for AAPL on the same box.
The two disagreed by orders of magnitude for one reason: profile_compute
predated the quiet-window metrics and never called them, so the only new code
in the run was the only code not being measured.

profile_compute now includes the quiet path. This tool goes further and
answers the question that one cannot: WHICH SYMBOLS, and what about them.
A per-symbol-day cost that is fine on AAPL and pathological elsewhere is not
visible from a single-symbol profile at all.

The cost model, so the columns mean something. Each window length runs one
searchsorted over the trade times (cheap, O(W log n)), two prefix sums
(O(n)), and then ONE np.percentile per eligible window over that window's
slice. The last term is the only one that can misbehave: it is

    sum over windows of (trades in window) x log(trades in window)

and since every trade falls in about window/step = 3 windows, the sum is
about 3n regardless of how the trades are distributed in time. So the
expected cost is near-linear in row count, and it was measured that way on
synthetic tapes from 60k to 900k rows (0.19s to 0.43s, sub-linear per row).

IF A SYMBOL DEPARTS FROM THAT, THE DEPARTURE IS THE FINDING. The columns are
chosen to say which term ran away:

    rows        raw records in the parquet
    trades      after excluded prints are dropped -- what quiet actually sees
    uniq_t      distinct timestamps; rows/uniq_t is the duplication factor
    tr/min      arrival rate over the session
    span_min    first to last trade, in minutes. A span far above the session
                means the day holds data it should not, and every window count
                derives from the session bounds rather than from this.
    windows     windows built per length, which depends ONLY on the session
                and the grid -- if this varies between symbols, the bounds are
                wrong and that alone would explain a runaway
    elig        windows clearing the trade guard
    us/row      the normalised number. This is the column to sort by; a symbol
                an order of magnitude above the others is the one to look at.

Reads parquet and computes. Touches no database, writes nothing except an
optional CSV.
"""
from __future__ import annotations

import argparse
import time
from datetime import date

import numpy as np
import pandas as pd

from scalp import compute, config, metrics, quiet, store


def _fmt(seconds: float) -> str:
    if seconds < 90:
        return f"{seconds:.2f}s"
    if seconds < 5400:
        return f"{seconds / 60:.1f} min"
    return f"{seconds / 3600:.2f} h"


def _rule(title: str) -> None:
    print(f"\n{'-' * 78}\n{title}\n{'-' * 78}")


def pick_symbols(args, day: date) -> list[str]:
    """Which symbols to profile, and deliberately BY SIZE.

    The default is the largest and the smallest stored days rather than an
    alphabetical slice: a cost that scales with rows shows up as a gradient
    across that spread, and a cost that does not shows up as a symbol sitting
    off the line. An alphabetical sample can show neither.
    """
    if args.symbols:
        return [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    have = [s for s in store.stored_symbols() if store.has_day(s, day)]
    if not have:
        raise SystemExit(f"no stored symbol-days for {day}")
    if args.all:
        return sorted(have)

    sized = []
    for s in have:
        try:
            sized.append((store.day_path(s, day).stat().st_size, s))
        except OSError:
            continue
    sized.sort(reverse=True)
    top = [s for _, s in sized[:args.top]]
    bottom = [s for _, s in sized[-args.bottom:]] if args.bottom else []
    seen, out = set(), []
    for s in top + bottom:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def profile_one(symbol: str, day: date, *, with_compute: bool) -> dict | None:
    """Stage timings for one symbol-day. None if nothing is stored."""
    t0 = time.perf_counter()
    raw = store.read_day(symbol, day)
    t_read = time.perf_counter() - t0
    if raw.empty:
        return None

    t0 = time.perf_counter()
    df, cols = compute.prepare(raw, day)
    t_prep = time.perf_counter() - t0
    start, end = compute.session_bounds(day)

    t0 = time.perf_counter()
    window = metrics.slice_window(df, cols.time, start, end)
    t_slice = time.perf_counter() - t0

    t0 = time.perf_counter()
    dropped = metrics.excluded_mask(window, cols.condition_cols)
    trades = window[~dropped]
    t_mask = time.perf_counter() - t0

    # Per window length, so a single row of the grid running away is visible
    # rather than averaged into the other two.
    per_window = {}
    t0 = time.perf_counter()
    series = quiet.from_trades(trades, time_col=cols.time, price_col=cols.price,
                               size_col=cols.size, start=start, end=end)
    t_series = time.perf_counter() - t0
    for w in quiet.WINDOWS_SEC:
        tw = time.perf_counter()
        quiet.from_trades(trades, time_col=cols.time, price_col=cols.price,
                          size_col=cols.size, start=start, end=end,
                          windows=(w,))
        per_window[w] = time.perf_counter() - tw

    t0 = time.perf_counter()
    quiet.daily_metrics(series)
    t_daily = time.perf_counter() - t0

    t_quiet = t_slice + t_mask + t_series + t_daily

    t_window = t_buckets = float("nan")
    if with_compute:
        t0 = time.perf_counter()
        metrics.compute_window(df, cols, start, end)
        t_window = time.perf_counter() - t0
        t0 = time.perf_counter()
        metrics.compute_buckets(df, cols, start, end,
                                config.INTRADAY_BUCKET_MINUTES)
        t_buckets = time.perf_counter() - t0

    times = pd.to_datetime(trades[cols.time], errors="coerce").dropna()
    span_min = ((times.max() - times.min()).total_seconds() / 60.0
                if len(times) else 0.0)
    n_win = {w: int(len(series[w]["w_start"])) for w in quiet.WINDOWS_SEC}
    elig = {w: int(series[w]["eligible"].sum()) for w in quiet.WINDOWS_SEC}

    return {
        "symbol": symbol, "rows": len(raw), "trades": len(trades),
        "uniq_t": int(times.nunique()) if len(times) else 0,
        "tr_per_min": len(trades) / max(span_min, 1e-9),
        "span_min": span_min,
        "read": t_read, "prepare": t_prep, "slice": t_slice, "mask": t_mask,
        "series": t_series, "daily": t_daily, "quiet": t_quiet,
        "window": t_window, "buckets": t_buckets,
        "us_per_row": t_quiet / max(len(raw), 1) * 1e6,
        "n_win": n_win, "elig": elig, "per_window": per_window,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--symbols", default=None, help="comma-separated")
    ap.add_argument("--top", type=int, default=6,
                    help="largest stored days to profile (default 6)")
    ap.add_argument("--bottom", type=int, default=6,
                    help="smallest stored days to profile (default 6)")
    ap.add_argument("--all", action="store_true",
                    help="every stored symbol for the date. Slow, and the "
                         "right thing to run once if the outlier is not in "
                         "the top or bottom slice.")
    ap.add_argument("--with-compute", action="store_true",
                    help="also time compute_window and compute_buckets, to "
                         "show quiet's share of the whole symbol-day")
    ap.add_argument("--workers", type=int, default=config.COMPUTE_WORKERS,
                    help="only used to project a full-run duration")
    ap.add_argument("--csv", default=None, help="write the table here too")
    args = ap.parse_args()

    day = date.fromisoformat(args.date)
    symbols = pick_symbols(args, day)

    _rule(f"quiet_session — {len(symbols)} symbol(s) on {day}")
    print(f"grid {quiet.WINDOWS_SEC}, steps "
          f"{ {w: quiet.step_for(w) for w in quiet.WINDOWS_SEC} }, "
          f"guard {quiet.MIN_TRADES}")
    print(f"{'symbol':<8s} {'rows':>9s} {'trades':>9s} {'uniq_t':>9s} "
          f"{'tr/min':>8s} {'span':>7s} {'quiet':>8s} {'us/row':>8s}")

    rows = []
    for s in symbols:
        try:
            r = profile_one(s, day, with_compute=args.with_compute)
        except Exception as exc:                                  # noqa: BLE001
            print(f"{s:<8s} FAILED: {type(exc).__name__}: {exc}")
            continue
        if r is None:
            continue
        rows.append(r)
        print(f"{r['symbol']:<8s} {r['rows']:>9,} {r['trades']:>9,} "
              f"{r['uniq_t']:>9,} {r['tr_per_min']:>8.0f} "
              f"{r['span_min']:>6.0f}m {_fmt(r['quiet']):>8s} "
              f"{r['us_per_row']:>8.2f}")

    if not rows:
        raise SystemExit("nothing profiled")

    df = pd.DataFrame(rows)

    _rule("STAGE BREAKDOWN — where quiet's time actually goes")
    print(f"{'symbol':<8s} {'read':>7s} {'prepare':>8s} {'slice':>7s} "
          f"{'mask':>7s} {'series':>8s} {'daily':>7s}" +
          ("  {:>8s} {:>8s}".format("window", "buckets") if args.with_compute else ""))
    for r in rows:
        line = (f"{r['symbol']:<8s} {r['read']:6.2f}s {r['prepare']:7.2f}s "
                f"{r['slice']:6.2f}s {r['mask']:6.2f}s {r['series']:7.2f}s "
                f"{r['daily']:6.2f}s")
        if args.with_compute:
            line += f"  {r['window']:7.2f}s {r['buckets']:7.2f}s"
        print(line)

    _rule("PER WINDOW LENGTH — is one row of the grid the problem?")
    hdr = "  ".join(f"{w}s".rjust(8) for w in quiet.WINDOWS_SEC)
    print(f"{'symbol':<8s} {hdr}      windows built        eligible")
    for r in rows:
        t = "  ".join(f"{r['per_window'][w]:7.2f}s" for w in quiet.WINDOWS_SEC)
        nw = "/".join(str(r["n_win"][w]) for w in quiet.WINDOWS_SEC)
        el = "/".join(str(r["elig"][w]) for w in quiet.WINDOWS_SEC)
        print(f"{r['symbol']:<8s} {t}   {nw:>16s} {el:>15s}")

    _rule("THE ANSWER")

    # us/row ALONE IS THE WRONG TEST, and reading it as one sends you after
    # the smallest symbol every time. The window grid is built from the
    # SESSION, not from the data, so every symbol pays the same ~4,000-window
    # cost whether it has nine thousand rows or seven hundred thousand. That
    # fixed term is amortised over few rows on a small symbol and shows up as
    # a large us/row that is not a problem.
    #
    # The honest model is therefore
    #
    #     quiet_time ~ fixed + per_row x rows
    #
    # fitted across the sample, with each symbol scored on how far it sits
    # ABOVE its own prediction. A pathological symbol is one whose actual cost
    # is a multiple of what its row count predicts -- that is what a runaway
    # term looks like, and it is invariant to how big the symbol is.
    if len(df) >= 3:
        coef = np.polyfit(df["rows"].to_numpy(dtype="float64"),
                          df["quiet"].to_numpy(dtype="float64"), 1)
        per_row, fixed = float(coef[0]), float(coef[1])
        pred = np.maximum(fixed + per_row * df["rows"], 1e-9)
        df = df.assign(predicted=pred, excess=df["quiet"] / pred)
        print(f"fitted cost      : {_fmt(max(fixed, 0.0))} fixed per symbol-day "
              f"+ {per_row * 1e6:.2f} us per row")
        print(f"                   (the fixed term is the window grid, which is "
              f"built from the session and not from the data)")
        print()
        df = df.sort_values("excess", ascending=False)
        print(f"{'symbol':<8s} {'rows':>9s} {'actual':>9s} {'predicted':>10s} "
              f"{'excess':>8s}")
        for _, r in df.head(6).iterrows():
            print(f"{r['symbol']:<8s} {r['rows']:>9,} {_fmt(r['quiet']):>9s} "
                  f"{_fmt(r['predicted']):>10s} {r['excess']:>7.2f}x")
        print()
        worst = df.iloc[0]
        if worst["excess"] < 2.0:
            print("NO SYMBOL IS MORE THAN 2x ITS PREDICTION. quiet is behaving")
            print("linearly across this sample, and whatever stalled the run is")
            print("not in this path on these symbols. Two things to try next:")
            print("  - widen the sample with --all; the outlier may not be in")
            print("    the top/bottom slice")
            print("  - run compute on a few symbols with and without --no-quiet")
            print("    and compare, which settles it in one pass")
        else:
            print(f"{worst['symbol']} costs {worst['excess']:.1f}x what its row count")
            print("predicts. That is the symbol to reproduce. The stage table says")
            print("which stage, and the per-window table says whether one row of")
            print("the grid is responsible.")
        print()
        dup = df["rows"] / df["uniq_t"].clip(lower=1)
        print(f"correlation, quiet time vs ROW COUNT      : "
              f"{np.corrcoef(df['rows'], df['quiet'])[0, 1]:+.3f}   "
              f"(near +1.00 means linear, which is healthy)")
        print(f"correlation, excess vs ARRIVAL RATE       : "
              f"{np.corrcoef(df['tr_per_min'], df['excess'])[0, 1]:+.3f}")
        print(f"correlation, excess vs DUPLICATION factor : "
              f"{np.corrcoef(dup, df['excess'])[0, 1]:+.3f}")

    # The read is not quiet's cost, but it IS the run's, and on a large symbol
    # it can dominate everything else. Called out so a slow volume is not
    # mistaken for slow arithmetic.
    slow_read = df.sort_values("read", ascending=False).iloc[0]
    if slow_read["read"] > slow_read["quiet"]:
        print()
        print(f"NOTE: reading the parquet costs more than computing on it -- "
              f"{slow_read['symbol']} spent {_fmt(slow_read['read'])} in read "
              f"against {_fmt(slow_read['quiet'])} in quiet.")
        print("      That is storage, not arithmetic, and a run pegged at 100% CPU")
        print("      is NOT explained by it.")

    total = df["quiet"].mean()
    print(f"\nmean quiet cost per symbol-day : {_fmt(total)}")
    print(f"mean total per symbol-day      : "
          f"{_fmt(df['read'].mean() + df['prepare'].mean() + total + (df['window'].mean() + df['buckets'].mean() if args.with_compute else 0))}"
          + ("" if args.with_compute else "   (pass --with-compute for the rest)"))
    print(f"projected, 10,656 symbol-days at {args.workers} workers, quiet alone: "
          f"{_fmt(total * 10656 / max(args.workers, 1))}")

    if args.csv:
        out = df.drop(columns=[c for c in ("n_win", "elig", "per_window")
                               if c in df.columns])
        out.to_csv(args.csv, index=False)
        print(f"\nwrote {args.csv}")


if __name__ == "__main__":
    main()
