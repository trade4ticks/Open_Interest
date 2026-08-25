"""
rv_vrp_noise_report.py — quantify the short-tenor VRP noise. Read-only.

A 5-trading-day realized vol is noisy, and vrp_7d inherits that noise. That is
the accepted cost of matching implied and realized over the same horizon — a
noisy correct quantity beats a precise wrong one — but "noisy" should be a
number rather than an adjective, which is what this prints.

--- Two measures, and why the second one matters ---------------------------

  LEVEL  stdev(vrp_{t}d) per ticker.
         What was asked for, and the right scale for reading a z-score: the z
         denominator IS this quantity. But it conflates two things — genuine
         variation in the premium across vol regimes, and estimator noise.

  DIFF   stdev(day-over-day change in vrp_{t}d) per ticker.
         Isolates the noise. A real premium regime moves slowly, so most of
         what shows up in the daily difference is the realized-vol estimator
         resampling its window. If DIFF/LEVEL is near 1.4 the series is close
         to a random walk of noise; well below that means the level carries
         signal the differences do not.

The ratio of the two across tenors is the actual answer to "how much worse is
the short end", and it is a ratio rather than an absolute so it does not need a
per-ticker vol adjustment to be read.

--- Why this is not an argument against the short tenors --------------------

The z-score is computed against THIS ticker's own history of THIS column, so
the same estimator noise is in the baseline. Noise inflates the z denominator
and shrinks the score toward zero — it costs power, it does not create false
signal. What it would cost, if the windows were mismatched instead, is
correctness, and that is not recoverable by any amount of averaging.

Usage:
    python rv_vrp_noise_report.py
    python rv_vrp_noise_report.py --snapshot 1545 --min-obs 40
    python rv_vrp_noise_report.py --ticker SPY          # per-ticker detail
    python rv_vrp_noise_report.py --ratio               # vrp_ratio_{t}d instead
"""
from __future__ import annotations

import argparse
import statistics
import sys

from db import get_connection
from lib.metrics_config import BASELINE_SNAPSHOT, RV_WINDOWS


def _pct(xs: list, q: float):
    """Linear-interpolated quantile. statistics.quantiles needs n >= 2 and
    returns cut points; this is simpler to read at the call site."""
    if not xs:
        return None
    s = sorted(xs)
    if len(s) == 1:
        return s[0]
    i = q * (len(s) - 1)
    lo = int(i)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (i - lo)


def _series(conn, cols: list, snapshot: str) -> dict:
    """{ticker: {col: [values in trade_date order]}} at one snapshot."""
    sel = ", ".join(cols)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT ticker, trade_date, {sel} FROM equity_metrics "
            f"WHERE snapshot = %s ORDER BY ticker, trade_date", (snapshot,))
        rows = cur.fetchall()
    out: dict = {}
    for r in rows:
        tk = r[0]
        d = out.setdefault(tk, {c: [] for c in cols})
        for j, c in enumerate(cols):
            d[c].append(r[2 + j])
    return out


def _stats(vals: list, min_obs: int):
    """(n, level stdev, diff stdev) over the non-null run.

    Differences are taken on CONSECUTIVE stored rows. Gaps make a difference
    span more than one session and overstate DIFF, so a gap-heavy ticker reads
    noisier than it is — n is printed alongside so that is visible rather than
    silent.
    """
    xs = [v for v in vals if v is not None]
    if len(xs) < min_obs:
        return len(xs), None, None
    lvl = statistics.stdev(xs)
    diffs = [b - a for a, b in zip(xs[:-1], xs[1:])]
    dif = statistics.stdev(diffs) if len(diffs) >= 2 else None
    return len(xs), lvl, dif


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--snapshot", default=BASELINE_SNAPSHOT,
                    help=f"bucket to measure (default {BASELINE_SNAPSHOT}, "
                         f"the daily baseline)")
    ap.add_argument("--min-obs", type=int, default=30,
                    help="minimum non-null rows per ticker (default 30)")
    ap.add_argument("--ticker", help="print this ticker's own numbers too")
    ap.add_argument("--ratio", action="store_true",
                    help="report vrp_ratio_{t}d instead of vrp_{t}d")
    args = ap.parse_args()

    stem = "vrp_ratio" if args.ratio else "vrp"
    cols = [f"{stem}_{lbl}" for lbl, _, _ in RV_WINDOWS]

    with get_connection() as conn:
        data = _series(conn, cols, args.snapshot)
    if not data:
        print(f"  no equity_metrics rows at snapshot {args.snapshot}")
        return 1

    print(f"=== {stem}_* noise at snapshot {args.snapshot} ===")
    print(f"  {len(data)} ticker(s), minimum {args.min_obs} observations each\n")
    print(f"  {'column':<16}{'tenor':>6}{'td':>5}{'tickers':>9}"
          f"{'median':>10}{'p25':>10}{'p75':>10}   {'median':>9}{'ratio':>8}")
    print(f"  {'':<16}{'':>6}{'':>5}{'':>9}"
          f"{'--- LEVEL stdev ---':^30}   {'DIFF':^9}{'D/L':^8}")

    rows_out = []
    for lbl, n, tenor in RV_WINDOWS:
        col = f"{stem}_{lbl}"
        lv, df = [], []
        for tk in data:
            _n, l, d = _stats(data[tk][col], args.min_obs)
            if l is not None:
                lv.append(l)
            if d is not None:
                df.append(d)
        if not lv:
            print(f"  {col:<16}{tenor:>6}{n:>5}{0:>9}"
                  f"{'  — not populated yet (rebuild pending)':<40}")
            continue
        med_l, med_d = statistics.median(lv), (statistics.median(df)
                                               if df else None)
        ratio = (med_d / med_l) if (med_d and med_l) else None
        rows_out.append((tenor, med_l, med_d, ratio))
        print(f"  {col:<16}{tenor:>6}{n:>5}{len(lv):>9}"
              f"{med_l:>10.4f}{_pct(lv, 0.25):>10.4f}{_pct(lv, 0.75):>10.4f}"
              f"   {(f'{med_d:.4f}' if med_d else '-'):>9}"
              f"{(f'{ratio:.2f}' if ratio else '-'):>8}")

    if len(rows_out) >= 2:
        base = next((r for r in rows_out if r[0] == 30), rows_out[-1])
        print(f"\n  Relative to {stem}_{base[0]}d (the pre-existing window):")
        for tenor, med_l, med_d, _ in rows_out:
            print(f"    {tenor:>3}d   level stdev {med_l / base[1]:>5.2f}x"
                  + (f"   day-over-day {med_d / base[2]:>5.2f}x"
                     if med_d and base[2] else ""))
        print("\n  A ratio above 1 at the short end is expected and is the "
              "cost being\n  accepted. It shrinks z toward zero rather than "
              "fabricating signal, because\n  the same noise sits in the "
              "baseline the z is scored against.")

    if args.ticker:
        tk = args.ticker.upper()
        if tk not in data:
            print(f"\n  {tk}: no rows at this snapshot")
            return 0
        print(f"\n=== {tk} ===")
        print(f"  {'column':<16}{'n':>6}{'level sd':>11}{'diff sd':>11}"
              f"{'D/L':>8}")
        for lbl, n, tenor in RV_WINDOWS:
            col = f"{stem}_{lbl}"
            cnt, l, d = _stats(data[tk][col], args.min_obs)
            r = (d / l) if (l and d) else None
            print(f"  {col:<16}{cnt:>6}"
                  f"{(f'{l:.4f}' if l else '-'):>11}"
                  f"{(f'{d:.4f}' if d else '-'):>11}"
                  f"{(f'{r:.2f}' if r else '-'):>8}")

    print("\nRead-only — nothing was written.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
