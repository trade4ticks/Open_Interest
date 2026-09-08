"""Step 1b: what the fills actually look like, and how much the episode
boundary N matters. Reads the CSV from dump_fills.sql. Touches nothing else.

This exists to be run BEFORE the metric analysis is written. Grouping 1,582
round trips into episodes on a gap threshold is the one modelling choice in
the whole design, and picking N=10 because it sounds reasonable would be
guessing at the thing the data can answer directly: if almost no gaps fall
between 5 and 15 minutes, every N in that range yields the same grouping and
the choice is free; if many do, the boundary is load-bearing and has to be
argued for rather than assumed.

An episode is one ticker, one contiguous stretch of activity. It ends when
more than N minutes pass without a round trip in that name.

THE GAP IS EXIT-TO-ENTRY, not entry-to-entry. What ends an episode is time
spent NOT in the name; entry-to-entry would fold the hold duration into the
gap and make a long trip look like a pause.
"""
from __future__ import annotations

import argparse
import sys

import numpy as np
import pandas as pd

GRID_MIN = (2.0, 3.0, 5.0, 7.0, 10.0, 15.0, 20.0, 30.0, 45.0, 60.0)


def load(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["entry_ts", "exit_ts"])
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return df.sort_values(["trade_date", "symbol", "entry_ts", "seq"],
                          kind="stable").reset_index(drop=True)


def gaps(df: pd.DataFrame) -> pd.Series:
    """Seconds flat between consecutive trips in the same ticker-day.

    NaN at the first trip of each ticker-day -- there is no preceding trip, so
    there is no gap, and filling it with 0 would invent an unbroken run.
    """
    g = df.groupby(["trade_date", "symbol"], sort=False)
    prev_exit = g["exit_ts"].shift(1)
    return (df["entry_ts"] - prev_exit).dt.total_seconds()


def label_episodes(df: pd.DataFrame, gap_s: pd.Series, n_min: float) -> pd.Series:
    """Episode id per trip. A new episode starts at each ticker-day boundary
    (gap is NaN there) and wherever the gap exceeds the threshold."""
    brk = gap_s.isna() | (gap_s > n_min * 60.0)
    return brk.cumsum()


def episode_frame(df: pd.DataFrame, ep: pd.Series) -> pd.DataFrame:
    out = df.assign(_ep=ep).groupby("_ep").agg(
        symbol=("symbol", "first"), trade_date=("trade_date", "first"),
        trips=("seq", "size"), start=("entry_ts", "min"), end=("exit_ts", "max"),
        net=("net_pnl", "sum"), capital=("capital", "sum"),
        shares=("peak_shares", "median"))
    out["minutes"] = (out["end"] - out["start"]).dt.total_seconds() / 60.0
    out["bps"] = np.where(out["capital"] > 0,
                          out["net"] / out["capital"] * 1e4, np.nan)
    return out


def pct(v: np.ndarray, q) -> str:
    v = v[np.isfinite(v)]
    if v.size == 0:
        return "n/a"
    return "  ".join(f"p{int(x)}={np.percentile(v, x):.1f}" for x in q)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("csv", nargs="?", default="fills_dump.csv")
    a = ap.parse_args()

    df = load(a.csv)
    print(f"=== fills: {a.csv} ===")
    print(f"trips              {len(df)}")
    print(f"sessions           {df['trade_date'].nunique()}  "
          f"{min(df['trade_date'])} .. {max(df['trade_date'])}")
    print(f"symbols            {df['symbol'].nunique()}")
    print(f"ticker-days        {df.groupby(['trade_date','symbol']).ngroups}")
    print(f"columns            {list(df.columns)}")
    print()
    print("trips per session:")
    print(df.groupby("trade_date").agg(
        trips=("seq", "size"), symbols=("symbol", "nunique"),
        net=("net_pnl", "sum")).to_string())
    print()

    g = gaps(df)
    inner = g.dropna().to_numpy()
    neg = int((inner < 0).sum())
    print(f"=== gaps between consecutive trips in a ticker-day ({inner.size}) ===")
    if neg:
        print(f"NOTE {neg} gaps are NEGATIVE -- a trip's entry precedes the "
              "previous trip's exit. Overlapping positions in one name; these "
              "never break an episode.")
    print("seconds  " + pct(inner, (10, 25, 50, 75, 90, 95, 99)))
    print("minutes  " + pct(inner / 60.0, (10, 25, 50, 75, 90, 95, 99)))
    print()
    edges = [0, 1, 2, 3, 5, 7, 10, 15, 20, 30, 45, 60, 120, 1e9]
    print("gap histogram (minutes):")
    for lo, hi in zip(edges[:-1], edges[1:]):
        k = int(((inner / 60.0 >= lo) & (inner / 60.0 < hi)).sum())
        bar = "#" * min(60, k // 5)
        hi_s = "inf" if hi > 1e8 else f"{hi:g}"
        print(f"  [{lo:>5g}, {hi_s:>5}) {k:>5}  {bar}")
    print()

    print("=== episode count vs the threshold N ===")
    print(f"{'N min':>6} {'episodes':>9} {'trips/ep med':>13} {'1-trip':>7} "
          f"{'2-3trip':>8} {'>=10':>6} {'min/ep med':>11} {'longest':>8}")
    for n in GRID_MIN:
        ef = episode_frame(df, label_episodes(df, g, n))
        t = ef["trips"].to_numpy()
        print(f"{n:>6g} {len(ef):>9} {np.median(t):>13.1f} "
              f"{int((t == 1).sum()):>7} {int(((t >= 2) & (t <= 3)).sum()):>8} "
              f"{int((t >= 10).sum()):>6} {ef['minutes'].median():>11.1f} "
              f"{ef['minutes'].max():>8.1f}")
    print()

    # THE SENSITIVITY QUESTION, answered directly. Every gap strictly between
    # two thresholds is a boundary that flips when N moves between them. If
    # that count is a small fraction of all gaps, N is not load-bearing.
    m = inner / 60.0
    tot = m.size
    print("=== how much N actually matters ===")
    for lo, hi in ((5, 10), (10, 15), (5, 15)):
        k = int(((m > lo) & (m <= hi)).sum())
        print(f"gaps in ({lo:>2}, {hi:>2}] min: {k:>4}  "
              f"({k / tot * 100:.1f}% of all gaps) -- episode splits that flip "
              f"as N moves {lo}->{hi}")
    print()
    ef10 = episode_frame(df, label_episodes(df, g, 10.0))
    print("=== episodes at N=10 ===")
    print(f"episodes           {len(ef10)}")
    print(f"trips              {int(ef10['trips'].sum())} (must equal {len(df)})")
    print("trips/episode  " + pct(ef10["trips"].to_numpy().astype(float),
                                  (10, 25, 50, 75, 90, 99)))
    print("minutes/episode" + pct(ef10["minutes"].to_numpy(),
                                  (10, 25, 50, 75, 90, 99)))
    print("bps/episode    " + pct(ef10["bps"].to_numpy(), (10, 25, 50, 75, 90)))
    print()
    print("trip-count distribution:")
    vc = ef10["trips"].value_counts().sort_index()
    for k, v in vc.items():
        print(f"  {k:>3} trips  {v:>4}  {'#' * min(60, v)}")
    print()
    print("episodes per session (N=10):")
    print(ef10.groupby("trade_date").agg(
        episodes=("trips", "size"), trips=("trips", "sum"),
        symbols=("symbol", "nunique")).to_string())


if __name__ == "__main__":
    sys.exit(main())
