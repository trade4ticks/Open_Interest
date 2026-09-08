"""Step 2b: the same correlations, at episode grain, with a chance table that
accounts for the fact that 170 episodes are not 170 independent observations.

Reads episodes.csv (or episodes_full.csv). Writes nothing but a report.

THE DENOMINATOR IS BETTER THAN 77 TICKER-DAYS BUT IT IS NOT 170. The 170
episodes come from 77 ticker-days, 49 symbols and 7 sessions. Two episodes in
FDX on the same morning share a name, a session, a spread regime and an
operator's mood; treating them as independent draws is what makes a chance
table optimistic. So the null here is a BLOCK permutation -- whole ticker-days
of the outcome are shuffled against the metrics, keeping each block's internal
structure intact -- and the free permutation is reported beside it so the size
of the difference is visible rather than argued about. Where they diverge, the
free one is wrong.

Spearman throughout. Trip counts run 1 to 103 and bps has a long left tail;
rank correlation is the only summary that survives both without a transform
chosen after seeing the data.
"""
from __future__ import annotations

import argparse
import sys

import numpy as np
import pandas as pd

# Identity, outcome and bookkeeping. Never candidate predictors: correlating
# `net` against `bps` measures arithmetic, not the market.
NOT_METRICS = {
    "ep_id", "symbol", "trade_date", "start", "end",
    "trips", "bps", "net", "duration_min", "win_rate", "wins",
    "capital_sum", "capital_median", "shares_sum", "trips_per_min",
    "first_in_name", "lead_overlaps_prior", "lead_truncated",
    "lead_minutes_actual", "legs_gt2", "hold_s_median",
}
OUTCOMES = ("trips", "duration_min", "bps")
THRESHOLDS = (0.2, 0.3, 0.4, 0.5)


def ranks(a: np.ndarray) -> np.ndarray:
    """Average ranks, NaN preserved."""
    return pd.Series(a).rank(method="average").to_numpy(dtype="float64")


def rank_matrix(df: pd.DataFrame, cols: list) -> np.ndarray:
    return np.column_stack([ranks(df[c].to_numpy(dtype="float64")) for c in cols])


def corr_all(R0: np.ndarray, M: np.ndarray, n: np.ndarray, Sx: np.ndarray,
             Sxx: np.ndarray, y: np.ndarray) -> np.ndarray:
    """Pearson-on-ranks of every column against y, honouring per-column NaN.

    Vectorised over metrics so a permutation costs three matrix-vector
    products rather than a Python loop over 150 columns. R0 is the rank matrix
    with NaN replaced by zero and M its 0/1 indicator, so every sum that must
    ignore a missing cell is expressed as a product with M.

    The outcome is ranked ONCE over all episodes and then subsetted per metric,
    rather than re-ranked within each metric's complete cases. For a column
    with no missing values -- which is most of them -- that is exactly
    Spearman; where a column is sparse it is a monotone-equivalent
    approximation, and it is what keeps the null distribution affordable.
    """
    Sy = M.T @ y
    Syy = M.T @ (y * y)
    Sxy = R0.T @ y
    num = n * Sxy - Sx * Sy
    den = np.sqrt(np.clip(n * Sxx - Sx * Sx, 0, None)
                  * np.clip(n * Syy - Sy * Sy, 0, None))
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(den > 0, num / den, np.nan)


def block_permute(y: np.ndarray, blocks: list, rng) -> np.ndarray:
    """Shuffle whole ticker-days of the outcome against the metrics.

    Block order is permuted and the outcome values are then laid back down in
    that order, positionally. The multiset of outcomes is exactly preserved
    and, crucially, values that were contiguous within a ticker-day stay
    contiguous -- which is the dependence that makes the free permutation
    optimistic. Block sizes differ, so the mapping is not block-for-block; it
    does not need to be. What it has to destroy is the alignment between a
    ticker-day's tape and that ticker-day's outcome, and it does.
    """
    order = rng.permutation(len(blocks))
    return np.concatenate([y[blocks[i]] for i in order])


def chance_table(name: str, obs: np.ndarray, R0, M, n, Sx, Sxx, y,
                 blocks: list, n_perm: int, rng) -> None:
    """Observed exceedances against what shuffling produces."""
    free = np.empty((n_perm, len(THRESHOLDS)))
    blk = np.empty((n_perm, len(THRESHOLDS)))
    for b in range(n_perm):
        rf = np.abs(corr_all(R0, M, n, Sx, Sxx, rng.permutation(y)))
        rb = np.abs(corr_all(R0, M, n, Sx, Sxx, block_permute(y, blocks, rng)))
        for j, t in enumerate(THRESHOLDS):
            free[b, j] = np.nansum(rf >= t)
            blk[b, j] = np.nansum(rb >= t)

    print(f"\n--- chance table: {name} ({np.isfinite(obs).sum()} metrics, "
          f"{n_perm} permutations) ---")
    print(f"{'|rho|>=':>8} {'observed':>9} {'free mean':>10} {'free p95':>9} "
          f"{'block mean':>11} {'block p95':>10} {'p (block)':>10}")
    for j, t in enumerate(THRESHOLDS):
        k = int(np.nansum(np.abs(obs) >= t))
        p = float((blk[:, j] >= k).mean())
        print(f"{t:>8.1f} {k:>9} {free[:, j].mean():>10.1f} "
              f"{np.percentile(free[:, j], 95):>9.1f} "
              f"{blk[:, j].mean():>11.1f} "
              f"{np.percentile(blk[:, j], 95):>10.1f} {p:>10.3f}")


def top_table(cols: list, obs: np.ndarray, k: int = 15) -> None:
    ok = np.isfinite(obs)
    idx = np.argsort(-np.abs(np.where(ok, obs, 0)))[:k]
    print(f"{'metric':<52} {'rho':>7}")
    for i in idx:
        if not ok[i]:
            continue
        print(f"{cols[i]:<52} {obs[i]:>7.3f}")


def spearman(x: np.ndarray, y: np.ndarray) -> float:
    ok = np.isfinite(x) & np.isfinite(y)
    if ok.sum() < 4:
        return float("nan")
    rx, ry = ranks(x[ok]), ranks(y[ok])
    return float(np.corrcoef(rx, ry)[0, 1])


def circularity_check(df: pd.DataFrame) -> None:
    """Is the two_sided_balance signal my own footprint.

    My buys sit at or near the bid and my sells sit below the mid, so my
    prints add to at_bid_share and never to at_ask_share. two_sided_balance is
    min/max of the two, so removing my flow should push balance UP, and most
    where my share of the tape was highest. The prediction is stated before
    the number is read:

      if the balance-outcome relation SURVIVES ex-self, it is a market property
      if it weakens in step with self_share_trades, it was me
    """
    print("\n=== circularity check: two_sided_balance ===")
    for pref in ("ep", "lead"):
        b, be = f"{pref}_two_sided_balance", f"{pref}_two_sided_balance_exself"
        ss = f"{pref}_self_share_trades"
        if b not in df or be not in df:
            continue
        print(f"\n[{pref}]")
        d = (df[be] - df[b]).to_numpy(dtype="float64")
        print(f"  balance shift on removing my prints: "
              f"median {np.nanmedian(d):+.4f}, "
              f"share positive {np.nanmean(d > 0):.2f}  "
              f"(predicted: positive)")
        if ss in df:
            print(f"  self share of prints: median "
                  f"{df[ss].median():.3f}, p90 {df[ss].quantile(0.9):.3f}")
            print(f"  rho(balance shift, self share) = "
                  f"{spearman(d, df[ss].to_numpy(dtype='float64')):+.3f}  "
                  f"(predicted: positive -- more of me, more distortion)")
        for out in ("trips", "bps"):
            if out not in df:
                continue
            r1 = spearman(df[b].to_numpy(dtype="float64"),
                          df[out].to_numpy(dtype="float64"))
            r2 = spearman(df[be].to_numpy(dtype="float64"),
                          df[out].to_numpy(dtype="float64"))
            print(f"  rho(balance, {out:<5}) with self {r1:+.3f}  "
                  f"ex-self {r2:+.3f}   shift {r2 - r1:+.3f}")


def size_check(df: pd.DataFrame) -> None:
    """Position size against outcome, overall and inside each name.

    Size and market conditions are confounded across names -- a 10-share day
    in LLY is also an LLY day. The within-symbol column is the one that can
    separate them, and it only means anything where a symbol actually varied
    its size, so the size range is printed next to it.
    """
    print("\n=== position size ===")
    for out in ("bps", "trips"):
        if out not in df:
            continue
        r = spearman(df["shares_median"].to_numpy(dtype="float64"),
                     df[out].to_numpy(dtype="float64"))
        print(f"rho(shares_median, {out:<5}) all episodes = {r:+.3f}")
    print(f"\n{'symbol':<8} {'eps':>4} {'shares lo-hi':>14} "
          f"{'rho(sh,bps)':>12} {'rho(sh,trips)':>14}")
    for sym, g in df.groupby("symbol"):
        if len(g) < 6:
            continue
        sh = g["shares_median"].to_numpy(dtype="float64")
        if np.nanmin(sh) == np.nanmax(sh):
            rng_s, rb, rt = f"{np.nanmin(sh):.0f} (flat)", np.nan, np.nan
        else:
            rng_s = f"{np.nanmin(sh):.0f}-{np.nanmax(sh):.0f}"
            rb = spearman(sh, g["bps"].to_numpy(dtype="float64"))
            rt = spearman(sh, g["trips"].to_numpy(dtype="float64"))
        print(f"{sym:<8} {len(g):>4} {rng_s:>14} {rb:>12.3f} {rt:>14.3f}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("csv", nargs="?", default="episodes.csv")
    ap.add_argument("--permutations", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--min-coverage", type=float, default=0.7,
                    help="drop metrics missing on more than this share of rows")
    a = ap.parse_args()

    df = pd.read_csv(a.csv, parse_dates=["start", "end"])
    rng = np.random.default_rng(a.seed)
    print(f"=== {a.csv}: {len(df)} episodes, "
          f"{df.groupby(['trade_date', 'symbol']).ngroups} ticker-days, "
          f"{df['symbol'].nunique()} symbols, "
          f"{df['trade_date'].nunique()} sessions ===")
    print("Effective sample is the ticker-day count, not the episode count. "
          "For anything\nthat varies at session level it is 7.")

    num = df.select_dtypes(include=[np.number])
    cand = [c for c in num.columns if c not in NOT_METRICS]
    cov = num[cand].notna().mean()
    cols = [c for c in cand if cov[c] >= a.min_coverage
            and num[c].nunique(dropna=True) > 1]
    dropped = [c for c in cand if c not in cols]
    print(f"\n{len(cols)} candidate metrics ({len(dropped)} dropped: missing "
          f"on >{(1 - a.min_coverage) * 100:.0f}% of rows, or constant)")

    R = rank_matrix(df, cols)
    M = np.isfinite(R).astype("float64")
    R0 = np.where(np.isfinite(R), R, 0.0)
    n = M.sum(axis=0)
    Sx = R0.sum(axis=0)
    Sxx = (R0 * R0).sum(axis=0)

    key = df["trade_date"].astype(str) + "|" + df["symbol"].astype(str)
    blocks = [np.flatnonzero((key == k).to_numpy()) for k in key.unique()]

    groups = {
        "episode window, with my flow":
            [c for c in cols if c.startswith("ep_") and not c.endswith("_exself")],
        "episode window, ex-self":
            [c for c in cols if c.startswith("ep_") and c.endswith("_exself")],
        "lead-in, with my flow":
            [c for c in cols if c.startswith("lead_") and not c.endswith("_exself")],
        "lead-in, ex-self":
            [c for c in cols if c.startswith("lead_") and c.endswith("_exself")],
    }

    for out in OUTCOMES:
        if out not in df:
            continue
        y_all = ranks(df[out].to_numpy(dtype="float64"))
        if not np.isfinite(y_all).all():
            print(f"\n!! {out} has missing values; those rows correlate as NaN")
            y_all = np.nan_to_num(y_all, nan=float(np.nanmean(y_all)))
        obs = corr_all(R0, M, n, Sx, Sxx, y_all)
        print(f"\n\n########## outcome: {out} ##########")
        print("\ntop |rho|, all metrics:")
        top_table(cols, obs)
        chance_table(f"{out}, all metrics", obs, R0, M, n, Sx, Sxx, y_all,
                     blocks, a.permutations, rng)
        for gname, gcols in groups.items():
            if not gcols:
                continue
            gi = [cols.index(c) for c in gcols]
            sub = (R0[:, gi], M[:, gi], n[gi], Sx[gi], Sxx[gi])
            chance_table(f"{out} / {gname}", obs[gi], *sub, y_all,
                         blocks, a.permutations, rng)

    circularity_check(df)
    size_check(df)
    print("\nThe lead-in is the only window that could be predictive: it is "
          "fixed at 15\nminutes for every episode, and it is self-free wherever "
          "lead_overlaps_prior is\nFalse. The episode window is descriptive -- "
          "it measures the tape during the\ntrading it is being correlated "
          "against.")


if __name__ == "__main__":
    sys.exit(main())
