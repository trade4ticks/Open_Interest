"""Step 2: one row per trading episode, with the tape measured over exactly
the window I was exposed to -- and over the 15 minutes before I arrived.

Read-only. Reads the fills CSV and the parquet store, writes two CSVs. Opens
no database connection and writes nothing to Postgres.

WHY EPISODE GRAIN. Four metric families have been tested against realised
results at session grain and none predicted anything. Every one of those
metrics describes 390 minutes; the trading happens in a median of 3.2. The
session average includes six hours of tape nobody was exposed to, which is a
better explanation of four failures than any hypothesis about the metrics.

WHAT THE SHAPE OF THE FILLS FORCED (measured, by episode_shape.py):

  N IS NOT LOAD-BEARING. Gaps are strongly bimodal -- 81% under a minute, then
  a tail beyond 15. Only 3.5% fall in (5, 15] minutes, so every threshold in
  that range yields nearly the same grouping and N=10 is a free choice rather
  than a tuned one. It stays a flag so that is checkable, not assertable.

  THE EPISODE IS SHORTER THAN THE PRE-REGISTERED WINDOW. Median episode is 3.2
  minutes and the 25th percentile is 54 SECONDS. The pre-registered primary is
  a 60-second window, which does not fit inside a quarter of the sample at
  all. See `quiet_range` for what is done about it, and why the lead-in is
  unaffected.

  MY OWN PRINTS ARE IN THE TAPE. $51.4M traded over 77 ticker-days, and on the
  concentrated names it is a large share of the flow during the episodes
  themselves. Every flow metric is emitted twice, with and without.

NO MINIMUM TRIP COUNT, ANYWHERE. 69 of 170 episodes have three trips or
fewer. Those are the record that a name was looked at and abandoned -- the
negative examples the 77 ticker-days do not contain, and the restriction of
range that is the other candidate explanation for the vanishing correlations.
Filtering them would rebuild exactly the problem this analysis exists to fix.
"""
from __future__ import annotations

import argparse
import pathlib
import sys
import time
from datetime import timedelta

import numpy as np
import pandas as pd

# The analysis lives beside the package it reads, not inside it, so the repo
# root goes on the path here rather than being demanded as PYTHONPATH at every
# invocation. Placed before the `scalp` import deliberately.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from scalp import compute, config, metrics, quiet, store
from episode_shape import gaps, label_episodes, load

# Whole seconds is all the statement carries (measured: 0.0% of fills have a
# sub-second component), so a tolerance under 1s would match nothing. 2s
# absorbs the reporting lag between execution and print.
SELF_TIME_TOL_S = 2.0
# My fill price IS a print price for a two-leg trip, so exact matches are
# expected and are ranked first. The tolerance exists for the 22% of trips
# that scaled in or out, whose stored entry price is a VWAP over several
# prints and may equal none of them.
SELF_PRICE_TOL = 0.01
LEAD_MINUTES = 15.0


# --- my own prints ----------------------------------------------------------

def self_legs(trips: pd.DataFrame) -> pd.DataFrame:
    """The prints I am responsible for, reconstructed from round trips.

    `fills` stores trips, not executions, so this is a reconstruction and its
    accuracy is bounded by `legs`: 77.6% of trips have exactly two, where the
    entry and exit prints are recovered exactly. The rest scaled in or out,
    and for those this yields two legs of `peak_shares` when the true count is
    higher -- so the correction UNDER-removes on 22% of trips and never
    over-removes. That asymmetry is deliberate; see `build_self_mask`.

    The exit price is derived, not stored. For a long,
    net_pnl = peak x (exit - entry) - fees, so exit = entry + net/peak,
    understated by fees/peak -- a fraction of a cent at these clip sizes, and
    well inside SELF_PRICE_TOL.
    """
    sgn = np.where(trips["is_long"].to_numpy(dtype=bool), 1.0, -1.0)
    shares = trips["peak_shares"].to_numpy(dtype="float64")
    entry = trips["entry_price"].to_numpy(dtype="float64")
    net = trips["net_pnl"].to_numpy(dtype="float64")
    with np.errstate(divide="ignore", invalid="ignore"):
        exit_px = entry + sgn * np.where(shares > 0, net / shares, np.nan)
    return pd.concat([
        pd.DataFrame({"ts": trips["entry_ts"].to_numpy(), "price": entry,
                      "shares": shares, "side": "open"}),
        pd.DataFrame({"ts": trips["exit_ts"].to_numpy(), "price": exit_px,
                      "shares": shares, "side": "close"}),
    ], ignore_index=True).sort_values("ts", kind="stable")


def build_self_mask(df: pd.DataFrame, cols: metrics.Columns,
                    legs: pd.DataFrame, *,
                    time_tol: float = SELF_TIME_TOL_S,
                    price_tol: float = SELF_PRICE_TOL) -> tuple:
    """A boolean mask over `df` marking the prints that were mine.

    A SHARE BUDGET, NOT A SIZE MATCH. For each leg, candidates within the time
    and price tolerance are consumed in rank order until the leg's share count
    is used up, and a print LARGER than the remaining budget is skipped
    outright. So the shares removed can never exceed the shares I actually
    traded. That guarantee is the point: an exact-size match at +/-2s and
    +/-1c would, in a name where 50-share prints at my price are routine,
    delete other people's flow and bias the corrected metric as badly as the
    contamination it is fixing. Under-removal leaves a known residue and is
    reported; over-removal would manufacture the answer.

    Candidates are ranked off-exchange first. That is a HYPOTHESIS about
    routing, not a filter -- a retail broker sends this flow to a wholesaler,
    so my prints should carry a TRF code. It costs nothing if wrong, and
    self_matched_off_share tests it: if my matched prints come back
    overwhelmingly off-exchange, that confirms the routing and simultaneously
    confirms off_exchange_share is itself contaminated.

    No print is claimed twice, so two of my trips one second apart cannot both
    consume the same execution.
    """
    n = len(df)
    diag = {"self_legs": int(len(legs)), "self_legs_filled": 0,
            "self_legs_partial": 0, "self_legs_unmatched": 0,
            "self_shares_expected": float(legs["shares"].sum()) if len(legs) else 0.0,
            "self_shares_matched": 0.0, "self_prints_matched": 0,
            "self_matched_off_share": float("nan")}
    if n == 0 or legs.empty:
        return pd.Series(False, index=df.index), diag

    t = df[cols.time].to_numpy("datetime64[ns]").astype("int64") / 1e9
    order = np.argsort(t, kind="stable")
    ts = t[order]
    px = pd.to_numeric(df[cols.price], errors="coerce").to_numpy()[order]
    sz = pd.to_numeric(df[cols.size], errors="coerce").to_numpy()[order]
    if cols.exchange is not None:
        off = (df[cols.exchange].map(config.is_off_exchange)
               .fillna(False).to_numpy(dtype=bool)[order])
    else:
        off = np.zeros(n, dtype=bool)

    claimed = np.zeros(n, dtype=bool)
    leg_ts = (pd.to_datetime(legs["ts"]).to_numpy("datetime64[ns]")
              .astype("int64") / 1e9)
    leg_px = legs["price"].to_numpy(dtype="float64")
    leg_sh = legs["shares"].to_numpy(dtype="float64")
    off_hits = 0

    for k in range(len(legs)):
        budget = leg_sh[k]
        if not np.isfinite(budget) or budget <= 0 or not np.isfinite(leg_px[k]):
            diag["self_legs_unmatched"] += 1
            continue
        lo = int(np.searchsorted(ts, leg_ts[k] - time_tol, side="left"))
        hi = int(np.searchsorted(ts, leg_ts[k] + time_tol, side="right"))
        if hi <= lo:
            diag["self_legs_unmatched"] += 1
            continue
        idx = np.arange(lo, hi)
        ok = (~claimed[idx] & np.isfinite(px[idx]) & np.isfinite(sz[idx])
              & (sz[idx] > 0) & (np.abs(px[idx] - leg_px[k]) <= price_tol))
        idx = idx[ok]
        if idx.size == 0:
            diag["self_legs_unmatched"] += 1
            continue
        # lexsort applies the LAST key first: off-exchange, then closest in
        # time, then closest in price.
        idx = idx[np.lexsort((np.abs(px[idx] - leg_px[k]),
                              np.abs(ts[idx] - leg_ts[k]),
                              ~off[idx]))]
        taken = 0.0
        for i in idx:
            if budget - taken <= 0:
                break
            if sz[i] > budget - taken:
                continue          # would remove more than I traded
            claimed[i] = True
            taken += float(sz[i])
            off_hits += int(off[i])
            diag["self_prints_matched"] += 1
        diag["self_shares_matched"] += taken
        if taken <= 0:
            diag["self_legs_unmatched"] += 1
        elif taken >= leg_sh[k] - 1e-9:
            diag["self_legs_filled"] += 1
        else:
            diag["self_legs_partial"] += 1

    if diag["self_prints_matched"]:
        diag["self_matched_off_share"] = off_hits / diag["self_prints_matched"]
    mask = np.zeros(n, dtype=bool)
    mask[order] = claimed
    return pd.Series(mask, index=df.index), diag


# --- quiet, restricted to a time range --------------------------------------

def quiet_range(series: dict, start: pd.Timestamp, end: pd.Timestamp) -> dict:
    """quiet.daily_metrics over only the windows starting inside [start, end).

    THE SERIES IS THE SESSION'S, SLICED -- not a fresh series bounded to the
    episode, and the difference matters twice.

    First, the shift is a difference against the PREVIOUS window. A series
    bounded to the episode gives its first window no predecessor, so the
    episode's opening window would never be eligible. Sliced from the session,
    that window's predecessor is the real minute before I arrived and its
    shift is real.

    Second, at the 25th percentile an episode is 54 seconds and no 60-second
    window fits inside it at all; a bounded series would return nothing for a
    quarter of the sample. Attributing windows by their START recovers two or
    three of them. They extend past the episode end, so on the shortest
    episodes this metric describes a window that is mostly outside the episode
    -- which is why the eligible-window COUNT is emitted on every row, and why
    the 30s window is carried alongside the pre-registered 60s one.

    The lead-in has neither problem: it is 15 minutes for every episode.

    Aggregation is quiet.daily_metrics itself, applied to sliced arrays,
    rather than a reimplementation. Contiguity survives the slice -- the mask
    selects a contiguous block of a uniform grid -- so the run-counting in
    quiet._episodes stays correct.
    """
    def to_s(x):
        return (pd.Timestamp(x).to_datetime64().astype("datetime64[ns]")
                .astype("int64") / 1e9)

    s_sec, e_sec = to_s(start), to_s(end)
    sliced = {}
    for w, ser in series.items():
        m = (ser["w_start"] >= s_sec) & (ser["w_start"] < e_sec)
        sliced[w] = {k: (v[m] if isinstance(v, np.ndarray) else v)
                     for k, v in ser.items()}
    out = quiet.daily_metrics(sliced)
    # Counts scale with duration, and duration is proportional to trips --
    # which is the outcome variable. A raw count would correlate with trips
    # mechanically and mean nothing, so the share is published alongside and
    # is the one that gets to be compared.
    for w in quiet.WINDOWS_SEC:
        elig = out.get(f"quiet_eligible_windows_{w}s", 0)
        for key, _ in quiet.THRESHOLDS:
            cnt = out.get(f"quiet_windows_{w}s_{key}")
            out[f"quiet_share_{w}s_{key}"] = (
                cnt / elig if elig else float("nan"))
    return out


# --- per-window assembly ----------------------------------------------------

def window_row(df: pd.DataFrame, cols: metrics.Columns, qseries: dict,
               self_mask: pd.Series, start: pd.Timestamp, end: pd.Timestamp,
               prefix: str) -> dict:
    """Every metric for one window, with and without my own prints."""
    out: dict = {}
    if end <= start:
        return out
    for suffix, mask in (("", None), ("_exself", self_mask)):
        m = metrics.compute_window(df, cols, start, end,
                                   with_provenance=False, exclude_extra=mask)
        for k, v in m.items():
            if k in ("window_start", "window_end"):
                continue
            out[f"{prefix}_{k}{suffix}"] = v

    # Dollar volume per minute is not in flow_metrics, and the trade frame is
    # already being sliced here for the self-share diagnostics, so it is
    # computed here rather than adding a column to a pipeline function for
    # the sake of one analysis.
    win = metrics.slice_window(df, cols.time, start, end)
    minutes = max((end - start).total_seconds() / 60.0, 1e-9)
    if win.empty:
        out[f"{prefix}_dollar_vol_per_min"] = 0.0
        out[f"{prefix}_dollar_vol_per_min_exself"] = 0.0
        out[f"{prefix}_self_prints"] = 0
        out[f"{prefix}_self_share_trades"] = float("nan")
        out[f"{prefix}_self_share_dollars"] = float("nan")
    else:
        keep = ~metrics.excluded_mask(win, cols.condition_cols)
        mine = self_mask.reindex(win.index).fillna(False).astype(bool)
        p = pd.to_numeric(win[cols.price], errors="coerce")
        s = pd.to_numeric(win[cols.size], errors="coerce")
        dollars = (p * s).fillna(0.0)
        out[f"{prefix}_dollar_vol_per_min"] = float(dollars[keep].sum()) / minutes
        out[f"{prefix}_dollar_vol_per_min_exself"] = (
            float(dollars[keep & ~mine].sum()) / minutes)

        # How contaminated this window was. The whole with/without comparison
        # is uninterpretable without it: a metric that barely moves in a
        # window where I was 2% of the tape says nothing about one where I was
        # 30%.
        n_keep = int(keep.sum())
        tot_d = float(dollars[keep].sum())
        out[f"{prefix}_self_prints"] = int((keep & mine).sum())
        out[f"{prefix}_self_share_trades"] = (
            float((keep & mine).sum()) / n_keep if n_keep else float("nan"))
        out[f"{prefix}_self_share_dollars"] = (
            float(dollars[keep & mine].sum()) / tot_d if tot_d > 0
            else float("nan"))

    for k, v in quiet_range(qseries, start, end).items():
        out[f"{prefix}_{k}"] = v
    return out


# --- episodes ---------------------------------------------------------------

def build_episodes(fills: pd.DataFrame, n_min: float,
                   lead_minutes: float = LEAD_MINUTES) -> pd.DataFrame:
    ep = label_episodes(fills, gaps(fills), n_min)
    f = fills.assign(_ep=ep)
    out = f.groupby("_ep").agg(
        symbol=("symbol", "first"), trade_date=("trade_date", "first"),
        trips=("seq", "size"), start=("entry_ts", "min"),
        end=("exit_ts", "max"), net=("net_pnl", "sum"),
        capital_sum=("capital", "sum"), capital_median=("capital", "median"),
        shares_median=("peak_shares", "median"),
        shares_sum=("peak_shares", "sum"),
        hold_s_median=("duration_s", "median"),
        legs_gt2=("legs", lambda x: int((x > 2).sum())),
        wins=("net_pnl", lambda x: int((x > 0).sum())),
    ).reset_index(drop=True)
    out["duration_min"] = (out["end"] - out["start"]).dt.total_seconds() / 60.0
    out["bps"] = np.where(out["capital_sum"] > 0,
                          out["net"] / out["capital_sum"] * 1e4, np.nan)
    out["win_rate"] = out["wins"] / out["trips"]
    out["trips_per_min"] = np.where(out["duration_min"] > 0,
                                    out["trips"] / out["duration_min"], np.nan)
    out = out.sort_values(["trade_date", "symbol", "start"],
                          kind="stable").reset_index(drop=True)
    out["ep_id"] = [f"{r.trade_date}_{r.symbol}_{i}"
                    for i, r in enumerate(out.itertuples())]

    # Is the 15 minutes before this episode clean? For the first episode in a
    # name that day it is tape I had not touched. For a later one it often
    # contains my OWN earlier trading in that name, which is not a market
    # condition I observed -- it is my footprint. The lead-in is the only
    # window that could be predictive rather than descriptive, so whether it
    # is self-free decides which rows that claim survives on.
    prior_end = out.groupby(["trade_date", "symbol"])["end"].shift(1)
    out["first_in_name"] = prior_end.isna()
    lead_start = out["start"] - pd.to_timedelta(lead_minutes, unit="m")
    out["lead_overlaps_prior"] = (prior_end.notna()
                                  & (prior_end > lead_start)).fillna(False)
    return out


def _win_cols(prefix: str) -> list:
    """The curated column set for one window, in reading order."""
    base = [
        "self_prints", "self_share_trades", "self_share_dollars",
        "spread_cents_tw", "spread_bps_tw", "spread_cents_median",
        "trades", "trades_per_min", "shares_per_min", "dollar_vol_per_min",
        "trade_size_median", "odd_lot_share",
        "at_bid_share", "at_ask_share", "between_share", "two_sided_balance",
        "off_exchange_share", "off_mid_bps",
        "noise_bps_tw_mid_10s_p75", "noise_bps_trade_price_10s_p75",
    ]
    # Only the metrics my own flow can move. Spread is carried as the control:
    # it must be identical to the with-self version, and if it ever is not,
    # the mask leaked into the quote path.
    exself = [
        "trades", "trades_per_min", "shares_per_min", "dollar_vol_per_min",
        "trade_size_median", "odd_lot_share",
        "at_bid_share", "at_ask_share", "between_share", "two_sided_balance",
        "off_exchange_share", "off_mid_bps",
        "noise_bps_trade_price_10s_p75", "spread_cents_tw",
    ]
    q = []
    for w in (30, 60):
        q += [f"quiet_eligible_windows_{w}s", f"quiet_windows_{w}s_10",
              f"quiet_share_{w}s_10", f"shift_over_range_median_{w}s",
              f"quiet_range_iqr_cents_{w}s", f"quiet_range_iqr_bps_{w}s",
              f"quiet_range_p10p90_cents_{w}s", f"quiet_range_p10p90_bps_{w}s"]
    q.append("quiet_episodes_60s_10")
    return ([f"{prefix}_{c}" for c in base]
            + [f"{prefix}_{c}_exself" for c in exself]
            + [f"{prefix}_{c}" for c in q])


CURATED = (
    ["ep_id", "symbol", "trade_date", "start", "end", "duration_min",
     "trips", "trips_per_min", "hold_s_median", "shares_median", "shares_sum",
     "capital_median", "capital_sum", "net", "bps", "win_rate", "legs_gt2",
     "first_in_name", "lead_overlaps_prior", "lead_minutes_actual",
     "lead_truncated",
     "day_self_legs", "day_self_legs_filled", "day_self_legs_partial",
     "day_self_legs_unmatched", "day_self_shares_expected",
     "day_self_shares_matched", "day_self_matched_off_share"]
    + _win_cols("ep") + _win_cols("lead")
)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("csv", nargs="?", default="fills_dump.csv")
    ap.add_argument("--gap-minutes", type=float, default=10.0)
    ap.add_argument("--lead-minutes", type=float, default=LEAD_MINUTES)
    ap.add_argument("--out", default="episodes.csv")
    ap.add_argument("--out-full", default="episodes_full.csv")
    ap.add_argument("--time-tol", type=float, default=SELF_TIME_TOL_S)
    ap.add_argument("--price-tol", type=float, default=SELF_PRICE_TOL)
    a = ap.parse_args()

    fills = load(a.csv)
    eps = build_episodes(fills, a.gap_minutes, a.lead_minutes)
    print(f"{len(fills)} trips -> {len(eps)} episodes at N={a.gap_minutes:g} min "
          f"over {eps.groupby(['trade_date', 'symbol']).ngroups} ticker-days")

    ep_by_key = dict(tuple(eps.groupby(["trade_date", "symbol"], sort=True)))
    fills_by_key = dict(tuple(fills.groupby(["trade_date", "symbol"],
                                            sort=True)))

    rows, t_start = [], time.time()
    for i, (key, ep_grp) in enumerate(sorted(ep_by_key.items()), start=1):
        day, sym = key
        t0 = time.time()
        raw = store.read_day(sym, day)
        if raw.empty:
            print(f"  [{i}/{len(ep_by_key)}] {sym} {day}: NO PARQUET, skipped")
            continue
        df, cols = compute.prepare(raw, day)
        rth_start, rth_end = compute.session_bounds(day)
        self_mask, diag = build_self_mask(df, cols, self_legs(fills_by_key[key]),
                                          time_tol=a.time_tol,
                                          price_tol=a.price_tol)
        qseries = metrics.quiet_session(df, cols, rth_start, rth_end)

        for ep in ep_grp.itertuples():
            row = {c: getattr(ep, c) for c in eps.columns}
            row.update({f"day_{k}": v for k, v in diag.items()})
            lead_start = max(rth_start,
                             ep.start - timedelta(minutes=a.lead_minutes))
            row["lead_minutes_actual"] = (
                (ep.start - lead_start).total_seconds() / 60.0)
            row["lead_truncated"] = bool(
                row["lead_minutes_actual"] < a.lead_minutes - 1e-9)
            row.update(window_row(df, cols, qseries, self_mask,
                                  ep.start, ep.end, "ep"))
            row.update(window_row(df, cols, qseries, self_mask,
                                  lead_start, ep.start, "lead"))
            rows.append(row)
        print(f"  [{i}/{len(ep_by_key)}] {sym} {day}: {len(ep_grp)} episodes, "
              f"{len(df)} rows, self {diag['self_prints_matched']} prints "
              f"({diag['self_shares_matched']:.0f}/"
              f"{diag['self_shares_expected']:.0f} sh), "
              f"{time.time() - t0:.1f}s")

    out = pd.DataFrame(rows)
    out.to_csv(a.out_full, index=False)
    print(f"\nwrote {a.out_full}  {len(out)} rows x {len(out.columns)} cols")

    curated = [c for c in CURATED if c in out.columns]
    missing = [c for c in CURATED if c not in out.columns]
    out[curated].to_csv(a.out, index=False)
    print(f"wrote {a.out}  {len(out)} rows x {len(curated)} cols")
    if missing:
        print(f"NOTE {len(missing)} curated columns were not produced: "
              f"{missing[:10]}")

    # The quote path must be untouched by the self-mask. Spread is computed
    # from the collapsed quote frame, which is built from `window` and never
    # from `trades`, so it has to come out bit-identical. If it does not, the
    # mask leaked and every "clean" metric is suspect.
    if {"ep_spread_cents_tw", "ep_spread_cents_tw_exself"} <= set(out.columns):
        d = (out["ep_spread_cents_tw"] - out["ep_spread_cents_tw_exself"]).abs()
        worst = float(np.nanmax(d.to_numpy())) if len(d) else 0.0
        ok = not np.isfinite(worst) or worst < 1e-9
        print(f"quote-path check: max |spread_cents_tw diff| = {worst:.6g} -- "
              + ("PASS, the mask never touched the quote side" if ok
                 else "FAIL, the mask leaked into the quotes"))
    print(f"total {time.time() - t_start:.1f}s")


if __name__ == "__main__":
    sys.exit(main())
