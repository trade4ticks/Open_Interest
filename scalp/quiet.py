"""
Quiet-window metrics: is the level shift small relative to the range I work?

TRADES ONLY. No quotes, anywhere in this file, deliberately.

A 29-share bid is pulled, the next level down becomes the best bid, and the
midpoint moves 19 cents while the stock does not move at all (observed on
EXPE, 30 seconds). On books this thin the midpoint reports order flicker as
price movement, and every quote-derived noise metric inherits it. A trade
price means somebody paid it. That is the entire reason this module exists
alongside the noise family rather than extending it.

--- The hypothesis ---------------------------------------------------------

Not "quiet" in the abstract. Whether the level shift is small RELATIVE to the
range being worked:

    a shift smaller than the capture      a scratch
    a shift about equal to it             one loss in a string of winners
                                          (the old bid becomes the new ask)
    a shift larger than it                a loss

So the quantity is a ratio, and the thresholds 0.5 / 1.0 / 2.0 are those three
regimes. Everything here is in service of computing that ratio honestly.

--- Definitions ------------------------------------------------------------

For a window of trades:

    range   interquartile spread of trade prices (p75 - p25). The middle 50%,
            so one stray print does not define it. Reported in cents AND bps.
    level   volume-weighted mean trade price.
    shift   |level(this window) - level(previous window)|.
    ratio   shift / range, both in cents so the units cancel.

RANGE IS REPORTED IN CENTS AS WELL AS BPS, and the cents figure is the one
that matches the constraint. A bps normalisation assumes fixed capital:
shares = capital / price, so profit = capital * range / price. But the binding
constraint is liquidity, not capital -- size is set by what the book absorbs.
Under that constraint profit = range_in_cents * shares_available, and bps
penalises an expensive name for being expensive when the expense is not what
limits the trade. A 15-cent IQR is 15 cents on a $700 name and a $70 one; what
differs is how many shares each will take.

--- The step is part of the definition -------------------------------------

Windows advance by STEP_SEC, not by their own length. With a 30s window and a
10s step consecutive windows share 20 seconds of trades, so the shift measures
a 10-second displacement smoothed over 30 seconds of data. A ratio of 1.0
means something DIFFERENT at a different step, so the step is not a rendering
choice and is not exposed as a per-call knob without also changing what the
thresholds mean.

--- Windows overlap, so windows are not chances -----------------------------

At a 30s window and a 10s step, one 30-second quiet patch produces THREE
overlapping quiet windows. A raw window count is therefore inflated by roughly
window/step -- 1.5x at 15s, 3x at 30s, 6x at 60s -- and the three window
lengths are NOT comparable to each other on it.

`quiet_episodes` counts maximal runs instead: one quiet patch is one episode
whatever the window length. That is the "separate chances" quantity, and it is
the operational metric. The window counts are kept because they carry duration
information the episode count throws away, not because they are chances.

--- Vendoring --------------------------------------------------------------

This module imports numpy and pandas and NOTHING from scalp. That is a
requirement, not an accident: the live tape tool is a separate project which
must not import from scalp (`rm -rf scalp/` has to leave it standing), so it
vendors this file verbatim and diffs it in its own check_vendored.py. Any
`from scalp import ...` added here silently breaks that copy.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# --- the grid ---------------------------------------------------------------

WINDOWS_SEC: tuple[int, ...] = (15, 30, 60)
STEP_SEC: int = 10

# Suffix -> ratio threshold. The suffix is the column name, so it is fixed
# text rather than a formatted float: '05' not '0.5', which would put a dot in
# a SQL identifier.
THRESHOLDS: tuple[tuple[str, float], ...] = (("05", 0.5), ("10", 1.0),
                                             ("20", 2.0))

# THE PRE-REGISTERED PRIMARY. Nine window/threshold combinations on 77
# ticker-days of realised P&L is nine chances to find a spurious winner, which
# is how the previous 180-column sweep produced a top correlation of +0.474
# that fell to +0.321 when the sample doubled. So one combination is named
# BEFORE the data is looked at, and the rest are diagnostics that do not get
# to be the answer:
#
#   quiet_episodes_30s_10        the OPERATIONAL metric -- separate chances at
#                                the 30s window (about four holds at a 6-8s
#                                median) and the 1.0 threshold (the old bid
#                                becoming the new ask).
#   shift_over_range_median_30s  the STATISTICAL test -- continuous, uses
#                                every window instead of thresholding, so it
#                                has more power on a small realised sample.
PRIMARY_WINDOW_SEC: int = 30
PRIMARY_THRESHOLD_KEY: str = "10"

# Minimum trades in a window before its IQR is believed.
#
# Measured rather than assumed: sampling a finely-resolved price path at n
# points and comparing the sampled IQR against the path's true IQR gives a
# median relative error of 33.6% at n=6, 28.8% at n=8, 25.5% at n=10, 20.5% at
# n=15. There is no knee -- it is a smooth 1/sqrt(n) decay -- so this is a
# judgement, not a discovery.
#
# 10, because the thresholds are spaced by factors of two (0.5 / 1.0 / 2.0)
# and a ~25% error in the denominator will not usually carry a ratio across a
# boundary, while at n=6 (34%) it starts to. It also matches
# DEFAULT_FILTERS['min_trades_per_min'] = 10: one minute of the thinnest name
# the universe admits at all.
#
# The cost is coverage on slow names. At a 10-trade guard and Poisson
# arrivals, a 15s window needs ~40 trades/min before half its windows qualify;
# at 30 trades/min only 22% do. That is why the eligible count is stored
# alongside every quiet count -- see quiet_eligible_windows_*.
MIN_TRADES: int = 10

# Cents per dollar. Named because it appears in both the range and the shift,
# and the ratio only cancels if it is the same on both.
_CENTS = 100.0


def threshold_keys() -> tuple[str, ...]:
    return tuple(k for k, _ in THRESHOLDS)


# --- the per-window series --------------------------------------------------

def window_series(t_sec: np.ndarray, price: np.ndarray, size: np.ndarray, *,
                  window_s: float, start_s: float, end_s: float,
                  step_s: float = STEP_SEC,
                  min_trades: int = MIN_TRADES) -> dict:
    """Per-window arrays for one window length over one session.

    `t_sec` must be sorted ascending. Returns arrays all of the same length,
    one entry per window:

        w_start    window start, seconds, same origin as t_sec
        n          trades in the window
        level      volume-weighted mean trade price
        range_c    IQR in cents
        range_bps  IQR in bps of the level
        dollar     dollar volume traded in the window
        shift_c    |level - previous level| in cents
        ratio      shift_c / range_c
        eligible   this window AND its predecessor cleared min_trades

    ELIGIBILITY NEEDS BOTH WINDOWS. The shift is a difference against the
    previous window, so a window whose predecessor was too thin has no
    trustworthy shift even if it is itself dense. Requiring only the current
    window would silently admit a ratio built on an unstable level.
    """
    if end_s <= start_s or window_s <= 0 or step_s <= 0:
        return _empty_series()

    n_win = int(np.floor((end_s - start_s - window_s) / step_s)) + 1
    if n_win <= 0:
        return _empty_series()

    w_start = start_s + np.arange(n_win, dtype="float64") * step_s
    w_end = w_start + window_s

    t = np.asarray(t_sec, dtype="float64")
    p = np.asarray(price, dtype="float64")
    s = np.asarray(size, dtype="float64")

    lo = np.searchsorted(t, w_start, side="left")
    hi = np.searchsorted(t, w_end, side="left")
    n = (hi - lo).astype("int64")

    # Volume-weighted level and dollar volume come from prefix sums, so they
    # cost O(1) per window however many trades it holds.
    pv = np.concatenate(([0.0], np.cumsum(p * s)))
    sv = np.concatenate(([0.0], np.cumsum(s)))
    dollar = pv[hi] - pv[lo]
    shares = sv[hi] - sv[lo]
    with np.errstate(divide="ignore", invalid="ignore"):
        level = np.where(shares > 0, dollar / np.where(shares > 0, shares, 1.0),
                         np.nan)

    # The IQR needs an order statistic per window, so it is the one quantity
    # that cannot come from a prefix sum. Only dense windows are visited.
    range_c = np.full(n_win, np.nan)
    dense = np.flatnonzero(n >= min_trades)
    for i in dense:
        seg = p[lo[i]:hi[i]]
        q75, q25 = np.percentile(seg, (75.0, 25.0))
        range_c[i] = (q75 - q25) * _CENTS

    with np.errstate(divide="ignore", invalid="ignore"):
        range_bps = np.where(level > 0, (range_c / _CENTS) / level * 1e4, np.nan)

    shift_c = np.full(n_win, np.nan)
    shift_c[1:] = np.abs(level[1:] - level[:-1]) * _CENTS

    dense_now = n >= min_trades
    eligible = np.zeros(n_win, dtype=bool)
    eligible[1:] = dense_now[1:] & dense_now[:-1]

    ratio = _ratio(shift_c, range_c)
    ratio[~eligible] = np.nan

    return {"w_start": w_start, "n": n, "level": level, "range_c": range_c,
            "range_bps": range_bps, "dollar": dollar, "shift_c": shift_c,
            "ratio": ratio, "eligible": eligible, "window_s": float(window_s)}


def _ratio(shift_c: np.ndarray, range_c: np.ndarray) -> np.ndarray:
    """shift / range, with the degenerate range handled explicitly.

    A window whose trades all printed at one price has range 0. That is not an
    error and not a missing value -- it is maximal stillness, and what it means
    depends entirely on the shift:

        range 0, shift 0   perfectly still. Ratio 0: as quiet as it gets.
        range 0, shift > 0 the whole window traded at one price, and that price
                           is not the previous window's. A clean level jump
                           with no local range to absorb it -- the worst case,
                           and infinity is the honest answer rather than a
                           divide-by-zero NaN that would drop the window.

    Left to plain float division the first case is 0/0 = NaN and the window
    disappears from the count, which would systematically discard the quietest
    windows in the session.
    """
    out = np.full(shift_c.shape, np.nan)
    ok = np.isfinite(shift_c) & np.isfinite(range_c)
    pos = ok & (range_c > 0)
    out[pos] = shift_c[pos] / range_c[pos]
    still = ok & (range_c <= 0)
    out[still] = np.where(shift_c[still] <= 0, 0.0, np.inf)
    return out


def _empty_series() -> dict:
    z = np.zeros(0)
    return {"w_start": z, "n": z.astype("int64"), "level": z, "range_c": z,
            "range_bps": z, "dollar": z, "shift_c": z, "ratio": z,
            "eligible": np.zeros(0, dtype=bool), "window_s": 0.0}


def session_series(t_sec, price, size, *, start_s: float, end_s: float,
                   windows=WINDOWS_SEC, step_s: float = STEP_SEC,
                   min_trades: int = MIN_TRADES) -> dict:
    """window_series for every window length, computed ONCE per session.

    The daily row and the 15-minute rows are both aggregations of this, rather
    than separate computations over re-sliced trades. That is what makes the
    daily count equal the sum of the bucket counts by construction instead of
    by coincidence.
    """
    return {int(w): window_series(t_sec, price, size, window_s=float(w),
                                  start_s=start_s, end_s=end_s, step_s=step_s,
                                  min_trades=min_trades)
            for w in windows}


# --- aggregation ------------------------------------------------------------

def _episodes(mask: np.ndarray) -> int:
    """Maximal runs of consecutive True. One quiet patch, one episode."""
    if mask.size == 0:
        return 0
    m = mask.astype("int8")
    return int(m[0] + np.sum((m[1:] == 1) & (m[:-1] == 0)))


def _median(values: np.ndarray) -> float:
    v = values[np.isfinite(values)]
    return float(np.median(v)) if v.size else float("nan")


def daily_metrics(series: dict, *, thresholds=THRESHOLDS,
                  primary_window: int = PRIMARY_WINDOW_SEC,
                  primary_key: str = PRIMARY_THRESHOLD_KEY) -> dict:
    """The stored per-symbol-day metrics, from session_series output.

    Counts, not shares. A name quiet 60% of the day is not better than one
    quiet 40% if the 40% arrives in more separate chances -- only one name can
    be traded at a time. The share is derivable from the count and the
    eligible count, which is the other reason the eligible count is stored: a
    quiet count of 0 otherwise conflates "never quiet" with "never enough
    trades to tell", and on a 15s window at 30 trades/min the second is the
    common case.

    The range and dollar-flow figures are taken over windows that are quiet AT
    THE PRIMARY THRESHOLD, not over all eligible windows. They exist to answer
    "what do I capture when the name is tradeable", so they have to be
    conditioned on tradeable; and since their names carry no threshold suffix,
    the threshold has to be the pre-registered one rather than a free choice.
    """
    out: dict = {}
    for w, ser in series.items():
        elig = ser["eligible"]
        ratio = ser["ratio"]
        out[f"quiet_eligible_windows_{w}s"] = int(elig.sum())
        for key, thr in thresholds:
            quiet = elig & np.isfinite(ratio) & (ratio < thr)
            out[f"quiet_windows_{w}s_{key}"] = int(quiet.sum())

        # Across the whole session, not just the quiet part: this is the
        # continuous statistic, and conditioning it on quietness would throw
        # away the half of the distribution that says how bad the rest is.
        out[f"shift_over_range_median_{w}s"] = _median(ratio[elig])

        thr_primary = dict(thresholds)[primary_key]
        sel = elig & np.isfinite(ratio) & (ratio < thr_primary)
        out[f"quiet_range_cents_{w}s"] = _median(ser["range_c"][sel])
        out[f"quiet_range_bps_{w}s"] = _median(ser["range_bps"][sel])
        # Dollar flow per MINUTE, so the three window lengths are comparable.
        per_min = ser["dollar"][sel] / (ser["window_s"] / 60.0) if sel.any() \
            else np.zeros(0)
        out[f"quiet_dollar_vol_per_min_{w}s"] = _median(per_min)

    ser = series.get(int(primary_window))
    if ser is not None:
        thr = dict(thresholds)[primary_key]
        quiet = ser["eligible"] & np.isfinite(ser["ratio"]) & (ser["ratio"] < thr)
        out[f"quiet_episodes_{primary_window}s_{primary_key}"] = _episodes(quiet)
    return out


def bucket_metrics(series: dict, bucket_start_s: float, bucket_end_s: float, *,
                   window_s: int = PRIMARY_WINDOW_SEC,
                   threshold_key: str = PRIMARY_THRESHOLD_KEY,
                   thresholds=THRESHOLDS) -> dict:
    """The primary combination only, restricted to one intraday bucket.

    Only the primary goes to intraday_metrics. All nine would be nine wide
    columns for a table whose whole design note is that it is a SUBSET -- the
    long format at every metric produced 32M rows and 96% of the database.

    A window is attributed to the bucket its START falls in, so a window
    straddling a boundary belongs to one bucket and is not double-counted.
    """
    ser = series.get(int(window_s))
    if ser is None or ser["w_start"].size == 0:
        return {f"quiet_windows_{window_s}s_{threshold_key}": 0,
                f"quiet_eligible_windows_{window_s}s": 0}
    thr = dict(thresholds)[threshold_key]
    inb = (ser["w_start"] >= bucket_start_s) & (ser["w_start"] < bucket_end_s)
    elig = inb & ser["eligible"]
    quiet = elig & np.isfinite(ser["ratio"]) & (ser["ratio"] < thr)
    return {f"quiet_windows_{window_s}s_{threshold_key}": int(quiet.sum()),
            f"quiet_eligible_windows_{window_s}s": int(elig.sum())}


def metric_names(windows=WINDOWS_SEC, thresholds=THRESHOLDS,
                 primary_window: int = PRIMARY_WINDOW_SEC,
                 primary_key: str = PRIMARY_THRESHOLD_KEY) -> tuple:
    """Every daily key this module produces. The writer and the tests both
    read it, so a new metric cannot be added without both seeing it."""
    names = []
    for w in windows:
        names.append(f"quiet_eligible_windows_{w}s")
        names += [f"quiet_windows_{w}s_{k}" for k, _ in thresholds]
        names += [f"shift_over_range_median_{w}s", f"quiet_range_cents_{w}s",
                  f"quiet_range_bps_{w}s", f"quiet_dollar_vol_per_min_{w}s"]
    names.append(f"quiet_episodes_{primary_window}s_{primary_key}")
    return tuple(names)


# --- frame entry point ------------------------------------------------------

def from_trades(trades: pd.DataFrame, *, time_col: str, price_col: str,
                size_col: str, start: pd.Timestamp, end: pd.Timestamp,
                windows=WINDOWS_SEC, step_s: float = STEP_SEC,
                min_trades: int = MIN_TRADES) -> dict:
    """session_series from a trade frame. The one place pandas is required.

    Takes the EXCLUDED-PRINTS-REMOVED frame, not the raw window: a restatement
    or an off-quote print is not a price somebody paid at that moment, and the
    range is a measure of what could have been captured.
    """
    if trades.empty:
        return {int(w): _empty_series() for w in windows}
    t = pd.to_datetime(trades[time_col], errors="coerce")
    p = pd.to_numeric(trades[price_col], errors="coerce")
    s = pd.to_numeric(trades[size_col], errors="coerce")
    ok = t.notna() & p.notna() & s.notna() & (p > 0) & (s > 0)
    t, p, s = t[ok], p[ok], s[ok]
    if t.empty:
        return {int(w): _empty_series() for w in windows}
    order = np.argsort(t.to_numpy(), kind="stable")
    t_sec = t.to_numpy(dtype="datetime64[ns]").astype("int64")[order] / 1e9
    return session_series(
        t_sec, p.to_numpy(dtype="float64")[order],
        s.to_numpy(dtype="float64")[order],
        start_s=start.to_datetime64().astype("datetime64[ns]").astype("int64") / 1e9,
        end_s=end.to_datetime64().astype("datetime64[ns]").astype("int64") / 1e9,
        windows=windows, step_s=step_s, min_trades=min_trades)
