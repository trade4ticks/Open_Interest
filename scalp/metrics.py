"""Metric computation for the equities-scalp ranker.

EVERY FUNCTION TAKES ARBITRARY [start, end) BOUNDS. Nothing here knows what a
"day" is. The nightly batch passes the RTH window; the Phase 2 intraday
re-rank will pass the last 30 minutes; the 15-minute breakdown passes each
bucket in turn. Same functions, different bounds — that is the whole reason
the bounds are parameters rather than constants.

TWO SOURCES, TWO ENTRY POINTS, DELIBERATELY NOT MERGED.

  compute_window()        `history/trade_quote` — every trade paired with the
                          prevailing NBBO at that trade. Drives spread, noise
                          and flow. Its quote series is SAMPLED AT TRADE
                          TIMES, which is fine for spread and midpoint levels
                          and useless for counting quote events.

  compute_quote_window()  `history/quote` at tick — the real quote stream.
                          Drives flicker and BBO lifetime.

They stay separate so a trade-sampled quote series cannot reach a metric that
needs the real stream. That mistake already happened once and produced
`bid_persist_ms_median_tradesampled`, which has been removed rather than kept
beside the correct figure: a knowingly-worse metric sitting next to available
correct data only invites someone to use it.

ONE METRIC STILL RETURNS NaN. `bbo_change_without_trade_share` — cancel vs
consumption — needs both sources at once: the quote stream to see the BBO
change and the trade tape to say whether a trade met it at that price. It is
now computable in principle and is NOT implemented; see METRICS.md.

Returning NaN is deliberate. A number computed from the wrong source would
rank tickers and look reasonable doing it.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from scalp import config


# --- the timestamp collapse --------------------------------------------------

def collapse_to_distinct_instants(df: pd.DataFrame,
                                  time_col: str) -> pd.DataFrame:
    """Reduce records sharing an instant to one observation: the LAST.

    THIS IS MANDATORY BEFORE ANY DURATION WEIGHTING, AND THE ORDER MATTERS.

    49.8% of quote records share a timestamp with another record. If durations
    are computed first and collapsed second, every record but the last at each
    instant gets a gap of exactly zero — so half the observations enter the
    time-weighted midpoint with zero weight and vanish. No error is raised, no
    row count changes, and the resulting number looks entirely normal.

    Collapsing first and then measuring the gap to the next DISTINCT timestamp
    gives every surviving observation a real duration.

    The last record at an instant is kept because it is the state that stood
    when the clock moved on — the one that actually persisted into the
    following interval. The sort is stable, so "last" means last-arrived, not
    an arbitrary pick.

    See tests/test_timestamp_collapse.py, which asserts the failure mode
    directly rather than only the fix.
    """
    if df.empty:
        return df
    ordered = df.sort_values(time_col, kind="mergesort")     # stable
    return ordered.drop_duplicates(subset=[time_col], keep="last")


def durations_seconds(times: pd.Series, end: pd.Timestamp) -> pd.Series:
    """Seconds each observation stood, measured to the next distinct instant.

    The final observation runs to `end`. Requires `times` to already be
    distinct and sorted — call collapse_to_distinct_instants first.
    """
    if len(times) == 0:
        return pd.Series(dtype="float64")
    nxt = times.shift(-1)
    nxt.iloc[-1] = end
    out = (nxt - times).dt.total_seconds()
    return out.clip(lower=0.0)


def time_weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce")
    ok = v.notna() & w.notna() & (w > 0)
    if not ok.any():
        return float("nan")
    return float((v[ok] * w[ok]).sum() / w[ok].sum())


# --- windowing ---------------------------------------------------------------

def slice_window(df: pd.DataFrame, time_col: str,
                 start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """Rows with start <= t < end. Half-open so adjacent windows never
    double-count a record that lands exactly on a boundary."""
    t = df[time_col]
    return df[(t >= start) & (t < end)]


def exclude_auction_edges(start: pd.Timestamp, end: pd.Timestamp
                          ) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Trim the auction minutes off a window's edges.

    Applied to SPREAD and NOISE only. An auction quote is not a tradeable
    condition — FDX opened bid 330.45 / ask 336.15, a $5.70 spread — and one
    such observation distorts a daily average. Trade counts keep the full
    window, because an arrival is an arrival.
    """
    return (start + pd.Timedelta(minutes=config.EXCLUDE_OPEN_MINUTES),
            end - pd.Timedelta(minutes=config.EXCLUDE_CLOSE_MINUTES))


def _bucket_index(times: pd.Series, start: pd.Timestamp,
                  horizon_sec: float) -> pd.Series:
    """Fixed-clock bucket id. Anchored to `start`, never to trade arrivals —
    trade-to-trade bucketing makes busy stocks look artificially calm."""
    return ((times - start).dt.total_seconds() // horizon_sec).astype("int64")


# --- excluded prints ---------------------------------------------------------

# Condition flags are computed ONCE PER SYMBOL-DAY and carried as columns.
#
# A condition code is a property of the trade row, not of the window the row
# falls in, so recomputing it per window was pure repetition — and it was
# repeated 27 times through a `.apply(axis=1)`, which builds a Series per row
# and calls a Python function on it across all seven condition columns.
#
# Profiling put that single call at 134.8s of 159.7s cumulative — 84% of the
# whole compute — and the vectorized form below measured 690x faster with
# identical output. `isin` runs in pandas' C layer over a whole column at a
# time; nothing else in the metric path was ever a Python row loop.
FLAG_PREFIX = "_scalp_"
FLAG_EXCLUDED = f"{FLAG_PREFIX}excluded"
FLAG_ODD_LOT_VENDOR = f"{FLAG_PREFIX}odd_lot_vendor"
VENDOR_ODD_LOT_CODE = 115


def flag_col_for_code(code: int) -> str:
    return f"{FLAG_PREFIX}cond_{code}"


def _match_values(values) -> set:
    """Membership set that matches whether the column parsed as int or float.

    The condition columns arrive from CSV and may be float64 when any value in
    the column was null. `isin({4})` against a float column is not something to
    leave to a cast, so both forms go in.
    """
    out: set = set()
    for v in values:
        out.add(int(v))
        out.add(float(v))
    return out


def any_column_matches(df: pd.DataFrame, condition_cols: list[str],
                       values) -> pd.Series:
    """True where ANY condition column carries one of `values`. Vectorized.

    0 and 255 are padding in `ext_condition1..4` rather than codes, and need
    no special handling here: they are not members of any set this is asked
    about, so they never match.
    """
    mask = pd.Series(False, index=df.index)
    if not condition_cols:
        return mask
    wanted = _match_values(values)
    for col in condition_cols:
        mask |= df[col].isin(wanted)
    return mask


def attach_condition_flags(df: pd.DataFrame,
                           condition_cols: list[str]) -> pd.DataFrame:
    """Add the boolean condition columns. Call once per symbol-day, at load.

    Every downstream mask then reads a column instead of re-deriving it:
    the exclusion mask, the vendor odd-lot check, and the per-code provenance
    counts.
    """
    df[FLAG_EXCLUDED] = any_column_matches(
        df, condition_cols, config.EXCLUDED_CONDITION_CODES)
    df[FLAG_ODD_LOT_VENDOR] = any_column_matches(
        df, condition_cols, {VENDOR_ODD_LOT_CODE})
    for code in sorted(config.EXCLUDED_CONDITION_CODES):
        df[flag_col_for_code(code)] = any_column_matches(df, condition_cols,
                                                         {code})
    return df


def _flag(df: pd.DataFrame, column: str, condition_cols: list[str],
          values) -> pd.Series:
    """Read a precomputed flag column, or derive it if the frame lacks one.

    The fallback keeps every function here usable on a raw frame — a test, a
    diagnostic, an ad-hoc slice — without making the pipeline pay for it. The
    pipeline calls attach_condition_flags once and always hits the fast path.
    """
    if column in df.columns:
        return df[column]
    return any_column_matches(df, condition_cols, values)


def excluded_mask(df: pd.DataFrame, condition_cols: list[str]) -> pd.Series:
    """True where a print must not count as an arrival.

    Restatements (51, 66) re-report shares already counted. Off-quote codes
    (4, 96, 124) print systematically away from the NBBO and are not
    executions at the quote. Both sets were established by measurement — see
    config.
    """
    return _flag(df, FLAG_EXCLUDED, condition_cols,
                 config.EXCLUDED_CONDITION_CODES)


# --- spread ------------------------------------------------------------------

def spread_metrics(q: pd.DataFrame, *, bid: str, ask: str, time_col: str,
                   end: pd.Timestamp) -> dict:
    """Quoted spread in cents and bps, plus the duration-weighted version.

    `q` must already be collapsed to distinct instants and sliced to the
    window. Crossed and locked quotes (ask <= bid) are dropped: they are
    transient artefacts of a consolidated feed and a negative spread is not a
    capturable one.
    """
    out = {
        "spread_cents_mean": float("nan"), "spread_cents_median": float("nan"),
        "spread_bps_mean": float("nan"), "spread_bps_median": float("nan"),
        "spread_bps_tw": float("nan"), "spread_cents_tw": float("nan"),
        "quote_observations": 0, "crossed_locked_share": float("nan"),
    }
    if q.empty:
        return out

    b = pd.to_numeric(q[bid], errors="coerce")
    a = pd.to_numeric(q[ask], errors="coerce")
    mid = (a + b) / 2.0
    spread = a - b
    usable = b.notna() & a.notna() & (mid > 0)
    if not usable.any():
        return out

    out["crossed_locked_share"] = float((spread[usable] <= 0).mean())
    good = usable & (spread > 0)
    if not good.any():
        return out

    sp = spread[good]
    md = mid[good]
    bps = sp / md * 10_000.0

    out["quote_observations"] = int(good.sum())
    out["spread_cents_mean"] = float(sp.mean() * 100)
    out["spread_cents_median"] = float(sp.median() * 100)
    out["spread_bps_mean"] = float(bps.mean())
    out["spread_bps_median"] = float(bps.median())

    w = durations_seconds(q.loc[good, time_col], end)
    out["spread_cents_tw"] = time_weighted_mean(sp * 100, w)
    out["spread_bps_tw"] = time_weighted_mean(bps, w)
    return out


# --- noise -------------------------------------------------------------------

def _bps_changes(values: pd.Series) -> pd.Series:
    """Absolute change between consecutive bucket values, in bps of their own
    level. Scaling each change by its own level rather than a window-wide
    average keeps a 10c move on an $1,100 stock calm and the same move on a
    $130 stock not."""
    v = pd.to_numeric(values, errors="coerce").dropna()
    if len(v) < 2:
        return pd.Series(dtype="float64")
    prev = v.shift(1)
    level = (v + prev) / 2.0
    ok = level > 0
    return (v - prev).abs()[ok] / level[ok] * 10_000.0


def bucket_values(q: pd.DataFrame, *, value: pd.Series, time_col: str,
                  start: pd.Timestamp, end: pd.Timestamp,
                  horizon_sec: float, weighted: bool) -> pd.Series:
    """One representative value per fixed-clock bucket.

    weighted=True  -> duration-weighted mean inside the bucket. Flicker
                      averages out; genuine repricing survives.
    weighted=False -> last observation in the bucket (instantaneous).

    A duration that straddles a bucket boundary is attributed whole to the
    bucket its observation STARTED in. With a ~1 ms median gap against a 5-30 s
    bucket this is a rounding effect, not a bias — but it is an approximation
    and is written down as one.
    """
    if q.empty:
        return pd.Series(dtype="float64")
    idx = _bucket_index(q[time_col], start, horizon_sec)
    if not weighted:
        return value.groupby(idx).last()
    w = durations_seconds(q[time_col], end)
    frame = pd.DataFrame({"v": value, "w": w, "b": idx}).dropna()
    frame = frame[frame["w"] > 0]
    if frame.empty:
        return pd.Series(dtype="float64")
    num = (frame["v"] * frame["w"]).groupby(frame["b"]).sum()
    den = frame["w"].groupby(frame["b"]).sum()
    return num / den


def noise_metrics(q: pd.DataFrame, trades: pd.DataFrame, *,
                  bid: str, ask: str, price: str, time_col: str,
                  trade_time_col: str,
                  start: pd.Timestamp, end: pd.Timestamp,
                  horizons=config.NOISE_HORIZONS_SEC) -> dict:
    """All five noise variants at every horizon, in bps.

    Each is the MEDIAN absolute change between consecutive fixed-clock
    buckets. Median rather than mean because a single auction-sized jump would
    otherwise define the number.

    The variants exist because each has a known flaw and no one of them is
    trusted:

      tw_mid       duration-weighted midpoint. Flicker averages out. Current
                   best guess.
      last_mid     instantaneous midpoint. A 40-share bid appearing and
                   vanishing 10c away moves it with no economic content.
      trade_price  contains the spread itself, since consecutive trades
                   alternate bid/ask — so spread/noise partly divides spread
                   by spread.
      bid_side }   the strategy's actual case is an unstable bid against a
      ask_side }   still offer, and the midpoint destroys that asymmetry by
                   construction. s5 measured 602 bid-only moves, 664 ask-only
                   and ONE two-sided over 10,278 records.
    """
    out: dict[str, float] = {}
    b = pd.to_numeric(q[bid], errors="coerce") if not q.empty else pd.Series(dtype=float)
    a = pd.to_numeric(q[ask], errors="coerce") if not q.empty else pd.Series(dtype=float)
    mid = (a + b) / 2.0 if not q.empty else pd.Series(dtype=float)

    tp = (pd.to_numeric(trades[price], errors="coerce")
          if not trades.empty else pd.Series(dtype=float))

    for h in horizons:
        specs = {
            f"noise_bps_tw_mid_{h}s":
                (q, mid, time_col, True),
            f"noise_bps_last_mid_{h}s":
                (q, mid, time_col, False),
            f"noise_bps_bid_side_{h}s":
                (q, b, time_col, True),
            f"noise_bps_ask_side_{h}s":
                (q, a, time_col, True),
            f"noise_bps_trade_price_{h}s":
                (trades, tp, trade_time_col, False),
        }
        for name, (frame, series, tcol, weighted) in specs.items():
            if frame.empty or series.empty:
                out[name] = float("nan")
                continue
            vals = bucket_values(frame, value=series, time_col=tcol,
                                 start=start, end=end, horizon_sec=h,
                                 weighted=weighted)
            changes = _bps_changes(vals)
            out[name] = float(changes.median()) if len(changes) else float("nan")
            out[f"{name}__buckets"] = int(len(vals))
    return out


# --- flicker / quote stability ----------------------------------------------

def flicker_metrics(q_raw: pd.DataFrame, q: pd.DataFrame, *,
                    bid: str, ask: str, time_col: str,
                    start: pd.Timestamp, end: pd.Timestamp) -> dict:
    """Flicker and BBO lifetime, FROM THE TICK QUOTE STREAM.

    This runs on `history/quote` at tick resolution, never on trade_quote.
    The earlier trade-sampled variant has been removed rather than kept
    alongside: a knowingly-worse metric sitting next to available correct data
    only invites someone to use it, and `bid_persist_ms_median_tradesampled`
    was an honest name for an inferior quantity, not a reason to keep it.

    `q_raw` is pre-collapse (every record); `q` is collapsed to distinct
    instants.

    s5 found 78.4% of records identical on price, size AND venue. Venue
    turnover explained only 1.7 points of the 80.1% raw figure, so the repeats
    are largely unexplained by anything visible in the response. The likely
    cause is that the NBBO is recomputed on every participant's quote update
    rather than only when the best changes — a venue behind the inside
    adjusting its quote fires a record while the NBBO is unchanged. The
    endpoint returns the NBBO, so that cannot be confirmed from these columns.

    If that is right, quote_records_per_min measures TOTAL QUOTE TRAFFIC
    across all venues, while nbbo_changes_per_min measures inside-market
    instability. Those may rank tickers very differently, so both are computed
    and calibration decides.
    """
    minutes = max((end - start).total_seconds() / 60.0, 1e-9)
    out = {
        "quote_records_per_min": float("nan"),
        "nbbo_changes_per_min": float("nan"),
        "bid_changes_per_min": float("nan"),
        "ask_changes_per_min": float("nan"),
        "two_sided_change_share": float("nan"),
        "same_instant_share": float("nan"),
        # True lifetime of the best bid / best offer: how long a price stood
        # before it was replaced. Correct here because the source is the tick
        # quote stream, which sees quotes that came and went between trades.
        "bid_lifetime_ms_median": float("nan"),
        "ask_lifetime_ms_median": float("nan"),
        "bid_lifetime_ms_mean": float("nan"),
        "ask_lifetime_ms_mean": float("nan"),
        # Cancel vs consumption. Needs the trade tape alongside this quote
        # stream to say whether a BBO change was met by a trade at that price,
        # so it is not computable inside this function. See
        # cross_source_metrics and METRICS.md.
        "bbo_change_without_trade_share": float("nan"),
    }
    if q_raw.empty:
        return out

    out["quote_records"] = int(len(q_raw))
    out["quote_records_per_min"] = len(q_raw) / minutes
    out["same_instant_share"] = float(1.0 - len(q) / len(q_raw))

    if q.empty or len(q) < 2:
        return out

    b = pd.to_numeric(q[bid], errors="coerce")
    a = pd.to_numeric(q[ask], errors="coerce")
    bid_moved = b.diff().fillna(0) != 0
    ask_moved = a.diff().fillna(0) != 0
    changed = bid_moved | ask_moved

    out["nbbo_changes_per_min"] = int(changed.sum()) / minutes
    out["bid_changes_per_min"] = int(bid_moved.sum()) / minutes
    out["ask_changes_per_min"] = int(ask_moved.sum()) / minutes
    both = int((bid_moved & ask_moved).sum())
    out["two_sided_change_share"] = (both / int(changed.sum())
                                     if changed.any() else float("nan"))

    # BBO lifetime: sum the durations of consecutive records holding the same
    # price, so one "life" runs from the moment a price becomes the best until
    # it is replaced. cumsum over the change flag groups those runs.
    #
    # The final run is left-censored by the window end — it was still standing
    # when observation stopped — so it is excluded rather than counted as
    # having ended, which would drag the median down.
    dur = durations_seconds(q[time_col], end) * 1000.0
    for side, moved, key in ((bid, bid_moved, "bid"), (ask, ask_moved, "ask")):
        if not moved.any():
            continue
        runs = dur.groupby(moved.cumsum()).sum()
        if len(runs) > 1:
            runs = runs.iloc[:-1]           # drop the still-standing quote
        out[f"{key}_lifetime_ms_median"] = float(runs.median())
        out[f"{key}_lifetime_ms_mean"] = float(runs.mean())
    return out


# --- flow --------------------------------------------------------------------

def flow_metrics(trades: pd.DataFrame, *, price: str, size: str,
                 bid: str, ask: str, exchange: str | None,
                 start: pd.Timestamp, end: pd.Timestamp,
                 vendor_odd_lot: pd.Series | None = None) -> dict:
    """Arrival rate, size distribution, two-sidedness, off-exchange share.

    `trades` must already have excluded prints removed.

    Two-sidedness is the point of the classification: this strategy needs
    buyers AND sellers arriving, not one-way flow. `two_sided_balance` is
    min/max of the at-bid and at-ask shares, so 1.0 is perfectly balanced and
    0.0 is entirely one-directional.
    """
    minutes = max((end - start).total_seconds() / 60.0, 1e-9)
    out = {
        "trades_per_min": float("nan"), "shares_per_min": float("nan"),
        "trade_size_mean": float("nan"), "trade_size_median": float("nan"),
        "odd_lot_share": float("nan"),          # against the TIERED round lot
        "sub_100_share": float("nan"),          # fixed 100, for comparability
        "round_lot_size": float("nan"),
        "reference_price": float("nan"),
        "odd_lot_flag_disagree_share": float("nan"),
        "at_bid_share": float("nan"), "at_ask_share": float("nan"),
        "between_share": float("nan"), "two_sided_balance": float("nan"),
        "off_exchange_share": float("nan"),
        "unidentified_exchange_share": float("nan"),
        "off_mid_bps": float("nan"), "trades": 0,
    }
    if trades.empty:
        out["trades_per_min"] = 0.0
        return out

    p = pd.to_numeric(trades[price], errors="coerce")
    s = pd.to_numeric(trades[size], errors="coerce")
    b = pd.to_numeric(trades[bid], errors="coerce")
    a = pd.to_numeric(trades[ask], errors="coerce")

    out["trades"] = int(len(trades))
    out["trades_per_min"] = len(trades) / minutes
    out["shares_per_min"] = float(s.fillna(0).sum()) / minutes
    out["trade_size_mean"] = float(s.mean())
    out["trade_size_median"] = float(s.median())

    # Odd lots against the PRICE-TIERED round lot, not a fixed 100. FDX at
    # ~$330 has a 40-share round lot and quotes 40 x 40 at the inside —
    # exactly one round lot — so a fixed boundary would call every inside-size
    # print an odd lot in the names this strategy targets.
    #
    # One reference price for the whole window, because the exchanges assign
    # the tier periodically rather than per print. Median trade price is used
    # rather than last, so a closing spike cannot move the boundary.
    reference = float(p.median()) if p.notna().any() else float("nan")
    out["reference_price"] = reference
    if np.isfinite(reference) and reference > 0:
        lot = config.round_lot_size(reference)
        out["round_lot_size"] = float(lot)
        tiered_odd = s < lot
        out["odd_lot_share"] = float(tiered_odd.mean())
    else:
        tiered_odd = None
    out["sub_100_share"] = float((s < 100).mean())

    # Free correctness check: the vendor flags odd lots independently with
    # condition 115. Disagreement between that flag and the tiered calculation
    # is either a wrong tier here or a vendor flag on a different basis, and
    # either way it is worth seeing rather than assuming.
    if vendor_odd_lot is not None and tiered_odd is not None:
        vendor_odd = vendor_odd_lot.reindex(trades.index).fillna(False)
        out["odd_lot_flag_disagree_share"] = float(
            (vendor_odd != tiered_odd).mean())
        out["odd_lot_vendor_share"] = float(vendor_odd.mean())

    mid = (a + b) / 2.0
    ok = p.notna() & b.notna() & a.notna() & (mid > 0)
    if ok.any():
        # off_mid_bps: a spread-and-noise composite available free from every
        # trade — no bucketing, no time-weighting, no horizon choice.
        out["off_mid_bps"] = float(
            ((p[ok] - mid[ok]).abs() / mid[ok] * 10_000.0).median())

        at_bid = (p[ok] <= b[ok])
        at_ask = (p[ok] >= a[ok])
        between = ~at_bid & ~at_ask
        n = int(ok.sum())
        out["at_bid_share"] = float(at_bid.sum()) / n
        out["at_ask_share"] = float(at_ask.sum()) / n
        out["between_share"] = float(between.sum()) / n
        hi = max(out["at_bid_share"], out["at_ask_share"])
        lo = min(out["at_bid_share"], out["at_ask_share"])
        out["two_sided_balance"] = (lo / hi) if hi > 0 else float("nan")

    if exchange is not None:
        codes = trades[exchange]
        out["off_exchange_share"] = float(codes.map(config.is_off_exchange).mean())
        out["unidentified_exchange_share"] = float(
            codes.map(lambda c: _is_unidentified(c)).mean())
    return out


def _is_unidentified(code) -> bool:
    try:
        return int(code) in config.UNIDENTIFIED_EXCHANGE_CODES
    except (TypeError, ValueError):
        return False


# --- orchestration -----------------------------------------------------------

class Columns:
    """Resolved column names for one frame. Built once by the caller."""

    def __init__(self, *, time: str, price: str, size: str, bid: str, ask: str,
                 exchange: str | None = None,
                 condition_cols: list[str] | None = None):
        self.time = time
        self.price = price
        self.size = size
        self.bid = bid
        self.ask = ask
        self.exchange = exchange
        self.condition_cols = condition_cols or []


def compute_window(df: pd.DataFrame, cols: Columns,
                   start: pd.Timestamp, end: pd.Timestamp, *,
                   exclude_auction_edges_for_quotes: bool = True,
                   with_provenance: bool = True) -> dict:
    """Every metric for one symbol over [start, end).

    This is THE function. The daily row, each 15-minute row, and the future
    intraday re-rank all call it with different bounds and nothing else
    differs.

    Auction-edge trimming applies to the quote-derived metrics only. Trade
    counts and volume use the full window, because an arrival is an arrival.
    """
    window = slice_window(df, cols.time, start, end)
    result: dict = {
        "window_start": start, "window_end": end,
        "window_minutes": (end - start).total_seconds() / 60.0,
        "rows_raw": int(len(window)),
    }
    if window.empty:
        result.update(spread_metrics(window, bid=cols.bid, ask=cols.ask,
                                     time_col=cols.time, end=end))
        result.update(flow_metrics(window, price=cols.price, size=cols.size,
                                   bid=cols.bid, ask=cols.ask,
                                   exchange=cols.exchange,
                                   start=start, end=end))
        result.update(noise_metrics(window, window, bid=cols.bid, ask=cols.ask,
                                    price=cols.price, time_col=cols.time,
                                    trade_time_col=cols.time,
                                    start=start, end=end))
        return result

    # Trades: drop restatements and off-quote prints before any counting.
    # Both flags are read off precomputed columns — attach_condition_flags has
    # already run once for the whole symbol-day.
    dropped = excluded_mask(window, cols.condition_cols)
    vendor_odd = _flag(window, FLAG_ODD_LOT_VENDOR, cols.condition_cols,
                       {VENDOR_ODD_LOT_CODE})
    trades = window[~dropped]
    result["rows_excluded"] = int(dropped.sum())
    result["excluded_share"] = float(dropped.mean())

    # Quotes: collapse same-instant records BEFORE any duration weighting.
    if exclude_auction_edges_for_quotes:
        q_start, q_end = exclude_auction_edges(start, end)
    else:
        q_start, q_end = start, end
    q_window = slice_window(window, cols.time, q_start, q_end)
    q = collapse_to_distinct_instants(q_window, cols.time)

    result.update(spread_metrics(q, bid=cols.bid, ask=cols.ask,
                                 time_col=cols.time, end=q_end))
    result.update(noise_metrics(q, trades, bid=cols.bid, ask=cols.ask,
                                price=cols.price, time_col=cols.time,
                                trade_time_col=cols.time,
                                start=q_start, end=q_end))
    result.update(flow_metrics(trades, price=cols.price, size=cols.size,
                               bid=cols.bid, ask=cols.ask,
                               exchange=cols.exchange, start=start, end=end,
                               vendor_odd_lot=vendor_odd))
    result.update(ranking_ratios(result))
    if with_provenance:
        result["_provenance"] = trade_provenance(
            window, q_window, q, dropped, cols,
            start=start, end=end, q_start=q_start, q_end=q_end)
    return result


def compute_quote_window(qdf: pd.DataFrame, *, bid: str, ask: str,
                         time_col: str,
                         start: pd.Timestamp, end: pd.Timestamp) -> dict:
    """Flicker and BBO lifetime from the TICK QUOTE STREAM.

    Separate from compute_window because it takes a different frame from a
    different endpoint. Keeping them apart is what stops a trade-sampled
    quote series being fed to metrics that need the real stream — which is
    exactly how `bid_persist_ms_median_tradesampled` came to exist.

    Auction edges are NOT trimmed here: flicker is a count over a window, and
    the caller passes whatever window it wants counted.
    """
    window = slice_window(qdf, time_col, start, end)
    collapsed = collapse_to_distinct_instants(window, time_col)
    out = {
        "quote_window_start": start, "quote_window_end": end,
        "quote_rows_raw": int(len(window)),
    }
    out.update(flicker_metrics(window, collapsed, bid=bid, ask=ask,
                               time_col=time_col, start=start, end=end))
    out["_quote_provenance"] = {
        "quote_records_raw": int(len(window)),
        "quote_records_after_collapse": int(len(collapsed)),
        "quote_records_lost_to_collapse": int(len(window) - len(collapsed)),
    }
    return out


# --- provenance --------------------------------------------------------------

def trade_provenance(window: pd.DataFrame, q_window: pd.DataFrame,
                     q: pd.DataFrame, dropped: pd.Series, cols: Columns, *,
                     start: pd.Timestamp, end: pd.Timestamp,
                     q_start: pd.Timestamp, q_end: pd.Timestamp) -> dict:
    """What was dropped, by what rule, getting from raw tape to the metrics.

    Several decisions silently change the numbers — condition-code exclusions,
    auction-edge trimming, crossed and locked quotes, same-instant collapsing.
    Reading METRICS.md should not be the only way to find out that a row was
    computed from 94% of the tape.

    Every count is per symbol-day, and the condition drops are broken out by
    code so a change in one code's share is visible without re-deriving it.
    """
    n_raw = len(window)
    prov: dict[str, float] = {
        "rows_raw": int(n_raw),
        "auction_minutes_trimmed": float(
            (q_start - start).total_seconds() / 60.0
            + (end - q_end).total_seconds() / 60.0),
    }

    # Trades dropped by condition code, one entry per code. Each count reads a
    # precomputed boolean column; previously this was one row-wise pass PER
    # CODE, which is five of them, times every window.
    dropped_total = 0
    for code in sorted(config.EXCLUDED_CONDITION_CODES):
        n = int(_flag(window, flag_col_for_code(code), cols.condition_cols,
                      {code}).sum())
        prov[f"dropped_condition_{code}"] = n
        dropped_total += n
    # Sum of per-code counts can exceed the row count when a print carries two
    # excluded codes, so the retained figure uses the union, not the sum.
    union_dropped = int(dropped.sum())
    prov["dropped_condition_any"] = union_dropped
    prov["dropped_condition_code_sum"] = dropped_total
    prov["trades_retained"] = int(n_raw - union_dropped)
    prov["trade_retained_share"] = (
        float((n_raw - union_dropped) / n_raw) if n_raw else float("nan"))

    # Quotes dropped as crossed or locked.
    if not q.empty:
        b = pd.to_numeric(q[cols.bid], errors="coerce")
        a = pd.to_numeric(q[cols.ask], errors="coerce")
        usable = b.notna() & a.notna()
        crossed = usable & ((a - b) <= 0)
        prov["quotes_in_window"] = int(len(q))
        prov["quotes_crossed_locked"] = int(crossed.sum())
        prov["quote_retained_share"] = (
            float(1.0 - crossed.sum() / usable.sum()) if usable.any()
            else float("nan"))

    # Records lost to same-instant collapsing.
    prov["quote_records_before_collapse"] = int(len(q_window))
    prov["quote_records_after_collapse"] = int(len(q))
    prov["records_lost_to_collapse"] = int(len(q_window) - len(q))

    return prov


def ranking_ratios(m: dict) -> dict:
    """spread_bps / noise_bps for every noise variant and horizon.

    Computed for all of them so they can be compared. None is privileged
    until calibration says which separates the top of the realised results
    from the bottom — and if none does, that is the finding.
    """
    out: dict[str, float] = {}
    spread = m.get("spread_bps_tw")
    if spread is None or not np.isfinite(spread):
        spread = m.get("spread_bps_median", float("nan"))
    for key, val in list(m.items()):
        if not key.startswith("noise_bps_") or key.endswith("__buckets"):
            continue
        name = key[len("noise_bps_"):]
        out[f"ratio_{name}"] = (spread / val
                                if val and np.isfinite(val) and val > 0
                                else float("nan"))
    return out


def cross_source_metrics(trade_metrics: dict,
                         quote_metrics: dict | None) -> dict:
    """Metrics that need BOTH the trade tape and the quote stream.

    Kept separate from compute_window because that function takes one frame.
    These combine a trade_quote result with a history/quote result for the
    same symbol-day, and are simply absent when the quote pull is missing
    rather than being filled with a substitute.

    quotes_per_trade — quote records divided by trades. High churn per
    execution is a book moving without trading, which is the "I sat there
    re-pricing and nothing filled" case. Both inputs are already in the pull,
    so it is free.

    A ranking candidate on the same footing as the noise variants: n = 4 in
    the calibration evidence, monotonic against realised results apart from
    the FDX/LLY swap. Calibration decides.
    """
    out: dict[str, float] = {}
    if not quote_metrics:
        return out

    trades = trade_metrics.get("trades")
    records = quote_metrics.get("quote_records")
    if records is None:
        rpm = quote_metrics.get("quote_records_per_min")
        minutes = quote_metrics.get("window_minutes")
        if rpm is not None and minutes:
            records = rpm * minutes

    if trades and records is not None and trades > 0:
        out["quotes_per_trade"] = float(records) / float(trades)
    else:
        out["quotes_per_trade"] = float("nan")
    return out


def compute_buckets(df: pd.DataFrame, cols: Columns,
                    start: pd.Timestamp, end: pd.Timestamp,
                    bucket_minutes: int = config.INTRADAY_BUCKET_MINUTES
                    ) -> list[dict]:
    """The same metrics over consecutive fixed windows.

    Stored for the morning-vs-afternoon question — mornings substantially
    outperform afternoons in the realised results and that wants
    investigating.

    Auction-edge trimming is off here: the first and last buckets ARE the
    auction-adjacent periods, and silently trimming them would hide exactly
    what these rows exist to show. Filter them at analysis time instead.

    Provenance is NOT computed per bucket. compute.py discarded it anyway, and
    producing it cost a pass per excluded code per bucket. Provenance is a
    symbol-day fact; the daily row carries it.
    """
    rows = []
    step = pd.Timedelta(minutes=bucket_minutes)
    t = start
    while t < end:
        stop = min(t + step, end)
        rows.append(compute_window(df, cols, t, stop,
                                   exclude_auction_edges_for_quotes=False,
                                   with_provenance=False))
        t = stop
    return rows
