"""
clean_chain — computed fields and data-quality flags for equity option chains.

Stage two of the pipeline:  fetch -> CLEAN -> interpolate -> metrics.

Two entry points:

    clean_chain(df)   -> df with computed fields and flag columns added
    clean_summary(df) -> flag rates by ticker x DTE bucket x delta bucket

--- What this module deliberately does not do ------------------------------

NO FILE I/O. It reads nothing and writes nothing. The live intraday path
fetches bars, cleans in memory and hands the result straight to interpolation,
which owns the Postgres write; nothing reads cleaned columns back off disk.
Writing to the parquet store mid-session would also collide with the fetcher,
which appends row groups incrementally through the trading day.

NO ROW DROPPING. Flags mark rows; filtering belongs to the interpolation
stage. Every input row appears in the output, in the input's order and under
the input's index.

IDEMPOTENT. Every computed column is derived from the raw columns only, never
from a previously computed one, so calling twice on the same frame gives the
same result even when the output columns are already present.

--- The moneyness convention is inverted on purpose ------------------------

    moneyness     = underlying / strike        (S/K, NOT the usual K/S)
    log_moneyness = log(S/K)                   (negative of the usual log(K/F))

This matches a sibling project so metrics stay comparable across the two. It
is not a bug. The consequence worth holding onto is that the sign reads
backwards from habit: moneyness > 1 means the strike is BELOW spot (OTM put /
ITM call), and moneyness < 1 means the strike is ABOVE spot (OTM call / ITM
put). The deep_otm flag is a band around 1 and so catches both tails.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "flag_config.yaml"

# Group key for the gamma finite difference. `expiration` is LOAD-BEARING —
# see _add_gamma.
GAMMA_KEYS = ["timestamp", "expiration", "option_type"]

FLAG_COLUMNS = [
    "flag_wide_spread_abs",
    "flag_wide_spread_pct",
    "flag_negative_extrinsic",
    "flag_crossed_market",
    "flag_zero_bid",
    "flag_iv_extreme_high",
    "flag_iv_extreme_low",
    "flag_iv_missing",
    "flag_delta_missing",
    "flag_deep_otm",
    "flag_near_expiry_wide",
    "flag_stale_underlying",
    "flag_iv_error_high",
]

COMPUTED_COLUMNS = [
    "quote_time", "dte", "bdte", "mid_price", "spread", "spread_pct",
    "intrinsic", "extrinsic", "moneyness", "log_moneyness", "gamma",
]

# A frozen underlying across this many consecutive distinct timestamps is
# treated as a stale feed. On 5-minute data three means 15 minutes without a
# tick, so every IV and greek stamped at those timestamps was priced against a
# possibly-stale underlying.
STALE_RUN_LENGTH = 3

DTE_BINS = [-np.inf, 7, 21, 45, 90, np.inf]
DTE_LABELS = ["0-7", "8-21", "22-45", "46-90", "91+"]

DELTA_BINS = [0.0, 0.10, 0.25, 0.40, 0.60, np.inf]
DELTA_LABELS = ["0-0.10", "0.10-0.25", "0.25-0.40", "0.40-0.60", "0.60+"]

MISSING_BUCKET = "missing"


# --- Config -----------------------------------------------------------------

def load_config(config=None) -> dict:
    """Thresholds as a dict.

    Accepts a dict (used as-is), a path, or None for flag_config.yaml at the
    project root. Loaded rather than hardcoded because these values are
    expected to be retuned from observed flag rates.
    """
    if isinstance(config, dict):
        return dict(config)
    path = Path(config) if config is not None else DEFAULT_CONFIG_PATH
    with open(path, "r", encoding="utf-8") as f:
        loaded = yaml.safe_load(f) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} did not parse to a mapping")
    return loaded


# --- Helpers ----------------------------------------------------------------

def _as_bool(values, index) -> pd.Series:
    """Coerce a comparison result to plain numpy bool with no NAs.

    Flag columns MUST be plain `bool`, never nullable `boolean`. A downstream
    consumer filters with `~df[flag].astype(bool)`, and on a nullable column an
    NA becomes True under that expression — silently dropping rows that should
    have been kept. Any comparison touching a nullable or all-NA column can
    produce NA, so every flag goes through here.
    """
    s = pd.Series(values, index=index)
    if s.dtype == object or str(s.dtype) == "boolean":
        s = s.fillna(False)
    else:
        s = s.where(s.notna(), False)
    return s.astype(bool)


def _num(s: pd.Series) -> pd.Series:
    """Numeric as float64 numpy, so comparisons yield plain bool rather than
    NA. Missing columns become all-NaN of the right length."""
    return pd.to_numeric(s, errors="coerce").astype("float64")


def _session_index(start: pd.Timestamp, end: pd.Timestamp) -> np.ndarray:
    """Sorted NYSE session dates as datetime64[D], covering [start, end].

    Built ONCE per call. The naive alternative — one .schedule() per unique
    trade_date — is ~250 calendar calls for a year-spanning snapshot file, and
    at 121 tickers that dominates the run. One schedule plus searchsorted makes
    each row an O(log n) lookup.
    """
    cal = mcal.get_calendar("NYSE")
    # Pad so an expiration on the last day, or a trade_date on the first, is
    # still inside the index rather than clipped by it.
    lo = (start - pd.Timedelta(days=7)).date().isoformat()
    hi = (end + pd.Timedelta(days=7)).date().isoformat()
    days = cal.valid_days(start_date=lo, end_date=hi)
    return np.asarray(days.tz_localize(None).values, dtype="datetime64[D]")


def _business_dte(trade_date: pd.Series, expiration: pd.Series) -> pd.Series:
    """NYSE sessions in (trade_date, expiration] — exclusive of the start."""
    td = pd.to_datetime(trade_date, errors="coerce")
    ex = pd.to_datetime(expiration, errors="coerce")

    valid = td.notna() & ex.notna()
    out = pd.Series(np.nan, index=trade_date.index, dtype="float64")
    if not valid.any():
        return out

    lo = min(td[valid].min(), ex[valid].min())
    hi = max(td[valid].max(), ex[valid].max())
    sessions = _session_index(lo, hi)

    td_d = td[valid].to_numpy(dtype="datetime64[D]")
    ex_d = ex[valid].to_numpy(dtype="datetime64[D]")
    # sessions <= x, so the difference counts sessions strictly after
    # trade_date and up to and including expiration.
    out.loc[valid] = (np.searchsorted(sessions, ex_d, side="right")
                      - np.searchsorted(sessions, td_d, side="right"))
    return out


def _add_gamma(out: pd.DataFrame) -> pd.Series:
    """Gamma as a finite difference of delta with respect to strike.

    The vendor supplies delta, theta, vega and rho but not gamma.

        interior:  (delta[i+1] - delta[i-1]) / (strike[i+1] - strike[i-1])
        first:     (delta[1]   - delta[0])   / (strike[1]   - strike[0])
        last:      (delta[n-1] - delta[n-2]) / (strike[n-1] - strike[n-2])
        n < 2:     NaN

    EXPIRATION IN THE GROUP KEY IS NOT OPTIONAL. One file holds every listed
    expiration and their strike ladders overlap heavily, so grouping on
    (timestamp, option_type) alone would difference delta values belonging to
    DIFFERENT expirations that happen to sit at adjacent strikes. That yields
    plausible-looking numbers, silently, with nothing raised.

    Vectorised via groupby().shift() rather than a per-group Python loop: a
    file has many expirations x many timestamps, so the group count is large
    and a loop with per-group .loc assignment is orders of magnitude slower.
    """
    ordered = out.sort_values(GAMMA_KEYS + ["strike"], kind="mergesort")
    g = ordered.groupby(GAMMA_KEYS, sort=False, dropna=False)

    d = _num(ordered["delta"])
    k = _num(ordered["strike"])
    d_prev, d_next = g["delta"].shift(1), g["delta"].shift(-1)
    k_prev, k_next = g["strike"].shift(1), g["strike"].shift(-1)
    d_prev, d_next = _num(d_prev), _num(d_next)
    k_prev, k_next = _num(k_prev), _num(k_next)

    has_prev, has_next = k_prev.notna(), k_next.notna()

    num = np.where(has_prev & has_next, d_next - d_prev,
          np.where(has_next, d_next - d,
          np.where(has_prev, d - d_prev, np.nan)))
    den = np.where(has_prev & has_next, k_next - k_prev,
          np.where(has_next, k_next - k,
          np.where(has_prev, k - k_prev, np.nan)))

    # A zero denominator would mean two identical strikes inside one
    # (timestamp, expiration, option_type) group, which the stores' dedupe
    # keys make impossible. Guarded anyway: NaN is a usable "unknown", whereas
    # an inf would propagate silently through every downstream metric.
    den = np.where(den == 0, np.nan, den)

    gamma = pd.Series(num / den, index=ordered.index, dtype="float64")
    return gamma.reindex(out.index)


def _stale_underlying(ts: pd.Series, px: pd.Series) -> pd.Series:
    """True for every row whose timestamp sits in a frozen-underlying run.

    Runs are found over DISTINCT timestamps, not rows: a single timestamp holds
    the whole chain, so counting rows would call any wide chain a long run.
    """
    uniq = (pd.DataFrame({"ts": ts, "px": px})
            .dropna(subset=["ts"])
            .drop_duplicates(subset=["ts"])
            .sort_values("ts", kind="mergesort"))
    if len(uniq) < STALE_RUN_LENGTH:
        return pd.Series(False, index=ts.index, dtype=bool)

    prices = uniq["px"].to_numpy(dtype="float64")
    # NaN == NaN is False, so a missing underlying always starts a new run and
    # can never be part of a "frozen" one. That is the intended reading: an
    # absent price is not evidence the feed is stuck.
    new_run = np.ones(len(prices), dtype=bool)
    new_run[1:] = ~(prices[1:] == prices[:-1])
    run_id = np.cumsum(new_run)

    sizes = pd.Series(run_id).map(pd.Series(run_id).value_counts())
    stale_ts = uniq.loc[sizes.to_numpy() >= STALE_RUN_LENGTH, "ts"]
    if stale_ts.empty:
        return pd.Series(False, index=ts.index, dtype=bool)
    return _as_bool(ts.isin(set(stale_ts)), ts.index)


# --- Entry point ------------------------------------------------------------

def clean_chain(df: pd.DataFrame, config=None) -> pd.DataFrame:
    """Add computed fields and data-quality flags. Drops nothing.

    Returns a new frame; the input is not mutated. Row order and index are
    preserved, so the result can be assigned back column-wise if a caller
    wants that.
    """
    cfg = load_config(config)
    required = {"strike", "option_type", "bid", "ask", "delta",
                "implied_vol", "underlying_price", "timestamp",
                "expiration", "trade_date"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"clean_chain: input is missing {sorted(missing)}")

    out = df.copy()
    original_index = out.index
    # Positional index internally so groupby/shift alignment is unambiguous
    # even when the caller's index has duplicates; restored before returning.
    out = out.reset_index(drop=True)

    ts = pd.to_datetime(out["timestamp"], errors="coerce")
    exp = pd.to_datetime(out["expiration"], errors="coerce")

    bid = _num(out["bid"])
    ask = _num(out["ask"])
    strike = _num(out["strike"])
    spot = _num(out["underlying_price"])
    iv = _num(out["implied_vol"])
    delta = _num(out["delta"])
    iv_error = _num(out["iv_error"]) if "iv_error" in out.columns else \
        pd.Series(np.nan, index=out.index, dtype="float64")

    # --- computed fields ---------------------------------------------------
    out["quote_time"] = ts.dt.time
    out["dte"] = (exp - ts.dt.normalize()).dt.days.astype("float64")
    out["bdte"] = _business_dte(out["trade_date"], out["expiration"])

    mid = (bid + ask) / 2.0
    out["mid_price"] = mid
    out["spread"] = ask - bid
    # NaN, not inf, at or below zero mid: an infinite spread_pct would survive
    # every comparison below and poison any downstream aggregate.
    out["spread_pct"] = (out["spread"] / mid.where(mid > 0)).astype("float64")

    is_call = out["option_type"].astype("string").str.upper().eq("C")
    intrinsic = np.where(is_call.fillna(False),
                         np.maximum(spot - strike, 0.0),
                         np.maximum(strike - spot, 0.0))
    out["intrinsic"] = pd.Series(intrinsic, index=out.index, dtype="float64")
    out["extrinsic"] = mid - out["intrinsic"]

    moneyness = spot / strike.where(strike != 0)
    out["moneyness"] = moneyness.astype("float64")
    out["log_moneyness"] = pd.Series(
        np.log(moneyness.where(moneyness > 0)), index=out.index,
        dtype="float64")

    out["gamma"] = _add_gamma(out)

    # --- flags -------------------------------------------------------------
    idx = out.index
    spread_pct = _num(out["spread_pct"])
    dte = _num(out["dte"])

    out["flag_wide_spread_abs"] = _as_bool(
        out["spread"] > cfg["wide_spread_abs"], idx)
    out["flag_wide_spread_pct"] = _as_bool(
        spread_pct > cfg["wide_spread_pct"], idx)
    out["flag_negative_extrinsic"] = _as_bool(out["extrinsic"] < 0, idx)
    out["flag_crossed_market"] = _as_bool(bid > ask, idx)
    out["flag_zero_bid"] = _as_bool(bid == 0, idx)
    out["flag_iv_extreme_high"] = _as_bool(iv > cfg["iv_extreme_high"], idx)
    out["flag_iv_extreme_low"] = _as_bool(
        (iv < cfg["iv_extreme_low"]) & (iv > 0), idx)
    out["flag_iv_missing"] = _as_bool(iv.isna(), idx)
    out["flag_delta_missing"] = _as_bool(delta.isna(), idx)
    out["flag_deep_otm"] = _as_bool(
        (moneyness < cfg["deep_otm_lower"]) | (moneyness > cfg["deep_otm_upper"]),
        idx)
    out["flag_near_expiry_wide"] = _as_bool(
        (dte <= cfg["near_expiry_dte"]) & (spread_pct > cfg["near_expiry_spread_pct"]),
        idx)
    out["flag_stale_underlying"] = _stale_underlying(ts, spot)
    out["flag_iv_error_high"] = _as_bool(iv_error > cfg["iv_error_high"], idx)

    flags = out[FLAG_COLUMNS].to_numpy(dtype=bool)
    out["flag_any"] = pd.Series(flags.any(axis=1), index=idx, dtype=bool)

    out.index = original_index
    return out


# --- Summary ----------------------------------------------------------------

def _bucket(values: pd.Series, bins, labels) -> pd.Series:
    """pd.cut with an explicit `missing` level, so rows with an unusable
    bucketing key are still counted rather than silently dropped by groupby."""
    cut = pd.cut(values, bins=bins, labels=labels,
                 include_lowest=True, right=True)
    cut = cut.cat.add_categories([MISSING_BUCKET]).fillna(MISSING_BUCKET)
    return cut


def clean_summary(df: pd.DataFrame, config=None) -> pd.DataFrame:
    """Flag rates by ticker x DTE bucket x delta bucket, with a row count.

    This is the mechanism for retuning the thresholds: it answers "what
    fraction of 10-delta puts on this ticker trip each flag". n_rows is what
    makes a rate interpretable — a 100% rate over four rows is not a finding.

    Accepts either a raw frame or one already through clean_chain; the flags
    are recomputed either way, which keeps the summary honest if a caller
    passes a frame cleaned under different thresholds.

    Rate columns are named rate_<flag-without-the-flag-prefix>.
    Rows whose DTE or delta cannot be bucketed land in a `missing` bucket, so
    n_rows always sums to len(df).
    """
    cleaned = clean_chain(df, config=config)

    ticker = (cleaned["ticker"] if "ticker" in cleaned.columns
              else pd.Series("", index=cleaned.index))
    work = pd.DataFrame({
        "ticker": ticker.astype("string").fillna(MISSING_BUCKET),
        "dte_bucket": _bucket(_num(cleaned["dte"]), DTE_BINS, DTE_LABELS),
        "delta_bucket": _bucket(_num(cleaned["delta"]).abs(),
                                DELTA_BINS, DELTA_LABELS),
    })
    all_flags = FLAG_COLUMNS + ["flag_any"]
    for c in all_flags:
        work[c] = cleaned[c].to_numpy(dtype=bool)

    grouped = work.groupby(["ticker", "dte_bucket", "delta_bucket"],
                           observed=True, dropna=False)
    summary = grouped[all_flags].mean()
    summary.insert(0, "n_rows", grouped.size())
    summary = summary.rename(
        columns={c: f"rate_{c[len('flag_'):]}" for c in all_flags})
    return summary.reset_index()
