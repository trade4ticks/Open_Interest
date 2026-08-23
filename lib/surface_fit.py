"""
Equity option surface: stages 1-4 (clean, fit, sample, greeks).

Pure computation — no file I/O and no database. Callers hand in a DataFrame
already through lib/clean_chain.py and get back rows ready for the store.

Stage 1  clean    filter quotes, time to expiry, forward + rate from parity
Stage 2  fit      smoothing spline of total variance vs log-moneyness, arb checks
Stage 3  sample   blend bracketing expiries, solve strike at each target delta
Stage 4  greeks   Black-Scholes put price, theta, vega, gamma at every node

--- Two things here are easy to get silently wrong -------------------------

EXTRAPOLATION. UnivariateSpline(..., ext=3) returns its BOUNDARY VALUE outside
the fitted domain — a flat extrapolation, not an error. Delta keeps varying
with k even where w is pinned flat, so the delta solver will happily find a
root out there and hand back a node whose IV is just the last real strike's IV,
with nothing marking it as fabricated. Every surface row therefore carries
`extrapolated`, computed against the smile's own k_min/k_max. The rows are
written, not dropped: gaps break rolling percentiles downstream, and a flagged
value lets the metrics layer decide. This matters because the strategy this
feeds trades 10-15 delta puts, which are exactly the nodes most likely to fall
outside a listed strike ladder.

THE FORWARD FALLBACK. When the parity regression fails, the fallback prices the
forward off spot with a default carry, which ignores dividends. On an index
that is nearly free; on a dividend-paying single name the forward is overstated
by roughly the dividend inside the tenor, which shifts where every strike sits
on the smile. `forward_method` records which path was taken so the frequency
can be measured on real chains. No dividend model — just the record.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, time

import numpy as np
import pandas as pd
from scipy.interpolate import UnivariateSpline
from scipy.optimize import brentq
from scipy.stats import linregress, norm

from lib.surface_config import (
    ARB_CHECK_POINTS, BUTTERFLY_TOL, CALENDAR_TOL, DELTA_SOLVER_K_BOUNDS,
    DELTA_SOLVER_XTOL, FALLBACK_MAX_T_GAP, MAX_IV, MAX_SPREAD_RATIO, MIN_BID,
    MIN_IV, MIN_OPTION_PRICE, MIN_STRIKES_FOR_FIT, MINUTES_PER_YEAR,
    NARROW_DOMAIN_MAX_EXCLUDED_FRAC, NARROW_DOMAIN_RATIO,
    NOISE_FLOOR, PCP_MONEYNESS_BAND, PM_EXPIRY_HOUR, PM_EXPIRY_MINUTE,
    R_DEFAULT, R_MAX, R_MIN, STEP2_FLAG_COLS, TARGET_DELTAS, TARGET_DTES,
    VEGA_FLOOR, W_FLOOR,
)

log = logging.getLogger(__name__)

FORWARD_PCP = "pcp"
FORWARD_SPOT_FALLBACK = "spot_fallback"


class ParityError(RuntimeError):
    """The put-call parity regression could not produce a usable (F, r)."""


# --- Stage 1: clean ---------------------------------------------------------

def time_to_expiry(snapshot_dt: pd.Timestamp, expiration) -> float:
    """Years to expiry from calendar minutes remaining.

    Expiry is 16:00 ET on the expiration date — equity options settle at the
    close. The index implementation this is adapted from uses 16:15, which is
    an SPX convention and would overstate T by 15 minutes on every row; at
    0DTE near the close that is a large relative error.
    """
    exp_date = pd.Timestamp(expiration).date()
    expiry_dt = datetime.combine(exp_date, time(PM_EXPIRY_HOUR, PM_EXPIRY_MINUTE))
    minutes = (expiry_dt - pd.Timestamp(snapshot_dt).to_pydatetime()).total_seconds() / 60.0
    return max(minutes, 0.0) / MINUTES_PER_YEAR


def filter_quotes(df: pd.DataFrame) -> pd.DataFrame:
    """Drop quotes unusable as smile points. Expects clean_chain output."""
    bid = pd.to_numeric(df["bid"], errors="coerce")
    ask = pd.to_numeric(df["ask"], errors="coerce")
    iv = pd.to_numeric(df["implied_vol"], errors="coerce")

    mask = bid >= MIN_BID
    mask &= ask > bid
    # bid >= MIN_BID above already excludes bid <= 0, so this ratio is safe.
    mask &= (ask - bid) / bid.where(bid > 0) <= MAX_SPREAD_RATIO
    mask &= iv >= MIN_IV
    mask &= iv <= MAX_IV
    mask = mask.fillna(False)

    for flag_col in STEP2_FLAG_COLS:
        if flag_col in df.columns:
            mask &= ~df[flag_col].astype(bool)

    return df[mask.to_numpy(dtype=bool)]


def solve_forward_rate(clean: pd.DataFrame, T: float) -> tuple:
    """(F, r) from put-call parity. Raises ParityError if unusable.

        C_mid - P_mid = e^(-rT)(F - K)      fitted as y = A - B*K
        A = e^(-rT) * F,  B = e^(-rT)  ->  r = -ln(B)/T,  F = A/B
    """
    calls = clean[clean["option_type"].astype("string").str.upper() == "C"]
    puts = clean[clean["option_type"].astype("string").str.upper() == "P"]
    if calls.empty or puts.empty:
        raise ParityError("no calls or no puts")

    pairs = calls[["strike", "mid_price"]].merge(
        puts[["strike", "mid_price"]], on="strike", suffixes=("_c", "_p"))
    pairs = pairs[(pairs["mid_price_c"] >= MIN_OPTION_PRICE)
                  & (pairs["mid_price_p"] >= MIN_OPTION_PRICE)]
    if len(pairs) < 3:
        raise ParityError(f"only {len(pairs)} matched pair(s) above "
                          f"MIN_OPTION_PRICE, need 3")

    pairs = pairs.sort_values("strike")
    diff = pairs["mid_price_c"] - pairs["mid_price_p"]

    # Rough ATM: parity says C - P crosses zero at the forward.
    atm_strike = float(pairs.loc[diff.abs().idxmin(), "strike"])
    band = pairs[(pairs["strike"] >= atm_strike * (1 - PCP_MONEYNESS_BAND))
                 & (pairs["strike"] <= atm_strike * (1 + PCP_MONEYNESS_BAND))]
    # Narrowing to the band keeps the regression on the strikes where both legs
    # carry real premium; if it leaves too few, the wings are better than
    # nothing.
    use = band if len(band) >= 3 else pairs

    y = (use["mid_price_c"] - use["mid_price_p"]).to_numpy(dtype=float)
    x = use["strike"].to_numpy(dtype=float)
    reg = linregress(x, y)
    B = -float(reg.slope)
    A = float(reg.intercept)

    if not np.isfinite(B) or B <= 0:
        raise ParityError(f"discount factor B={B!r} not positive")
    F = A / B
    if not np.isfinite(F) or F <= 0:
        raise ParityError(f"forward F={F!r} not positive")
    if T <= 0:
        raise ParityError("T <= 0")
    r = -math.log(B) / T
    if not np.isfinite(r) or not (R_MIN <= r <= R_MAX):
        raise ParityError(f"rate r={r!r} outside [{R_MIN}, {R_MAX}]")
    return F, r


def forward_and_rate(clean: pd.DataFrame, T: float) -> tuple:
    """(F, r, method). Falls back to spot with a default carry on failure.

    The fallback ignores dividends, so on a dividend-paying name the forward is
    overstated by roughly the dividend inside the tenor. `method` is what makes
    that measurable rather than invisible.
    """
    try:
        F, r = solve_forward_rate(clean, T)
        return F, r, FORWARD_PCP
    except (ParityError, ValueError):
        S = float(pd.to_numeric(clean["underlying_price"],
                                errors="coerce").median())
        r = R_DEFAULT
        return S * math.exp(r * T), r, FORWARD_SPOT_FALLBACK


def build_smile_points(clean: pd.DataFrame, F: float, T: float) -> pd.DataFrame:
    """OTM quotes as [k, w, w_noise], one row per unique k, sorted by k.

    OTM selection carries a small ATM overlap band (puts to k <= +0.01, calls
    from k >= -0.01) so the two wings meet rather than leaving a hole at the
    money; duplicate k in the overlap are averaged.
    """
    df = clean.copy()
    strike = pd.to_numeric(df["strike"], errors="coerce")
    iv = pd.to_numeric(df["implied_vol"], errors="coerce")
    df["k"] = np.log(strike.where(strike > 0) / F)
    df["w"] = iv ** 2 * T

    right = df["option_type"].astype("string").str.upper()
    otm = df[((right == "P") & (df["k"] <= 0.01))
             | ((right == "C") & (df["k"] >= -0.01))]
    otm = otm[otm["k"].notna() & otm["w"].notna()]
    if otm.empty:
        return pd.DataFrame(columns=["k", "w", "w_noise"])

    spread = pd.to_numeric(otm["spread"], errors="coerce")
    if "vega" in otm.columns and pd.to_numeric(
            otm["vega"], errors="coerce").notna().any():
        vega = pd.to_numeric(otm["vega"], errors="coerce").abs().clip(lower=VEGA_FLOOR)
        noise = (pd.to_numeric(otm["implied_vol"], errors="coerce")
                 * T * spread / vega)
    else:
        mid = pd.to_numeric(otm["mid_price"], errors="coerce")
        noise = otm["w"] * (spread / mid.where(mid > 0))
    otm = otm.assign(w_noise=noise.clip(lower=NOISE_FLOOR).fillna(NOISE_FLOOR))

    out = (otm.groupby("k", as_index=False)
              .agg(w=("w", "mean"), w_noise=("w_noise", "mean"))
              .sort_values("k")
              .reset_index(drop=True))
    return out[["k", "w", "w_noise"]]


# --- Stage 2: fit -----------------------------------------------------------

@dataclass
class SmileFit:
    """One fitted expiry. Never raises — failures arrive as skipped=True."""
    ticker: str = ""
    trade_date: object = None
    snapshot: str = ""
    expiry: object = None
    T: float = float("nan")
    F: float = float("nan")
    r: float = float("nan")
    forward_method: str = ""
    n_strikes_raw: int = 0
    n_strikes_clean: int = 0
    k_min: float = float("nan")
    k_max: float = float("nan")
    rmse: float = float("nan")
    spline: object = None
    # Put-side domain reach in standard deviations: |k_min| / sqrt(w_atm).
    # sqrt(w) is sigma*sqrt(T), so this is how many sigma below the forward the
    # fitted domain extends — directly interpretable, since a 25-delta put sits
    # near 0.67 sigma and a 10-delta near 1.28. Raw k_min is NOT comparable
    # across expiries: a 928-day expiry spans far more log-moneyness than an
    # 8-day one at the same quality.
    domain_reach: float = float("nan")
    # Set by select_bracketing_fits. Bracketing only — an excluded fit is still
    # computed, still stored, still checked for calendar arbitrage.
    excluded_from_bracketing: bool = False
    butterfly_arb_flag: bool = False
    calendar_arb_flag: bool = False
    skipped: bool = False
    skip_reason: str = ""

    @property
    def usable(self) -> bool:
        return (not self.skipped) and self.spline is not None and self.T > 0

    def w(self, k):
        """Total variance at k. Flat outside [k_min, k_max] — see the module
        docstring; callers must check `extrapolated` themselves."""
        return np.asarray(self.spline(np.asarray(k, dtype=float)), dtype=float)

    @property
    def dte_actual(self) -> float:
        return self.T * 365.0


def fit_smile(points: pd.DataFrame) -> tuple:
    """(spline, k_min, k_max, rmse) for a weighted cubic smoothing spline.

    s = n targets residuals of ~1 sigma on average, which respects the
    per-point noise estimate: wide-spread wings get more slack than tight
    near-the-money quotes.
    """
    k = points["k"].to_numpy(dtype=float)
    w = points["w"].to_numpy(dtype=float)
    noise = points["w_noise"].to_numpy(dtype=float)
    spline = UnivariateSpline(k, w, w=1.0 / noise, s=float(len(noise)),
                              k=3, ext=3)
    resid = spline(k) - w
    rmse = float(np.sqrt(np.mean(resid ** 2)))
    return spline, float(k[0]), float(k[-1]), rmse


def check_butterfly(spline, k_min: float, k_max: float) -> bool:
    """Durrleman condition. True means the smile admits butterfly arbitrage.

        g(k) = (1 - k*w'/(2w))^2 - (w')^2/4 * (1/w + 1/4) + w''/2
    """
    ks = np.linspace(k_min, k_max, ARB_CHECK_POINTS)
    w = np.asarray(spline(ks), dtype=float)
    d1 = np.asarray(spline.derivative(1)(ks), dtype=float)
    d2 = np.asarray(spline.derivative(2)(ks), dtype=float)
    if np.any(w < -1e-8):
        return True
    wc = np.clip(w, W_FLOOR, None)
    g = ((1.0 - ks * d1 / (2.0 * wc)) ** 2
         - (d1 ** 2) / 4.0 * (1.0 / wc + 0.25)
         + d2 / 2.0)
    return bool(np.any(g < -BUTTERFLY_TOL))


def check_calendar(fits: list) -> None:
    """Flag calendar arbitrage PER EXPIRY, in place.

    w(k, T1) <= w(k, T2) whenever T1 < T2. The reference implementation sets
    one shared boolean across every fit at a snapshot, so a violation tells you
    only that something was wrong somewhere. Flagging both members of each
    offending pair says which expiries are implicated.
    """
    usable = [f for f in fits if f.usable]
    if len(usable) < 2:
        return
    usable.sort(key=lambda f: f.T)
    for i in range(len(usable) - 1):
        lo, hi = usable[i], usable[i + 1]
        k_lo = max(lo.k_min, hi.k_min)
        k_hi = min(lo.k_max, hi.k_max)
        if not np.isfinite(k_lo) or not np.isfinite(k_hi) or k_hi <= k_lo:
            continue
        ks = np.linspace(k_lo, k_hi, ARB_CHECK_POINTS)
        if np.any(lo.w(ks) > hi.w(ks) + CALENDAR_TOL):
            lo.calendar_arb_flag = True
            hi.calendar_arb_flag = True


def fit_expiry(df: pd.DataFrame, ticker: str, trade_date, snapshot: str,
               expiry, snapshot_dt: pd.Timestamp) -> SmileFit:
    """Stages 1-2 for one (snapshot, expiry). Never raises."""
    fit = SmileFit(ticker=ticker, trade_date=trade_date, snapshot=snapshot,
                   expiry=expiry, n_strikes_raw=len(df))
    T = time_to_expiry(snapshot_dt, expiry)
    fit.T = T
    if T <= 0:
        fit.skipped, fit.skip_reason = True, "T <= 0"
        return fit

    clean = filter_quotes(df)
    fit.n_strikes_clean = len(clean)
    if len(clean) < MIN_STRIKES_FOR_FIT:
        fit.skipped = True
        fit.skip_reason = (f"only {len(clean)} quote(s) after filtering, "
                           f"need {MIN_STRIKES_FOR_FIT}")
        return fit

    F, r, method = forward_and_rate(clean, T)
    fit.F, fit.r, fit.forward_method = F, r, method

    points = build_smile_points(clean, F, T)
    if len(points) < MIN_STRIKES_FOR_FIT:
        fit.skipped = True
        fit.skip_reason = (f"only {len(points)} smile point(s), "
                           f"need {MIN_STRIKES_FOR_FIT}")
        return fit

    try:
        spline, k_min, k_max, rmse = fit_smile(points)
    except Exception as exc:                                  # noqa: BLE001
        fit.skipped = True
        fit.skip_reason = f"spline fit failed: {type(exc).__name__}: {exc}"
        return fit

    fit.spline, fit.k_min, fit.k_max, fit.rmse = spline, k_min, k_max, rmse
    fit.domain_reach = _domain_reach(fit)
    try:
        fit.butterfly_arb_flag = check_butterfly(spline, k_min, k_max)
    except Exception as exc:                                  # noqa: BLE001
        fit.skip_reason = f"butterfly check failed: {type(exc).__name__}"
    return fit


def _domain_reach(fit: SmileFit) -> float:
    """|k_min| / sqrt(w_atm) — put-side domain reach in sigma."""
    if fit.spline is None or not np.isfinite(fit.k_min):
        return float("nan")
    w_atm = float(fit.w(0.0))
    if not np.isfinite(w_atm) or w_atm <= 0:
        return float("nan")
    return abs(fit.k_min) / math.sqrt(max(w_atm, W_FLOOR))


def select_bracketing_fits(fits: list) -> list:
    """Usable fits eligible as interpolation endpoints, degenerates removed.

    A newly-listed expiry fits fine but over a tiny domain, and because the
    blended domain is the INTERSECTION of its two endpoints, it destroys the
    wing of a tenor whose other endpoint is excellent. Dropping it from the
    candidate pool lets the sampler reach past it to two wide neighbours that
    bracket the same target just as well.

    The rule is relative to the snapshot's own median reach — see
    NARROW_DOMAIN_RATIO for why an absolute threshold cannot work.

    Sets excluded_from_bracketing on whatever it removes, and returns the
    survivors. Exclusion is for BRACKETING ONLY: the fit is still computed,
    still written to diagnostics, and still participates in the
    calendar-arbitrage check, which runs over every usable fit.

    Two guards, either of which abandons the filter entirely:
      * fewer than 2 fits would survive — bracketing needs two, so a filter
        that breaks it is worse than the narrow domain it was avoiding
      * more than NARROW_DOMAIN_MAX_EXCLUDED_FRAC of fits trip the rule — that
        means the median is being dragged by a cluster of narrow fits, i.e. the
        whole chain is thin rather than one expiry being degenerate
    """
    for f in fits:
        f.excluded_from_bracketing = False

    usable = [f for f in fits if f.usable]
    if len(usable) < 2:
        return usable

    reaches = [f.domain_reach for f in usable if np.isfinite(f.domain_reach)]
    if len(reaches) < 2:
        return usable
    median_reach = float(np.median(reaches))
    if not np.isfinite(median_reach) or median_reach <= 0:
        return usable

    threshold = NARROW_DOMAIN_RATIO * median_reach
    # A non-finite reach is never grounds for exclusion: it means the metric
    # could not be computed, not that the domain is narrow.
    narrow = [f for f in usable
              if np.isfinite(f.domain_reach) and f.domain_reach < threshold]
    if not narrow:
        return usable
    if len(narrow) > len(usable) * NARROW_DOMAIN_MAX_EXCLUDED_FRAC:
        log.debug("narrow-domain filter not applied: %d of %d fits tripped it",
                  len(narrow), len(usable))
        return usable

    narrow_ids = {id(f) for f in narrow}
    kept = [f for f in usable if id(f) not in narrow_ids]
    if len(kept) < 2:
        return usable

    for f in narrow:
        f.excluded_from_bracketing = True
    return kept


# --- Stage 3: sample --------------------------------------------------------

@dataclass
class InterpolatedSmile:
    dte: int
    T: float
    F: float
    r: float
    k_min: float
    k_max: float
    dte_actual: float
    is_fallback: bool = False
    _lo: SmileFit = None
    _hi: SmileFit = None
    _alpha: float = 0.0

    def w(self, k):
        """Linear in TOTAL VARIANCE, not IV — that is what keeps the blend
        arbitrage-free between the two bracketing expiries."""
        lo = self._lo.w(k)
        if self._hi is None:
            return lo
        return lo + self._alpha * (self._hi.w(k) - lo)


def interpolate_tenor(fits: list, dte: int) -> InterpolatedSmile | None:
    """Blend the two fits bracketing dte, or None if none bracket it."""
    usable = sorted([f for f in fits if f.usable], key=lambda f: f.T)
    if not usable:
        return None
    T_target = dte / 365.0

    lo = hi = None
    for i in range(len(usable) - 1):
        if usable[i].T <= T_target <= usable[i + 1].T:
            lo, hi = usable[i], usable[i + 1]
            break
    if lo is None:
        # Exact hit on a single listed expiry still counts as bracketed.
        for f in usable:
            if abs(f.T - T_target) < 1e-12:
                return InterpolatedSmile(
                    dte=dte, T=f.T, F=f.F, r=f.r, k_min=f.k_min, k_max=f.k_max,
                    dte_actual=float(dte), _lo=f, _hi=None, _alpha=0.0)
        return None

    span = hi.T - lo.T
    alpha = 0.0 if span <= 0 else (T_target - lo.T) / span
    return InterpolatedSmile(
        dte=dte,
        T=T_target,
        F=lo.F + alpha * (hi.F - lo.F),
        r=lo.r + alpha * (hi.r - lo.r),
        k_min=max(lo.k_min, hi.k_min),
        k_max=min(lo.k_max, hi.k_max),
        dte_actual=float(dte),
        _lo=lo, _hi=hi, _alpha=alpha,
    )


def near_expiry_fallback(fits: list, dtes=None) -> InterpolatedSmile | None:
    """The one un-bracketable short bucket the nearest expiry may serve.

    Targets below the nearest listed expiry cannot bracket from below. This
    matters most at dte = 0: there is never an expiry with T < 0, so a 0DTE row
    can only come from using the nearest expiry's smile directly. When that
    expiry IS today's, this is not an approximation — it is the correct smile,
    with T shrinking through the trading day.

    Only the LARGEST un-bracketable bucket is eligible, and only if the nearest
    fit's T is within FALLBACK_MAX_T_GAP of that bucket's nominal T. Without
    the cap a nearest expiry 25 days out would populate a row labelled dte=21
    while carrying a 25-day smile — a mislabelled tenor in a column the metrics
    layer trusts. dte_actual carries the smile's true tenor either way.
    """
    dtes = TARGET_DTES if dtes is None else dtes
    usable = sorted([f for f in fits if f.usable], key=lambda f: f.T)
    if not usable:
        return None
    nearest = usable[0]

    below = [d for d in dtes if d / 365.0 < nearest.T]
    if not below:
        return None
    bucket = max(below)

    if abs(nearest.T - bucket / 365.0) > FALLBACK_MAX_T_GAP:
        return None

    return InterpolatedSmile(
        dte=bucket,
        T=nearest.T,                      # the ACTUAL tenor, not the label
        F=nearest.F, r=nearest.r,
        k_min=nearest.k_min, k_max=nearest.k_max,
        dte_actual=nearest.dte_actual,
        is_fallback=True,
        _lo=nearest, _hi=None, _alpha=0.0,
    )


def put_delta_from_k(k: float, w: float) -> float:
    """Forward put delta (negative). d1 = (-k + w/2)/sqrt(w)."""
    if w <= 0:
        return float("nan")
    d1 = (-k + 0.5 * w) / math.sqrt(w)
    return float(norm.cdf(d1) - 1.0)


def solve_delta_node(smile: InterpolatedSmile, target_delta: int) -> dict | None:
    """Strike and IV at |put delta| = target/100, or None if unsolvable.

    `extrapolated` is set against the SMILE's fitted domain, not the solver
    bounds. Outside that domain ext=3 pins w flat at the boundary value while
    delta keeps moving with k, so a root exists out there and the returned IV
    is simply the last real strike's IV. The row is still written — dropping it
    would leave a hole that breaks rolling percentiles downstream — but it is
    marked.
    """
    target = target_delta / 100.0

    def resid(k: float) -> float:
        w = float(smile.w(k))
        if not np.isfinite(w) or w <= 0:
            return float("nan")
        return abs(put_delta_from_k(k, w)) - target

    lo_k, hi_k = DELTA_SOLVER_K_BOUNDS
    f_lo, f_hi = resid(lo_k), resid(hi_k)
    if not (np.isfinite(f_lo) and np.isfinite(f_hi)) or f_lo * f_hi > 0:
        return None
    try:
        k_sol = float(brentq(resid, lo_k, hi_k, xtol=DELTA_SOLVER_XTOL))
    except (ValueError, RuntimeError):
        return None

    w_sol = float(smile.w(k_sol))
    if not np.isfinite(w_sol) or w_sol <= 0 or smile.T <= 0:
        return None
    iv = math.sqrt(w_sol / smile.T)
    return {
        "put_delta": int(target_delta),
        "k": k_sol,
        "iv": iv,
        "strike": smile.F * math.exp(k_sol),
        "extrapolated": not (smile.k_min <= k_sol <= smile.k_max),
    }


def atm_node(smile: InterpolatedSmile) -> dict | None:
    """The at-the-forward row, k = 0.

    Computed separately from the delta grid because the grid never lands
    exactly on the money. atm_delta is slightly more negative than -0.5.
    """
    w_atm = float(smile.w(0.0))
    if not np.isfinite(w_atm) or w_atm <= 0 or smile.T <= 0:
        return None
    iv = math.sqrt(w_atm / smile.T)
    d1 = 0.5 * math.sqrt(w_atm)
    return {
        "atm_put_delta": float(norm.cdf(d1) - 1.0),
        "atm_strike": smile.F,
        "atm_iv": iv,
        "atm_forward": smile.F,
        "total_var": w_atm,
    }


# --- Stage 4: greeks --------------------------------------------------------

def bs_put_forward(F: float, K: float, T: float, sigma: float,
                   r: float) -> dict:
    """Forward Black-Scholes put: price, theta, vega, gamma.

    Every node is priced as a put, with the underlying taken as the forward.
    Theta can be positive for deep-ITM puts — that is correct, not a bug: the
    discounting term dominates when the option is nearly all intrinsic.
    """
    none = {"price": None, "theta": None, "vega": None, "gamma": None}
    if not all(np.isfinite([F, K, T, sigma, r])) or sigma <= 0 or T <= 0:
        return none
    sqrtT = math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
    disc = math.exp(-r * T)
    npd1 = float(norm.pdf(d1))

    price = disc * (K * float(norm.cdf(-d2)) - F * float(norm.cdf(-d1)))
    vega = disc * F * npd1 * sqrtT / 100.0          # per 1% IV
    gamma = disc * npd1 / (F * sigma * sqrtT)       # forward gamma, d2V/dF2
    theta = (-disc * F * npd1 * sigma / (2.0 * sqrtT)
             + r * disc * K * float(norm.cdf(-d2))
             - r * disc * F * float(norm.cdf(-d1))) / 365.0
    return {"price": price, "theta": theta, "vega": vega, "gamma": gamma}


# --- Orchestration for one (ticker, snapshot) -------------------------------

def build_snapshot(df: pd.DataFrame, ticker: str, trade_date, snapshot: str,
                   dtes=None, deltas=None) -> dict:
    """All fits, surface rows, ATM rows and diagnostics for one snapshot.

    `df` must already be through clean_chain and hold exactly one
    (ticker, trade_date, snapshot).
    """
    dtes = TARGET_DTES if dtes is None else dtes
    deltas = TARGET_DELTAS if deltas is None else deltas

    snapshot_dt = pd.to_datetime(df["timestamp"], errors="coerce").min()
    underlying = float(pd.to_numeric(df["underlying_price"],
                                     errors="coerce").median())

    fits = []
    for expiry, sub in df.groupby("expiration", sort=True):
        fits.append(fit_expiry(sub, ticker, trade_date, snapshot, expiry,
                               snapshot_dt))
    # Calendar arbitrage is checked across EVERY usable fit — a degenerate
    # expiry can still violate it, and excluding it here would hide that.
    check_calendar(fits)

    # Bracketing endpoints only. The fallback uses the same filtered list, so a
    # degenerate nearest expiry cannot serve a 0DTE row either.
    candidates = select_bracketing_fits(fits)

    smiles = {}
    for dte in dtes:
        s = interpolate_tenor(candidates, dte)
        if s is not None:
            smiles[dte] = s
    fb = near_expiry_fallback(candidates, dtes)
    # Only fills a bucket interpolation could not reach; never overrides one.
    if fb is not None and fb.dte not in smiles:
        smiles[fb.dte] = fb

    surface_rows, atm_rows = [], []
    for dte in sorted(smiles):
        smile = smiles[dte]
        for d in deltas:
            node = solve_delta_node(smile, d)
            if node is None:
                continue
            g = bs_put_forward(smile.F, node["strike"], smile.T, node["iv"],
                               smile.r)
            surface_rows.append({
                "ticker": ticker, "trade_date": trade_date,
                "snapshot": snapshot, "dte": int(dte),
                "put_delta": node["put_delta"], "iv": node["iv"],
                "strike": node["strike"], "forward": smile.F,
                "log_moneyness": node["k"],
                "price": g["price"], "theta": g["theta"],
                "vega": g["vega"], "gamma": g["gamma"],
                "dte_actual": smile.dte_actual,
                "extrapolated": bool(node["extrapolated"]),
            })
        a = atm_node(smile)
        if a is not None:
            g = bs_put_forward(smile.F, a["atm_strike"], smile.T, a["atm_iv"],
                               smile.r)
            atm_rows.append({
                "ticker": ticker, "trade_date": trade_date,
                "snapshot": snapshot, "dte": int(dte), **a,
                "underlying_price": underlying,
                "price": g["price"], "theta": g["theta"],
                "vega": g["vega"], "gamma": g["gamma"],
                "dte_actual": smile.dte_actual,
            })

    diagnostics = [{
        "ticker": f.ticker, "trade_date": f.trade_date, "snapshot": f.snapshot,
        "expiry": f.expiry, "dte_actual": f.dte_actual if f.T > 0 else None,
        "forward_price": f.F, "risk_free_rate": f.r,
        "forward_method": f.forward_method or None,
        "n_strikes_raw": f.n_strikes_raw, "n_strikes_clean": f.n_strikes_clean,
        "k_min": f.k_min, "k_max": f.k_max, "spline_rmse": f.rmse,
        "domain_reach": f.domain_reach,
        "excluded_from_bracketing": bool(f.excluded_from_bracketing),
        "calendar_arb_flag": bool(f.calendar_arb_flag),
        "butterfly_arb_flag": bool(f.butterfly_arb_flag),
        "skipped": bool(f.skipped), "skip_reason": f.skip_reason or None,
    } for f in fits]

    return {"fits": fits, "surface": surface_rows, "atm": atm_rows,
            "diagnostics": diagnostics}
