"""
Equity surface metrics: compute (pipeline stage 4).

Reads one (ticker, trade_date, snapshot) out of equity_surface / equity_atm /
equity_surface_diagnostics, joins the daily OHLC table, and returns one flat
dict keyed by the column names in lib/metrics_config.py.

Callable as a module. Two consumers:
    * a batch backfill over a date range  (build_equity_metrics.py)
    * the live pipeline, immediately after each 5-minute surface capture

MISSING INPUTS PRODUCE NULL, NEVER ZERO AND NEVER AN EXCEPTION. Thin chains
legitimately lack wing nodes; a 10-delta put that does not exist must not
become a 0.0 that a scanner reads as "flat skew".

AS-OF SEMANTICS — READ THIS BEFORE CHANGING THE OHLC WINDOWS
------------------------------------------------------------
Every OHLC-derived window ends at T-1, STRICTLY. A row stamped
(T, '1345') describes what is knowable at 13:45 on T, and T's close is not:
it has not happened. Letting rv_1m see close[T] would put a full session of
lookahead into vrp_1m — the exact bias that makes a variance-premium backtest
look excellent and live trading not. This mirrors the knowledge-at-time rule
the daily_features data dictionary states for the OI block.

The IV-history windows (vov, spotvol) are snapshot-aligned instead: they pull
the SAME snapshot value on prior trading days, which is knowable by
construction and keeps like compared with like.

ONE DELIBERATE DEPARTURE FROM THE SPEC, FLAGGED
-----------------------------------------------
spotvol_beta is specified as the OLS beta of d(iv_30d_atm) on `log_ret_d`.
Taken literally that regresses the IV change from T-1 to T against the OHLC
return from T-2 to T-1 — the two are one day apart, so the contemporaneous
relationship the metric exists to measure ("a 1% drop lifts ATM IV by 1.8
points") is not what gets estimated, and it is also unknowable at an intraday
snapshot without lookahead.

So the regressor here is the snapshot-aligned underlying return from
equity_atm.underlying_price: same snapshot, same pair of days, same instant as
the IV it is paired with. `log_ret_d` is still stored exactly as specified, as
its own column. Change this back only with the misalignment in mind.
"""
from __future__ import annotations

import logging
import math
from datetime import date, timedelta

from lib.earnings_store import days_to_earnings
from lib.metrics_config import (
    CONVEX_TRIPLES, DAYS_PER_YEAR, DELTA_COORD, DELTA_LABELS, DELTA_NODE,
    MIN_LOG_STRIKE_GAP, RATIO_LONG_NODE, RET_WINDOWS, RR_NODES, RV_WINDOWS,
    SKEW_PAIRS, SPOTVOL_WINDOWS, TENORS, TERM_PAIRS, TERM_SLOPE_DELTAS,
    TRADING_DAYS_PER_YEAR, VOV_WINDOW, WING_NODE,
)

log = logging.getLogger(__name__)

ATM_PROXY_NODE = 50      # equity_atm carries no extrapolation flag; k=0 sits
                         # beside the 50-delta node, so that node's flag is the
                         # honest proxy for "was the ATM read fabricated".

# A diff spanning a long gap is not a one-day observation. Snapshot history can
# have holes (a skipped cycle, a ticker that failed one afternoon); regressing
# a 9-day move as if it were a daily one would inflate beta badly.
MAX_DIFF_GAP_DAYS = 5

_SQRT_252 = math.sqrt(TRADING_DAYS_PER_YEAR)
_LN2 = math.log(2.0)


# =============================================================================
# Small numeric helpers. All of them return None rather than raising.
# =============================================================================
def _f(v):
    """Anything -> a finite float, or None."""
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _div(a, b):
    a, b = _f(a), _f(b)
    if a is None or b is None or abs(b) < 1e-15:
        return None
    return _f(a / b)


def _stdev(xs):
    """Sample stdev (ddof=1). None below two points."""
    xs = [x for x in (_f(v) for v in xs) if x is not None]
    if len(xs) < 2:
        return None
    m = sum(xs) / len(xs)
    return _f(math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1)))


def _median(xs):
    xs = sorted(x for x in (_f(v) for v in xs) if x is not None)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def _ols(x, y):
    """(beta, r2) of y on x. None, None when it cannot be estimated."""
    pairs = [(a, b) for a, b in zip(x, y)
             if _f(a) is not None and _f(b) is not None]
    n = len(pairs)
    if n < 3:
        return None, None
    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((a - mx) ** 2 for a in xs)
    if sxx < 1e-18:
        return None, None
    sxy = sum((a - mx) * (b - my) for a, b in pairs)
    syy = sum((b - my) ** 2 for b in ys)
    beta = sxy / sxx
    r2 = (sxy * sxy) / (sxx * syy) if syy > 1e-18 else None
    return _f(beta), _f(r2)


def _interp_node(nodes: dict, target: float, field: str):
    """Linear interpolation of `field` across put_delta nodes.

    nodes: {put_delta: {field: value, ...}}. Returns None outside the range of
    nodes that actually carry a value for `field` — extrapolating a price past
    the 5-delta node would be inventing a wing.
    """
    have = sorted(d for d in nodes if _f(nodes[d].get(field)) is not None)
    if not have:
        return None
    if target in nodes and _f(nodes[target].get(field)) is not None:
        return _f(nodes[target].get(field))
    if target < have[0] or target > have[-1]:
        return None
    lo = max(d for d in have if d <= target)
    hi = min(d for d in have if d >= target)
    if lo == hi:
        return _f(nodes[lo].get(field))
    a, b = _f(nodes[lo][field]), _f(nodes[hi][field])
    return _f(a + (b - a) * (target - lo) / (hi - lo))


# =============================================================================
# Loading
# =============================================================================
class HistoryCache:
    """Per-ticker OHLC and snapshot-aligned IV history, loaded once.

    A backfill over a year of dates would otherwise re-issue the same 252-row
    lookback query per date. Full history per ticker is a few thousand rows;
    holding it is cheaper than re-reading it 250 times.
    """

    def __init__(self, conn):
        self.conn = conn
        self._ohlc: dict = {}
        self._iv: dict = {}
        self._earnings: dict = {}

    def earnings(self, ticker: str) -> list:
        """Ascending earnings dates for one ticker; [] for a fund.

        Cached for the same reason as the OHLC above: days_to_earnings is
        evaluated once per (ticker, trade_date, snapshot), so a backfill over
        158 days would re-read the same ~25 dates tens of thousands of times.
        """
        if ticker not in self._earnings:
            from lib.earnings_store import load_dates
            try:
                self._earnings[ticker] = load_dates(self.conn, ticker)
            except Exception:                                 # noqa: BLE001
                # Table absent (pre-migration) or unreadable. NULL is the
                # honest reading and must not take the whole metrics row down.
                self._earnings[ticker] = []
        return self._earnings[ticker]

    def ohlc(self, ticker: str) -> list:
        if ticker not in self._ohlc:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT trade_date, open, high, low, close "
                    "FROM underlying_ohlc WHERE ticker = %s "
                    "ORDER BY trade_date", (ticker,))
                self._ohlc[ticker] = [
                    {"d": r[0], "o": _f(r[1]), "h": _f(r[2]),
                     "l": _f(r[3]), "c": _f(r[4])} for r in cur.fetchall()]
        return self._ohlc[ticker]

    def iv_history(self, ticker: str, snapshot: str) -> list:
        """30d ATM IV and the underlying, at the same snapshot, by date."""
        key = (ticker, snapshot)
        if key not in self._iv:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT trade_date, atm_iv, underlying_price "
                    "FROM equity_atm "
                    "WHERE ticker = %s AND snapshot = %s AND dte = 30 "
                    "ORDER BY trade_date", (ticker, snapshot))
                self._iv[key] = [
                    {"d": r[0], "iv": _f(r[1]), "s": _f(r[2])}
                    for r in cur.fetchall()]
        return self._iv[key]

    def invalidate(self, ticker: str | None = None) -> None:
        """Drop cached history. The live path calls this after writing a new
        snapshot, since that snapshot's own row is part of the next lookback."""
        if ticker is None:
            self._ohlc.clear()
            self._iv.clear()
            return
        self._ohlc.pop(ticker, None)
        for k in [k for k in self._iv if k[0] == ticker]:
            self._iv.pop(k, None)


def _load_snapshot(conn, ticker, trade_date, snapshot) -> dict:
    """The three surface tables for one (ticker, date, snapshot)."""
    out = {"nodes": {}, "atm": {}, "diag": []}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT dte, put_delta, iv, strike, price, call_price, "
            "       extrapolated, captured_at, source "
            "FROM equity_surface "
            "WHERE ticker = %s AND trade_date = %s AND snapshot = %s",
            (ticker, trade_date, snapshot))
        for dte, pd_, iv, k, px, cpx, ex, cap, src in cur.fetchall():
            out["nodes"].setdefault(int(dte), {})[int(pd_)] = {
                "iv": _f(iv), "strike": _f(k), "price": _f(px),
                "call_price": _f(cpx), "extrapolated": bool(ex),
                "captured_at": cap, "source": src}

        cur.execute(
            "SELECT dte, atm_iv, atm_strike, atm_forward, underlying_price, "
            "       price, captured_at, source "
            "FROM equity_atm "
            "WHERE ticker = %s AND trade_date = %s AND snapshot = %s",
            (ticker, trade_date, snapshot))
        for dte, iv, k, fwd, spot, px, cap, src in cur.fetchall():
            out["atm"][int(dte)] = {
                "atm_iv": _f(iv), "atm_strike": _f(k), "atm_forward": _f(fwd),
                "underlying_price": _f(spot), "price": _f(px),
                "captured_at": cap, "source": src}

        cur.execute(
            "SELECT forward_method, n_strikes_clean, domain_reach, "
            "       calendar_arb_flag, butterfly_arb_flag, skipped "
            "FROM equity_surface_diagnostics "
            "WHERE ticker = %s AND trade_date = %s AND snapshot = %s",
            (ticker, trade_date, snapshot))
        for fm, nsc, dr, cal, bfly, skip in cur.fetchall():
            out["diag"].append({
                "forward_method": fm, "n_strikes_clean": _f(nsc),
                "domain_reach": _f(dr), "calendar_arb": bool(cal),
                "butterfly_arb": bool(bfly), "skipped": bool(skip)})
    return out


# =============================================================================
# Metric families
# =============================================================================
def _level(snap: dict) -> tuple:
    """Returns (row_fragment, iv, strike, extrap) — the last three keyed
    (tenor, delta_label) and reused by every downstream family."""
    row, iv, strike, extrap = {}, {}, {}, {}

    spot = None
    for t in sorted(snap["atm"]):
        s = snap["atm"][t].get("underlying_price")
        if s is not None:
            spot = s
            break
    row["spot"] = spot

    for t in TENORS:
        atm = snap["atm"].get(t) or {}
        nd = snap["nodes"].get(t) or {}
        row[f"forward_{t}d"] = atm.get("atm_forward")

        for lbl in DELTA_LABELS:
            if lbl == "atm":
                iv[(t, lbl)] = atm.get("atm_iv")
                strike[(t, lbl)] = atm.get("atm_strike")
                proxy = nd.get(ATM_PROXY_NODE)
                extrap[(t, lbl)] = (bool(proxy["extrapolated"])
                                    if proxy else None)
            else:
                n = nd.get(DELTA_NODE[lbl])
                iv[(t, lbl)] = n["iv"] if n else None
                strike[(t, lbl)] = n["strike"] if n else None
                extrap[(t, lbl)] = bool(n["extrapolated"]) if n else None
            row[f"iv_{t}d_{lbl}"] = iv[(t, lbl)]

    return row, iv, strike, extrap


def _skew(iv: dict, strike: dict) -> dict:
    """sqrt(dte/365) * (iv_b - iv_a) / ln(K_b / K_a).

    Strike-space, sqrt-time normalised, using the ACTUAL fitted strikes rather
    than the delta labels — the delta label is a name for the node, not a
    moneyness, and the strike behind a 10-delta put moves with vol.
    """
    row = {}
    for t in TENORS:
        scale = math.sqrt(t / DAYS_PER_YEAR)
        for a, b in SKEW_PAIRS:
            iv_a, iv_b = iv.get((t, a)), iv.get((t, b))
            k_a, k_b = strike.get((t, a)), strike.get((t, b))
            val = None
            if (iv_a is not None and iv_b is not None
                    and k_a is not None and k_b is not None
                    and k_a > 0 and k_b > 0):
                dk = math.log(k_b / k_a)
                if abs(dk) >= MIN_LOG_STRIKE_GAP:
                    val = _f(scale * (iv_b - iv_a) / dk)
            row[f"skew_{t}d_{a}_{b}"] = val
    return row


def _convexity(iv: dict) -> dict:
    """Wings' delta-interpolated line, minus the centre."""
    row = {}
    for t in TENORS:
        for l, c, r in CONVEX_TRIPLES:
            dl, dc, dr = DELTA_COORD[l], DELTA_COORD[c], DELTA_COORD[r]
            wl = (dr - dc) / (dr - dl)
            wr = (dc - dl) / (dr - dl)
            a, b, m = iv.get((t, l)), iv.get((t, r)), iv.get((t, c))
            row[f"convex_{t}d_{l}_{c}_{r}"] = (
                _f(wl * a + wr * b - m)
                if None not in (a, b, m) else None)
    return row


def _risk_reversal(iv: dict) -> dict:
    row = {}
    for t in TENORS:
        for n, call, put in RR_NODES:
            c, p = iv.get((t, call)), iv.get((t, put))
            row[f"rr_{t}d_{n}"] = _f(c - p) if None not in (c, p) else None
    return row


def _term(iv: dict) -> dict:
    row = {}
    for a, b in TERM_PAIRS:
        row[f"term_ratio_{a}d_{b}d"] = _div(iv.get((a, "atm")),
                                            iv.get((b, "atm")))
    for a, b in TERM_PAIRS:
        t_a, t_b = a / DAYS_PER_YEAR, b / DAYS_PER_YEAR
        for d in TERM_SLOPE_DELTAS:
            iv_a, iv_b = iv.get((a, d)), iv.get((b, d))
            val = None
            if iv_a is not None and iv_b is not None and t_b > t_a:
                fwd_var = (iv_b * iv_b * t_b - iv_a * iv_a * t_a) / (t_b - t_a)
                # Negative forward variance is calendar arbitrage: total
                # variance fell with maturity. NULL, not a complex number and
                # not a clamp to zero, which would read as "no forward vol".
                if fwd_var >= 0:
                    val = _f(math.sqrt(fwd_var))
            row[f"term_slope_{a}d_{b}d_{d}"] = val
    return row


def _structure(snap: dict, iv: dict, spot) -> dict:
    """Theoretical structure prices off the fitted surface.

    equity_surface.price is a Black-Scholes PUT at every node. The 25-delta
    CALL leg of the risk reversal is call_price at the put_delta 75 node — the
    same strike, the same fit, the call formula instead of the put. That column
    exists precisely so this does not have to be recovered via parity, which
    would need r, and r is not on the surface row.
    """
    row = {}
    for t in TENORS:
        nd = snap["nodes"].get(t) or {}
        atm = snap["atm"].get(t) or {}
        tl = f"{t}d"

        p25 = _interp_node(nd, RATIO_LONG_NODE, "price")
        p10 = _interp_node(nd, DELTA_NODE["10p"], "price")
        p05 = _interp_node(nd, WING_NODE, "price")
        c25 = _interp_node(nd, DELTA_NODE["25c"], "call_price")

        row[f"ratio_price_{tl}"] = (_f(2 * p10 - p25)
                                    if None not in (p10, p25) else None)
        row[f"straddle_price_{tl}"] = _f(2 * atm["price"]) \
            if _f(atm.get("price")) is not None else None
        row[f"rr_price_{tl}"] = (_f(c25 - p25)
                                 if None not in (c25, p25) else None)
        row[f"wing_cost_10p_5p_{tl}"] = (_f(p10 - p05)
                                         if None not in (p10, p05) else None)

        # Delta-neutral reference: short delta at half the long's, so the 1x2
        # nets to zero delta. Pure arithmetic — no skew in it. The gap between
        # this and the zero-cost point IS the skew reading in trade units.
        p_neutral = _interp_node(nd, RATIO_LONG_NODE / 2.0, "price")
        row[f"cost_at_delta_neutral_{tl}"] = (
            _f(2 * p_neutral - p25) if None not in (p_neutral, p25) else None)

        # The long leg's position on the same axis as the short leg's. Every
        # input is already in hand — no extra query — and storing it is what
        # lets the panel band the marker and draw a median "ghost tent".
        row[f"long_sigma_{tl}"] = _sigma_from_spot(
            _interp_node(nd, RATIO_LONG_NODE, "strike"), spot,
            iv.get((t, "atm")), t)

        d_short, k_short = _zero_cost_short(nd, p25)
        row[f"zc_short_delta_{tl}"] = d_short
        row[f"zc_width_sigma_{tl}"] = _sigma_from_spot(
            k_short, spot, iv.get((t, "atm")), t)
    return row


def _zero_cost_short(nd: dict, p25) -> tuple:
    """Short put delta and strike where 2 * price(short) = price(25p).

    Price rises monotonically with put_delta (further ITM costs more), so the
    solution sits below 25. Solved by inverting the price/delta relation on the
    node ladder rather than by iterating the pricer: the ladder is what the
    surface actually fitted, and inverting it cannot land outside the fit.

    None when the target price is below the 5-delta node's — the surface simply
    does not reach far enough out to say, and inventing the level would be
    worse than saying nothing.
    """
    if p25 is None or p25 <= 0:
        return None, None
    target = p25 / 2.0
    have = sorted(d for d in nd
                  if d <= RATIO_LONG_NODE and _f(nd[d].get("price")) is not None)
    if len(have) < 2:
        return None, None
    prices = [_f(nd[d]["price"]) for d in have]
    if target < prices[0] or target > prices[-1]:
        return None, None
    for i in range(len(have) - 1):
        lo_p, hi_p = prices[i], prices[i + 1]
        if lo_p <= target <= hi_p:
            span = hi_p - lo_p
            frac = 0.0 if abs(span) < 1e-15 else (target - lo_p) / span
            d_short = _f(have[i] + frac * (have[i + 1] - have[i]))
            k_lo = _f(nd[have[i]].get("strike"))
            k_hi = _f(nd[have[i + 1]].get("strike"))
            k_short = (_f(k_lo + frac * (k_hi - k_lo))
                       if None not in (k_lo, k_hi) else None)
            return d_short, k_short
    return None, None


def _sigma_from_spot(strike, spot, atm_iv, dte) -> float | None:
    """ln(spot / strike) / (atm_iv * sqrt(dte/365)).

    Shared by BOTH legs of the 1x2 — long_sigma (the 25-delta long) and
    zc_width_sigma (the zero-cost short) — deliberately. One formula, one
    reference point, one sign convention, so the two markers on the tent panel
    cannot drift apart from each other.

    POSITIVE by construction and increasing as the strike moves further out.
    The sign is chosen so the column sorts the way a scanner reads it:
    ORDER BY ... DESC puts the widest first. Taking ln(strike/spot) instead
    would be the same magnitude negated, sorting narrowest-first.

    REFERENCED TO SPOT, NOT TO THE FORWARD. equity_surface.log_moneyness is
    already stored and looks like the obvious input; it is ln(strike/forward)
    and is the wrong basis. Measured across the universe the difference is
    0.0353 sigma on average and 0.1083 at the extreme — against a long leg
    whose entire daily standard deviation is 0.0397. Reusing it would inject an
    error the size of the signal, and the two sigma families would disagree
    silently, which is exactly how the tent drew every marker wrong once.

    Sigma rather than percent so a 12-vol utility and an 80-vol biotech are on
    one scale.
    """
    if None in (strike, spot, atm_iv) or strike <= 0 or spot <= 0:
        return None
    denom = atm_iv * math.sqrt(dte / DAYS_PER_YEAR)
    if denom < 1e-12:
        return None
    return _f(math.log(spot / strike) / denom)


# --- Realized vol -----------------------------------------------------------
def _realized(ohlc: list, trade_date, iv: dict) -> dict:
    """OHLC-derived vol and the VRP built on it.

    `bars` is every session STRICTLY BEFORE trade_date. See the module
    docstring: T's close does not exist at an intraday snapshot on T.
    """
    row = {}
    bars = [b for b in ohlc if b["d"] < trade_date]
    closes = [b["c"] for b in bars]

    for lbl, n in RET_WINDOWS:
        val = None
        if len(closes) >= n + 1:
            c0, c1 = closes[-(n + 1)], closes[-1]
            if None not in (c0, c1) and c0 > 0 and c1 > 0:
                val = _f(math.log(c1 / c0))
        row[f"log_ret_{lbl}"] = val

    rets = []
    for prev, cur in zip(closes[:-1], closes[1:]):
        rets.append(math.log(cur / prev)
                    if None not in (prev, cur) and prev > 0 and cur > 0
                    else None)

    for lbl, n, tenor in RV_WINDOWS:
        win = [r for r in rets[-n:] if r is not None] if len(rets) >= n else []
        row[f"rv_{lbl}"] = (_f(_stdev(win) * _SQRT_252)
                            if _stdev(win) is not None else None)
        row[f"rv_park_{lbl}"] = _parkinson(bars[-n:] if len(bars) >= n else [])
        row[f"rv_gk_{lbl}"] = _garman_klass(bars[-n:] if len(bars) >= n else [])

    for lbl, n, tenor in RV_WINDOWS:
        atm_iv = iv.get((tenor, "atm"))
        rv = row.get(f"rv_{lbl}")
        row[f"vrp_{lbl}"] = (_f(atm_iv - rv) if None not in (atm_iv, rv)
                             else None)
        # NULL rather than inf when rv -> 0. _div already guards the divisor.
        row[f"vrp_ratio_{lbl}"] = _div(atm_iv, rv)

    down = [r for r in rets[-21:] if r is not None and r < 0] \
        if len(rets) >= 21 else []
    sd = _stdev(down)
    row["downside_semivol_1m"] = _f(sd * _SQRT_252) if sd is not None else None
    return row


def _parkinson(bars: list):
    """sqrt(sum(ln(h/l)^2) / (4 ln2 n)) * sqrt(252)."""
    terms = [math.log(b["h"] / b["l"]) ** 2 for b in bars
             if None not in (b["h"], b["l"]) and b["h"] > 0 and b["l"] > 0]
    if not terms:
        return None
    return _f(math.sqrt(sum(terms) / (4.0 * _LN2 * len(terms))) * _SQRT_252)


def _garman_klass(bars: list):
    """sqrt(mean(0.5 ln(h/l)^2 - (2 ln2 - 1) ln(c/o)^2)) * sqrt(252).

    The estimand can go negative on a single bar; it is a variance estimator,
    not a variance, so only the mean is required to be non-negative.
    """
    terms = []
    for b in bars:
        h, l, c, o = b["h"], b["l"], b["c"], b["o"]
        if None in (h, l, c, o) or min(h, l, c, o) <= 0:
            continue
        terms.append(0.5 * math.log(h / l) ** 2
                     - (2 * _LN2 - 1) * math.log(c / o) ** 2)
    if not terms:
        return None
    mean = sum(terms) / len(terms)
    return _f(math.sqrt(mean) * _SQRT_252) if mean >= 0 else None


def _spot_vol(iv_hist: list, trade_date) -> dict:
    """vov and the spot-vol regression, both snapshot-aligned.

    History is inclusive of trade_date — the snapshot's own row is already in
    equity_atm by the time this stage runs, and excluding it would make every
    reading a day stale.
    """
    row = {"vov_30d_1m": None}
    for lbl, _n in SPOTVOL_WINDOWS:
        row[f"spotvol_beta_{lbl}"] = None
        row[f"spotvol_r2_{lbl}"] = None

    hist = [h for h in iv_hist if h["d"] <= trade_date]
    d_iv, d_spot = [], []
    for prev, cur in zip(hist[:-1], hist[1:]):
        gap = (cur["d"] - prev["d"]).days
        if gap > MAX_DIFF_GAP_DAYS:
            continue          # a hole in the history, not a daily move
        if None in (prev["iv"], cur["iv"]):
            continue
        s0, s1 = prev["s"], cur["s"]
        d_iv.append(cur["iv"] - prev["iv"])
        d_spot.append(math.log(s1 / s0)
                      if None not in (s0, s1) and s0 > 0 and s1 > 0 else None)

    if len(d_iv) >= VOV_WINDOW:
        sd = _stdev(d_iv[-VOV_WINDOW:])
        row["vov_30d_1m"] = _f(sd * _SQRT_252) if sd is not None else None

    for lbl, n in SPOTVOL_WINDOWS:
        if len(d_iv) < max(5, n // 2):
            continue
        beta, r2 = _ols(d_spot[-n:], d_iv[-n:])
        row[f"spotvol_beta_{lbl}"] = beta
        row[f"spotvol_r2_{lbl}"] = r2
    return row


def _quality(snap: dict, extrap: dict) -> dict:
    row = {}
    for t in TENORS:
        for lbl in DELTA_LABELS:
            row[f"extrap_{lbl}_{t}d"] = extrap.get((t, lbl))

    short = [extrap[(t, d)] for t in TENORS if t <= 30 for d in DELTA_LABELS
             if extrap.get((t, d)) is not None]
    row["extrap_rate_short"] = (_f(sum(1 for x in short if x) / len(short))
                                if short else None)

    diag = snap["diag"]
    fitted = [d for d in diag if not d["skipped"]]
    row["n_expiries_fitted"] = len(fitted)
    row["n_expiries_skipped"] = sum(1 for d in diag if d["skipped"])
    row["pct_spot_fallback"] = (
        _f(sum(1 for d in fitted if d["forward_method"] == "spot_fallback")
           / len(fitted)) if fitted else None)
    row["n_butterfly_arb"] = sum(1 for d in fitted if d["butterfly_arb"])
    row["n_calendar_arb"] = sum(1 for d in fitted if d["calendar_arb"])
    row["median_domain_reach"] = _median(d["domain_reach"] for d in fitted)
    row["median_n_strikes_clean"] = _median(d["n_strikes_clean"]
                                            for d in fitted)

    caps, srcs = [], []
    for nd in snap["nodes"].values():
        for n in nd.values():
            if n.get("captured_at") is not None:
                caps.append(n["captured_at"])
            if n.get("source"):
                srcs.append(n["source"])
    row["captured_at"] = max(caps) if caps else None
    row["source"] = max(set(srcs), key=srcs.count) if srcs else None
    return row


def third_friday(year: int, month: int) -> date:
    first = date(year, month, 1)
    return first + timedelta(days=(4 - first.weekday()) % 7 + 14)


def _calendar(trade_date, earnings_dates: list | None = None) -> dict:
    tf = third_friday(trade_date.year, trade_date.month)
    if tf < trade_date:
        nxt = (trade_date.year + (trade_date.month == 12),
               trade_date.month % 12 + 1)
        tf = third_friday(*nxt)
    return {
        "day_of_week": trade_date.isoweekday(),
        "days_to_monthly_opex": (tf - trade_date).days,
        # NULL means "no known date at or after trade_date", which covers both
        # a fund and the gap past the last confirmed date — Yahoo publishes
        # only the next one. earnings_coverage is what tells those apart.
        "days_to_earnings": days_to_earnings(earnings_dates or [], trade_date),
    }


# =============================================================================
# Entry point
# =============================================================================
def compute_metrics(conn, ticker: str, trade_date, snapshot: str,
                    cache: HistoryCache | None = None) -> dict | None:
    """One flat metric row, or None if the surface holds nothing for this key.

    None means "no surface" — a real absence the caller should skip. Every
    other failure mode is a NULL inside the row.
    """
    cache = cache or HistoryCache(conn)
    snap = _load_snapshot(conn, ticker, trade_date, snapshot)
    if not snap["nodes"] and not snap["atm"]:
        return None

    row = {"ticker": ticker, "trade_date": trade_date, "snapshot": snapshot}
    level, iv, strike, extrap = _level(snap)
    row.update(level)
    row.update(_skew(iv, strike))
    row.update(_convexity(iv))
    row.update(_risk_reversal(iv))
    row.update(_term(iv))
    row.update(_structure(snap, iv, level.get("spot")))
    row.update(_realized(cache.ohlc(ticker), trade_date, iv))
    row.update(_spot_vol(cache.iv_history(ticker, snapshot), trade_date))
    row.update(_quality(snap, extrap))
    row.update(_calendar(trade_date, cache.earnings(ticker)))
    return row
