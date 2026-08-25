"""
Equity surface metrics: the column registry (pipeline stage 4).

ONE source of truth. This module defines every column the metrics stage
produces, and three consumers read it:

    lib/metrics_store.py    ALTER TABLE ... ADD COLUMN, from .sql_type
    lib/metrics_compute.py  which values to compute, from .name
    equity_metrics_catalog  the dashboard's metric picker, from everything else

With ~600 columns a hand-written schema and a hardcoded dropdown both rot
immediately, and they rot silently — a renamed column becomes a permanently
NULL column, not an error. Generating all three from this list makes drift
impossible by construction, and metrics_store.check_catalog_drift() asserts it
against information_schema on every run.

WHY THE IV MATRIX IS DENORMALISED
---------------------------------
iv_{t}d_{delta} duplicates data that is already in equity_surface. That is
deliberate. It is the input to every other metric here, so a new skew
definition can be added later and backfilled from these 30 columns without
re-reading a 19x17 surface per snapshot.

WHY TENOR 1 IS ABSENT
---------------------
A 0DTE fit near the close passes the narrow-domain check — its sigma-reach is
large precisely because sigma*sqrt(T) is tiny that close to expiry — and then
corrupts the 1/2/3 DTE buckets as a bracketing endpoint. Observed on SPY
2026-06-05 1545: DTE 1/2/3 all extrapolated, 10-delta IVs 0.168/0.180/0.194
sitting between 0.303 at DTE 0 and 0.240 at DTE 5, non-monotone in both
directions. TENORS starts at 7.
"""
from __future__ import annotations

from dataclasses import dataclass

# --- Grid -------------------------------------------------------------------
TENORS = [7, 14, 21, 30, 60, 90]
DELTA_LABELS = ["10p", "25p", "atm", "25c", "10c"]

# Label -> put_delta node in equity_surface. 'atm' is absent on purpose: it
# comes from equity_atm.atm_iv, evaluated at k = ln(K/F) = 0, NOT from
# put_delta 50. The 50-delta node is a delta-space solve and sits slightly away
# from the forward; at k=0 the ATM value is the smile's own anchor.
DELTA_NODE = {"10p": 10, "25p": 25, "25c": 75, "10c": 90}

# Coordinate scale for delta-interpolated convexity weights.
DELTA_COORD = {"10p": 10, "25p": 25, "atm": 50, "25c": 75, "10c": 90}

WING_NODE = 5          # the far wing of wing_cost_10p_5p
RATIO_LONG_NODE = 25   # the fixed long leg of the 1x2 ratio

SKEW_PAIRS = [("10p", "25p"), ("25p", "atm"), ("10p", "atm"),
              ("atm", "25c"), ("atm", "10c"), ("25p", "25c")]

CONVEX_TRIPLES = [("10p", "25p", "atm"), ("atm", "25c", "10c"),
                  ("25p", "atm", "25c"), ("10p", "atm", "10c")]

RR_NODES = [(25, "25c", "25p"), (10, "10c", "10p")]

TERM_PAIRS = [(7, 14), (14, 30), (30, 90), (7, 30)]
TERM_SLOPE_DELTAS = ["25p", "atm", "25c"]

# (label, trading-day window, the ATM tenor VRP measures it against)
RV_WINDOWS = [("1w", 5, 7), ("1m", 21, 30), ("3m", 63, 90)]
RET_WINDOWS = [("d", 1), ("1w", 5), ("1m", 21)]
SPOTVOL_WINDOWS = [("1m", 21), ("3m", 63)]
VOV_WINDOW = 21

Z_WINDOWS = [63, 252]
# A z-score off 3 observations is noise wearing a number's clothes. Require a
# third of the window present before emitting one.
Z_MIN_OBS = {63: 21, 252: 63}

# --- Z baseline -------------------------------------------------------------
# THE SINGLE SOURCE. The dashboard imports these rather than re-declaring them;
# two copies of a baseline definition is what produced divergent z estimators
# in the first place.
#
# Every snapshot's z is scored against the ticker's daily series at THIS
# bucket, never against its own bucket's history. At 1545 the two are the same
# computation, so the daily close series is unaffected by the change of
# definition. Away from 1545 they differ, and the own-bucket version is
# unusable: the 5-minute grid began 2026-08-24, so a bucket like 1015 has one
# or two observations of itself, against which any reading is its own maximum.
BASELINE_SNAPSHOT = "1545"

# Absolute floor on window observations, applied on top of the per-window
# Z_MIN_OBS above. Fewer than this and a mean and a standard deviation are
# noise wearing a measurement's clothes; NULL beats a confident number off n=3.
BASELINE_MIN_N = 20

TRADING_DAYS_PER_YEAR = 252
DAYS_PER_YEAR = 365.0

MIN_LOG_STRIKE_GAP = 1e-10   # below this the skew slope denominator is noise

# Families excluded from z-scoring. A rolling z of a trending price level is
# meaningless, and "unusually high relative to this ticker's own history of
# fallback rates" is not a reading anyone wants.
NO_Z_FAMILIES = {"level_price", "quality", "calendar"}


@dataclass(frozen=True)
class Col:
    """One output column, and everything the catalog needs to describe it."""
    name: str
    family: str
    sql_type: str
    units: str
    description: str
    formula: str = ""
    tenor: int | None = None
    wing: str | None = None
    form: str = "base"
    base_column: str = ""        # defaults to self; z columns point at the base

    @property
    def base(self) -> str:
        return self.base_column or self.name

    @property
    def z_eligible(self) -> bool:
        return self.family not in NO_Z_FAMILIES


def _tenor_label(t: int) -> str:
    return f"{t}d"


# =============================================================================
# Base columns
# =============================================================================
BASE_COLUMNS: list = []
_add = BASE_COLUMNS.append

# --- Level ------------------------------------------------------------------
_add(Col("spot", "level_price", "DOUBLE PRECISION", "price",
         "Underlying price at this snapshot.",
         "equity_atm.underlying_price"))

for _t in TENORS:
    _add(Col(f"forward_{_tenor_label(_t)}", "level_price", "DOUBLE PRECISION",
             "price", f"Forward price for the {_t}-day tenor.",
             "equity_atm.atm_forward", tenor=_t))

for _t in TENORS:
    for _d in DELTA_LABELS:
        _src = ("equity_atm.atm_iv (k=0)" if _d == "atm"
                else f"equity_surface.iv at put_delta {DELTA_NODE[_d]}")
        _add(Col(f"iv_{_tenor_label(_t)}_{_d}", "level_iv",
                 "DOUBLE PRECISION", "vol_decimal",
                 f"Implied vol at {_t}d, {_d}.", _src, tenor=_t, wing=_d))

# --- Skew slopes ------------------------------------------------------------
for _t in TENORS:
    for _a, _b in SKEW_PAIRS:
        _add(Col(f"skew_{_tenor_label(_t)}_{_a}_{_b}", "skew",
                 "DOUBLE PRECISION", "vol_per_log_strike",
                 f"Strike-space skew slope {_a}->{_b} at {_t}d, "
                 f"sqrt-time normalised.",
                 f"sqrt(dte/365) * (iv_{_b} - iv_{_a}) / ln(K_{_b} / K_{_a}); "
                 f"NULL if either strike is missing or "
                 f"|ln(K_b/K_a)| < {MIN_LOG_STRIKE_GAP:g}",
                 tenor=_t, wing=f"{_a}_{_b}"))

# --- Convexity --------------------------------------------------------------
for _t in TENORS:
    for _l, _c, _r in CONVEX_TRIPLES:
        _dl, _dc, _dr = DELTA_COORD[_l], DELTA_COORD[_c], DELTA_COORD[_r]
        _wl = (_dr - _dc) / (_dr - _dl)
        _wr = (_dc - _dl) / (_dr - _dl)
        _add(Col(f"convex_{_tenor_label(_t)}_{_l}_{_c}_{_r}", "convexity",
                 "DOUBLE PRECISION", "vol_decimal",
                 f"Smile convexity {_l}/{_c}/{_r} at {_t}d. Positive means the "
                 f"centre sits below the wings' delta-interpolated line.",
                 f"({_wl:g} * iv_{_l} + {_wr:g} * iv_{_r}) - iv_{_c}",
                 tenor=_t, wing=f"{_l}_{_c}_{_r}"))

# --- Risk reversal ----------------------------------------------------------
for _t in TENORS:
    for _n, _call, _put in RR_NODES:
        _add(Col(f"rr_{_tenor_label(_t)}_{_n}", "risk_reversal",
                 "DOUBLE PRECISION", "vol_decimal",
                 f"{_n}-delta risk reversal at {_t}d. Call minus put, so "
                 f"normally negative on equities.",
                 f"iv_{_call} - iv_{_put}", tenor=_t, wing=f"{_n}d"))

# --- Term structure ---------------------------------------------------------
for _a, _b in TERM_PAIRS:
    _add(Col(f"term_ratio_{_tenor_label(_a)}_{_tenor_label(_b)}", "term_ratio",
             "DOUBLE PRECISION", "ratio",
             f"ATM IV ratio, {_a}d over {_b}d. Above 1 is front-loaded.",
             f"iv_{_a}d_atm / iv_{_b}d_atm", wing="atm"))

for _a, _b in TERM_PAIRS:
    for _d in TERM_SLOPE_DELTAS:
        _add(Col(f"term_slope_{_tenor_label(_a)}_{_tenor_label(_b)}_{_d}",
                 "term_slope", "DOUBLE PRECISION", "vol_decimal",
                 f"Annualised forward vol between {_a}d and {_b}d at {_d}.",
                 "sqrt((iv_b^2 * T_b - iv_a^2 * T_a) / (T_b - T_a)); "
                 "NULL when the forward variance is negative "
                 "(calendar arbitrage)", wing=_d))

# --- Structure prices -------------------------------------------------------
# Theoretical Black-Scholes prices off the fitted surface. No bid-ask, so these
# are a shape reading, not a fill.
for _t in TENORS:
    _tl = _tenor_label(_t)
    _add(Col(f"ratio_price_{_tl}", "structure", "DOUBLE PRECISION", "price",
             f"1x2 put ratio at {_t}d: long one 25-delta, short two 10-delta. "
             f"Positive is a credit.",
             "2 * price(10p) - price(25p)", tenor=_t, wing="10p_25p"))
    _add(Col(f"straddle_price_{_tl}", "structure", "DOUBLE PRECISION", "price",
             f"ATM straddle at {_t}d.", "2 * equity_atm.price",
             tenor=_t, wing="atm"))
    _add(Col(f"rr_price_{_tl}", "structure", "DOUBLE PRECISION", "price",
             f"25-delta risk reversal price at {_t}d: call minus put.",
             "call_price(node put_delta 75) - price(node put_delta 25)",
             tenor=_t, wing="25d"))
    _add(Col(f"wing_cost_10p_5p_{_tl}", "structure", "DOUBLE PRECISION",
             "price",
             f"Cost of the far wing at {_t}d - what a broken-wing butterfly "
             f"pays to cap the ratio's tail. The BWB-vs-ratio decision.",
             "price(10p) - price(5p)", tenor=_t, wing="10p_5p"))
    # The long leg's position. Stored rather than left to the dashboard: the
    # tent panel needs a historical band on this marker and a "ghost tent" at
    # the median long/short pair, and equity_metrics holds no strikes to
    # assemble it from. Reading equity_surface for SHAPE the metrics table does
    # not hold is fine; recomputing a scalar it does hold is what drew every
    # tent marker wrong once already.
    _add(Col(f"long_sigma_{_tl}", "structure", "DOUBLE PRECISION", "sigma",
             f"How far out, in sigma, the 25-delta long put of the 1x2 sits, "
             f"at {_t}d. POSITIVE and increasing with distance — same "
             f"convention and sign as zc_width_sigma_{_tl}, so the two are "
             f"directly comparable and their difference is the tent's width.",
             "ln(spot / K_25p) / (atm_iv * sqrt(dte/365)); referenced to SPOT, "
             "not the forward — equity_surface.log_moneyness is ln(K/forward) "
             "and is NOT usable here",
             tenor=_t, wing="25p"))
    _add(Col(f"zc_width_sigma_{_tl}", "structure", "DOUBLE PRECISION", "sigma",
             f"How far out, in sigma, the short strike of a 25-delta-long 1x2 "
             f"sits when the structure prices at zero, at {_t}d. POSITIVE and "
             f"increasing with distance, so ORDER BY ... DESC ranks the widest "
             f"structures first. Sigma rather than percent so it compares "
             f"across a 12-vol name and an 80-vol name. Pairs with "
             f"long_sigma_{_tl} — the difference is the tent's width.",
             "solve price(short) = price(25p)/2, then "
             "ln(spot/K_short) / (atm_iv * sqrt(dte/365))",
             tenor=_t, wing="short"))
    _add(Col(f"zc_short_delta_{_tl}", "structure", "DOUBLE PRECISION",
             "put_delta",
             f"Put delta of that zero-cost short strike at {_t}d — the delta "
             f"you get for free today. Steep skew pushes it further out than "
             f"the delta-neutral 12.5.",
             "put_delta where 2 * price(short) = price(25p)",
             tenor=_t, wing="short"))
    _add(Col(f"cost_at_delta_neutral_{_tl}", "structure", "DOUBLE PRECISION",
             "price",
             f"What the same 1x2 costs at {_t}d with the short delta at half "
             f"the long's (net delta zero) — skew-independent arithmetic. The "
             f"gap to zc_width_sigma IS the skew reading in trade-native units.",
             "2 * price(put_delta 12.5) - price(25p)",
             tenor=_t, wing="12.5d"))

# --- Realized vol and VRP ---------------------------------------------------
# NOTE ON AS-OF: every OHLC window below ends at T-1, strictly. At 13:45 on T
# the session's close does not exist; using it would put a full day of
# lookahead into vrp, which is precisely the bias that makes a VRP backtest
# look good and live trading not.
for _lbl, _n in RET_WINDOWS:
    _add(Col(f"log_ret_{_lbl}", "realized_vol", "DOUBLE PRECISION",
             "log_return", f"{_n}-trading-day log return, closes through T-1.",
             f"ln(close[T-1] / close[T-1-{_n}])"))

for _lbl, _n, _tenor in RV_WINDOWS:
    _add(Col(f"rv_{_lbl}", "realized_vol", "DOUBLE PRECISION", "vol_decimal",
             f"Close-to-close realized vol over {_n} trading days, "
             f"annualised. Closes through T-1.",
             f"stdev(log returns, ddof=1) over {_n}td * sqrt(252)"))
    _add(Col(f"rv_park_{_lbl}", "realized_vol", "DOUBLE PRECISION",
             "vol_decimal",
             f"Parkinson realized vol over {_n} trading days. Uses the "
             f"high-low range, so far less noisy than close-close at this "
             f"window.",
             f"sqrt(sum(ln(h/l)^2) / (4*ln2*{_n})) * sqrt(252)"))
    _add(Col(f"rv_gk_{_lbl}", "realized_vol", "DOUBLE PRECISION",
             "vol_decimal",
             f"Garman-Klass realized vol over {_n} trading days, from full "
             f"OHLC.",
             "sqrt(mean(0.5*ln(h/l)^2 - (2*ln2-1)*ln(c/o)^2)) * sqrt(252)"))

for _lbl, _n, _tenor in RV_WINDOWS:
    _add(Col(f"vrp_{_lbl}", "vrp", "DOUBLE PRECISION", "vol_decimal",
             f"Variance risk premium at {_tenor}d: ATM IV minus {_n}td "
             f"close-close realized vol. Close-close is kept as the "
             f"denominator for comparability even though Parkinson is less "
             f"noisy.",
             f"iv_{_tenor}d_atm - rv_{_lbl}", tenor=_tenor, wing="atm"))
    _add(Col(f"vrp_ratio_{_lbl}", "vrp", "DOUBLE PRECISION", "ratio",
             f"ATM IV over realized vol at {_tenor}d. NULL rather than "
             f"infinity when rv -> 0.",
             f"iv_{_tenor}d_atm / rv_{_lbl}", tenor=_tenor, wing="atm"))

_add(Col("vov_30d_1m", "spot_vol",
         "DOUBLE PRECISION", "vol_decimal",
         "Vol of vol: annualised stdev of the daily change in 30d ATM IV over "
         "21 trading days, snapshot-aligned.",
         "stdev(diff(iv_30d_atm), ddof=1) over 21td * sqrt(252)",
         tenor=30, wing="atm"))

for _lbl, _n in SPOTVOL_WINDOWS:
    _add(Col(f"spotvol_beta_{_lbl}", "spot_vol", "DOUBLE PRECISION",
             "vol_per_log_return",
             f"Rolling OLS beta of the change in 30d ATM IV on the underlying "
             f"log return, {_n}td, snapshot-aligned. Beta rather than "
             f"correlation because it has magnitude: -1.8 says a 1% drop "
             f"lifts ATM IV by 1.8 vol points, which is what sizes a short-vega "
             f"position. A correlation of -0.7 does not.",
             f"OLS slope of d(iv_30d_atm) on d(ln underlying_price) over {_n}td",
             tenor=30, wing="atm"))
    _add(Col(f"spotvol_r2_{_lbl}", "spot_vol", "DOUBLE PRECISION", "ratio",
             f"R-squared of the {_lbl} spot-vol regression. A low-R2 beta is "
             f"not a beta — read them together or not at all.",
             "R^2 of the same regression", tenor=30, wing="atm"))

_add(Col("downside_semivol_1m", "realized_vol", "DOUBLE PRECISION",
         "vol_decimal",
         "Annualised stdev of close-close log returns over DOWN days only, "
         "21td window, closes through T-1.",
         "stdev(r for r in 21td returns if r < 0, ddof=1) * sqrt(252)"))

# --- Quality ----------------------------------------------------------------
for _t in TENORS:
    for _d in DELTA_LABELS:
        _note = (" Proxied by the put_delta 50 node: equity_atm carries no "
                 "extrapolation flag, and k=0 sits beside that node."
                 if _d == "atm" else "")
        _add(Col(f"extrap_{_d}_{_tenor_label(_t)}", "quality", "BOOLEAN",
                 "bool",
                 f"TRUE if the {_t}d {_d} node fell outside the fitted smile's "
                 f"domain, where the spline is pinned flat. The IV is then the "
                 f"last real strike's, not an observation.{_note}",
                 "equity_surface.extrapolated", tenor=_t, wing=_d))

_add(Col("extrap_rate_short", "quality", "DOUBLE PRECISION", "fraction",
         "Fraction of nodes extrapolated across tenors <= 30. Varies "
         "enormously by ticker — SPY 0.0%, AAPL 2.6%, T 21.6% on the same "
         "date — so a scanner that averages over fabricated nodes will rank "
         "thin names as unusually flat. Filter on this.",
         "count(extrapolated) / count(nodes) over tenors <= 30"))
_add(Col("n_expiries_fitted", "quality", "INTEGER", "count",
         "Expiries that produced a fit at this snapshot.",
         "count(NOT skipped) in equity_surface_diagnostics"))
_add(Col("n_expiries_skipped", "quality", "INTEGER", "count",
         "Expiries that were skipped. See skip_reason in the diagnostics "
         "table for why.", "count(skipped)"))
_add(Col("pct_spot_fallback", "quality", "DOUBLE PRECISION", "fraction",
         "Fraction of fitted expiries whose forward came from the "
         "dividend-blind spot fallback rather than put-call parity. The "
         "fallback overstates the forward by roughly the dividend inside the "
         "tenor on a dividend payer.",
         "count(forward_method = 'spot_fallback') / n_expiries_fitted"))
_add(Col("n_butterfly_arb", "quality", "INTEGER", "count",
         "Fitted expiries tripping the Durrleman butterfly condition.",
         "count(butterfly_arb_flag)"))
_add(Col("n_calendar_arb", "quality", "INTEGER", "count",
         "Fitted expiries where total variance fell against a shorter tenor.",
         "count(calendar_arb_flag)"))
_add(Col("median_domain_reach", "quality", "DOUBLE PRECISION", "sigma",
         "Median put-side domain reach in sigma across fitted expiries. A "
         "25-delta put sits near 0.67 sigma, a 10-delta near 1.28.",
         "median(|k_min| / sqrt(w_atm))"))
_add(Col("median_n_strikes_clean", "quality", "DOUBLE PRECISION", "count",
         "Median surviving strikes per fitted expiry after cleaning.",
         "median(n_strikes_clean)"))
_add(Col("source", "quality", "TEXT", "text",
         "'live' — captured intraday, approximate time. 'exact' — rebuilt "
         "from the historical 5-minute record, on the grid.",
         "carried from equity_surface"))
_add(Col("captured_at", "quality", "TIMESTAMP", "timestamp",
         "When the chain was actually captured. `snapshot` is the 5-minute "
         "grid bucket it was filed under; this is the truth.",
         "carried from equity_surface"))

# --- Calendar ---------------------------------------------------------------
_add(Col("day_of_week", "calendar", "SMALLINT", "dow",
         "ISO weekday, Monday = 1.", "trade_date.isoweekday()"))
_add(Col("days_to_monthly_opex", "calendar", "SMALLINT", "days",
         "Calendar days to the third Friday of the month. Rolls to next "
         "month's once this month's has passed; 0 on opex day itself.",
         "third_friday - trade_date"))
_add(Col("days_to_earnings", "calendar", "SMALLINT", "days",
         "Calendar days to the next earnings date on or after trade_date, "
         "from earnings_calendar (yfinance). 0 on the earnings date itself. "
         "CALENDAR days, not trading days: the metric is a proximity flag and "
         "the two differ only by intervening weekends, while trading days "
         "would make every historical value depend on the exchange calendar "
         "and shift on recompute. NULL means no known date at or after "
         "trade_date — which covers BOTH a fund with no earnings and the gap "
         "past the last confirmed date, since Yahoo publishes only the next "
         "one. earnings_coverage.has_earnings is what distinguishes those. "
         "The stored earnings_ts also carries before-open vs after-close, "
         "which this metric does not yet use.",
         "min(earnings_date >= trade_date) - trade_date"))


# =============================================================================
# Z-score columns — derived, so they cannot drift from the base list
# =============================================================================
def _z_col(base: Col, window: int) -> Col:
    return Col(
        name=f"{base.name}_z_{window}",
        family=base.family,
        sql_type="DOUBLE PRECISION",
        units="z_score",
        description=(
            f"{base.name} as a z-score against the ticker's trailing "
            f"{window} trading days at the {BASELINE_SNAPSHOT} DAILY "
            f"BASELINE — not against this row's own snapshot — and "
            f"EXCLUDING the value being scored. Requires "
            f"{max(Z_MIN_OBS[window], BASELINE_MIN_N)} non-null observations. "
            f"TIME-OF-DAY BIAS: an intraday reading (say 10:15) is measured "
            f"against {BASELINE_SNAPSHOT} closes, so it carries the mean "
            f"bucket-vs-close drift of the session. That bias is uniform "
            f"within a bucket, so it shifts the whole distribution rather "
            f"than reordering tickers within it, and cross-sectional ranking "
            f"is unaffected. At {BASELINE_SNAPSHOT} there is no bias: the "
            f"baseline is that bucket's own series. FUTURE REFINEMENT: "
            f"de-mean by bucket once each bucket has enough history of its "
            f"own to estimate its drift, which removes the bias without "
            f"reintroducing a second definition."),
        formula=(f"(x_snapshot - mean(baseline over prior {window}td)) / "
                 f"stdev(baseline, ddof=1), baseline = snapshot "
                 f"{BASELINE_SNAPSHOT} strictly before this trade_date; NULL "
                 f"if fewer than {max(Z_MIN_OBS[window], BASELINE_MIN_N)} "
                 f"observations or stdev ~ 0"),
        tenor=base.tenor,
        wing=base.wing,
        form=f"z_{window}",
        base_column=base.name,
    )


Z_BASE_COLUMNS = [c for c in BASE_COLUMNS if c.z_eligible]
Z_COLUMNS = [_z_col(c, w) for w in Z_WINDOWS for c in Z_BASE_COLUMNS]

KEY_COLUMNS = ["ticker", "trade_date", "snapshot"]

BASE_NAMES = [c.name for c in BASE_COLUMNS]
Z_NAMES = [c.name for c in Z_COLUMNS]


def catalog_rows() -> list:
    """One dict per column, for equity_metrics_catalog.

    base_column points at itself for base rows, so `GROUP BY base_column`
    returns a metric together with its two z variants — which is exactly the
    grouping a metric picker wants.
    """
    out = []
    for c in BASE_COLUMNS + Z_COLUMNS:
        out.append({
            "column_name": c.name,
            "table_name": "equity_metrics" if c.form == "base"
                          else "equity_metrics_z",
            "family": c.family,
            "tenor": c.tenor,
            "wing": c.wing,
            "form": c.form,
            "base_column": c.base,
            "units": c.units,
            "description": c.description,
            "formula": c.formula,
        })
    return out


def _self_check() -> None:
    """Guard the invariants the generators depend on. Runs at import: a
    duplicate or over-long name is a bug that must not reach ALTER TABLE."""
    for names, label in ((BASE_NAMES, "base"), (Z_NAMES, "z")):
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise ValueError(f"duplicate {label} column name(s): {sorted(dupes)}")
        too_long = [n for n in names if len(n) > 63]
        if too_long:
            raise ValueError(f"{label} column name(s) over 63 chars "
                             f"(Postgres truncates): {too_long}")
    clash = set(BASE_NAMES) & set(KEY_COLUMNS)
    if clash:
        raise ValueError(f"metric column collides with a key column: {clash}")
    for t in TENORS:
        if t not in (0, 1, 2, 3, 5, 7, 10, 14, 21, 30, 45, 60, 90,
                     120, 180, 270, 360):
            raise ValueError(f"tenor {t} is not on surface_config.TARGET_DTES")


_self_check()
