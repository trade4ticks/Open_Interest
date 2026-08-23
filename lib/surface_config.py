"""
Configuration for the equity option surface interpolation stage.

Kept in one module so the fit code, the CLI and the tests all read the same
numbers, and so a retune is a single-file diff.
"""
from __future__ import annotations

# --- Output grid ------------------------------------------------------------
# Reaches down to 0. The universe is liquid names, most with weeklies and
# several with dailies, so short buckets bracket on most days. Buckets that
# cannot bracket on a given day are skipped, so the front of the grid is
# legitimately sparser than the rest — a row count below the full tenor set is
# normal, not an error.
TARGET_DTES = [0, 1, 2, 3, 5, 7, 10, 14, 21, 30, 45, 60, 90, 120, 180, 270, 360]

# Put delta as a positive integer, 5..95.
TARGET_DELTAS = list(range(5, 100, 5))

# --- Time to expiry ---------------------------------------------------------
# 16:00, NOT 16:15. Equity options settle at the close; 16:15 is an
# SPX-specific convention inherited from the index implementation.
PM_EXPIRY_HOUR = 16
PM_EXPIRY_MINUTE = 0
MINUTES_PER_YEAR = 365 * 24 * 60

# --- Quote filtering --------------------------------------------------------
MIN_BID = 0.05
MAX_SPREAD_RATIO = 5.0        # (ask - bid) / bid
MIN_IV = 0.01
MAX_IV = 5.00
MIN_OPTION_PRICE = 0.05

# Flags from lib/clean_chain.py that disqualify a quote from the fit. Only the
# ones that make a quote unusable as a smile point; the rest (wide spread, deep
# OTM, stale underlying) are recorded but not filtered here, since the spline
# weighting already de-emphasises noisy points.
STEP2_FLAG_COLS = [
    "flag_crossed_market",
    "flag_zero_bid",
    "flag_negative_extrinsic",
    "flag_iv_missing",
    "flag_iv_extreme_high",
    "flag_iv_extreme_low",
]

# --- Put-call parity --------------------------------------------------------
PCP_MONEYNESS_BAND = 0.15

# Bounds on the rate implied by the parity regression. Outside them
# solve_forward_rate raises and the caller falls back to spot-plus-carry.
#
# Was (-0.05, 0.20), which is far too wide to catch a bad regression. Observed
# on real data, all passing the old validation:
#     AAPL 2026-06-01 0945:  +12.5%, -4.7%, -2.5%, +0.16%
#     T    2026-06-01 1545:  +14.9%, +10.3%, -2.8%
# The true rate is near 4-5% and roughly flat across tenors; none of those
# resembles it.
#
# The cause is structural, not a coding error: put-call parity assumes European
# exercise and equity options are American, so early-exercise premium turns the
# parity equality into an inequality and biases the regression. This does not
# arise on European index options, which is why the inherited bounds were
# adequate there.
#
# Impact is bounded. r does not enter implied vol (vendor-supplied) or
# k = ln(K/F) (which uses F only). It appears solely in the discount factor for
# price, theta, vega and gamma, so the error is negligible at short tenors and
# reaches roughly 10% at multi-year ones. Tightening routes these fits to the
# spot fallback rather than accepting an implausible rate; expect the
# spot_fallback share in diagnostics to rise, which is the intended effect and
# is already recorded by forward_method.
R_MIN, R_MAX = 0.0, 0.10
R_DEFAULT = 0.05

# --- Fit --------------------------------------------------------------------
# Inherited, and barely above the cubic spline's minimum of 4. Likely too
# permissive for thin single-name chains; left unchanged pending observed data.
# The diagnostics table records n_strikes_clean so the distribution can be
# inspected before retuning.
MIN_STRIKES_FOR_FIT = 5
BUTTERFLY_TOL = 1e-4
ARB_CHECK_POINTS = 200
CALENDAR_TOL = 1e-6

# --- Degenerate-expiry exclusion from bracketing ----------------------------
# A newly-listed expiry has almost no quoted strikes yet. Its fit is valid but
# covers a tiny log-moneyness range, and because InterpolatedSmile takes the
# INTERSECTION of its two endpoints' domains, one narrow expiry destroys the
# wing of a tenor whose other endpoint is excellent.
#
# Observed simultaneously on QQQ, SPY, GLD and IWM at 2026-06-01 1545, all
# failing at exactly DTE 14 and clean at 5/7/10/21/30 — the same newly-listed
# 06-15 expiry in each. QQQ:
#     2026-06-12   11.01 DTE   388 strikes   k_min -0.293
#     2026-06-15   14.01 DTE    52 strikes   k_min -0.026   <-- newly listed
#     2026-06-18   17.01 DTE   468 strikes   k_min -0.399
# The 14 DTE target bracketed 06-12/06-15, clipping a -0.293 domain to -0.026,
# so a 10-delta put (~5% OTM) landed outside and was written extrapolated.
# 06-12 and 06-18 bracket 14 days perfectly well. Because new dailies list
# continuously, whichever tenor sits beside the newest listing breaks — a hole
# that wanders through the grid rather than a stable missing value.
#
# THE RULE IS RELATIVE, NOT ABSOLUTE, and that is load-bearing. Measured in
# standardised moneyness, QQQ's degenerate fit reaches ~0.7 sigma — but T's
# perfectly legitimate 11 DTE fit reaches ~1.0 sigma. It genuinely has no
# 10-delta wing while its 25-delta node is real and useful. Any absolute
# threshold catching the first discards the second, trading a good 25-delta
# node to save a 10-delta one that never existed. Relative separates them:
# QQQ's outlier is ~1/10th of its neighbours' reach, while T's fits are all
# similarly narrow so none is an outlier.
#
# RAISED FROM 0.30 TO 0.40 on observed data. The same newly-listed 2026-06-15
# expiry sits at very different ratios across tickers, because the ratio
# depends on how wide the ticker's OTHER expiries are:
#
#     QQQ   reach 0.53 vs median 6.75   ratio 0.098   caught at 0.30
#     IWM   reach 1.345 vs median ~4.2  ratio 0.32    SURVIVED 0.30
#
# IWM's degenerate fit cleared the 0.30 cutoff (~1.25) and went on to clip the
# DTE-14 blend exactly as QQQ's did. At 0.40 the cutoff is ~1.66 and both are
# excluded, while IWM's next-lowest reach — 2.726 — is well clear of it, so
# the wider setting does not start catching legitimate fits. T's counter-example
# at ~1.2 sigma against a ~1.2 median is likewise unaffected: it is the RATIO
# that matters, and a uniformly narrow chain has no outlier at any ratio.
NARROW_DOMAIN_RATIO = 0.40

# If more than this fraction of a snapshot's fits trip the rule, the median is
# being dragged by a cluster of narrow fits rather than one outlier — the whole
# chain is thin, not one expiry degenerate — so the filter is not applied.
NARROW_DOMAIN_MAX_EXCLUDED_FRAC = 1.0 / 3.0

# --- Delta solving ----------------------------------------------------------
DELTA_SOLVER_K_BOUNDS = (-4.0, 4.0)
DELTA_SOLVER_XTOL = 1e-8

# --- Near-expiry fallback ---------------------------------------------------
# Only emit the fallback row if the nearest fit's T is within this many years
# of the bucket's nominal T. 4/365 allows a Monday 0DTE bucket to be served by
# Friday's expiry on a weekly-only name, but blocks a 25-day expiry from
# populating a dte=21 row with a mislabelled tenor.
FALLBACK_MAX_T_GAP = 4.0 / 365.0

# --- Numerical floors -------------------------------------------------------
VEGA_FLOOR = 1e-6
NOISE_FLOOR = 1e-8
W_FLOOR = 1e-12

# --- Stores -----------------------------------------------------------------
SOURCE_SNAPSHOTS = "snapshots"
SOURCE_INTRADAY = "intraday"
