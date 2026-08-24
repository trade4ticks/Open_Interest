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

# --- Fix switches -----------------------------------------------------------
# Three fixes land together because each changes the fitted surface and all
# three need one re-surface. They are switchable INDIVIDUALLY so a change in
# output can be attributed to a specific one: run the sample with all on, then
# flip one off and diff. Remove the switches once the re-surface has validated
# them; until then a regression can be bisected without a rebuild.

# Fix 1. A target tenor within DIRECT_EXPIRY_TOL_DAYS of a fitted expiry uses
# that smile DIRECTLY instead of blending.
#
# compute_T measures calendar minutes to the 16:00 settlement, so from an 09:45
# snapshot every expiry carries a +0.26 day fraction and from 15:45 a +0.01
# one. A target of 21 is therefore BELOW the real 21-day expiry's 21.26, which
# makes that expiry the UPPER bracket and reaches downward for a shorter one.
# The blend weight is not the problem (alpha ~0.96, so the right expiry
# dominates the value) — the DOMAIN is: InterpolatedSmile intersects k_min/
# k_max, so a wide fit gets clipped to whatever the shorter one covers. Worst
# case the shorter one is 0DTE and the target produces no rows at all.
#
# Tolerance 0.30 days:
#   * must exceed 0.2604 (= 6h15m/24), the drift at the 09:45 snapshot, or the
#     fix does not fire on the snapshot that needs it most
#   * must stay well under the ~0.74 minimum distance to the NEXT listed
#     expiry, which for a daily-expiry name like SPY is one calendar day away
#     and reads >= 1.01 - 0.26 = 0.74 from the target
# 0.30 sits with 15% headroom above the drift and 2.5x clearance below the
# nearest neighbour, so it cannot swallow an adjacent expiry.
DIRECT_EXPIRY_MATCH = True
DIRECT_EXPIRY_TOL_DAYS = 0.30

# Fix 2. Compute F CBOE-style from the ATM pair with r exogenous, instead of
# solving both from the parity regression and discarding F when r fails bounds.
#
# On American options early-exercise premium inflates the ITM leg and tilts the
# regression steeper than -disc, so B = -slope > disc and r = -ln(B)/T is
# biased low — through R_MIN, rejecting the whole fit. Measured on synthetic
# American chains with zero noise and true r = 4.5%, the bias tracks VOLATILITY
# rather than dividend yield: -30.7% at 12 vol / 7 DTE against +1.2% at 95 vol.
# Low vol is worse because MIN_OPTION_PRICE bounds the usable strikes in
# dollars, so a low-vol chain reaching +/-6% of spot is +/-3.5 sigma and its ITM
# legs sit deep in early-exercise territory.
#
# But F survives what r does not: intercept and slope errors partly cancel in
# F = A/B, while r amplifies B's error by 1/T. The rejected regression's F was
# MORE accurate than the fallback's (-0.01% vs +0.03% at 7 DTE, -0.27% vs
# +0.42% at 90). So the bounds test discards a better forward to protect a rate
# that only scales the discount factor, while F sets the entire log-moneyness
# grid.
CBOE_FORWARD = True

# Fix 3. Fixed knots instead of a knot per point.
#
# s = len(noise) targets ~1 sigma per-point residual and restrains curvature not
# at all, so w'' scales as wiggle/dk^2 and DENSER chains flag more butterfly
# arbitrage from identical noise. Simulated on one true smile with per-point
# noise held constant, varying only n: 2% -> 20% at 12 vol / 7 DTE going from
# 25 to 219 strikes. SPY is worst on every axis at once and observed 12/33.
# The flag currently ranks the best chains worst, so it cannot be used as a
# scanner quality filter.
#
# Knots on a fixed grid bound w'' by construction and make the flag rate
# independent of density. 12 knots is the middle of the 10-15 range: enough for
# a smile plus one wing kink, few enough that noise cannot be tracked.
# DEFAULT OFF. The first real-chain verification made this WORSE, not better:
# SPY 12/33 -> 21/33 butterfly flags, ADBE 1/21 -> 14/21, AAPL unchanged. The
# cause is not too few knots, which was the initial reading — it is too many
# knots RELATIVE TO POINT COUNT on sparse expiries. A fixed 12 knots over a
# 24-point LEAP leaves ~2 points per interval, so a least-squares spline with
# no smoothing penalty very nearly interpolates and wiggles; the smoothing
# spline's s = n penalty is what had been holding those together. ADBE and SPY
# carry many long-dated sparse expiries, AAPL fewer, which is exactly the
# observed ordering.
#
# Scaling the knot count to n and falling back to the smoothing spline when
# there is not enough data fixes it in simulation across five regimes:
#
#     case                  smoothing   ppk=10   knots
#     12v   7DTE n=120           0%       0%      12
#     25v  21DTE n=80            2%       0%       8
#     30v  90DTE n=45            9%       0%       4
#     30v 500DTE n=24            5%       5%   fallback
#     35v 700DTE n=16            5%       5%   fallback
#
# Better where dense, never worse where sparse. But that is SIMULATION, on a
# synthetic dense-ATM/sparse-wing ladder, and the first synthetic for this fix
# was already shown to have no signal against real chains. Stays off until
# `verify_surface_fixes.py --sweep-knots` has been run on real ADBE and SPY.
FIXED_KNOT_SPLINE = False
SPLINE_TARGET_KNOTS = 12
# Minimum quotes per knot. The knot count is min(TARGET, n // this), so density
# sets how much curvature the fit may express and sparse expiries route to the
# smoothing spline instead of being force-fitted.
SPLINE_POINTS_PER_KNOT = 10
# Below this many interior knots the fit is too coarse to be worth forcing;
# fall back to the smoothing spline rather than fit a near-parabola.
SPLINE_MIN_KNOTS = 4
# Schoenberg-Whitney needs at least one point between consecutive knots; 3
# gives margin so a knot interval is never determined by a single quote.
SPLINE_MIN_PTS_PER_KNOT = 3

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
