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
R_MIN, R_MAX = -0.05, 0.20
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
