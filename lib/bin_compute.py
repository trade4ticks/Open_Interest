"""
Pure compute helpers for wf_bins, is_bins, and tt_thresholds.

These functions reproduce the dashboard's _walk_forward_bins and _bin_for_value
conventions byte-for-byte. After the dashboard migrates to read the bin tables
this project produces, the dashboard will DELETE its own binning code — these
functions become the canonical implementation.

Dashboard references:
    _walk_forward_bins  — dashboard/app/routers/row_compute.py:1299-1363
    _bin_for_value      — dashboard/app/routers/row_compute.py:1275-1296

Conventions (all confirmed in plan):
    - "Valid" observation = not None, not NaN, numeric. Inf is NOT excluded
      (dashboard uses math.isnan but not math.isinf — match exactly).
    - 0.0 is valid (not filtered).
    - Walk-forward (wf_bins):
        rank      = bisect_left(sorted_prior, value)   # strictly-less among prior
        n_after   = len(sorted_prior) + 1              # includes current row
        insert AFTER computing rank
        warmup    = n_after < max(252, n_bins) → None
        frac      = rank / n_after
        bin_k     = min(int(frac * k) + 1, k)
    - In-sample (is_bins):
        population = all valid values for the ticker×metric (full history, fixed)
        rank       = bisect_left(sorted_all, value)
        n_total    = len(sorted_all)          # includes current value
        frac       = rank / n_total
        bin_k      = min(int(frac * k) + 1, k)
        NO warmup — every row with a valid value is binned.
    - Train-test (read-side, per-ticker pre-cutoff history):
        rank      = bisect_left(history_vals, value)
        n         = len(history_vals)
        return None if n < n_bins
        bin_k     = min(int(rank / n * k) + 1, k)
        (Denominator is n_train ONLY — no +1.  Distinct from walk-forward.)
"""
from __future__ import annotations

import math
from bisect import bisect_left
from typing import Optional

from sortedcontainers import SortedList

# Constants ------------------------------------------------------------------

# Walk-forward warmup floor.  warm = max(252, n_bins); for bin counts {5, 10, 20}
# this is always 252.  Hard-coding 252 in the production path; reference path
# uses the max(...) formula for general n_bins.
WALK_FORWARD_WARMUP = 252

# Canonical bin resolution stored in wf_bins.  Other resolutions derive from
# frac at read time via min(int(frac * k) + 1, k).
BIN20_BUCKETS = 20

# Standard train-test cutoff (2020-2023 train / 2024+ test).  trade_date < cutoff
# is the training half.
TRAIN_TEST_CUTOFF_DEFAULT = "2024-01-01"


# Validity helpers -----------------------------------------------------------

def is_valid_value(v) -> bool:
    """True iff v should be included in walk-forward / train-test computation.

    Matches the dashboard's filter (None / NaN / non-numeric excluded; Inf NOT
    excluded; 0.0 included).  Do NOT add Inf exclusion here — would diverge
    from the dashboard."""
    if v is None:
        return False
    if isinstance(v, bool):
        # bool is a subclass of int in Python; we want to exclude True/False
        # because the dashboard treats only numeric data, and a stray bool in
        # the daily_features pipeline would be a bug worth surfacing.
        return False
    if not isinstance(v, (int, float)):
        return False
    if isinstance(v, float) and math.isnan(v):
        return False
    return True


# Walk-forward ---------------------------------------------------------------

def walk_forward_series(values: list, dates: list) -> tuple[list, list]:
    """Compute walk-forward (frac, bin20) for one (ticker, metric) chronological
    series.  Production entry point; produces only the canonical bin20.

    Parameters
    ----------
    values : list
        Metric values, parallel to `dates`, in chronological order.
    dates  : list
        Trade dates, parallel to `values`, chronologically ascending.

    Returns
    -------
    fracs  : list[Optional[float]]
        rank / n_after for each row.  None for invalid values OR warmup
        (n_after < 252).
    bin20s : list[int]
        min(int(frac * 20) + 1, 20) for each row.  0 for invalid / warmup.
    """
    if len(values) != len(dates):
        raise ValueError("values and dates must have equal length")

    n = len(values)
    fracs: list = [None] * n
    bin20s: list = [0] * n

    sorted_prior = SortedList()
    for i, v in enumerate(values):
        if not is_valid_value(v):
            continue
        rank = sorted_prior.bisect_left(v)
        n_after = len(sorted_prior) + 1
        sorted_prior.add(v)
        if n_after < WALK_FORWARD_WARMUP:
            continue
        frac = rank / n_after
        bin20 = min(int(frac * BIN20_BUCKETS) + 1, BIN20_BUCKETS)
        fracs[i] = frac
        bin20s[i] = bin20
    return fracs, bin20s


def reference_walk_forward_bins(values: list, dates: list, n_bins: int) -> list:
    """Reference implementation that mirrors the dashboard's _walk_forward_bins
    SIGNATURE (takes n_bins; returns bin assignments directly).

    Used ONLY by validate_bins.py for the seam check.  warm = max(252, n_bins),
    not the hard-coded 252.  For n_bins in {5, 10, 20} the result is identical
    to walk_forward_series's bin formula.
    """
    if len(values) != len(dates):
        raise ValueError("values and dates must have equal length")
    warm = max(WALK_FORWARD_WARMUP, n_bins)
    n = len(values)
    out: list = [None] * n
    sorted_prior = SortedList()
    for i, v in enumerate(values):
        if not is_valid_value(v):
            continue
        rank = sorted_prior.bisect_left(v)
        n_after = len(sorted_prior) + 1
        sorted_prior.add(v)
        if n_after < warm:
            continue
        frac = rank / n_after
        out[i] = min(int(frac * n_bins) + 1, n_bins)
    return out


# In-sample -----------------------------------------------------------------

def in_sample_series(values: list, dates: list) -> tuple[list, list]:
    """Compute in-sample (frac, bin20) for one (ticker, metric) series.

    Ranks each value against the ticker's FULL history — no expanding window,
    no warmup.  Every row with a valid value gets a frac and bin20.

    The ranking population is all valid values across the entire date range,
    sorted ascending.  frac = rank / n_total where n_total includes the current
    value itself (consistent with walk_forward_series where n_after includes
    the current row).  bisect_left convention: ties resolve by rank = count of
    strictly-less values.

    Parameters
    ----------
    values : list
        Metric values, parallel to `dates`.  Order is not used in ranking
        (in-sample is date-agnostic); dates is accepted only for API symmetry
        with walk_forward_series and to validate equal length.
    dates  : list
        Trade dates, parallel to `values`.

    Returns
    -------
    fracs  : list[Optional[float]]
        rank / n_total for each row.  None for invalid values.
    bin20s : list[int]
        min(int(frac * 20) + 1, 20) for each row.  0 for invalid values.
    """
    if len(values) != len(dates):
        raise ValueError("values and dates must have equal length")

    n = len(values)
    fracs:  list = [None] * n
    bin20s: list = [0]    * n

    # Full-history population: all valid values sorted ascending (fixed).
    all_valid = sorted(v for v in values if is_valid_value(v))
    n_total = len(all_valid)
    if n_total == 0:
        return fracs, bin20s

    for i, v in enumerate(values):
        if not is_valid_value(v):
            continue
        rank    = bisect_left(all_valid, v)
        frac    = rank / n_total
        bin20   = min(int(frac * BIN20_BUCKETS) + 1, BIN20_BUCKETS)
        fracs[i]  = frac
        bin20s[i] = bin20

    return fracs, bin20s


# Train-test -----------------------------------------------------------------

# Per-ticker minimum count of valid pre-cutoff observations required to
# build a usable in-sample ruler for tt_bins.  Below this, the (ticker,
# metric) returns bin=0 for ALL of that ticker's rows (train and test)
# for that metric.  Train-window analog of wf_bins' warmup floor — a
# late-listing ticker without enough pre-cutoff history shouldn't
# contaminate the bins.  Spec sets this at 500 (~2 years of trading days)
# rather than reusing wf's 252 because tt's ruler is computed ONCE and
# frozen — we want materially more stability than wf's "hit-the-ground-
# running" expanding window.
TT_MIN_TRAIN_DEFAULT = 500


def train_test_series(values: list, dates: list, cutoff,
                      min_train: int = TT_MIN_TRAIN_DEFAULT) -> list:
    """Compute tt_bins bin20 for one (ticker, metric) chronological series.

    Algorithm
    ---------
    1. train_sorted = sorted valid values where trade_date < cutoff.
    2. If len(train_sorted) < min_train: return all zeros (no usable ruler).
    3. Otherwise, for each row in `values` (train AND test):
         - if value invalid → bin20 = 0
         - else:
             rank   = bisect_left(train_sorted, value)
             bin20  = min(int(rank / n_train * 20) + 1, 20)

    Sentinel 0 has a SINGLE meaning here: NULL/invalid source value OR
    (ticker, metric) had fewer than min_train pre-cutoff rows.  No warmup
    — wf_bins' warmup is a walk-forward artifact and doesn't apply.

    Returns
    -------
    bin20s : list[int]
        Length-`len(values)` list of bin assignments, 0 or 1..20.
        No frac output — tt_bins stores bin20 only.

    Edge cases handled
    ------------------
    - Ticker with zero pre-cutoff rows (post-cutoff listing, e.g. QBTS):
      train_sorted is empty, n_train = 0 < min_train, returns all-zeros
      WITHOUT entering the bisect loop (no divide-by-zero, no empty-list
      access).
    - Ticker with some valid metrics and some invalid: per-metric check
      is independent; each metric's series is evaluated on its own.
    """
    if len(values) != len(dates):
        raise ValueError("values and dates must have equal length")

    n = len(values)
    bin20s: list = [0] * n

    train_sorted = sorted(
        v for v, d in zip(values, dates)
        if d < cutoff and is_valid_value(v)
    )
    n_train = len(train_sorted)
    if n_train < min_train:
        return bin20s

    for i, v in enumerate(values):
        if not is_valid_value(v):
            continue
        rank = bisect_left(train_sorted, v)
        bin20s[i] = min(int(rank / n_train * BIN20_BUCKETS) + 1, BIN20_BUCKETS)

    return bin20s


def train_test_history(values) -> tuple[list, int]:
    """Build one ticker's sorted pre-cutoff history for one metric.

    Parameters
    ----------
    values : iterable of numbers
        Pre-cutoff values for the (ticker, metric), in any order.

    Returns
    -------
    history_vals : list[float]
        Filtered + sorted ascending.
    n_train      : int
        len(history_vals).
    """
    filtered = [float(v) for v in values if is_valid_value(v)]
    filtered.sort()
    return filtered, len(filtered)


def reference_bin_for_value(history_vals: list, value, n_bins: int) -> Optional[int]:
    """Reference implementation that mirrors the dashboard's _bin_for_value.

    Used ONLY by validate_bins.py for the train-test seam check.  Denominator
    is n_train (== len(history_vals)) — NO +1, distinct from walk-forward.
    Returns None when value is invalid OR n_train < n_bins.
    """
    if not is_valid_value(value):
        return None
    n = len(history_vals)
    if n < n_bins:
        return None
    rank = bisect_left(history_vals, value)
    return min(int(rank / n * n_bins) + 1, n_bins)
