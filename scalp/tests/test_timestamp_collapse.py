"""Same-timestamp collapsing must happen BEFORE duration weighting.

49.8% of quote records share an instant with another record. Getting the order
wrong silently drops observations from every time-weighted number: no error, no
change in row count, and a result that looks entirely normal.

These tests assert the FAILURE MODE as well as the fix, because the fix on its
own is untestable in the way that matters — the correct answer and the wrong
answer are both plausible numbers, and only a case constructed to separate them
proves which one the code produces.

    python -m scalp.tests.test_timestamp_collapse     # standalone
    pytest scalp/tests/test_timestamp_collapse.py     # or under pytest
"""
from __future__ import annotations

import pandas as pd

from scalp.metrics import (
    collapse_to_distinct_instants,
    durations_seconds,
    time_weighted_mean,
)


BASE = pd.Timestamp("2026-08-28 10:00:00")


def _frame(offsets_sec, mids):
    """Quote frame with the given second-offsets and midpoints."""
    return pd.DataFrame({
        "t": [BASE + pd.Timedelta(seconds=s) for s in offsets_sec],
        "mid": mids,
    })


# --- the collapse itself -----------------------------------------------------

def test_collapse_keeps_last_at_each_instant():
    """The surviving record is the one that stood when the clock moved on."""
    df = _frame([0, 0, 1, 2], [100.0, 200.0, 300.0, 400.0])
    out = collapse_to_distinct_instants(df, "t")

    assert len(out) == 3, "one row per distinct instant"
    assert list(out["mid"]) == [200.0, 300.0, 400.0], (
        "kept the last record at the shared instant, not the first")


def test_collapse_is_stable():
    """'Last' means last-arrived, not an arbitrary pick among equals."""
    df = _frame([0, 0, 0], [1.0, 2.0, 3.0])
    out = collapse_to_distinct_instants(df, "t")
    assert list(out["mid"]) == [3.0]


def test_collapse_of_empty_frame():
    df = _frame([], [])
    assert collapse_to_distinct_instants(df, "t").empty


# --- durations ---------------------------------------------------------------

def test_durations_are_forward_gaps_and_cover_the_window():
    """Every observation carries real weight, and the weights tile the window.

    The duration of an observation is the gap FORWARD to the next distinct
    instant — not backward from the previous one. The direction is the whole
    bug: a backward difference gives the surviving record at a shared instant a
    weight of zero.
    """
    end = BASE + pd.Timedelta(seconds=3)
    df = collapse_to_distinct_instants(
        _frame([0, 0, 1, 2], [100.0, 200.0, 300.0, 400.0]), "t")

    d = durations_seconds(df["t"], end)

    assert list(d) == [1.0, 1.0, 1.0]
    assert (d > 0).all(), "no observation may carry zero weight after collapse"
    assert d.sum() == (end - BASE).total_seconds(), (
        "durations must tile the window exactly")


def test_last_observation_runs_to_the_window_end():
    end = BASE + pd.Timedelta(seconds=10)
    df = collapse_to_distinct_instants(_frame([0, 4], [100.0, 200.0]), "t")
    d = durations_seconds(df["t"], end)
    assert list(d) == [4.0, 6.0]


# --- the failure mode this ordering prevents ---------------------------------

def test_wrong_order_silently_drops_an_observation():
    """Durations before collapsing zero out records that should carry weight.

    Constructed so the right and wrong answers DIFFER numerically. Both are
    perfectly plausible midpoints; nothing about the wrong one looks wrong.
    """
    end = BASE + pd.Timedelta(seconds=3)
    raw = _frame([0, 0, 1, 2], [100.0, 200.0, 300.0, 400.0])

    # Correct: collapse first, then measure forward gaps.
    collapsed = collapse_to_distinct_instants(raw, "t")
    correct = time_weighted_mean(collapsed["mid"],
                                 durations_seconds(collapsed["t"], end))

    # Wrong: weight the raw records by the backward difference, which is the
    # natural one-liner and is what this ordering exists to prevent.
    naive_weights = raw["t"].diff().dt.total_seconds().fillna(0.0)
    naive = time_weighted_mean(raw["mid"], naive_weights)

    assert correct == 300.0
    assert naive == 350.0
    assert correct != naive, (
        "the two orderings must be distinguishable, or this test proves nothing")

    # And the specific harm: the 200.0 observation — the one that actually
    # stood for the first second — carries zero weight and vanishes.
    assert naive_weights.iloc[1] == 0.0


def test_zero_weight_count_matches_duplicate_count():
    """Every duplicated instant costs exactly one silently-dropped record.

    Scaled up to the real proportion: with ~50% of records sharing an instant,
    ~50% of observations disappear from every time-weighted number.
    """
    offsets, mids = [], []
    for i in range(100):
        offsets += [i, i]                      # every instant duplicated
        mids += [100.0 + i, 200.0 + i]
    raw = _frame(offsets, mids)

    collapsed = collapse_to_distinct_instants(raw, "t")
    assert len(collapsed) == 100
    assert len(raw) - len(collapsed) == 100

    naive_weights = raw["t"].diff().dt.total_seconds().fillna(0.0)
    zero_weighted = int((naive_weights == 0).sum())
    assert zero_weighted == 100, (
        f"expected 100 observations silently zero-weighted, got {zero_weighted}")

    # After collapsing, nothing is zero-weighted.
    end = BASE + pd.Timedelta(seconds=100)
    good = durations_seconds(collapsed["t"], end)
    assert (good > 0).all()


def test_time_weighted_mean_ignores_zero_and_nan_weights():
    v = pd.Series([1.0, 2.0, 3.0])
    w = pd.Series([0.0, 1.0, float("nan")])
    assert time_weighted_mean(v, w) == 2.0
    assert pd.isna(time_weighted_mean(pd.Series([1.0]), pd.Series([0.0])))


def _main() -> None:
    tests = [obj for name, obj in sorted(globals().items())
             if name.startswith("test_") and callable(obj)]
    failures = 0
    for fn in tests:
        try:
            fn()
        except AssertionError as exc:
            failures += 1
            print(f"FAIL  {fn.__name__}: {exc}")
        else:
            print(f"ok    {fn.__name__}")
    print()
    if failures:
        raise SystemExit(f"{failures} of {len(tests)} tests FAILED")
    print(f"All {len(tests)} tests passed.")


if __name__ == "__main__":
    _main()
