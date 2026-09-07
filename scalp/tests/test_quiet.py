"""Hand-checked tests for scalp/quiet.py and the metric cut.

    python -m scalp.tests.test_quiet        (exit 1 on any failure)

Every expected value is worked out by hand from the trades in the test, not
captured from a run. A golden-master snapshot would have frozen whatever the
first implementation did, and the failures worth catching here are precisely
the ones that produce plausible numbers: a ratio computed against the wrong
window's range, an episode count that is really a window count, a guard that
admits a window whose predecessor was too thin to give it a level.

No database, no parquet, no API.
"""
import sys

import numpy as np
import pandas as pd

from scalp import config, db, metrics, quiet

PASS, FAIL = [], []


def check(name, got, want, tol=1e-9):
    ok = (abs(got - want) <= tol) if isinstance(want, float) else (got == want)
    (PASS if ok else FAIL).append(name)
    print(f"  [{'ok  ' if ok else 'FAIL'}] {name:<56} got={got!r} want={want!r}")


def series(prices_by_window, *, window_s=30.0, step_s=30.0, min_trades=10,
           per_window=30):
    """One window per price list, laid out back to back at `window_s`."""
    t, p = [], []
    for i, prices in enumerate(prices_by_window):
        base = i * window_s
        for j in range(per_window):
            t.append(base + j * (window_s / per_window))
            p.append(prices[j % len(prices)])
    n = len(t)
    return quiet.window_series(
        np.array(t, dtype="float64"), np.array(p, dtype="float64"),
        np.ones(n), window_s=window_s, step_s=step_s, start_s=0.0,
        end_s=len(prices_by_window) * window_s, min_trades=min_trades)


print("=== 1. level, range and shift, hand-checked ===")
# Window 0: prices alternate 99.99 / 100.01 -> p25=99.99, p75=100.01,
#           IQR = 0.02 = 2 cents; equal sizes so the VWAP is 100.00.
# Window 1: same shape 10 cents higher -> level 100.10, IQR still 2 cents.
# shift = 10 cents, range = 2 cents, ratio = 5.0.
s = series([[99.99, 100.01], [100.09, 100.11]])
check("window 0 level", float(s["level"][0]), 100.00, 1e-9)
check("window 1 level", float(s["level"][1]), 100.10, 1e-9)
check("window 0 range (cents)", float(s["iqr_c"][0]), 2.0, 1e-9)
check("window 1 shift (cents)", float(s["shift_c"][1]), 10.0, 1e-7)
check("window 1 ratio = shift/range", float(s["ratio"][1]), 5.0, 1e-7)
check("window 0 has no predecessor, so no ratio",
      bool(np.isnan(s["ratio"][0])), True)
check("window 0 is not eligible", bool(s["eligible"][0]), False)
check("window 1 is eligible", bool(s["eligible"][1]), True)

# The ratio must use the CURRENT window's range. Widen window 1 only: the
# shift is unchanged, so a ratio built on window 0's range would not move.
s2 = series([[99.99, 100.01], [100.05, 100.15]])
check("window 1 range widens to 10c", float(s2["iqr_c"][1]), 10.0, 1e-7)
check("...and the ratio uses it, not the predecessor's",
      float(s2["ratio"][1]), 1.0, 1e-7)


print("\n=== 2. bps and cents are the same measurement, differently scaled ===")
# A 15-cent IQR is 15 cents on a $700 name and on a $70 one; the bps figure
# is what differs, and that is the whole reason both are stored.
cheap = series([[69.925, 70.075], [69.925, 70.075]])
dear = series([[699.925, 700.075], [699.925, 700.075]])
check("cheap name range (cents)", round(float(cheap["iqr_c"][1]), 6), 15.0)
check("expensive name range (cents)", round(float(dear["iqr_c"][1]), 6), 15.0)
check("cheap name range (bps) is ~10x the expensive one's",
      round(float(cheap["iqr_bps"][1]) / float(dear["iqr_bps"][1])), 10)


print("\n=== 2b. the two span measures ===")
# 20 prints: 10 at 100.00 and 10 spread from 99.80 to 100.20. The IQR sees the
# middle 50%; the p10-p90 span sees the middle 80% and therefore more of the
# tails. Both are computed from ONE sort, so they cannot disagree about which
# prints were in the window.
_p = np.concatenate([np.full(10, 100.00),
                      np.linspace(99.80, 100.20, 10)])
_t = np.arange(20, dtype="float64")
_s = quiet.window_series(_t, _p, np.ones(20), window_s=20.0, step_s=20.0,
                         start_s=0.0, end_s=20.0, min_trades=10)
_iqr, _pp = float(_s["iqr_c"][0]), float(_s["pp_c"][0])
check("p10-p90 is wider than the IQR on a tailed window", _pp > _iqr, True)
check("both are finite", bool(np.isfinite(_iqr) and np.isfinite(_pp)), True)

# On a window with no tails they coincide, which is the sanity check that the
# difference above is the distribution and not an arithmetic slip.
_flat = np.concatenate([np.full(10, 99.95), np.full(10, 100.05)])
_s2 = quiet.window_series(_t, _flat, np.ones(20), window_s=20.0, step_s=20.0,
                          start_s=0.0, end_s=20.0, min_trades=10)
check("with no tails the two measures agree",
      round(float(_s2["iqr_c"][0]), 6), round(float(_s2["pp_c"][0]), 6))

# Both spans are the same measurement in two units, so the cents/bps ratio has
# to be the level, identically for each.
_lvl = float(_s["level"][0])
check("iqr bps = iqr cents scaled by the level",
      round(float(_s["iqr_bps"][0]), 6),
      round(_iqr / 100.0 / _lvl * 1e4, 6))
check("p10p90 bps uses the SAME level scaling",
      round(float(_s["pp_bps"][0]), 6),
      round(_pp / 100.0 / _lvl * 1e4, 6))

# The stray-print case, which is the whole reason both are stored. One print
# 30 cents away in a 10-trade window is 10% of the sample and lands on the p90
# boundary; the IQR does not move, the span roughly doubles.
_core = np.full(9, 100.00)
_with = np.concatenate([_core, [100.30]])
_without = np.concatenate([_core, [100.00]])
_ta = np.arange(10, dtype="float64")
_a = quiet.window_series(_ta, _with, np.ones(10), window_s=10.0, step_s=10.0,
                         start_s=0.0, end_s=10.0, min_trades=10)
_b = quiet.window_series(_ta, _without, np.ones(10), window_s=10.0,
                         step_s=10.0, start_s=0.0, end_s=10.0, min_trades=10)
check("one stray print leaves the IQR alone",
      round(float(_a["iqr_c"][0]), 6), round(float(_b["iqr_c"][0]), 6))
check("...but moves the p10-p90 span at the guard",
      float(_a["pp_c"][0]) > float(_b["pp_c"][0]), True)

# The ratio must stay on the IQR: the thresholds, the counts and the episodes
# are all defined against it, and swapping the denominator would silently
# redefine what "quiet" means.
_r = quiet._ratio(_s["shift_c"], _s["iqr_c"])
check("the ratio is computed against the IQR, not the wider span",
      bool(np.array_equal(np.nan_to_num(_r, nan=-1.0),
                           np.nan_to_num(_s["ratio"], nan=-1.0))), True)


print("\n=== 3. the degenerate range is handled, not dropped ===")
# All trades at one price: range 0. This is maximal stillness, not missing
# data, and plain division would make the quietest windows in the session
# vanish from the count as 0/0 = NaN.
still = series([[100.00], [100.00]])
check("range is zero", float(still["iqr_c"][1]), 0.0)
check("no shift either -> ratio 0, the quietest possible",
      float(still["ratio"][1]), 0.0)
jump = series([[100.00], [100.10]])
check("range zero but level jumped -> infinite, not NaN",
      bool(np.isinf(jump["ratio"][1])), True)
check("...so it is not counted as quiet",
      bool(jump["ratio"][1] < 1.0), False)


print("\n=== 4. the trade guard, and why it needs BOTH windows ===")
# Window 1 is dense but its predecessor is thin, so its level is measured
# against an unstable one. Requiring only the current window would admit it.
t = list(np.arange(0, 4)) + list(np.arange(30, 60))
p = [100.0] * 4 + [100.05, 100.15] * 15
s3 = quiet.window_series(np.array(t, dtype="float64"),
                         np.array(p, dtype="float64"), np.ones(len(t)),
                         window_s=30.0, step_s=30.0, start_s=0.0, end_s=60.0,
                         min_trades=10)
check("thin window is not eligible", bool(s3["eligible"][0]), False)
check("dense window with a THIN PREDECESSOR is not eligible either",
      bool(s3["eligible"][1]), False)
check("its ratio is suppressed", bool(np.isnan(s3["ratio"][1])), True)
check("the guard default is 10", quiet.MIN_TRADES, 10)


print("\n=== 5. overlapping windows are not separate chances ===")
# The error this corrects: at a 30s window and a 10s step, one contiguous
# quiet patch yields ~3 overlapping quiet windows. The window count is
# inflated by ~window/step and differs per window length (1.5x / 3x / 6x), so
# the three rows are not comparable to each other on it. Episodes are.
check("one contiguous run is one episode",
      quiet._episodes(np.array([0, 1, 1, 1, 1, 0, 0], dtype=bool)), 1)
check("two runs are two episodes",
      quiet._episodes(np.array([1, 1, 0, 1, 1, 1, 0, 1], dtype=bool)), 3)
check("a run at the very start counts",
      quiet._episodes(np.array([1, 1, 0], dtype=bool)), 1)
check("no quiet windows, no episodes",
      quiet._episodes(np.zeros(5, dtype=bool)), 0)
check("empty session", quiet._episodes(np.zeros(0, dtype=bool)), 0)

rng = np.random.default_rng(4)
n = 40000
t = np.sort(rng.uniform(0, 3600, n))
px = 100 + np.cumsum(rng.normal(0, 0.002, n)) + rng.choice([-.01, .01], n)
ss = quiet.session_series(t, px, np.ones(n), start_s=0.0, end_s=3600.0)
dm = quiet.daily_metrics(ss)
# Keyed off the primary rather than a literal, so moving the primary does not
# leave this test quietly asserting about a window nobody uses.
_pw = quiet.PRIMARY_WINDOW_SEC
w30 = dm[f"quiet_windows_{_pw}s_10"]
e30 = dm[f"quiet_episodes_{_pw}s_10"]
check("window count exceeds episode count on a real-shaped session",
      w30 > e30, True)
check("...and the inflation is roughly window/step, not 1",
      w30 / max(e30, 1) > 1.5, True)


print("\n=== 5b. the step scales with the window ===")
# The property the scaled step buys, and the reason it is a ratio rather than
# three hand-picked numbers.
#
# OVERLAP IS NOW IDENTICAL ACROSS ROWS. At a fixed 10s step the inflation was
# 1.5x / 3x / 6x, so a raw window count meant something different on every row.
check("window/step is 3 at every length",
      {w: round(w / quiet.step_for(w), 6) for w in quiet.WINDOWS_SEC},
      {w: 3.0 for w in quiet.WINDOWS_SEC})

# SCALE INVARIANCE. On a random walk the shift grows as sqrt(step) and the
# range as sqrt(window), so a ratio built on both is constant when they scale
# together and drifts when they do not. Checked on a pure walk, with no
# bid-ask bounce: bounce puts a FLOOR under the range at short windows, which
# is real and is measured separately below.
_rng = np.random.default_rng(21)
_n = 120000
_t = np.sort(_rng.uniform(0, 23400, _n))
_walk = 100 + np.cumsum(_rng.normal(0, 0.0015, _n))

_ss = quiet.session_series(_t, _walk, np.ones(_n), start_s=0.0, end_s=23400.0)
_dm = quiet.daily_metrics(_ss)
_scaled = [_dm[f"shift_over_range_median_{w}s"] for w in quiet.WINDOWS_SEC]

_sf = quiet.session_series(_t, _walk, np.ones(_n), start_s=0.0, end_s=23400.0,
                           step_s=10.0)
_dmf = quiet.daily_metrics(_sf)
_fixed = [_dmf[f"shift_over_range_median_{w}s"] for w in quiet.WINDOWS_SEC]

check("scaled step: the ratio is ~constant across the grid",
      max(_scaled) / min(_scaled) < 1.15, True)
check("fixed step: it is not",
      max(_fixed) / min(_fixed) > 1.5, True)
check("...and the scaled step is strictly the flatter of the two",
      (max(_scaled) / min(_scaled)) < (max(_fixed) / min(_fixed)), True)

# The residual on REAL-shaped data is the bid-ask bounce, not the definition.
# It floors the range at short windows, so short windows read quieter. This is
# a property of the tape rather than an artefact to remove -- for a scalper
# the bounce at 30s IS part of what is captured -- so it is asserted as a
# known, directional effect rather than silently tolerated.
_bounced = _walk + _rng.choice([-.01, .01], _n)
_sb = quiet.session_series(_t, _bounced, np.ones(_n), start_s=0.0,
                           end_s=23400.0)
_dmb = quiet.daily_metrics(_sb)
check("bounce widens the range most at the SHORTEST window",
      _dmb["quiet_range_iqr_cents_30s"] / _dm["quiet_range_iqr_cents_30s"] >
      _dmb["quiet_range_iqr_cents_120s"] / _dm["quiet_range_iqr_cents_120s"],
      True)
check("...so the ratio drifts up with the window on a real tape",
      _dmb["shift_over_range_median_120s"] >
      _dmb["shift_over_range_median_30s"], True)

# The p10p90 twin of the continuous statistic exists at every window, so the
# denominator question is answerable from one recompute.
for _w in quiet.WINDOWS_SEC:
    check(f"shift_over_p10p90_median_{_w}s is produced",
          f"shift_over_p10p90_median_{_w}s" in _dmb, True)
check("the wider span gives a SMALLER ratio, as it must",
      _dmb["shift_over_p10p90_median_60s"] <
      _dmb["shift_over_range_median_60s"], True)


print("\n=== 6. counts, eligibility and the derivable share ===")
check("quiet count never exceeds the eligible count",
      dm["quiet_windows_30s_10"] <= dm["quiet_eligible_windows_30s"], True)
check("thresholds are nested: 0.5 <= 1.0 <= 2.0",
      dm["quiet_windows_30s_05"] <= dm["quiet_windows_30s_10"] <=
      dm["quiet_windows_30s_20"], True)
# A zero count is only interpretable next to the eligible count -- otherwise
# "never quiet" and "never enough trades to tell" are the same stored value.
thin_t = np.arange(0.0, 600.0, 12.0)             # 5 trades/min, far too thin
thin = quiet.session_series(thin_t, np.full(thin_t.size, 100.0),
                            np.ones(thin_t.size), start_s=0.0, end_s=600.0)
tdm = quiet.daily_metrics(thin)
check("a too-thin name has zero quiet windows", tdm["quiet_windows_30s_10"], 0)
check("...and zero ELIGIBLE windows, which is how you tell why",
      tdm["quiet_eligible_windows_30s"], 0)


print("\n=== 7. the metric set ===")
check("quiet metrics produced", len(quiet.metric_names()), 34)
check("every declared name is actually computed",
      sorted(dm) == sorted(quiet.metric_names()), True)
check("the primary is pre-registered as 60s / 1.0",
      (quiet.PRIMARY_WINDOW_SEC, quiet.PRIMARY_THRESHOLD_KEY), (60, "10"))
check("windows are 30/60/120", quiet.WINDOWS_SEC, (30, 60, 120))
check("the step scales with the window",
      {w: quiet.step_for(w) for w in quiet.WINDOWS_SEC},
      {30: 10.0, 60: 20.0, 120: 40.0})

check("noise variants cut to two", config.NOISE_VARIANTS,
      ("tw_mid", "trade_price"))
check("noise statistics cut to one", config.NOISE_STATISTICS, ("_p75",))
check("the decomposition is quote-only",
      config.NOISE_DECOMPOSITION_VARIANTS, ("tw_mid",))

# quiet.py is VENDORED VERBATIM into the live tape tool, which must not import
# from scalp. A `from scalp import ...` here would break that copy silently.
# Parsed, not grepped: the module's own docstring explains the vendoring rule
# and therefore contains the words "from scalp". Only real import statements
# count, so this walks the AST rather than matching prose.
import ast
tree = ast.parse(open(quiet.__file__, encoding="utf-8").read())
imported = set()
for node in ast.walk(tree):
    if isinstance(node, ast.Import):
        imported.update(a.name.split(".")[0] for a in node.names)
    elif isinstance(node, ast.ImportFrom) and node.module:
        imported.add(node.module.split(".")[0])
check("quiet.py imports only third-party libraries",
      sorted(imported), ["__future__", "numpy", "pandas"])
check("...nothing from scalp, so the vendored copy still runs",
      "scalp" in imported, False)


print("\n=== 8. the full daily row: 60 kept + 25 new ===")
day = pd.Timestamp("2026-09-04 09:30:00")
end = day + pd.Timedelta(hours=6.5)
n = 30000
rng = np.random.default_rng(9)
ts = day + pd.to_timedelta(np.sort(rng.uniform(0, 23400, n)), unit="s")
px = 100 + np.cumsum(rng.normal(0, 0.004, n)) + rng.choice([-.01, .01], n)
df = pd.DataFrame({"trade_timestamp": ts, "price": px,
                   "size": rng.integers(1, 400, n),
                   "exchange": rng.integers(1, 20, n),
                   "bid": px - 0.02, "ask": px + 0.02, "condition": 0})
cols = metrics.Columns(time="trade_timestamp", price="price", size="size",
                       bid="bid", ask="ask", exchange="exchange",
                       condition_cols=["condition"])
metrics.attach_condition_flags(df, cols.condition_cols)

daily = metrics.compute_window(df, cols, day, end)
daily.pop("_provenance", None)
kept = [k for k, v in daily.items() if db._storable(v)]
check("kept metrics after the cut", len(kept), 60)

qs = metrics.quiet_session(df, cols, day, end)
daily.update(quiet.daily_metrics(qs))
stored = [k for k, v in daily.items() if db._storable(v)]
check("total stored daily metrics", len(stored), 94)

dead = [k for k in stored if "_rms" in k or "_p90" in k or "_mean" in k
        and k.startswith("noise_bps")]
check("no rms or p90 survives the cut", dead, [])
check("both survivors are present",
      all(f"noise_bps_{v}_5s_p75" in stored for v in ("tw_mid", "trade_price")),
      True)
check("trade_price carries no move_rate (it would be ~1.0 always)",
      any(k.startswith("move_rate_trade_price") for k in stored), False)


print("\n=== 9. daily equals the sum of the buckets, by construction ===")
buckets = metrics.compute_buckets(df, cols, day, end,
                                  config.INTRADAY_BUCKET_MINUTES)
for row in buckets:
    row.update(metrics.quiet_bucket_row(qs, row))
check("bucket quiet counts sum to the daily count",
      sum(r[config.QUIET_PRIMARY_COLUMN] for r in buckets),
      daily[config.QUIET_PRIMARY_COLUMN])
check("bucket eligible counts sum to the daily eligible count",
      sum(r[config.QUIET_PRIMARY_ELIGIBLE_COLUMN] for r in buckets),
      daily[config.QUIET_PRIMARY_ELIGIBLE_COLUMN])
missing = [c for c, _ in config.INTRADAY_COLUMNS if c not in buckets[0]]
check("every intraday column is produced", missing, [])


print(f"\n{'=' * 68}")
print(f"PASSED {len(PASS)} / {len(PASS) + len(FAIL)}")
if FAIL:
    print("FAILED:")
    for f in FAIL:
        print("  -", f)
    sys.exit(1)
print("ALL GREEN")
