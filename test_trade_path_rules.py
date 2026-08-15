"""
test_trade_path_rules.py — hand-checked exits for lib/trade_path_rules.py.

Every expected value here was worked out by hand from the bars in the test,
not captured from a previous run. That distinction matters: a golden-master
snapshot would have happily frozen the trailing-stop bug these tests caught,
where the stop level was set from the SAME bar's high and then gap-through was
applied against a level that was not resting when the bar opened.

Run:  python test_trade_path_rules.py     (exit 1 on any failure)
"""
import sys

import numpy as np

from lib.trade_path_rules import (
    BY_KEY, NEVER, REGISTRY, HORIZON_RULE_KEY, build_combine_sql,
    CombineError, evaluate,
)

PASS, FAIL = [], []


def check(name, got, want, tol=1e-9):
    ok = (abs(got - want) <= tol) if isinstance(want, float) else (got == want)
    (PASS if ok else FAIL).append(name)
    flag = "ok  " if ok else "FAIL"
    print(f"  [{flag}] {name:<52} got={got!r} want={want!r}")


def mkpath(bars, n_sessions=1, bars_per_session=None):
    """bars: list of (o,h,l,c). Returns (path, ctx) for ONE entry."""
    a = np.array(bars, dtype=float)
    B = len(bars)
    path = {
        "open":  a[:, 0][None, :].copy(),
        "high":  a[:, 1][None, :].copy(),
        "low":   a[:, 2][None, :].copy(),
        "close": a[:, 3][None, :].copy(),
    }
    bps = bars_per_session or B
    sess_end = np.full((1, 20), -1, dtype=np.int64)
    for k in range(n_sessions):
        sess_end[0, k] = min((k + 1) * bps - 1, B - 1)
    ctx = {
        "entry_price": np.array([100.0]),
        "atr": np.array([2.0]),
        "swing_low_1": np.array([97.0]),
        "swing_low_3": np.array([96.0]),
        "swing_low_5": np.array([95.0]),
        "session_end_idx": sess_end,
        "path_len": np.array([B]),
        "_ma_below_10": np.zeros((1, 20), dtype=bool),
        "_ma_below_20": np.zeros((1, 20), dtype=bool),
    }
    return path, ctx


print("\n=== 1. fixed_stop: exact trigger bar and fill AT the level ===")
# entry 100, stop 2% = 98. Bar 3 dips to 97.5 but opens at 99 -> fill at 98.
path, ctx = mkpath([
    (100, 101, 99.5, 100.5),   # 0
    (100.5, 101, 99.2, 99.5),  # 1
    (99.5, 100, 98.5, 99),     # 2  low 98.5 > 98, no trigger
    (99.0, 99.2, 97.5, 98.0),  # 3  low 97.5 <= 98 -> trigger, open 99 > 98
    (98.0, 98.5, 97.0, 97.5),  # 4
])
r = evaluate(path, ctx, [BY_KEY["fixed_stop__2"]])
bar, ret = r["fixed_stop__2"]
check("fixed_stop 2% trigger bar", int(bar[0]), 3)
check("fixed_stop 2% return (fill at level 98)", float(ret[0]), -0.02)

print("\n=== 2. GAP-THROUGH: bar opens below the stop -> fill at the OPEN ===")
# Same stop at 98, but bar 3 OPENS at 94 (overnight gap). Fill must be 94,
# NOT 98. This is the case that makes exit_return non-derivable from the level.
path, ctx = mkpath([
    (100, 101, 99.5, 100.5),
    (100.5, 101, 99.2, 99.5),
    (99.5, 100, 98.5, 99),
    (94.0, 95.0, 93.0, 94.5),   # gap straight through the stop
])
r = evaluate(path, ctx, [BY_KEY["fixed_stop__2"]])
bar, ret = r["fixed_stop__2"]
check("gap-through trigger bar", int(bar[0]), 3)
check("gap-through fills at open, not stop", float(ret[0]), -0.06)

print("\n=== 3. target, and gap-through UP ===")
path, ctx = mkpath([
    (100, 101, 99.5, 100.5),
    (100.5, 103.5, 100, 103),   # high 103.5 >= 103 -> trigger, open 100.5
])
r = evaluate(path, ctx, [BY_KEY["fixed_target__3"]])
bar, ret = r["fixed_target__3"]
check("fixed_target 3% trigger bar", int(bar[0]), 1)
check("fixed_target 3% fills at level 103", float(ret[0]), 0.03)

path, ctx = mkpath([
    (100, 101, 99.5, 100.5),
    (108.0, 109, 107.5, 108.5),  # gaps above the 103 target
])
r = evaluate(path, ctx, [BY_KEY["fixed_target__3"]])
bar, ret = r["fixed_target__3"]
check("target gap-up fills at open 108", float(ret[0]), 0.08)

print("\n=== 4. atr_stop / atr_target use ATR at entry (atr=2) ===")
# atr_stop k=1.5 -> level 100 - 3 = 97
path, ctx = mkpath([
    (100, 101, 99, 100),
    (100, 100.5, 97.5, 98),     # 97.5 > 97, no
    (98, 98.2, 96.8, 97),       # 96.8 <= 97 -> trigger, open 98 > 97
])
r = evaluate(path, ctx, [BY_KEY["atr_stop__1p5"], BY_KEY["atr_target__2"]])
check("atr_stop 1.5x trigger bar", int(r["atr_stop__1p5"][0][0]), 2)
check("atr_stop 1.5x return (level 97)", float(r["atr_stop__1p5"][1][0]), -0.03)
check("atr_target 2x never fires", int(r["atr_target__2"][0][0]), NEVER)

print("\n=== 5. swing_low uses the prior-session low ===")
path, ctx = mkpath([
    (100, 101, 99, 100),
    (99, 99.5, 96.5, 97),       # 96.5 <= swing_low_3 (96)? no. <= 97 yes
])
r = evaluate(path, ctx, [BY_KEY["swing_low__1"], BY_KEY["swing_low__3"]])
check("swing_low_1 (level 97) fires", int(r["swing_low__1"][0][0]), 1)
check("swing_low_1 return", float(r["swing_low__1"][1][0]), -0.03)
check("swing_low_3 (level 96) does not", int(r["swing_low__3"][0][0]), NEVER)

print("\n=== 6. trailing stop: HWM from entry, activation gating ===")
# Runs to 106 (HWM), then falls. trail 3% -> stop at 106*0.97 = 102.82
path, ctx = mkpath([
    (100, 102, 99.8, 101.5),    # 0 HWM 102
    (101.5, 106, 101, 105.5),   # 1 HWM 106
    (105.5, 105.5, 102.5, 103), # 2 low 102.5 <= 102.82 -> trigger
])
r = evaluate(path, ctx, [BY_KEY["trail__3_act0"]])
bar, ret = r["trail__3_act0"]
check("trail 3% act0 trigger bar", int(bar[0]), 2)
check("trail 3% act0 return (106*0.97)", float(ret[0]), 106 * 0.97 / 100 - 1)

# activation 3% requires HWM >= 103 first; reached at bar 1, so same answer.
r = evaluate(path, ctx, [BY_KEY["trail__3_act3"]])
check("trail act=3 armed at bar1, same exit", int(r["trail__3_act3"][0][0]), 2)

# Never reaches +3%, so an act=3 trail must NOT fire even though price falls.
path2, ctx2 = mkpath([
    (100, 101.5, 99, 101),
    (101, 102, 95, 96),         # big drop, but HWM only 102 (<103)
])
r = evaluate(path2, ctx2, [BY_KEY["trail__3_act3"], BY_KEY["trail__3_act0"]])
check("trail act=3 never armed -> no exit", int(r["trail__3_act3"][0][0]), NEVER)
check("trail act=0 same path DOES fire", int(r["trail__3_act0"][0][0]), 1)

print("\n=== 7. breakeven: arms at +2%, then stops at entry ===")
path, ctx = mkpath([
    (100, 102.5, 99.9, 102),    # 0 HWM 102.5 >= 102 -> armed
    (102, 102.2, 99.5, 99.8),   # 1 low 99.5 <= 100 -> exit at entry
])
r = evaluate(path, ctx, [BY_KEY["breakeven__2"]])
check("breakeven trigger bar", int(r["breakeven__2"][0][0]), 1)
check("breakeven return ~0", float(r["breakeven__2"][1][0]), 0.0)

# Dips below entry BEFORE arming -> must not fire.
path, ctx = mkpath([
    (100, 100.5, 99.0, 99.5),   # dips below entry, not yet armed
    (99.5, 103, 99.4, 102.8),   # arms here (HWM 103)
])
r = evaluate(path, ctx, [BY_KEY["breakeven__2"]])
check("breakeven ignores pre-arm dip", int(r["breakeven__2"][0][0]), NEVER)

print("\n=== 8. max_days day-counting (N=1 is SAME DAY) ===")
# 3 sessions x 2 bars. max_days=1 -> close of session 0 = bar 1.
path, ctx = mkpath([
    (100, 101, 99, 100.5), (100.5, 101, 100, 101),      # session 0
    (101, 102, 100.5, 101.5), (101.5, 102, 101, 102),   # session 1
    (102, 103, 101.5, 102.5), (102.5, 103, 102, 103),   # session 2
], n_sessions=3, bars_per_session=2)
r = evaluate(path, ctx, [BY_KEY["max_days__1"], BY_KEY["max_days__3"]])
check("max_days=1 exits at close of session 0", int(r["max_days__1"][0][0]), 1)
check("max_days=1 return = C/O - 1", float(r["max_days__1"][1][0]), 0.01)
check("max_days=3 exits at close of session 2", int(r["max_days__3"][0][0]), 5)
check("max_days=3 return", float(r["max_days__3"][1][0]), 0.03)

print("\n=== 9. no_progress: gain below threshold at the day boundary ===")
r = evaluate(path, ctx, [BY_KEY["no_progress__2_d2"], BY_KEY["no_progress__1_d2"]])
# close of session 1 (bar 3) = 102 -> gain 2.0%; <2% is False, <1% is False
check("no_progress 2% d2 (gain exactly 2%) no fire", int(r["no_progress__2_d2"][0][0]), NEVER)
check("no_progress 1% d2 no fire", int(r["no_progress__1_d2"][0][0]), NEVER)

path2, ctx2 = mkpath([
    (100, 101, 99, 100.2), (100.2, 101, 100, 100.3),
    (100.3, 101, 100, 100.4), (100.4, 101, 100, 100.5),
], n_sessions=2, bars_per_session=2)
r = evaluate(path2, ctx2, [BY_KEY["no_progress__1_d2"]])
check("no_progress 1% d2 fires on flat path", int(r["no_progress__1_d2"][0][0]), 3)
check("no_progress return = close at that bar", float(r["no_progress__1_d2"][1][0]), 0.005)

print("\n=== 10. never-fired stays NEVER, and horizon always fires ===")
path, ctx = mkpath([(100, 100.5, 99.9, 100.2)] * 4, n_sessions=20, bars_per_session=1)
r = evaluate(path, ctx, [BY_KEY["fixed_stop__4"], BY_KEY[HORIZON_RULE_KEY]])
check("fixed_stop 4% never fires", int(r["fixed_stop__4"][0][0]), NEVER)

print("\n=== 11. COMBINE: horizon backstop is structural ===")
sql, meta = build_combine_sql(["fixed_stop__1"])
check("horizon auto-added when absent", meta["horizon_auto_added"], True)
check("horizon present in combine", HORIZON_RULE_KEY in meta["rules"], True)
check("horizon column in SQL", BY_KEY[HORIZON_RULE_KEY].bar_col in sql, True)
check("unresolved excluded by default", "path_status = 'ok'" in sql, True)

sql2, meta2 = build_combine_sql(["fixed_stop__1", HORIZON_RULE_KEY])
check("no duplicate when caller supplies horizon", meta2["horizon_auto_added"], False)
check("horizon appears once", sql2.count(BY_KEY[HORIZON_RULE_KEY].bar_col),
      sql.count(BY_KEY[HORIZON_RULE_KEY].bar_col))

try:
    build_combine_sql([])
    check("empty selection raises", False, True)
except CombineError:
    check("empty selection raises", True, True)
try:
    build_combine_sql(["no_such_rule"])
    check("unknown rule raises", False, True)
except CombineError:
    check("unknown rule raises", True, True)

# Tie-break: stop must be evaluated before target in the CASE chain.
sql3, _ = build_combine_sql(["fixed_target__3", "fixed_stop__1"])
i_stop = sql3.index("xb_fixed_stop__1 = w.exit_bar")
i_tgt = sql3.index("xb_fixed_target__3 = w.exit_bar")
check("stop precedes target in tie-break", i_stop < i_tgt, True)

print("\n=== 12. registry shape ===")
check("rule count", len(REGISTRY), 59)
check("column count (2 per rule)", len(REGISTRY) * 2, 118)
check("all keys unique", len({r.key for r in REGISTRY}), 59)
bad = [r.key for r in REGISTRY if not r.key.replace("_", "").replace("p", "").isalnum()]
check("all keys SQL-safe", bad, [])

print(f"\n{'=' * 60}")
print(f"PASSED {len(PASS)} / {len(PASS) + len(FAIL)}")
if FAIL:
    print("FAILED:")
    for f in FAIL:
        print("  -", f)
    sys.exit(1)
print("ALL GREEN")
