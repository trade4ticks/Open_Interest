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
    BY_KEY, MAX_HORIZON_SESSIONS, NEVER, REGISTRY, HORIZON_RULE_KEY,
    build_combine_sql, CombineError, compute_features, evaluate,
    _rule_atr_trail, _rule_trail,
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
    # Sized from the horizon, not a literal 20: max_days__40 indexes column 39
    # and would raise on a fixed-width 20 here.
    sess_end = np.full((1, MAX_HORIZON_SESSIONS), -1, dtype=np.int64)
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
        "_ma_below_10": np.zeros((1, MAX_HORIZON_SESSIONS), dtype=bool),
        "_ma_below_20": np.zeros((1, MAX_HORIZON_SESSIONS), dtype=bool),
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
check("unresolved excluded by default",
      f"WHERE {BY_KEY[HORIZON_RULE_KEY].bar_col} IS NOT NULL" in sql, True)
check("global path_status flag no longer used", "path_status" in sql, False)
check("meta names the resolution column", meta["resolution_column"],
      BY_KEY[HORIZON_RULE_KEY].bar_col)

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
check("rule count", len(REGISTRY), 143)
check("column count (2 per rule)", len(REGISTRY) * 2, 286)
check("all keys unique", len({r.key for r in REGISTRY}), 143)
_toolong = [r.ret_col for r in REGISTRY if len(r.ret_col) > 63]
check("every column name inside Postgres's 63-char limit", _toolong, [])
check("longest column name", max(len(r.ret_col) for r in REGISTRY) <= 63, True)
# Percent and ATR parameters must stay distinguishable by NAME -- the unit is
# inferred from it downstream, so an ATR multiple in a field called
# `activation` would render as a percent.
_mixed = [r.key for r in REGISTRY
          if "activation" in r.params and r.family.startswith("atr_")]
check("no ATR family uses the percent `activation` name", _mixed, [])
bad = [r.key for r in REGISTRY if not r.key.replace("_", "").replace("p", "").isalnum()]
check("all keys SQL-safe", bad, [])

print("\n=== 13. per-horizon resolution vs path_status, on the build itself ===")
# When the catalog topped out at max_days__20 these two agreed exactly, and an
# earlier revision of this test asserted that. They no longer agree, and the
# divergence is the entire point of the change: path_status is stamped against
# the BUILD WIDTH (now 40 sessions), so it marks the trailing 39 entries
# unresolved for every policy alike, while each time rule's own exit_bar knows
# only about the sessions that rule actually needs.
#
# So the equivalence now holds against the LONGEST rule -- the one whose
# horizon equals the build width -- and every shorter rule resolves strictly
# more entries. Both halves are asserted, because the first is what makes the
# filter sound and the second is what makes it worth having.
import pandas as pd
import build_trade_paths as btp

N_SESS, BARS_PER = MAX_HORIZON_SESSIONS + 20, 4
_rows, _day = [], pd.Timestamp("2024-01-02 09:30")
for si_ in range(N_SESS):
    base = 100.0 + si_ * 0.10          # drifts up; no stop or target is hit
    ts0 = _day + pd.Timedelta(days=si_)
    for b in range(BARS_PER):
        ts = ts0 + pd.Timedelta(minutes=b)
        _rows.append({"trade_date": ts.date(), "session": "regular",
                      "timestamp": ts, "open": base, "high": base + 0.05,
                      "low": base - 0.05, "close": base})
_bars = pd.DataFrame(_rows)

_orig_load = btp.load_bars
btp.load_bars = lambda conn, ticker, session_filter="regular": _bars.copy()
try:
    _entries = pd.DataFrame({"ticker": "TEST",
                             "trade_date": sorted(_bars["trade_date"].unique())})
    _out, _stats = btp.build_ticker(None, "TEST", _entries, "open", "regular", 400)
finally:
    btp.load_bars = _orig_load

_longest = max((r for r in REGISTRY if r.family == "max_days"),
               key=lambda r: r.params["n"])
check("longest rule equals the build width", _longest.params["n"],
      btp.MAX_HORIZON_SESSIONS)

_mismatch = [r["trade_date"] for r in _out
             if (r["path_status"] == "ok") != (r[_longest.bar_col] is not None)]
check("path_status == (longest rule's exit_bar IS NOT NULL)", _mismatch, [])
check("rows built", len(_out), N_SESS)

_n_ok = sum(1 for r in _out if r["path_status"] == "ok")
check("ok rows = sessions - (build width - 1)", _n_ok,
      N_SESS - (btp.MAX_HORIZON_SESSIONS - 1))

# The payload: every shorter horizon resolves strictly more entries than the
# global flag admits, which is what stops a longer catalog horizon from
# shrinking the eligible population of a short policy.
def _n_res(key):
    return sum(1 for r in _out if r[BY_KEY[key].bar_col] is not None)

check("max_days__20 resolves more than path_status admits",
      _n_res("max_days__20"), N_SESS - 19)
check("max_days__1 resolves every entry", _n_res("max_days__1"), N_SESS)
check("...and all three are strictly ordered",
      _n_res("max_days__1") > _n_res("max_days__20") > _n_ok, True)
check("the trailing window the old global filter would have cost the 1-day "
      "policy", _n_res("max_days__1") - _n_ok,
      btp.MAX_HORIZON_SESSIONS - 1)

print("\n=== 14. atr_trail: ATR distance and ATR arming, hand-checked ===")
# entry 100, atr 2.0 (mkpath). k=1 -> the stop rests 2.0 below hwm_prev.
#   bar  hwm_prev  level  low     fires?
#   0    100       98     99.5    no
#   1    101       99     100     no
#   2    102       100    100.5   no
#   3    103       101    99.8    YES -- opens at 102, so fills AT 101
path, ctx = mkpath([
    (100, 101, 99.5, 100.5),
    (100.5, 102, 100, 101),
    (101, 103, 100.5, 102),
    (102, 102.5, 99.8, 100),
])
r = evaluate(path, ctx, [BY_KEY["atr_trail__1_act0"]])
check("atr_trail k=1 trigger bar", int(r["atr_trail__1_act0"][0][0]), 3)
check("atr_trail k=1 return (fill at level 101)",
      float(r["atr_trail__1_act0"][1][0]), 0.01)

# Same bars, armed at 2 ATRs = entry + 4 = 104. The high-water mark tops out
# at 103, so the stop is never live and the dip below it is not an exit.
path, ctx = mkpath([
    (100, 101, 99.5, 100.5), (100.5, 102, 100, 101),
    (101, 103, 100.5, 102), (102, 102.5, 99.8, 100),
])
r = evaluate(path, ctx, [BY_KEY["atr_trail__1_act2"]])
check("atr_trail unarmed never fires", int(r["atr_trail__1_act2"][0][0]), NEVER)

print("\n=== 15. the trail caches are namespaced by family ===")
# trail=2.0 (percent) and atr_trail k=2.0 (ATR multiples) are different rules
# that share a numeric level. On a bare cache key they would hand each other
# the wrong crossing array and return entirely plausible numbers.
#   bar  hwm_prev  trail@2%  atr@k=2 (-4)  low
#   0    100       98        96            99.5   neither
#   1    101       98.98     97            97.5   trail only
#   2    101       98.98     97            96.0   atr
BARS = [(100, 101, 99.5, 100.5), (100.5, 101, 97.5, 98), (98, 98.5, 96.0, 96.5)]
p1, c1 = mkpath(list(BARS))
solo_pct = evaluate(p1, c1, [BY_KEY["trail__2_act0"]])["trail__2_act0"]
p2, c2 = mkpath(list(BARS))
solo_atr = evaluate(p2, c2, [BY_KEY["atr_trail__2_act0"]])["atr_trail__2_act0"]
p3, c3 = mkpath(list(BARS))
both = evaluate(p3, c3, [BY_KEY["trail__2_act0"], BY_KEY["atr_trail__2_act0"]])

check("percent trail alone fires bar 1", int(solo_pct[0][0]), 1)
check("atr trail alone fires bar 2", int(solo_atr[0][0]), 2)
check("percent trail unchanged when evaluated together",
      int(both["trail__2_act0"][0][0]), int(solo_pct[0][0]))
check("atr trail unchanged when evaluated together",
      int(both["atr_trail__2_act0"][0][0]), int(solo_atr[0][0]))
check("returns unchanged too (percent)",
      float(both["trail__2_act0"][1][0]), float(solo_pct[1][0]))
check("returns unchanged too (atr)",
      float(both["atr_trail__2_act0"][1][0]), float(solo_atr[1][0]))
# evaluate() shallow-copies ctx before adding _low/_close, so the cache it
# fills is not the caller's dict. Drive the rule functions directly to inspect
# the cache they actually share.
p5, c5 = mkpath(list(BARS))
c5["_low"], c5["_close"] = p5["low"], p5["close"]
F5 = compute_features(p5, {"hwm_prev", "trail_abs"}, c5)
_rule_trail(F5, c5, trail=2.0, activation=0.0)
_rule_atr_trail(F5, c5, k=2.0, activation_atr=0.0)
check("both cache entries coexist under one numeric level",
      len(c5["_nxt"]), 2)
check("cache keys are namespaced by family", sorted(c5["_nxt"]),
      [("atr_trail", 2.0), ("trail", 2.0)])

print("\n=== 16. crossing caches are int32, activation cached by value ===")
check("cached crossing arrays are int32",
      {str(v.dtype) for v in c5["_nxt"].values()}, {"int32"})
check("int32 covers the widest supported path (extended hours)",
      MAX_HORIZON_SESSIONS * 960 < np.iinfo(np.int32).max, True)
check("SMALLINT does NOT cover it -- hence the build guard",
      MAX_HORIZON_SESSIONS * 960 <= 32767, False)
check("SMALLINT does cover the regular session at this horizon",
      MAX_HORIZON_SESSIONS * 390 <= 32767, True)

# Seven distinct activations across 56 trail rules must cost seven crossings.
_acts = sorted({r.params["activation"] for r in REGISTRY
                if r.family == "trail" and r.params["activation"] != 0})
for _a in _acts:
    _rule_trail(F5, c5, trail=2.0, activation=_a)
check("activation cached by value, not per rule", len(c5["_act_pct"]), len(_acts))
check("...covering every non-zero threshold", sorted(c5["_act_pct"]), _acts)

print("\n=== 17. the backstop is a function of the SELECTION ===")
sql, meta = build_combine_sql(["fixed_stop__1"])
check("no time rule -> default backstop", meta["backstop_rule"], HORIZON_RULE_KEY)
check("default backstop is 20 sessions", meta["backstop_sessions"], 20)

sql, meta = build_combine_sql(["fixed_stop__1", "max_days__40"])
check("longest selected horizon becomes the backstop",
      meta["backstop_rule"], "max_days__40")
check("...and nothing shorter is appended over it",
      HORIZON_RULE_KEY in meta["rules"], False)
check("...and the filter demands its 40 sessions",
      "WHERE xb_max_days__40 IS NOT NULL" in sql, True)

sql, meta = build_combine_sql(["max_days__1", "max_days__40"])
check("SHORTEST selected horizon wins -- it is what fires first",
      meta["backstop_rule"], "max_days__1")
check("...so a 1-session policy needs only 1 session",
      "WHERE xb_max_days__1 IS NOT NULL" in sql, True)

sql, meta = build_combine_sql(["max_days__5"])
check("a short horizon is its own backstop", meta["backstop_rule"], "max_days__5")
check("no default appended over it", meta["horizon_auto_added"], False)

print("\n=== 18. generated SQL compiles and every name resolves ===")
# No Postgres here by design. sqlite3 is stdlib and will parse the CTE, the
# CASE and the WHERE, and -- the part a regex could not do -- verify that
# every column the outer SELECT references is actually projected by the CTE.
# LEAST is spelled MIN in sqlite; that one substitution is the only dialect
# concession, and it does not change the shape being checked.
import sqlite3

_cols = ["ticker TEXT", "trade_date TEXT", "entry_anchor TEXT",
         "path_status TEXT"]
for _r in REGISTRY:
    _cols += [f"{_r.bar_col} INTEGER", f"{_r.ret_col} REAL"]
_db = sqlite3.connect(":memory:")
_db.execute(f"CREATE TABLE trade_paths ({', '.join(_cols)})")

_sel = [r.key for r in REGISTRY]
for _n, _rules in ((1, _sel[:1]), (2, _sel[:2]), (10, _sel[:10]),
                   (len(_sel), _sel)):
    _sql, _m = build_combine_sql(_rules)
    try:
        _db.execute("EXPLAIN " + _sql.replace("LEAST(", "MIN("))
        _ok, _err = True, ""
    except sqlite3.Error as e:
        _ok, _err = False, str(e)
    check(f"SQL compiles at {_n} rule(s){(' — ' + _err) if not _ok else ''}",
          _ok, True)
    check(f"...exactly one resolution filter at {_n}",
          _sql.count("WHERE"), 1)
    check(f"...LEAST covers every selected rule at {_n}",
          _sql[_sql.index("LEAST("):].split(")")[0].count(",") + 1,
          len(_m["rules"]))

print(f"\n{'=' * 60}")
print(f"PASSED {len(PASS)} / {len(PASS) + len(FAIL)}")
if FAIL:
    print("FAILED:")
    for f in FAIL:
        print("  -", f)
    sys.exit(1)
print("ALL GREEN")
