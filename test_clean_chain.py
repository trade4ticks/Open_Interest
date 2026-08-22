"""
test_clean_chain.py — unit tests for lib/clean_chain.py.

Expected values are worked out by hand from the inputs, not captured from a
previous run, so a regression cannot be frozen in as correct.

The gamma-leak test is the one that matters most: omitting `expiration` from
the group key produces plausible-looking numbers with nothing raised, so it is
only ever caught by asserting against hand-computed differences.

Run:  python test_clean_chain.py     (exit 1 on any failure)
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from lib.clean_chain import (
    FLAG_COLUMNS, clean_chain, clean_summary,
)

PASS, FAIL = [], []


def check(name, got, want, tol=1e-9):
    if isinstance(want, float) and isinstance(got, float):
        ok = (np.isnan(got) and np.isnan(want)) or abs(got - want) <= tol
    else:
        ok = got == want
    (PASS if ok else FAIL).append(name)
    print(f"  [{'ok  ' if ok else 'FAIL'}] {name:<56} got={got!r} want={want!r}")


def row(**kw):
    base = dict(
        ticker="AAPL", trade_date=pd.Timestamp("2026-06-01").date(),
        snapshot="0945", feature_date=pd.Timestamp("2026-06-02").date(),
        timestamp="2026-06-01T09:45:00",
        expiration=pd.Timestamp("2026-06-08").date(),
        strike=100.0, option_type="C", bid=1.0, ask=1.2,
        delta=0.5, theta=-0.1, vega=0.2, rho=0.05,
        epsilon=0.0, **{"lambda": 0.0},
        implied_vol=0.25, iv_error=0.01,
        underlying_timestamp="2026-06-01T09:45:00", underlying_price=100.0,
    )
    base.update(kw)
    return base


def frame(rows):
    return pd.DataFrame([row(**r) for r in rows])


# ---------------------------------------------------------------------------
print("\n=== 1. gamma does NOT leak across expirations ===")
# Two expirations with FULLY OVERLAPPING strike ladders, interleaved in row
# order so a wrong group key would silently mix them.
EXP_A = pd.Timestamp("2026-06-08").date()
EXP_B = pd.Timestamp("2026-06-15").date()
rows = []
for k, da, db in ((90.0, 0.80, 0.70), (100.0, 0.50, 0.55), (110.0, 0.20, 0.40)):
    rows.append(dict(expiration=EXP_A, strike=k, delta=da))
    rows.append(dict(expiration=EXP_B, strike=k, delta=db))
out = clean_chain(frame(rows))

a = out[out["expiration"] == EXP_A].sort_values("strike")
b = out[out["expiration"] == EXP_B].sort_values("strike")
# A: forward (0.50-0.80)/(100-90) = -0.030
#    central (0.20-0.80)/(110-90) = -0.030
#    backward(0.20-0.50)/(110-100)= -0.030
check("expA gamma @90  (forward)", float(a["gamma"].iloc[0]), -0.030)
check("expA gamma @100 (central)", float(a["gamma"].iloc[1]), -0.030)
check("expA gamma @110 (backward)", float(a["gamma"].iloc[2]), -0.030)
# B: forward (0.55-0.70)/10 = -0.015; central (0.40-0.70)/20 = -0.015;
#    backward (0.40-0.55)/10 = -0.015
check("expB gamma @90  (forward)", float(b["gamma"].iloc[0]), -0.015)
check("expB gamma @100 (central)", float(b["gamma"].iloc[1]), -0.015)
check("expB gamma @110 (backward)", float(b["gamma"].iloc[2]), -0.015)

# If expiration were dropped from the key, the six rows would form ONE ladder
# of duplicated strikes and every gamma would differ from the above.
mixed_central = (0.40 - 0.80) / (110.0 - 90.0)      # -0.020
check("leak value (-0.020) appears nowhere",
      bool((out["gamma"].round(6) == round(mixed_central, 6)).any()), False)

print("\n  ...and the same holds when option_type also overlaps")
rows2 = []
for k, dc, dp in ((90.0, 0.80, -0.20), (100.0, 0.50, -0.50), (110.0, 0.20, -0.80)):
    rows2.append(dict(expiration=EXP_A, strike=k, delta=dc, option_type="C"))
    rows2.append(dict(expiration=EXP_A, strike=k, delta=dp, option_type="P"))
o2 = clean_chain(frame(rows2))
c = o2[o2["option_type"] == "C"].sort_values("strike")
p = o2[o2["option_type"] == "P"].sort_values("strike")
check("calls central gamma", float(c["gamma"].iloc[1]), (0.20 - 0.80) / 20)
check("puts  central gamma", float(p["gamma"].iloc[1]), (-0.80 + 0.20) / 20)

# ---------------------------------------------------------------------------
print("\n=== 2. a single-strike group yields NaN gamma, not an exception ===")
o3 = clean_chain(frame([dict(strike=100.0, delta=0.5)]))
check("single row -> NaN gamma", bool(np.isnan(o3["gamma"].iloc[0])), True)
o4 = clean_chain(frame([
    dict(expiration=EXP_A, strike=100.0, delta=0.5),
    dict(expiration=EXP_B, strike=100.0, delta=0.4),   # each group has n=1
]))
check("two 1-row groups -> both NaN",
      bool(o4["gamma"].isna().all()), True)

# ---------------------------------------------------------------------------
print("\n=== 3. spread_pct is NaN (not inf) when mid_price <= 0 ===")
o5 = clean_chain(frame([
    dict(bid=0.0, ask=0.0, strike=100.0),     # mid == 0
    dict(bid=-1.0, ask=0.0, strike=105.0),    # mid  < 0
    dict(bid=1.0, ask=1.2, strike=110.0),     # mid  > 0, normal
]))
check("mid == 0 -> spread_pct NaN", bool(np.isnan(o5["spread_pct"].iloc[0])), True)
check("mid <  0 -> spread_pct NaN", bool(np.isnan(o5["spread_pct"].iloc[1])), True)
check("no inf anywhere in spread_pct",
      bool(np.isinf(o5["spread_pct"].to_numpy(dtype="float64")).any()), False)
check("mid >  0 -> spread_pct computed",
      float(o5["spread_pct"].iloc[2]), 0.2 / 1.1)

# ---------------------------------------------------------------------------
print("\n=== 4. idempotence: clean twice, frames equal ===")
src = frame([
    dict(strike=95.0, delta=0.7, bid=0.0, ask=0.0),
    dict(strike=100.0, delta=0.5, implied_vol=None),
    dict(strike=105.0, delta=None, iv_error=0.9),
    dict(strike=110.0, delta=0.1, expiration=EXP_B),
])
once = clean_chain(src)
twice = clean_chain(once)
try:
    pd.testing.assert_frame_equal(once, twice)
    check("clean_chain(clean_chain(x)) == clean_chain(x)", True, True)
except AssertionError as exc:
    check("clean_chain(clean_chain(x)) == clean_chain(x)", str(exc)[:70], True)
check("input frame not mutated", "gamma" in src.columns, False)
check("row count unchanged (drops nothing)", len(once), len(src))
check("index preserved", list(once.index), list(src.index))

# ---------------------------------------------------------------------------
print("\n=== 5. flag columns: plain bool dtype, zero NaNs ===")
messy = frame([
    dict(strike=100.0, bid=None, ask=None, delta=None, implied_vol=None,
         iv_error=None, underlying_price=None),
    dict(strike=0.0, bid=2.0, ask=1.0, delta=0.5),          # crossed + strike 0
    dict(strike=100.0, implied_vol=9.0, iv_error=0.9),
    dict(strike=100.0, implied_vol=0.01),
])
om = clean_chain(messy)
bad_dtype = [c for c in FLAG_COLUMNS + ["flag_any"] if om[c].dtype != np.dtype("bool")]
has_na = [c for c in FLAG_COLUMNS + ["flag_any"] if om[c].isna().any()]
check("all flags are numpy bool", bad_dtype, [])
check("no NaN in any flag column", has_na, [])
check("NA implied_vol -> iv_missing True, extremes False",
      (bool(om["flag_iv_missing"].iloc[0]),
       bool(om["flag_iv_extreme_high"].iloc[0]),
       bool(om["flag_iv_extreme_low"].iloc[0])), (True, False, False))
check("iv_extreme_low excludes iv == 0/NA but catches 0.01",
      bool(om["flag_iv_extreme_low"].iloc[3]), True)
check("crossed market detected", bool(om["flag_crossed_market"].iloc[1]), True)
check("~flag.astype(bool) keeps the NA-source row",
      int((~om["flag_iv_extreme_high"].astype(bool)).sum()), 3)

# ---------------------------------------------------------------------------
print("\n=== 6. computed fields ===")
o6 = clean_chain(frame([dict(strike=80.0, underlying_price=100.0,
                             bid=20.5, ask=21.5, option_type="C")]))
check("mid_price", float(o6["mid_price"].iloc[0]), 21.0)
check("spread", float(o6["spread"].iloc[0]), 1.0)
check("intrinsic (call, ITM)", float(o6["intrinsic"].iloc[0]), 20.0)
check("extrinsic", float(o6["extrinsic"].iloc[0]), 1.0)
check("moneyness is S/K (inverted convention)",
      float(o6["moneyness"].iloc[0]), 100.0 / 80.0)
check("log_moneyness is log(S/K)",
      float(o6["log_moneyness"].iloc[0]), float(np.log(100.0 / 80.0)))
check("dte = 2026-06-08 - 2026-06-01", float(o6["dte"].iloc[0]), 7.0)
# 2026-06-01 is a Monday; sessions 06-02..06-08 inclusive = 5 weekdays + Mon 06-08
check("bdte counts NYSE sessions, start-exclusive",
      float(o6["bdte"].iloc[0]), 5.0)
check("quote_time", str(o6["quote_time"].iloc[0]), "09:45:00")
o6p = clean_chain(frame([dict(strike=120.0, underlying_price=100.0,
                              bid=20.0, ask=21.0, option_type="P")]))
check("intrinsic (put, ITM)", float(o6p["intrinsic"].iloc[0]), 20.0)

# ---------------------------------------------------------------------------
print("\n=== 7. flag_stale_underlying: runs of 3+ distinct timestamps ===")
def at(ts, px, strike=100.0):
    return dict(timestamp=f"2026-06-01T{ts}:00", underlying_price=px,
                strike=strike)

# 09:35,09:40,09:45 frozen at 100 (run of 3) -> flagged.
# 09:50,09:55 frozen at 101 (run of 2)       -> not flagged.
o7 = clean_chain(frame([
    at("09:35", 100.0), at("09:35", 100.0, 105.0),   # two rows, one timestamp
    at("09:40", 100.0), at("09:45", 100.0),
    at("09:50", 101.0), at("09:55", 101.0),
]))
flagged = o7.groupby(o7["timestamp"])["flag_stale_underlying"].first()
check("run of 3 flagged (09:35)", bool(flagged.iloc[0]), True)
check("run of 3 flagged (09:40)", bool(flagged.iloc[1]), True)
check("run of 3 flagged (09:45)", bool(flagged.iloc[2]), True)
check("run of 2 NOT flagged (09:50)", bool(flagged.iloc[3]), False)
check("run of 2 NOT flagged (09:55)", bool(flagged.iloc[4]), False)
check("both rows at a stale timestamp flagged",
      int(o7["flag_stale_underlying"].sum()), 4)
# A wide chain at ONE timestamp must not read as a run.
o7b = clean_chain(frame([at("09:35", 100.0, k) for k in (90.0, 95.0, 100.0, 105.0)]))
check("4 rows at one timestamp is not a run",
      bool(o7b["flag_stale_underlying"].any()), False)

# ---------------------------------------------------------------------------
print("\n=== 8. clean_summary ===")
summ = clean_summary(frame([
    dict(strike=100.0, delta=0.05, expiration=EXP_A),
    dict(strike=105.0, delta=0.30, expiration=EXP_A),
    dict(strike=110.0, delta=0.75, expiration=EXP_B),
    dict(strike=115.0, delta=None, expiration=EXP_B),
]))
check("n_rows sums to len(df)", int(summ["n_rows"].sum()), 4)
check("has a rate column per flag",
      all(f"rate_{c[len('flag_'):]}" in summ.columns
          for c in FLAG_COLUMNS + ["flag_any"]), True)
check("keyed by ticker x dte x delta",
      list(summ.columns[:3]), ["ticker", "dte_bucket", "delta_bucket"])
check("NA delta lands in a 'missing' bucket, not dropped",
      bool((summ["delta_bucket"] == "missing").any()), True)
check("rates are fractions in [0, 1]",
      bool(summ["rate_any"].between(0, 1).all()), True)

print("\n" + "=" * 60)
print(f"PASSED {len(PASS)} / {len(PASS) + len(FAIL)}")
if FAIL:
    for f in FAIL:
        print("  -", f)
    sys.exit(1)
print("ALL GREEN")
