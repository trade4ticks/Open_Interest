"""
test_equity_metrics.py — stage 4, with no database.

Every expected value here is hand-computed and written out as arithmetic, not
captured from a run. A snapshot test on 600 generated columns would lock in
whatever the code did the first time, including the sign errors.

The tests that matter most:

  * OHLC windows end at T-1. A close on T that the snapshot could not have
    known is planted in the fixture; if it leaks into log_ret_d or rv_7d, vrp
    carries a full session of lookahead and a variance-premium backtest looks
    excellent for the wrong reason.
  * The zero-cost solve returns NULL rather than extrapolating past the
    5-delta node.
  * compute_metrics emits EXACTLY the registry's column set. A typo'd key is
    otherwise a permanently NULL column, not an error.
  * A calendar-arbitrage forward variance is NULL, not a clamp to zero.

Run:  python test_equity_metrics.py     (exit 1 on any failure)
"""
from __future__ import annotations

import math
import sys
from datetime import date, datetime, timedelta

import lib.metrics_compute as C
import lib.metrics_config as M
from lib.metrics_store import zscore_row

PASS, FAIL = [], []
TOL = 1e-9


def check(name, got, want):
    ok = got == want
    (PASS if ok else FAIL).append(name)
    print(f"  [{'ok  ' if ok else 'FAIL'}] {name:<57} got={got!r} want={want!r}")


def close(name, got, want, tol=TOL):
    ok = (got is not None and want is not None
          and abs(got - want) <= tol * max(1.0, abs(want)))
    (PASS if ok else FAIL).append(name)
    g = f"{got:.10g}" if isinstance(got, float) else repr(got)
    print(f"  [{'ok  ' if ok else 'FAIL'}] {name:<57} got={g} want={want:.10g}")


# =============================================================================
# Fixture: one snapshot with a hand-chosen smile
# =============================================================================
SPOT = 100.0
# (tenor, delta label) -> (iv, strike)
SMILE = {
    (30, "10p"): (0.40, 80.0), (30, "25p"): (0.32, 90.0),
    (30, "atm"): (0.28, 100.0), (30, "25c"): (0.26, 110.0),
    (30, "10c"): (0.27, 120.0),
    (7, "10p"): (0.52, 88.0), (7, "25p"): (0.42, 94.0),
    (7, "atm"): (0.35, 100.0), (7, "25c"): (0.33, 106.0),
    (7, "10c"): (0.34, 112.0),
}
# put_delta -> (price, strike) at 30d
# Strikes at nodes 10 and 25 MUST match SMILE's — the same node cannot sit at
# two strikes, and .update() below would silently win.
PUT_LADDER = {5: (0.50, 76.0), 10: (1.00, 80.0), 15: (1.80, 84.0),
              20: (2.80, 87.0), 25: (4.00, 90.0)}
ATM_PUT_PRICE_30 = 5.00
CALL_25_PRICE_30 = 3.00

SQRT30 = math.sqrt(30.0 / 365.0)


def build_snap():
    nodes, atm = {}, {}
    for t in (7, 30):
        nodes[t] = {}
        for lbl, node in M.DELTA_NODE.items():
            iv, k = SMILE[(t, lbl)]
            nodes[t][node] = {"iv": iv, "strike": k, "price": None,
                              "call_price": None, "extrapolated": False,
                              "captured_at": datetime(2026, 6, 1, 15, 47),
                              "source": "live"}
        nodes[t][50] = {"iv": SMILE[(t, "atm")][0], "strike": 100.0,
                        "price": None, "call_price": None,
                        "extrapolated": (t == 7),   # 7d ATM read is fabricated
                        "captured_at": datetime(2026, 6, 1, 15, 47),
                        "source": "live"}
        atm[t] = {"atm_iv": SMILE[(t, "atm")][0], "atm_strike": 100.0,
                  "atm_forward": 100.5, "underlying_price": SPOT,
                  "price": ATM_PUT_PRICE_30 if t == 30 else 3.0,
                  "captured_at": datetime(2026, 6, 1, 15, 47),
                  "source": "live"}
    for d, (px, k) in PUT_LADDER.items():
        nodes[30].setdefault(d, {"iv": None, "extrapolated": False,
                                 "captured_at": None, "source": None})
        nodes[30][d].update({"price": px, "strike": k, "call_price": None})
    nodes[30][75] = {"iv": SMILE[(30, "25c")][0], "strike": 110.0,
                     "price": 14.0, "call_price": CALL_25_PRICE_30,
                     "extrapolated": False, "captured_at": None,
                     "source": "live"}
    diag = [
        {"forward_method": "pcp", "n_strikes_clean": 100.0,
         "domain_reach": 4.0, "calendar_arb": False, "butterfly_arb": True,
         "skipped": False},
        {"forward_method": "spot_fallback", "n_strikes_clean": 50.0,
         "domain_reach": 2.0, "calendar_arb": True, "butterfly_arb": False,
         "skipped": False},
        {"forward_method": None, "n_strikes_clean": None,
         "domain_reach": None, "calendar_arb": False, "butterfly_arb": False,
         "skipped": True},
    ]
    return {"nodes": nodes, "atm": atm, "diag": diag}


SNAP = build_snap()
_, IV, STRIKE, EXTRAP = C._level(SNAP)

print("\n=== 0. the fixture is self-consistent ===")
for _lbl, _node in M.DELTA_NODE.items():
    if _node in PUT_LADDER:
        check(f"ladder and smile agree on the {_lbl} strike",
              PUT_LADDER[_node][1], SMILE[(30, _lbl)][1])
check("ladder strikes rise with put_delta",
      [PUT_LADDER[d][1] for d in sorted(PUT_LADDER)]
      == sorted(PUT_LADDER[d][1] for d in PUT_LADDER), True)
check("ladder prices rise with put_delta (further ITM costs more)",
      [PUT_LADDER[d][0] for d in sorted(PUT_LADDER)]
      == sorted(PUT_LADDER[d][0] for d in PUT_LADDER), True)


print("\n=== 1. skew slopes: sqrt(dte/365) * dIV / dln(K) ===")
sk = C._skew(IV, STRIKE)
close("skew_30d_10p_25p", sk["skew_30d_10p_25p"],
      SQRT30 * (0.32 - 0.40) / math.log(90.0 / 80.0))
close("skew_30d_atm_10c", sk["skew_30d_atm_10c"],
      SQRT30 * (0.27 - 0.28) / math.log(120.0 / 100.0))
close("skew_30d_25p_25c (the wide pair)", sk["skew_30d_25p_25c"],
      SQRT30 * (0.26 - 0.32) / math.log(110.0 / 90.0))
check("put skew is negative (IV falls as strike rises)",
      sk["skew_30d_10p_25p"] < 0, True)
check("a missing tenor gives NULL, not 0.0", sk["skew_60d_10p_25p"], None)
# Uses the ACTUAL strikes: same IVs on a different ladder must differ.
alt = C._skew({(30, "10p"): 0.40, (30, "25p"): 0.32},
              {(30, "10p"): 70.0, (30, "25p"): 90.0})
check("slope tracks strikes, not delta labels",
      abs(alt["skew_30d_10p_25p"] - sk["skew_30d_10p_25p"]) > 1e-6, True)
# Degenerate denominator.
same = C._skew({(30, "10p"): 0.40, (30, "25p"): 0.32},
               {(30, "10p"): 90.0, (30, "25p"): 90.0})
check("identical strikes -> NULL, not a divide-by-zero",
      same["skew_30d_10p_25p"], None)

print("\n=== 2. convexity: delta-interpolated wings minus the centre ===")
cx = C._convexity(IV)
# 10p=10, 25p=25, atm=50 -> w_left=(50-25)/40=0.625, w_right=(25-10)/40=0.375
close("convex_30d_10p_25p_atm", cx["convex_30d_10p_25p_atm"],
      0.625 * 0.40 + 0.375 * 0.28 - 0.32)
# 25p=25, atm=50, 25c=75 -> symmetric, both weights 0.5
close("convex_30d_25p_atm_25c (symmetric)", cx["convex_30d_25p_atm_25c"],
      0.5 * 0.32 + 0.5 * 0.26 - 0.28)
close("convex_30d_10p_atm_10c", cx["convex_30d_10p_atm_10c"],
      0.5 * 0.40 + 0.5 * 0.27 - 0.28)
check("weights sum to 1 for every triple",
      all(abs(((M.DELTA_COORD[r] - M.DELTA_COORD[c])
               + (M.DELTA_COORD[c] - M.DELTA_COORD[l]))
              / (M.DELTA_COORD[r] - M.DELTA_COORD[l]) - 1.0) < 1e-12
          for l, c, r in M.CONVEX_TRIPLES), True)

print("\n=== 3. risk reversal: call minus put ===")
rr = C._risk_reversal(IV)
close("rr_30d_25", rr["rr_30d_25"], 0.26 - 0.32)
close("rr_30d_10", rr["rr_30d_10"], 0.27 - 0.40)
check("negative on an equity skew", rr["rr_30d_25"] < 0 and rr["rr_30d_10"] < 0,
      True)

print("\n=== 4. term structure ===")
tm = C._term(IV)
close("term_ratio_7d_30d", tm["term_ratio_7d_30d"], 0.35 / 0.28)
t7, t30 = 7.0 / 365.0, 30.0 / 365.0
close("term_slope_7d_30d_atm (forward vol)", tm["term_slope_7d_30d_atm"],
      math.sqrt((0.28 ** 2 * t30 - 0.35 ** 2 * t7) / (t30 - t7)))
check("front-loaded term structure -> ratio > 1",
      tm["term_ratio_7d_30d"] > 1.0, True)
# Calendar arbitrage: total variance FALLS with maturity.
arb = C._term({(7, "atm"): 0.90, (30, "atm"): 0.28})
check("negative forward variance -> NULL, not 0.0 and not a crash",
      arb["term_slope_7d_30d_atm"], None)
check("  (and it really was negative)",
      (0.28 ** 2 * t30 - 0.90 ** 2 * t7) < 0, True)

print("\n=== 5. structure prices ===")
st = C._structure(SNAP, IV, SPOT)
close("ratio_price_30d = 2*p(10p) - p(25p)", st["ratio_price_30d"],
      2 * 1.00 - 4.00)
close("straddle_price_30d = 2*atm put", st["straddle_price_30d"],
      2 * ATM_PUT_PRICE_30)
close("rr_price_30d = call(pd75) - put(pd25)", st["rr_price_30d"],
      CALL_25_PRICE_30 - 4.00)
close("wing_cost_10p_5p_30d", st["wing_cost_10p_5p_30d"], 1.00 - 0.50)
# Delta-neutral short = half of 25 = 12.5, interpolated between 10 and 15.
close("cost_at_delta_neutral_30d (p@12.5 interpolated)",
      st["cost_at_delta_neutral_30d"],
      2 * (1.00 + 0.5 * (1.80 - 1.00)) - 4.00)
# Zero cost: target = 4.00/2 = 2.00, between pd15 (1.80) and pd20 (2.80).
frac = (2.00 - 1.80) / (2.80 - 1.80)
close("zc_short_delta_30d", st["zc_short_delta_30d"], 15 + frac * 5)
k_short = 84.0 + frac * (87.0 - 84.0)
close("zc_width_sigma_30d", st["zc_width_sigma_30d"],
      math.log(SPOT / k_short) / (0.28 * SQRT30))
check("zc_width_sigma is POSITIVE", st["zc_width_sigma_30d"] > 0, True)
# The sign exists to make the column sortable: a scanner ordering DESC on
# "width" must get the widest first. Negating it would rank narrowest-first.
wide = C._sigma_from_spot(70.0, SPOT, 0.28, 30)
narrow = C._sigma_from_spot(95.0, SPOT, 0.28, 30)
check("a further-out short strike scores HIGHER", wide > narrow, True)
check("  (both positive, so DESC ranks widest first)",
      wide > 0 and narrow > 0, True)
close("magnitude is unchanged, only the sign", wide,
      -math.log(70.0 / SPOT) / (0.28 * SQRT30))
check("a strike at spot is 0 sigma wide",
      C._sigma_from_spot(SPOT, SPOT, 0.28, 30), 0.0)

print("\n=== 5b. long_sigma: the tent's OTHER leg, same axis ===")
close("long_sigma_30d = ln(spot / K_25p) / (atm_iv * sqrt(dte/365))",
      st["long_sigma_30d"], math.log(SPOT / 90.0) / (0.28 * SQRT30))
check("POSITIVE, same convention as zc_width_sigma",
      st["long_sigma_30d"] > 0, True)
check("the short leg sits FURTHER out than the long leg",
      st["zc_width_sigma_30d"] > st["long_sigma_30d"], True)
close("their difference is the tent's width",
      st["zc_width_sigma_30d"] - st["long_sigma_30d"],
      (math.log(SPOT / k_short) - math.log(SPOT / 90.0)) / (0.28 * SQRT30))

# THE TRAP. equity_surface.log_moneyness is ln(K / forward), not ln(spot / K).
# The fixture's forward (100.5) differs from spot (100.0) precisely so a
# forward-referenced implementation cannot pass this.
fwd_basis = math.log(100.5 / 90.0) / (0.28 * SQRT30)
check("NOT the forward-referenced value",
      abs(st["long_sigma_30d"] - fwd_basis) > 1e-6, True)
check("  and the basis error would be material (~0.06 sigma here, against a "
      "0.04 daily stdev)", abs(st["long_sigma_30d"] - fwd_basis) > 0.04, True)

# Convention anchor. Under flat vol with F = spot, the 25-delta put sits at
# k = v^2/2 - 0.6745v, so ln(spot/K)/v collapses to exactly 0.6745 - v/2.
# A sign flip, a forward basis or a wrong delta would all miss this.
_v = 0.25 * math.sqrt(30.0 / 365.0)
_k25 = _v * _v / 2.0 - 0.6744897501960817 * _v
close("flat-vol anchor: long_sigma == 0.6745 - v/2",
      C._sigma_from_spot(SPOT * math.exp(_k25), SPOT, 0.25, 30),
      0.6744897501960817 - _v / 2.0)
check("  which lands near the catalog's 0.67 anchor, pulled slightly in",
      0.60 < 0.6744897501960817 - _v / 2.0 < 0.68, True)

# Missing wing node -> NULL, not a fabricated distance.
_bare = {"nodes": {30: {50: {"iv": 0.28, "strike": 100.0, "price": 5.0,
                             "call_price": None, "extrapolated": False}}},
         "atm": {30: {"atm_iv": 0.28, "price": 5.0}}, "diag": []}
check("no 25-delta node -> long_sigma NULL",
      C._structure(_bare, {(30, "atm"): 0.28}, SPOT)["long_sigma_30d"], None)
check("no spot -> long_sigma NULL",
      C._structure(SNAP, IV, None)["long_sigma_30d"], None)
check("no ATM IV -> long_sigma NULL",
      C._structure(SNAP, {}, SPOT)["long_sigma_30d"], None)
check("zero-cost sits further out than delta-neutral on this skew",
      st["zc_short_delta_30d"] > 12.5, True)

print("\n=== 6. zero-cost solve refuses to extrapolate ===")
d, k = C._zero_cost_short({dd: {"price": p, "strike": kk}
                           for dd, (p, kk) in PUT_LADDER.items()}, 4.00)
close("  solves inside the ladder", d, 16.0)
# Target below the 5-delta node: the surface does not reach that far out.
thin = {dd: {"price": p, "strike": kk} for dd, (p, kk) in PUT_LADDER.items()}
d2, k2 = C._zero_cost_short(thin, 0.90)      # target 0.45 < p(5d) = 0.50
check("target beyond the 5-delta node -> NULL, not an invented strike",
      (d2, k2), (None, None))
check("no 25-delta long leg -> NULL", C._zero_cost_short(thin, None),
      (None, None))
check("interp refuses to extrapolate past the ladder",
      C._interp_node(thin, 2.0, "price"), None)
close("interp is exact on a node", C._interp_node(thin, 15, "price"), 1.80)
close("interp is linear between nodes",
      C._interp_node(thin, 12.5, "price"), 1.40)


print("\n=== 7. realized vol: the window ENDS AT T-1 ===")
T = date(2026, 6, 15)
# Closes ending the day before T, then a planted 100% move ON T that no
# 13:45 snapshot could know about.
hist = [100.0, 101.0, 99.0, 102.0, 103.0, 101.5, 104.0]
bars = []
for i, c in enumerate(hist):
    d = T - timedelta(days=len(hist) - i)
    bars.append({"d": d, "o": c * 0.995, "h": c * 1.01, "l": c * 0.99, "c": c})
bars.append({"d": T, "o": 104.0, "h": 210.0, "l": 104.0, "c": 200.0})

rvrow = C._realized(bars, T, {(7, "atm"): 0.35, (14, "atm"): 0.32,
                              (21, "atm"): 0.30, (30, "atm"): 0.28,
                              (60, "atm"): 0.29, (90, "atm"): 0.30})
close("log_ret_d uses close[T-1]/close[T-2]", rvrow["log_ret_d"],
      math.log(104.0 / 101.5))
check("  and NOT the planted close on T",
      abs(rvrow["log_ret_d"] - math.log(200.0 / 104.0)) > 0.1, True)
close("log_ret_1w over 5 sessions", rvrow["log_ret_1w"],
      math.log(104.0 / 101.0))

# The mapping is the thing most likely to be got wrong by a later edit, so it
# is asserted against the documented table rather than against itself.
check("RV_WINDOWS covers every tenor", [t for _, _, t in M.RV_WINDOWS],
      M.TENORS)
check("calendar tenor -> trading-day window mapping",
      [(t, n) for _, n, t in M.RV_WINDOWS],
      [(7, 5), (14, 10), (21, 15), (30, 21), (60, 42), (90, 63)])
check("rv_14d is named for the TENOR, not its 10-day window",
      dict((t, lbl) for lbl, _, t in M.RV_WINDOWS)[14], "14d")

rets = [math.log(b / a) for a, b in zip(hist[:-1], hist[1:])]
m = sum(rets[-5:]) / 5
sd = math.sqrt(sum((r - m) ** 2 for r in rets[-5:]) / 4)
close("rv_7d = stdev(5 returns, ddof=1) * sqrt(252)", rvrow["rv_7d"],
      sd * math.sqrt(252))
check("rv_7d excludes T's 92% move", rvrow["rv_7d"] < 1.0, True)

pk = [math.log(b["h"] / b["l"]) ** 2 for b in bars[-6:-1]]
close("rv_park_7d (Parkinson)", rvrow["rv_park_7d"],
      math.sqrt(sum(pk) / (4 * math.log(2) * 5)) * math.sqrt(252))
gk = [0.5 * math.log(b["h"] / b["l"]) ** 2
      - (2 * math.log(2) - 1) * math.log(b["c"] / b["o"]) ** 2
      for b in bars[-6:-1]]
close("rv_gk_7d (Garman-Klass)", rvrow["rv_gk_7d"],
      math.sqrt(sum(gk) / 5) * math.sqrt(252))
check("Parkinson is not equal to close-close (it is a different estimator)",
      abs(rvrow["rv_park_7d"] - rvrow["rv_7d"]) > 1e-6, True)

close("vrp_7d = iv_7d_atm - rv_7d", rvrow["vrp_7d"], 0.35 - rvrow["rv_7d"])
close("vrp_ratio_7d", rvrow["vrp_ratio_7d"], 0.35 / rvrow["rv_7d"])

# 7 sessions of history: 6 returns. The 5td window fills, the 10td one cannot.
check("6 returns is short of the 10td window -> NULL", rvrow["rv_14d"], None)
check("  and its vrp too", rvrow["vrp_14d"], None)
check("  and the 63td window at the long end", rvrow["rv_90d"], None)
check("  and its vrp too", rvrow["vrp_90d"], None)

# THE POINT OF THE WHOLE FAMILY: each VRP pairs its OWN tenor's implied with
# its OWN matched window, never a shared one. Needs 64 sessions so all six
# windows fill, and a distinct IV per tenor so a VRP built off the wrong
# implied cannot coincidentally agree.
_IV6 = {7: 0.35, 14: 0.32, 21: 0.30, 30: 0.28, 60: 0.29, 90: 0.31}
_long = []
for i in range(65):
    _c = 100.0 * (1.0 + 0.004 * ((i % 7) - 3))      # deterministic wiggle
    _long.append({"d": T - timedelta(days=65 - i), "o": _c * 0.998,
                  "h": _c * 1.006, "l": _c * 0.994, "c": _c})
six = C._realized(_long, T, {(t, "atm"): v for t, v in _IV6.items()})

for _lbl, _n, _t in M.RV_WINDOWS:
    check(f"rv_{_lbl} populated off 64 returns", six[f"rv_{_lbl}"] is not None,
          True)
    close(f"vrp_{_lbl} = iv_{_t}d_atm - rv_{_lbl}",
          six[f"vrp_{_lbl}"], _IV6[_t] - six[f"rv_{_lbl}"])
    close(f"vrp_ratio_{_lbl} = iv_{_t}d_atm / rv_{_lbl}",
          six[f"vrp_ratio_{_lbl}"], _IV6[_t] / six[f"rv_{_lbl}"])

check("all six vrp values are distinct — none is a copy of another",
      len({round(six[f"vrp_{lbl}"], 12) for lbl, _, _ in M.RV_WINDOWS}), 6)
check("swapping in the 30d implied would change vrp_7d",
      abs(six["vrp_7d"] - (_IV6[30] - six["rv_7d"])) > 1e-9, True)

flat = [{"d": T - timedelta(days=30 - i), "o": 100.0, "h": 100.0,
         "l": 100.0, "c": 100.0} for i in range(30)]
z = C._realized(flat, T, {(7, "atm"): 0.35, (14, "atm"): 0.32,
                          (21, "atm"): 0.30, (30, "atm"): 0.28,
                          (60, "atm"): 0.29, (90, "atm"): 0.30})
check("rv -> 0 gives vrp_ratio NULL, not inf", z["vrp_ratio_7d"], None)
check("  (rv really is zero)", z["rv_7d"], 0.0)
check("  at every tenor, not just the short one", z["vrp_ratio_21d"], None)
check("no down days -> downside_semivol NULL", z["downside_semivol_1m"], None)


print("\n=== 8. spot-vol regression, off the daily baseline ===")
# iv moves at exactly -2x the underlying log return, so beta = -2, R2 = 1.
pattern = [0.01, -0.02, 0.015, -0.005]
ivh, s, ivv = [], 100.0, 0.30
d0 = date(2026, 1, 5)
for i in range(30):
    ivh.append({"d": d0 + timedelta(days=i), "iv": ivv, "s": s})
    r = pattern[i % 4]
    s *= math.exp(r)
    ivv += -2.0 * r
sv = C._spot_vol(ivh, ivh[-1]["d"], M.BASELINE_SNAPSHOT)
close("spotvol_beta_1m recovers the planted -2.0", sv["spotvol_beta_1m"], -2.0,
      tol=1e-6)
close("spotvol_r2_1m = 1 on an exact relation", sv["spotvol_r2_1m"], 1.0,
      tol=1e-9)
check("vov_30d_1m computed with 21+ diffs", sv["vov_30d_1m"] is not None, True)
d_iv = [-2.0 * pattern[i % 4] for i in range(29)]
mm = sum(d_iv[-21:]) / 21
close("vov_30d_1m = stdev(d iv, 21) * sqrt(252)", sv["vov_30d_1m"],
      math.sqrt(sum((x - mm) ** 2 for x in d_iv[-21:]) / 20) * math.sqrt(252))
short = C._spot_vol(ivh[:4], ivh[3]["d"], M.BASELINE_SNAPSHOT)
check("too little history -> beta NULL", short["spotvol_beta_1m"], None)
check("  and vov NULL", short["vov_30d_1m"], None)
# A hole in the snapshot history is not a one-day move.
gapped = ivh[:10] + [{"d": ivh[9]["d"] + timedelta(days=40),
                      "iv": 9.9, "s": 500.0}] + ivh[10:]
gv = C._spot_vol(gapped, gapped[-1]["d"], M.BASELINE_SNAPSHOT)
check("a 40-day gap does not enter the regression as a daily move",
      abs(gv["spotvol_beta_1m"] - (-2.0)) < 1e-6, True)

# THE AS-OF RULE. At the baseline bucket the row being computed IS the day's
# daily observation and belongs inside its own window; at any other bucket that
# observation has not happened yet, so the window stops at T-1. Without the
# second half a REBUILD would hand a 09:45 row the 15:45 value from six hours
# in its future, and backfilled rows would beat live ones on the same key.
spiked = ivh + [{"d": ivh[-1]["d"] + timedelta(days=1), "iv": 9.9, "s": 500.0}]
TDx = spiked[-1]["d"]
at_close = C._spot_vol(spiked, TDx, M.BASELINE_SNAPSHOT)
at_noon = C._spot_vol(spiked, TDx, "1215")
check("the baseline bucket INCLUDES its own day's observation",
      abs(at_close["spotvol_beta_1m"] - (-2.0)) > 1e-6, True)
close("an intraday bucket stops at T-1, so it is unmoved by it",
      at_noon["spotvol_beta_1m"], -2.0, tol=1e-6)
check("  which is the same value the previous close carried",
      abs(at_noon["spotvol_beta_1m"]
          - C._spot_vol(ivh, ivh[-1]["d"], M.BASELINE_SNAPSHOT)
          ["spotvol_beta_1m"]) < 1e-12, True)


print("\n=== 9. quality ===")
q = C._quality(SNAP, EXTRAP)
check("n_expiries_fitted", q["n_expiries_fitted"], 2)
check("n_expiries_skipped", q["n_expiries_skipped"], 1)
close("pct_spot_fallback (1 of 2 FITTED, skipped excluded)",
      q["pct_spot_fallback"], 0.5)
check("n_butterfly_arb", q["n_butterfly_arb"], 1)
check("n_calendar_arb", q["n_calendar_arb"], 1)
close("median_domain_reach over fitted only", q["median_domain_reach"], 3.0)
check("source carried", q["source"], "live")
check("captured_at carried", q["captured_at"], datetime(2026, 6, 1, 15, 47))
# The 7d ATM proxy node was flagged; the 30d one was not.
check("extrap_atm_7d proxied from the put_delta 50 node",
      q["extrap_atm_7d"], True)
check("extrap_atm_30d", q["extrap_atm_30d"], False)
check("extrap_10p_30d", q["extrap_10p_30d"], False)
# 7d and 30d are both <= 30: 10 nodes, 1 of them extrapolated.
close("extrap_rate_short = 1/10", q["extrap_rate_short"], 0.1)
check("absent tenors are excluded from the rate, not counted as clean",
      q["extrap_10p_60d"], None)
empty = C._quality({"nodes": {}, "atm": {}, "diag": []}, {})
check("no diagnostics -> pct_spot_fallback NULL, not 0.0",
      empty["pct_spot_fallback"], None)
check("  and extrap_rate_short NULL", empty["extrap_rate_short"], None)


print("\n=== 10. calendar ===")
check("third Friday, June 2026", C.third_friday(2026, 6), date(2026, 6, 19))
check("third Friday, Jan 2027", C.third_friday(2027, 1), date(2027, 1, 15))
c1 = C._calendar(date(2026, 6, 1))
check("day_of_week: 2026-06-01 is a Monday", c1["day_of_week"], 1)
check("days_to_monthly_opex", c1["days_to_monthly_opex"], 18)
check("0 on opex day itself",
      C._calendar(date(2026, 6, 19))["days_to_monthly_opex"], 0)
check("rolls to next month's opex once passed",
      C._calendar(date(2026, 6, 20))["days_to_monthly_opex"],
      (date(2026, 7, 17) - date(2026, 6, 20)).days)
check("December rolls the YEAR, not to month 13",
      C._calendar(date(2026, 12, 25))["days_to_monthly_opex"],
      (date(2027, 1, 15) - date(2026, 12, 25)).days)
check("days_to_earnings: no dates (a fund) is NULL, not 0",
      c1["days_to_earnings"], None)
_earn = [date(2026, 4, 28), date(2026, 7, 28), date(2026, 10, 27)]
check("days_to_earnings counts to the next date on or after trade_date",
      C._calendar(date(2026, 6, 1), _earn)["days_to_earnings"],
      (date(2026, 7, 28) - date(2026, 6, 1)).days)
check("0 on the earnings date itself",
      C._calendar(date(2026, 7, 28), _earn)["days_to_earnings"], 0)
check("calendar days, not trading days (spans a weekend)",
      C._calendar(date(2026, 7, 24), _earn)["days_to_earnings"], 4)
check("past the last known date is NULL, not negative",
      C._calendar(date(2026, 11, 1), _earn)["days_to_earnings"], None)
check("a date before the first known one still counts forward",
      C._calendar(date(2026, 1, 1), _earn)["days_to_earnings"],
      (date(2026, 4, 28) - date(2026, 1, 1)).days)


print("\n=== 11. z-scores ===")
# The window and the value being scored are SEPARATE arguments: a 10:15 reading
# is measured against 15:45 closes and is not one of them. Self-inclusion is
# gone, so the window below is the PRIOR baseline observations only.
base = M.Z_BASE_COLUMNS[0].name
need = max(M.Z_MIN_OBS[63], M.BASELINE_MIN_N)
prior = [float(v) for v in range(need)]
today = 100.0
w = {(base, 63): prior, (base, 252): prior}
zr = zscore_row(w, {base: today}, "SPY", date(2026, 6, 1), "1545")

pm = sum(prior) / len(prior)
psd = math.sqrt(sum((x - pm) ** 2 for x in prior) / (len(prior) - 1))
close(f"{base}_z_63 scored against the PRIOR window, today excluded",
      zr[f"{base}_z_63"], (today - pm) / psd)
check(f"z_252 NULL — {need} obs is below its "
      f"{max(M.Z_MIN_OBS[252], M.BASELINE_MIN_N)} minimum",
      zr[f"{base}_z_252"], None)
check("the 63 minimum is the larger of Z_MIN_OBS and BASELINE_MIN_N",
      need, max(M.Z_MIN_OBS[63], M.BASELINE_MIN_N))

# Self-inclusion inflates sigma and pulls the score toward zero. Proving the
# two differ in the expected direction is the point of the redefinition.
incl = prior + [today]
im = sum(incl) / len(incl)
isd = math.sqrt(sum((x - im) ** 2 for x in incl) / (len(incl) - 1))
check("excluding today gives a LARGER |z| than including it would",
      abs(zr[f"{base}_z_63"]) > abs((today - im) / isd), True)

zs = zscore_row({(base, 63): prior[:need - 1]}, {base: today}, "SPY",
                date(2026, 6, 1), "1545")
check("one observation short of the minimum -> NULL", zs[f"{base}_z_63"], None)
zc = zscore_row({(base, 63): [5.0] * need}, {base: 5.0}, "SPY",
                date(2026, 6, 1), "1545")
check("constant window -> NULL, not 0/0", zc[f"{base}_z_63"], None)
zn = zscore_row(w, {base: None}, "SPY", date(2026, 6, 1), "1545")
check("no value to score -> z NULL", zn[f"{base}_z_63"], None)
zg = zscore_row({(base, 63): [None] * 10 + prior[:need - 1]}, {base: today},
                "SPY", date(2026, 6, 1), "1545")
check("gaps counted as absent, not as zeros", zg[f"{base}_z_63"], None)
check("z row carries the join key",
      (zr["ticker"], zr["trade_date"], zr["snapshot"]),
      ("SPY", date(2026, 6, 1), "1545"))
check("the baseline bucket is the single source", M.BASELINE_SNAPSHOT, "1545")
check("every vrp column is z-eligible",
      all(c.z_eligible for c in M.BASE_COLUMNS if c.family == "vrp"), True)


# =============================================================================
print("\n=== 12. registry / compute agreement (the drift check) ===")


class FakeCursor:
    """Dispatches on the SQL text. Ordered most-specific first."""

    def __init__(self, data):
        self.data, self._rows = data, []

    def execute(self, sql, params=None):
        if "equity_surface_diagnostics" in sql:
            self._rows = self.data["diag"]
        elif "underlying_ohlc" in sql:
            self._rows = self.data["ohlc"]
        elif "earnings_calendar" in sql:
            self._rows = [(d,) for d in self.data.get("earnings", [])]
        elif "dte = 30" in sql:
            # Honours the snapshot argument on purpose. If compute ever goes
            # back to asking for the row's own bucket, the thin history planted
            # under a non-baseline key makes every spot-vol column NULL and
            # section 12b fails — which is exactly how the bug reached
            # production unnoticed.
            snap = params[1] if params and len(params) > 1 else None
            hist = self.data["ivhist"]
            self._rows = hist if isinstance(hist, list) else hist.get(snap, [])
        elif "FROM equity_surface " in sql:
            self._rows = self.data["surface"]
        elif "FROM equity_atm" in sql:
            self._rows = self.data["atm"]
        else:
            raise AssertionError(f"unrouted query: {sql[:90]}")

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, data):
        self.data = data

    def cursor(self):
        return FakeCursor(self.data)


cap = datetime(2026, 6, 1, 15, 47)
data = {
    "surface": [(30, 10, 0.40, 80.0, 1.00, None, False, cap, "live"),
                (30, 25, 0.32, 90.0, 4.00, None, False, cap, "live"),
                (30, 50, 0.28, 100.0, 5.00, None, False, cap, "live"),
                (30, 75, 0.26, 110.0, 14.0, 3.00, False, cap, "live"),
                (30, 90, 0.27, 120.0, 24.0, 0.50, True, cap, "live")],
    "atm": [(30, 0.28, 100.0, 100.5, 100.0, 5.00, cap, "live")],
    "diag": [("pcp", 100.0, 4.0, False, False, False)],
    "ohlc": [(date(2026, 5, 1) + timedelta(days=i), 100.0, 101.0, 99.0,
              100.0 + i) for i in range(40)],
    "ivhist": [(date(2026, 5, 1) + timedelta(days=i), 0.28, 100.0)
               for i in range(40)],
}
row = C.compute_metrics(FakeConn(data), "SPY", date(2026, 6, 15), "1545")

check("compute_metrics returns a row", row is not None, True)
produced = set(row) - set(M.KEY_COLUMNS)
registry = set(M.BASE_NAMES)
missing = sorted(registry - produced)
extra = sorted(produced - registry)
check(f"every registry column is produced ({len(registry)} of them)",
      missing, [])
check("no column is produced that the registry does not know", extra, [])
check("key columns present",
      (row["ticker"], row["trade_date"], row["snapshot"]),
      ("SPY", date(2026, 6, 15), "1545"))
check("an absent surface returns None, not an empty row",
      C.compute_metrics(FakeConn({**data, "surface": [], "atm": []}),
                        "ZZZ", date(2026, 6, 15), "1545"), None)
# A tenor with no rows must not fabricate anything.
check("every 60d column is NULL on a fixture that has only 30d",
      all(row[n] is None for n in M.BASE_NAMES
          if n.endswith("_60d") and not n.startswith("extrap_")), True)
close("spot", row["spot"], 100.0)
close("iv_30d_10p", row["iv_30d_10p"], 0.40)
close("rr_30d_25", row["rr_30d_25"], 0.26 - 0.32)


# =============================================================================
print("\n=== 12b. daily-derived columns carry across the session ===")
# A 21-day rolling statistic is a property of the ticker, not of a 5-minute
# bucket. Every column below must hold the SAME value at 09:45 and at 15:45,
# and must not go NULL away from the close.
#
# The IV history here is planted ONLY under the baseline bucket, with a stub
# under 1215. That is what makes this a regression test rather than a
# tautology: computing spot-vol from the row's own snapshot — as it did until
# 2026-08-25 — sees two rows at 1215 and returns NULL for all five columns.

ivdates = [date(2026, 5, 1) + timedelta(days=i) for i in range(40)]
daily = {
    "1545": [(d, 0.28 + 0.004 * ((i % 5) - 2), 100.0 + 0.6 * ((i % 7) - 3))
             for i, d in enumerate(ivdates)],
    # What an intraday bucket actually has: days of history, not months.
    "1215": [(ivdates[-2], 0.281, 100.2), (ivdates[-1], 0.279, 99.8)],
}
ddata = {**data, "ivhist": daily,
         "earnings": [date(2026, 6, 3), date(2026, 9, 2)]}

TD = date(2026, 6, 15)
close_row = C.compute_metrics(FakeConn(ddata), "SPY", TD, "1545")
mid_row = C.compute_metrics(FakeConn(ddata), "SPY", TD, "1215")

DAILY_FAMILIES = ("realized_vol", "vrp", "spot_vol", "calendar")
daily_cols = [c.name for c in M.BASE_COLUMNS if c.family in DAILY_FAMILIES]
check(f"{len(daily_cols)} columns claim to be daily-derived",
      len(daily_cols) > 40, True)

check("spotvol_beta_1m is populated at an INTRADAY bucket",
      mid_row["spotvol_beta_1m"] is not None, True)
check("  and spotvol_r2_1m", mid_row["spotvol_r2_1m"] is not None, True)
check("  and vov_30d_1m", mid_row["vov_30d_1m"] is not None, True)
check("  (it is a real regression, not a fabricated zero)",
      abs(mid_row["spotvol_beta_1m"]) > 1e-9, True)

check("days_to_earnings is populated at an INTRADAY bucket",
      mid_row["days_to_earnings"], (date(2026, 9, 2) - TD).days)
check("  and agrees with the close",
      close_row["days_to_earnings"], mid_row["days_to_earnings"])

check("rv_30d agrees across buckets (it always did)",
      close_row["rv_30d"], mid_row["rv_30d"])
check("vrp_30d agrees across buckets (and is a real number)",
      close_row["vrp_30d"], mid_row["vrp_30d"])
check("  (not two matching NULLs)", close_row["vrp_30d"] is not None, True)
check("downside_semivol_1m agrees across buckets",
      close_row["downside_semivol_1m"], mid_row["downside_semivol_1m"])
check("log_ret_d agrees across buckets",
      close_row["log_ret_d"], mid_row["log_ret_d"])

# The one honest exception, and it is not carry-forward failing: the baseline
# bucket's own row IS the day's daily observation, so it is inside its window
# while 12:15 stops at T-1. Everything else in the family must match exactly.
asof_sensitive = {"vov_30d_1m", "spotvol_beta_1m", "spotvol_beta_3m",
                  "spotvol_r2_1m", "spotvol_r2_3m"}
mismatch = [c for c in daily_cols
            if c not in asof_sensitive and close_row[c] != mid_row[c]]
check("every other daily column is IDENTICAL at 1215 and 1545", mismatch, [])
check("no daily column is NULL intraday while set at the close",
      [c for c in daily_cols
       if close_row[c] is not None and mid_row[c] is None], [])

# ...and the as-of rule itself. A wild value planted at trade_date's own close
# must reach the 1545 row (that row IS the day's observation) and must NOT reach
# 12:15 (where it has not happened yet). On a REBUILD every bucket of T already
# exists, so without the second half a 09:45 row would be handed the 15:45 value
# from six hours in its future.
#
# The plant sits one day after the last fixture observation, deliberately: at a
# longer remove MAX_DIFF_GAP_DAYS drops the diff and the test would pass for the
# wrong reason.
TDX = ivdates[-1] + timedelta(days=1)
spiked = {**daily, "1545": daily["1545"] + [(TDX, 0.99, 500.0)]}
sdata = {**ddata, "ivhist": spiked}
noon_x = C.compute_metrics(FakeConn(sdata), "SPY", TDX, "1215")
close_x = C.compute_metrics(FakeConn(sdata), "SPY", TDX, "1545")
plain_x = C.compute_metrics(FakeConn(ddata), "SPY", TDX, "1215")
check("a value planted at trade_date's close does NOT reach the 1215 row",
      noon_x["spotvol_beta_1m"], plain_x["spotvol_beta_1m"])
check("  but it DOES reach the 1545 row — that row is the observation",
      close_x["spotvol_beta_1m"] != noon_x["spotvol_beta_1m"], True)
check("  and both are real values, so the difference is not None vs None",
      noon_x["spotvol_beta_1m"] is not None
      and close_x["spotvol_beta_1m"] is not None, True)



print("\n=== 13. the registry itself ===")
cat = M.catalog_rows()
check("catalog covers every column",
      len(cat), len(M.BASE_NAMES) + len(M.Z_NAMES))
check("catalog names are unique",
      len({c["column_name"] for c in cat}), len(cat))
check("every z row points at a real base column",
      all(c["base_column"] in registry for c in cat if c["form"] != "base"),
      True)
check("every base row points at itself",
      all(c["base_column"] == c["column_name"]
          for c in cat if c["form"] == "base"), True)
check("every column has units and a description",
      all(c["units"] and c["description"] for c in cat), True)
check("no name exceeds Postgres' 63-char identifier limit",
      max(len(c["column_name"]) for c in cat) <= 63, True)
check("table_name is one of the two fact tables",
      {c["table_name"] for c in cat},
      {"equity_metrics", "equity_metrics_z"})
check("tenor 1 is excluded from the grid", 1 in M.TENORS, False)
check("  (and 0)", 0 in M.TENORS, False)
excluded = {c.family for c in M.BASE_COLUMNS if not c.z_eligible}
check("spot and forwards are not z-scored",
      any(n.startswith(("spot_z", "forward_7d_z")) for n in M.Z_NAMES), False)
check("quality columns are not z-scored",
      any(n.startswith("extrap_") for n in M.Z_NAMES), False)
check("calendar columns are not z-scored",
      any(n.startswith(("day_of_week", "days_to_")) for n in M.Z_NAMES), False)
check("IV columns ARE z-scored",
      "iv_30d_25p_z_63" in set(M.Z_NAMES), True)
check("structure prices ARE z-scored",
      "zc_width_sigma_30d_z_252" in set(M.Z_NAMES), True)
check("long_sigma exists at every tenor",
      [f"long_sigma_{t}d" in registry for t in M.TENORS], [True] * 6)
check("long_sigma is z-scored at both windows",
      {"long_sigma_30d_z_63", "long_sigma_30d_z_252"} <= set(M.Z_NAMES), True)
# wing='25p' is what lets the dashboard's router resolve the extrapolation
# marker to extrap_25p_{t}d with no further change.
check("long_sigma carries wing='25p' so extrap marking routes",
      {c.wing for c in M.BASE_COLUMNS if c.name.startswith("long_sigma_")},
      {"25p"})
check("  and the extrap column it routes to exists",
      [f"extrap_25p_{t}d" in registry for t in M.TENORS], [True] * 6)
check("long_sigma and zc_width_sigma share family, units and tenors",
      [(c.family, c.units) for c in M.BASE_COLUMNS
       if c.name in ("long_sigma_30d", "zc_width_sigma_30d")],
      [("structure", "sigma")] * 2)
check("excluded families are exactly the three intended",
      excluded, {"level_price", "quality", "calendar"})


print("\n" + "=" * 68)
print(f"PASSED {len(PASS)} / {len(PASS) + len(FAIL)}")
if FAIL:
    for f in FAIL:
        print("  -", f)
    sys.exit(1)
print("ALL GREEN")
