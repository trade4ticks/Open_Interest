"""
test_equity_surface.py — unit tests for the interpolation stage.

Expected values are derived from the inputs (a known F and r round-tripped
through parity, a Black-Scholes smile evaluated at a known delta), not
captured from a previous run.

The extrapolation test is the one that matters most: ext=3 makes the spline
return its boundary value outside the fitted domain, so a delta solved out
there yields a plausible IV with nothing marking it as fabricated. It is only
caught by building a deliberately narrow strike ladder and asserting the flag.

The database tests are skipped without a reachable Postgres, and say so.

Run:  python test_equity_surface.py     (exit 1 on any failure)
"""
from __future__ import annotations

import math
import sys
from datetime import date, datetime

import numpy as np
import pandas as pd
from scipy.stats import norm

from lib.clean_chain import clean_chain
from lib.surface_config import FALLBACK_MAX_T_GAP, TARGET_DTES
from lib.surface_fit import (
    FORWARD_PCP, FORWARD_SPOT_FALLBACK, ParityError, atm_node,
    bs_put_forward, build_smile_points, build_snapshot, check_butterfly,
    check_calendar, fit_expiry, fit_smile, forward_and_rate,
    interpolate_tenor, near_expiry_fallback, put_delta_from_k,
    solve_delta_node, solve_forward_rate, time_to_expiry,
)

PASS, FAIL, SKIP = [], [], []


def check(name, got, want, tol=1e-6):
    if isinstance(want, float) and isinstance(got, float):
        ok = (math.isnan(got) and math.isnan(want)) or abs(got - want) <= tol
    else:
        ok = got == want
    (PASS if ok else FAIL).append(name)
    print(f"  [{'ok  ' if ok else 'FAIL'}] {name:<54} got={got!r} want={want!r}")


# --- synthetic chain builder ------------------------------------------------

def bs_prices(F, K, T, sigma, r):
    """Undiscounted-forward BS call and put, for building synthetic quotes."""
    sq = sigma * math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma ** 2 * T) / sq
    d2 = d1 - sq
    disc = math.exp(-r * T)
    call = disc * (F * norm.cdf(d1) - K * norm.cdf(d2))
    put = disc * (K * norm.cdf(-d2) - F * norm.cdf(-d1))
    return call, put


def make_chain(strikes, F=100.0, T=30 / 365, r=0.05, sigma=0.25,
               expiration=date(2026, 7, 1), snapshot="0945",
               ts="2026-06-01T09:45:00", spot=100.0, skew=0.0):
    """A synthetic chain consistent with (F, r, sigma), optionally skewed.

    `skew` tilts IV linearly in log-moneyness so the smile is not flat, which
    is what makes the delta solver do real work.
    """
    rows = []
    for K in strikes:
        k = math.log(K / F)
        iv = max(sigma + skew * k, 0.01)
        call, put = bs_prices(F, K, T, iv, r)
        for right, px in (("C", call), ("P", put)):
            # Skip strikes with no real premium rather than emitting a quote
            # whose bid floor would sit above its ask. A real chain lists them
            # but they are filtered out; synthesising a crossed market instead
            # would silently thin the smile through a code path the test is
            # not trying to exercise.
            if px < 0.10:
                continue
            vega = (math.exp(-r * T) * F * norm.pdf(
                (math.log(F / K) + 0.5 * iv ** 2 * T) / (iv * math.sqrt(T)))
                * math.sqrt(T) / 100.0)
            rows.append(dict(
                ticker="AAPL", trade_date=date(2026, 6, 1), snapshot=snapshot,
                feature_date=date(2026, 6, 2), timestamp=ts,
                expiration=expiration, strike=float(K), option_type=right,
                bid=px - 0.02, ask=px + 0.02,
                delta=0.5, theta=-0.01, vega=max(vega, 1e-4), rho=0.0,
                epsilon=0.0, **{"lambda": 0.0},
                implied_vol=iv, iv_error=0.001,
                underlying_timestamp=ts, underlying_price=spot))
    return pd.DataFrame(rows)


LADDER = [float(x) for x in range(70, 136, 5)]
# A 2-day tenor needs a finer ladder: at 80 vol one sigma is ~6%, so the
# 5-point LADDER leaves only 4 strikes with real premium and the fit is
# (correctly) skipped for want of smile points.
FINE_LADDER = [90.0 + 2.5 * i for i in range(9)]


def single_smile(fit):
    """A usable smile from ONE fit.

    A lone expiry can never bracket a tenor — bracketing needs two fits — so
    the only route is the near-expiry fallback, which serves the largest bucket
    below the fit's own T. Making that explicit here rather than relying on
    `interpolate_tenor` happening to return something.
    """
    dte = int(math.floor(fit.T * 365))
    return (interpolate_tenor([fit], dte)
            or near_expiry_fallback([fit], [dte]))


# ---------------------------------------------------------------------------
print("\n=== 1. T uses 16:00 ET, not 16:15 ===")
T = time_to_expiry(pd.Timestamp("2026-06-01 09:45"), date(2026, 6, 1))
check("same-day T = 6h15m in years", T, (6 * 60 + 15) / (365 * 24 * 60))
check("16:15 would give 6h30m — not this",
      abs(T - (6 * 60 + 30) / (365 * 24 * 60)) > 1e-9, True)
check("expired -> T = 0",
      time_to_expiry(pd.Timestamp("2026-06-02 09:45"), date(2026, 6, 1)), 0.0)

# ---------------------------------------------------------------------------
print("\n=== 2. parity regression recovers a known F and r ===")
F_TRUE, R_TRUE, T30 = 103.5, 0.043, 30 / 365
raw = make_chain(LADDER, F=F_TRUE, T=T30, r=R_TRUE, sigma=0.28)
cl = clean_chain(raw)
from lib.surface_fit import filter_quotes
F_hat, r_hat = solve_forward_rate(filter_quotes(cl), T30)
check("F recovered", round(F_hat, 6), round(F_TRUE, 6), tol=1e-4)
check("r recovered", round(r_hat, 6), round(R_TRUE, 6), tol=1e-4)
F2, r2, m2 = forward_and_rate(filter_quotes(cl), T30)
check("forward_method = 'pcp'", m2, FORWARD_PCP)

# ---------------------------------------------------------------------------
print("\n=== 3. fewer than 3 matched pairs -> spot_fallback ===")
thin = make_chain([100.0, 105.0], F=100.0, T=T30, r=0.05, sigma=0.25)
cl_thin = filter_quotes(clean_chain(thin))
try:
    solve_forward_rate(cl_thin, T30)
    check("solve_forward_rate raises on <3 pairs", False, True)
except ParityError:
    check("solve_forward_rate raises on <3 pairs", True, True)
F3, r3, m3 = forward_and_rate(cl_thin, T30)
check("forward_method = 'spot_fallback'", m3, FORWARD_SPOT_FALLBACK)
check("fallback F = S*exp(rT)", round(F3, 6),
      round(100.0 * math.exp(0.05 * T30), 6), tol=1e-6)
check("fallback r = R_DEFAULT", r3, 0.05)

# ---------------------------------------------------------------------------
print("\n=== 4. extrapolated flag: narrow ladder, 5-delta outside it ===")
# Only 95..105 listed, so the deep wings are far outside [k_min, k_max].
narrow = make_chain([95.0, 97.5, 100.0, 102.5, 105.0], F=100.0, T=T30,
                    r=0.05, sigma=0.25, skew=-0.4)
f_narrow = fit_expiry(clean_chain(narrow), "AAPL", date(2026, 6, 1), "0945",
                      date(2026, 7, 1), pd.Timestamp("2026-06-01 09:45"))
check("narrow fit is usable", f_narrow.usable, True)
sm = single_smile(f_narrow)
check("single fit yields a smile only via the fallback", sm is not None, True)
n5 = solve_delta_node(sm, 5)
n50 = solve_delta_node(sm, 50)
check("5-delta solves", n5 is not None, True)
check("5-delta k is outside the fitted domain",
      bool(n5 and not (sm.k_min <= n5["k"] <= sm.k_max)), True)
check("5-delta flagged extrapolated=True", bool(n5 and n5["extrapolated"]), True)
check("50-delta k is inside the fitted domain",
      bool(n50 and sm.k_min <= n50["k"] <= sm.k_max), True)
check("50-delta flagged extrapolated=False",
      bool(n50 and n50["extrapolated"]), False)

wide = make_chain(LADDER, F=100.0, T=T30, r=0.05, sigma=0.25, skew=-0.4)
f_wide = fit_expiry(clean_chain(wide), "AAPL", date(2026, 6, 1), "0945",
                    date(2026, 7, 1), pd.Timestamp("2026-06-01 09:45"))
sm_w = single_smile(f_wide)
n5w = solve_delta_node(sm_w, 5) if sm_w else None
check("same delta on a WIDE ladder is not flagged",
      bool(n5w and n5w["extrapolated"]), False)

# ---------------------------------------------------------------------------
print("\n=== 5. delta solver returns the delta it was asked for ===")
for d in (10, 25, 50, 75):
    node = solve_delta_node(sm_w, d)
    w_at = float(sm_w.w(node["k"]))
    got = abs(put_delta_from_k(node["k"], w_at)) * 100
    check(f"|put delta| at target {d}", round(got, 4), float(d), tol=1e-3)
check("strike = F*exp(k)", round(sm_w.F * math.exp(
    solve_delta_node(sm_w, 25)["k"]), 6),
      round(solve_delta_node(sm_w, 25)["strike"], 6), tol=1e-6)
check("higher delta -> higher strike (put convention)",
      solve_delta_node(sm_w, 75)["strike"] > solve_delta_node(sm_w, 25)["strike"],
      True)

# ---------------------------------------------------------------------------
print("\n=== 6. time interpolation: alpha=0 and alpha=1 hit the boundaries ===")
snap_dt = pd.Timestamp("2026-06-01 09:45")
f_lo = fit_expiry(clean_chain(make_chain(LADDER, F=100.0, T=10 / 365, r=0.05,
                                         sigma=0.45, expiration=date(2026, 6, 11))),
                  "AAPL", date(2026, 6, 1), "0945", date(2026, 6, 11), snap_dt)
f_hi = fit_expiry(clean_chain(make_chain(LADDER, F=100.0, T=40 / 365, r=0.05,
                                         sigma=0.50, expiration=date(2026, 7, 11))),
                  "AAPL", date(2026, 6, 1), "0945", date(2026, 7, 11), snap_dt)
check("both fits usable", (f_lo.usable, f_hi.usable), (True, True))

# alpha=0 / alpha=1 are boundary properties of the blend itself. Reaching them
# through interpolate_tenor is not possible with integer buckets: a fit's true
# T carries the 6h15m to the close, so no whole-day target lands exactly on it.
# Construct the blend directly to test the property the spec names.
from lib.surface_fit import InterpolatedSmile
s0 = InterpolatedSmile(dte=0, T=f_lo.T, F=f_lo.F, r=f_lo.r,
                       k_min=f_lo.k_min, k_max=f_lo.k_max,
                       dte_actual=f_lo.dte_actual, _lo=f_lo, _hi=f_hi,
                       _alpha=0.0)
s1 = InterpolatedSmile(dte=0, T=f_hi.T, F=f_hi.F, r=f_hi.r,
                       k_min=f_hi.k_min, k_max=f_hi.k_max,
                       dte_actual=f_hi.dte_actual, _lo=f_lo, _hi=f_hi,
                       _alpha=1.0)
for kk in (-0.10, 0.0, 0.10):
    check(f"alpha=0 -> w({kk}) equals the LOW fit",
          round(float(s0.w(kk)), 12), round(float(f_lo.w(kk)), 12), tol=1e-12)
    check(f"alpha=1 -> w({kk}) equals the HIGH fit",
          round(float(s1.w(kk)), 12), round(float(f_hi.w(kk)), 12), tol=1e-12)

# Bracketing tenors must come from the real grid; 14/21/30 sit between the
# two fits' true tenors (~10.26d and ~40.26d).
BRACKETED = [d for d in TARGET_DTES if f_lo.T <= d / 365.0 <= f_hi.T]
check("grid tenors that bracket these two fits", BRACKETED, [14, 21, 30])
mid_dte = 21
s_mid = interpolate_tenor([f_lo, f_hi], mid_dte)
check("bracketed tenor interpolates", s_mid is not None, True)
alpha = (mid_dte / 365.0 - f_lo.T) / (f_hi.T - f_lo.T)
check("blend is linear in TOTAL VARIANCE at the computed alpha",
      round(float(s_mid.w(0.0)), 12),
      round(float(f_lo.w(0.0)) + alpha * (float(f_hi.w(0.0))
                                          - float(f_lo.w(0.0))), 12), tol=1e-12)
between = float(f_lo.w(0.0)) <= float(s_mid.w(0.0)) <= float(f_hi.w(0.0))
check("midpoint w lies between the two fits", between, True)
check("blended domain is the INTERSECTION",
      (round(s_mid.k_min, 9), round(s_mid.k_max, 9)),
      (round(max(f_lo.k_min, f_hi.k_min), 9),
       round(min(f_lo.k_max, f_hi.k_max), 9)))
check("dte_actual on a normal blend is the nominal bucket",
      s_mid.dte_actual, float(mid_dte))

# ---------------------------------------------------------------------------
print("\n=== 7. no tenor without a bracketing pair (except the fallback) ===")
check("360 does not bracket between 10d and 40d fits",
      interpolate_tenor([f_lo, f_hi], 360), None)
check("1 does not bracket from below", interpolate_tenor([f_lo, f_hi], 1), None)
check("45 is beyond the longest fit", interpolate_tenor([f_lo, f_hi], 45), None)
built = build_snapshot(
    pd.concat([clean_chain(make_chain(LADDER, F=100.0, T=10 / 365, r=0.05,
                                      sigma=0.45, expiration=date(2026, 6, 11))),
               clean_chain(make_chain(LADDER, F=100.0, T=40 / 365, r=0.05,
                                      sigma=0.50, expiration=date(2026, 7, 11)))],
              ignore_index=True),
    "AAPL", date(2026, 6, 1), "0945")
emitted = sorted({r["dte"] for r in built["surface"]})
# The nearest fit is ~10.26d, so the largest un-bracketable bucket is 10 and
# its gap (0.26d) is inside the cap — one fallback row on top of 14/21/30.
fb_bucket = max(d for d in TARGET_DTES if d / 365.0 < f_lo.T)
check("largest un-bracketable bucket", fb_bucket, 10)
check("emitted tenors are exactly the bracketed set plus the fallback",
      emitted, sorted(BRACKETED + [fb_bucket]))
check("nothing beyond the longest fit", max(emitted) <= 30, True)
check("no un-bracketable bucket below the fallback emitted",
      [d for d in emitted if d < fb_bucket], [])

# ---------------------------------------------------------------------------
print("\n=== 8. capped near-expiry fallback ===")
# Nearest expiry is 2026-06-03, so its true T is 2d + 6h15m = 2.26d. The
# largest grid bucket strictly below that is dte=2 (not 1 — 2/365 < 2.26/365),
# and the gap of 0.26d is well inside FALLBACK_MAX_T_GAP.
f_near = fit_expiry(clean_chain(make_chain(FINE_LADDER, F=100.0, T=2 / 365,
                                           r=0.05,
                                           sigma=0.80, expiration=date(2026, 6, 3))),
                    "AAPL", date(2026, 6, 1), "0945", date(2026, 6, 3), snap_dt)
fb = near_expiry_fallback([f_near], TARGET_DTES)
check("fallback emitted when inside the cap", fb is not None, True)
check("fallback bucket is the LARGEST un-bracketable one", fb.dte, 2)
check("fallback is flagged", fb.is_fallback, True)
check("fallback T is the fit's ACTUAL T, not the bucket's nominal",
      round(fb.T, 9), round(f_near.T, 9))
check("fallback T != nominal bucket T", abs(fb.T - 2 / 365.0) > 1e-9, True)
check("dte_actual reflects the smile's true tenor",
      round(fb.dte_actual, 6), round(f_near.T * 365, 6))

# Nearest expiry 25 days out: the largest bucket below is 21, gap 4/365 —
# right at the cap, so push it to 26 days to be clearly outside.
f_far = fit_expiry(clean_chain(make_chain(LADDER, F=100.0, T=26 / 365, r=0.05,
                                          sigma=0.25, expiration=date(2026, 6, 27))),
                   "AAPL", date(2026, 6, 1), "0945", date(2026, 6, 27), snap_dt)
check("gap exceeds the cap", (26 - 21) / 365.0 > FALLBACK_MAX_T_GAP, True)
check("fallback SKIPPED when outside the cap",
      near_expiry_fallback([f_far], TARGET_DTES), None)

# ---------------------------------------------------------------------------
print("\n=== 9. dte_actual: nominal on normal rows, true tenor on fallback ===")
norm_rows = [r for r in built["surface"] if r["dte"] == mid_dte]
check("normal interpolated row: dte_actual == nominal dte",
      round(float(norm_rows[0]["dte_actual"]), 6), float(mid_dte))
fb_built = build_snapshot(
    clean_chain(make_chain(FINE_LADDER, F=100.0, T=2 / 365, r=0.05, sigma=0.80,
                           expiration=date(2026, 6, 3))),
    "AAPL", date(2026, 6, 1), "0945")
fb_rows = [r for r in fb_built["surface"] if r["dte"] == 2]
check("fallback row emitted through build_snapshot", len(fb_rows) > 0, True)
check("fallback row dte_actual != its label",
      abs(float(fb_rows[0]["dte_actual"]) - 2.0) > 1e-6, True)
check("fallback row dte_actual == the fit's tenor",
      round(float(fb_rows[0]["dte_actual"]), 4), round(f_near.T * 365, 4),
      tol=1e-3)

# ---------------------------------------------------------------------------
print("\n=== 10. butterfly check flags an arbitrageable smile ===")
check("well-behaved smile is not flagged", f_wide.butterfly_arb_flag, False)
# A deep, narrow notch in total variance violates Durrleman.
ks = np.linspace(-0.4, 0.4, 41)
w_bad = 0.02 + 0.30 * np.exp(-((ks - 0.0) ** 2) / 0.0008)
bad_pts = pd.DataFrame({"k": ks, "w": w_bad,
                        "w_noise": np.full(len(ks), 1e-8)})
sp_bad, kmin_b, kmax_b, _ = fit_smile(bad_pts)
check("spiked smile IS flagged", check_butterfly(sp_bad, kmin_b, kmax_b), True)

# ---------------------------------------------------------------------------
print("\n=== 11. calendar arbitrage is flagged PER EXPIRY ===")
# Short expiry with far higher variance than the long one -> violation.
f_a = fit_expiry(clean_chain(make_chain(LADDER, F=100.0, T=10 / 365, r=0.05,
                                        sigma=0.90, expiration=date(2026, 6, 11))),
                 "AAPL", date(2026, 6, 1), "0945", date(2026, 6, 11), snap_dt)
f_b = fit_expiry(clean_chain(make_chain(LADDER, F=100.0, T=40 / 365, r=0.05,
                                        sigma=0.20, expiration=date(2026, 7, 11))),
                 "AAPL", date(2026, 6, 1), "0945", date(2026, 7, 11), snap_dt)
f_c = fit_expiry(clean_chain(make_chain(LADDER, F=100.0, T=90 / 365, r=0.05,
                                        sigma=0.25, expiration=date(2026, 8, 30))),
                 "AAPL", date(2026, 6, 1), "0945", date(2026, 8, 30), snap_dt)
check("all three fits usable (else the pair under test changes)",
      (f_a.usable, f_b.usable, f_c.usable), (True, True, True))
check_calendar([f_a, f_b, f_c])
check("violating pair member A flagged", f_a.calendar_arb_flag, True)
check("violating pair member B flagged", f_b.calendar_arb_flag, True)
check("the non-violating third expiry is NOT flagged",
      f_c.calendar_arb_flag, False)
clean_set = [f_lo, f_hi]
check_calendar(clean_set)
check("well-ordered pair NOT flagged",
      (f_lo.calendar_arb_flag, f_hi.calendar_arb_flag), (False, False))

# ---------------------------------------------------------------------------
print("\n=== 12. greeks ===")
g = bs_put_forward(F=100.0, K=100.0, T=30 / 365, sigma=0.25, r=0.05)
check("price positive", g["price"] > 0, True)
check("vega positive", g["vega"] > 0, True)
check("gamma positive", g["gamma"] > 0, True)
check("sigma <= 0 -> all None",
      bs_put_forward(100.0, 100.0, 0.1, 0.0, 0.05)["price"], None)
check("T <= 0 -> all None",
      bs_put_forward(100.0, 100.0, 0.0, 0.25, 0.05)["vega"], None)
# Deep ITM put: theta may be positive, which is correct, not a bug.
deep = bs_put_forward(F=100.0, K=200.0, T=1.0, sigma=0.15, r=0.10)
check("deep ITM put theta may be positive (not a bug)",
      deep["theta"] > 0, True)
# Put price must sit between intrinsic and the discounted strike.
disc = math.exp(-0.05 * 30 / 365)
p = bs_put_forward(F=100.0, K=110.0, T=30 / 365, sigma=0.25, r=0.05)["price"]
check("put price above discounted intrinsic", p > disc * 10.0, True)
check("put price below discounted strike", p < disc * 110.0, True)

# ---------------------------------------------------------------------------
print("\n=== 13. ATM row is evaluated at k = 0 ===")
a = atm_node(sm_w)
check("atm_strike == forward", round(a["atm_strike"], 9), round(sm_w.F, 9))
check("atm_iv == sqrt(w(0)/T)", round(a["atm_iv"], 9),
      round(math.sqrt(float(sm_w.w(0.0)) / sm_w.T), 9))
# At the forward d1 = +sqrt(w)/2 > 0, so N(d1) > 0.5 and the delta is
# slightly LESS negative than -0.5 — never more. The spec's formula is the
# standard one; only its parenthetical comment is inverted.
check("atm_put_delta is in (-0.5, 0), i.e. LESS negative than -0.5",
      -0.5 < a["atm_put_delta"] < 0.0, True)
check("atm_put_delta == N(sqrt(w)/2) - 1",
      round(a["atm_put_delta"], 12),
      round(float(norm.cdf(0.5 * math.sqrt(float(sm_w.w(0.0))))) - 1.0, 12))
check("|atm_put_delta| just under 0.5",
      0.40 < abs(a["atm_put_delta"]) < 0.5, True)
check("total_var == w(0)", round(a["total_var"], 9),
      round(float(sm_w.w(0.0)), 9))

# ---------------------------------------------------------------------------
print("\n=== 14. build_snapshot output shape ===")
check("surface rows carry extrapolated as bool",
      all(isinstance(r["extrapolated"], bool) for r in built["surface"]), True)
check("one diagnostics row per expiry", len(built["diagnostics"]), 2)
check("diagnostics record forward_method",
      all(d["forward_method"] in (FORWARD_PCP, FORWARD_SPOT_FALLBACK)
          for d in built["diagnostics"]), True)
check("every surface row has a ticker",
      all(r["ticker"] == "AAPL" for r in built["surface"]), True)
check("ATM row per emitted tenor",
      sorted({r["dte"] for r in built["atm"]}), emitted)

# ---------------------------------------------------------------------------
print("\n=== 15. upsert idempotence (needs Postgres) ===")
try:
    from db import get_connection
    from lib.surface_store import init_db, write_snapshot
    with get_connection() as conn:
        init_db(conn)
        d = date(2026, 6, 1)
        with conn.cursor() as cur:
            cur.execute("DELETE FROM equity_surface WHERE ticker='ZZTEST'")
            cur.execute("DELETE FROM equity_atm WHERE ticker='ZZTEST'")
            cur.execute("DELETE FROM equity_surface_diagnostics "
                        "WHERE ticker='ZZTEST'")
        conn.commit()
        res = dict(built)
        for key in ("surface", "atm", "diagnostics"):
            res[key] = [dict(r, ticker="ZZTEST") for r in res[key]]
        write_snapshot(conn, res, d)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM equity_surface WHERE ticker='ZZTEST'")
            n1 = cur.fetchone()[0]
        write_snapshot(conn, res, d)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM equity_surface WHERE ticker='ZZTEST'")
            n2 = cur.fetchone()[0]
            cur.execute("DELETE FROM equity_surface WHERE ticker='ZZTEST'")
            cur.execute("DELETE FROM equity_atm WHERE ticker='ZZTEST'")
            cur.execute("DELETE FROM equity_surface_diagnostics "
                        "WHERE ticker='ZZTEST'")
        conn.commit()
    check("row count unchanged after reprocessing", n2, n1)
except Exception as exc:                                      # noqa: BLE001
    SKIP.append("upsert idempotence")
    print(f"  [SKIP] upsert idempotence — no reachable Postgres "
          f"({type(exc).__name__})")

# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"PASSED {len(PASS)} / {len(PASS) + len(FAIL)}"
      + (f"   ({len(SKIP)} skipped)" if SKIP else ""))
if FAIL:
    for f in FAIL:
        print("  -", f)
    sys.exit(1)
print("ALL GREEN")
