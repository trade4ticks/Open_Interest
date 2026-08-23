"""
test_live_surface.py — the live capture path, minus the database.

Covers the pieces that decide whether a cycle is correct rather than merely
fast: the grid-bucket mapping, the market window (including half-day closes),
and the CALL/PUT projection that surface_fit silently depends on.

The projection test is the one that matters. clean_chain tolerates 'CALL' and
'PUT', but surface_fit compares against exactly 'C' and 'P' — so passing the
vendor spelling straight through yields zero calls, zero puts, an empty smile
and no error anywhere. Only an assertion on the mapped values catches it.

fit_one is exercised on a synthetic vendor-shaped frame, so this runs with no
terminal and no database.

Run:  python test_live_surface.py     (exit 1 on any failure)
"""
from __future__ import annotations

import math
import sys
from datetime import date, datetime

import numpy as np
import pandas as pd
from scipy.stats import norm

import fetch_live_surface as L

PASS, FAIL = [], []


def check(name, got, want):
    ok = got == want
    (PASS if ok else FAIL).append(name)
    print(f"  [{'ok  ' if ok else 'FAIL'}] {name:<54} got={got!r} want={want!r}")


def vendor_frame(ticker="AAPL", spot=100.0, n_exp=4):
    """A frame shaped exactly like the snapshot endpoint's output, including
    'right' as CALL/PUT and per-contract timestamps."""
    rows = []
    for ei, dte in enumerate((7, 14, 30, 60)[:n_exp]):
        exp = date(2026, 6, 1) + pd.Timedelta(days=dte).to_pytimedelta()
        T, sig = dte / 365.0, 0.35
        for K in np.arange(70.0, 131.0, 2.5):
            sq = sig * math.sqrt(T)
            d1 = (math.log(spot / K) + 0.5 * sig * sig * T) / sq
            d2 = d1 - sq
            call = spot * norm.cdf(d1) - K * norm.cdf(d2)
            put = K * norm.cdf(-d2) - spot * norm.cdf(-d1)
            for right, px, dlt in (("CALL", call, norm.cdf(d1)),
                                   ("PUT", put, norm.cdf(d1) - 1)):
                if px < 0.10:
                    continue
                rows.append(dict(
                    symbol=ticker, expiration=exp.strftime("%Y-%m-%d"),
                    strike=float(K), right=right,
                    timestamp="2026-06-01T13:47:30.123",
                    bid=px - 0.02, ask=px + 0.02, delta=dlt, theta=-0.01,
                    vega=0.10, rho=0.0, epsilon=0.0, **{"lambda": 0.0},
                    implied_vol=sig, iv_error=0.001,
                    underlying_timestamp="2026-06-01T17:47:30.123",
                    underlying_price=spot))
    return pd.DataFrame(rows)


print("\n=== 1. grid bucket: capture time -> 5-minute slot ===")
for hh, mm, ss, want in ((13, 47, 30, "1345"), (13, 45, 0, "1345"),
                         (13, 49, 59, "1345"), (13, 50, 0, "1350"),
                         (9, 35, 12, "0935"), (16, 0, 5, "1600"),
                         (9, 59, 59, "0955")):
    check(f"{hh:02d}:{mm:02d}:{ss:02d}",
          L.grid_bucket(datetime(2026, 6, 1, hh, mm, ss)), want)

print("\n=== 2. market window ===")
# 2026-06-01 is a Monday, a normal session.
ok, why = L.market_window(datetime(2026, 6, 1, 10, 0))
check("inside the session", ok, True)
ok, _ = L.market_window(datetime(2026, 6, 1, 9, 20))
check("before 09:35 is closed", ok, False)
ok, _ = L.market_window(datetime(2026, 6, 1, 16, 30))
check("after the close is closed", ok, False)
ok, why = L.market_window(datetime(2026, 6, 6, 11, 0))   # Saturday
check("weekend is closed", ok, False)
check("  and says why", "not an NYSE trading day" in why, True)
# 2026-11-27 is the day after Thanksgiving: a 13:00 close.
ok, _ = L.market_window(datetime(2026, 11, 27, 12, 30))
check("half-day 12:30 is open", ok, True)
ok, why = L.market_window(datetime(2026, 11, 27, 14, 30))
check("half-day 14:30 is CLOSED (calendar, not hardcoded 16:00)", ok, False)
check("  and names the real close", "13:00" in why, True)

print("\n=== 3. projection: CALL/PUT -> C/P ===")
raw = vendor_frame()
proj = L.project(raw, "AAPL", date(2026, 6, 1), "1345")
check("rows preserved", len(proj), len(raw))
check("option_type values", sorted(proj["option_type"].unique()), ["C", "P"])
check("no CALL/PUT survives",
      bool(proj["option_type"].isin(["CALL", "PUT"]).any()), False)
# The failure this guards: surface_fit matches on exactly 'C'/'P'.
up = proj["option_type"].astype("string").str.upper()
check("surface_fit would see calls", int((up == "C").sum()) > 0, True)
check("surface_fit would see puts", int((up == "P").sum()) > 0, True)
check("vendor spelling would have yielded zero of each",
      int((raw["right"].str.upper() == "C").sum()), 0)
check("expiration parsed to dates",
      isinstance(proj["expiration"].iloc[0], date), True)
check("timestamp parsed", str(proj["timestamp"].iloc[0]),
      "2026-06-01 13:47:30.123000")
check("required clean_chain columns present",
      {"strike", "option_type", "bid", "ask", "delta", "implied_vol",
       "underlying_price", "timestamp", "expiration", "trade_date"}
      <= set(proj.columns), True)
check("empty frame in, empty out", L.project(pd.DataFrame(), "X",
                                             date(2026, 6, 1), "1345").empty,
      True)

print("\n=== 4. fit_one end to end (no terminal, no database) ===")
captured = datetime(2026, 6, 1, 13, 47, 30)
got = L.fit_one(("AAPL", vendor_frame(), date(2026, 6, 1), "1345", captured))
check("no error", got["error"], None)
check("surface rows produced", len(got["surface"]) > 0, True)
check("atm rows produced", len(got["atm"]) > 0, True)
check("diagnostics produced", len(got["diagnostics"]) > 0, True)
check("trade_date carried back", got["trade_date"], date(2026, 6, 1))
check("every surface row stamped source=live",
      {r["source"] for r in got["surface"]}, {"live"})
check("every surface row stamped captured_at",
      {r["captured_at"] for r in got["surface"]}, {captured})
check("every atm row stamped too",
      ({r["source"] for r in got["atm"]},
       {r["captured_at"] for r in got["atm"]}), ({"live"}, {captured}))
check("snapshot is the GRID bucket, not the capture minute",
      {r["snapshot"] for r in got["surface"]}, {"1345"})
check("captured_at differs from the bucket label",
      captured.strftime("%H%M"), "1347")
check("diagnostics are NOT stamped (they carry no capture columns)",
      "source" in got["diagnostics"][0], False)

print("\n=== 5. a bad frame fails the ticker, not the cycle ===")
bad = L.fit_one(("ZZZ", pd.DataFrame({"nonsense": [1]}), date(2026, 6, 1),
                 "1345", captured))
check("malformed frame returns an error", bad["error"] is not None, True)
check("and no rows", (len(bad["surface"]), len(bad["atm"])), (0, 0))
empty = L.fit_one(("ZZZ", pd.DataFrame(), date(2026, 6, 1), "1345", captured))
check("empty frame returns an error", empty["error"] is not None, True)

print("\n=== 6. store carries the new columns ===")
from lib.surface_store import ATM_COLS, SURFACE_COLS, SURFACE_KEYS
check("surface has captured_at + source",
      {"captured_at", "source"} <= set(SURFACE_COLS), True)
check("atm has captured_at + source",
      {"captured_at", "source"} <= set(ATM_COLS), True)
check("snapshot is still part of the key — an exact rebuild upserts over live",
      "snapshot" in SURFACE_KEYS, True)
check("captured_at/source are NOT keys (so they get overwritten)",
      bool({"captured_at", "source"} & set(SURFACE_KEYS)), False)

print("\n" + "=" * 60)
print(f"PASSED {len(PASS)} / {len(PASS) + len(FAIL)}")
if FAIL:
    for f in FAIL:
        print("  -", f)
    sys.exit(1)
print("ALL GREEN")
