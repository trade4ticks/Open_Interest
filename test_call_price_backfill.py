"""
test_call_price_backfill.py — recovering call_price from stored surface rows.

The claim under test is strong and worth stating plainly: rows written before
call_price existed already contain enough information to reproduce it exactly,
because disc = exp(-r*T) appears as a plain multiplicative factor in price,
vega and gamma, and d1/d2 need only F, K, T and sigma.

So the test is a round trip. Price a whole smile with a KNOWN r, throw the rate
away, keep only the columns equity_surface actually stores, recover, and
require every node's call price back to machine precision.

TWO THINGS THIS EXISTS TO PIN DOWN
----------------------------------
1. THE FAR WING. At F=100, K=55, 12 vol, 7 DTE, d1 is ~36: n(d1) underflows to
   exactly 0.0 and both CDFs saturate, so price, vega and gamma are all exactly
   zero and the node carries NO recoverable rate — while its CALL is worth ~45.
   Row-by-row recovery would abandon precisely the rows whose call price is
   largest. The test asserts both halves: that the node really is barren, and
   that the smile still delivers its call price.

2. THE VALIDATION BITES. A backfill that silently writes a wrong number is
   worse than one that writes nothing, so each stored greek is corrupted in
   turn and the recovery must refuse.

Run:  python test_call_price_backfill.py     (exit 1 on any failure)
"""
from __future__ import annotations

import sys

from backfill_call_price import (
    MAX_RATE_SPREAD, call_prices_for_group, disc_candidates, recover_rate,
)
from lib.surface_fit import bs_put_forward

PASS, FAIL = [], []


def check(name, got, want):
    ok = got == want
    (PASS if ok else FAIL).append(name)
    print(f"  [{'ok  ' if ok else 'FAIL'}] {name:<58} got={got!r} want={want!r}")


def close(name, got, want, tol=1e-10):
    ok = (got is not None and want is not None
          and abs(got - want) <= tol * max(1.0, abs(want)))
    (PASS if ok else FAIL).append(name)
    g = f"{got:.12g}" if isinstance(got, float) else repr(got)
    print(f"  [{'ok  ' if ok else 'FAIL'}] {name:<58} got={g} want={want:.12g}")


def node(F, K, T, sigma, r, put_delta=25):
    """Exactly the columns equity_surface holds — r deliberately absent."""
    g = bs_put_forward(F, K, T, sigma, r)
    return {
        "ticker": "TST", "trade_date": "2026-06-01", "snapshot": "1545",
        "dte": int(round(T * 365)), "put_delta": put_delta,
        "forward": F, "strike": K, "iv": sigma,
        "dte_actual": T * 365.0,        # what the fit writes: smile.T * 365
        "price": g["price"], "theta": g["theta"],
        "vega": g["vega"], "gamma": g["gamma"],
    }, g["call_price"]


def smile(F, T, r, mults, sigma=0.30):
    """One tenor's worth of nodes: same F, T, r, a ladder of strikes."""
    rows, wants = [], []
    for i, m in enumerate(mults):
        row, want = node(F, F * m, T, sigma, r, put_delta=5 * (i + 1))
        rows.append(row)
        wants.append(want)
    return rows, wants


LADDER = (0.55, 0.70, 0.85, 0.95, 1.0, 1.05, 1.15, 1.35, 1.80)

print("\n=== 1. round trip over whole smiles ===")
worst_cp, worst_r, n_nodes, n_smiles = 0.0, 0.0, 0, 0
for F in (100.0, 43.7, 512.25):
    for dte in (7, 30, 90, 360):
        for sigma in (0.12, 0.35, 0.80):
            for r in (0.0, 0.03, 0.05, 0.10):
                rows, wants = smile(F, dte / 365.0, r, LADDER, sigma)
                results, st = call_prices_for_group(rows)
                n_smiles += 1
                if st["r"] is None:
                    FAIL.append(f"smile F={F} dte={dte} sig={sigma} r={r} "
                                f"refused")
                    continue
                worst_r = max(worst_r, abs(st["r"] - r))
                for (_row, cp, reason), want in zip(results, wants):
                    if reason is not None:
                        FAIL.append(f"F={F} dte={dte} sig={sigma} r={r}: "
                                    f"{reason}")
                        continue
                    n_nodes += 1
                    worst_cp = max(worst_cp,
                                   abs(cp - want) / max(1.0, abs(want)))
print(f"  {n_smiles} smiles, {n_nodes} nodes")
check("every node recovered", len(FAIL), 0)
check(f"call_price relative error <= 1e-12 (worst {worst_cp:.2e})",
      worst_cp <= 1e-12, True)
check(f"recovered r absolute error <= 1e-10 (worst {worst_r:.2e})",
      worst_r <= 1e-10, True)
check("every node of every smile was covered", n_nodes, n_smiles * len(LADDER))

print("\n=== 2. the far wing: barren node, correct call price ===")
barren, want = node(100.0, 55.0, 7 / 365.0, 0.12, 0.05, put_delta=5)
# Not literally 0.0 — around 1e-285, which is worse than zero in one respect:
# it looks like a number. Every one is far below the reproduction tolerance,
# so comparing against it proves nothing.
check("price, vega and gamma are all ~1e-283 or smaller",
      max(barren["price"], barren["vega"], barren["gamma"]) < 1e-280, True)
check("so the node carries NO rate of its own", disc_candidates(barren), [])
check("  (row-by-row recovery would have abandoned it)",
      recover_rate([barren])[0], None)
check("but its call is deeply in the money", want > 40.0, True)
rows, wants = smile(100.0, 7 / 365.0, 0.05, (0.55, 0.95, 1.0, 1.05), 0.12)
results, st = call_prices_for_group(rows)
close("the smile still recovers the rate", st["r"], 0.05, tol=1e-10)
check("  the barren node gets a value", results[0][2], None)
close("  and it is exactly right", results[0][1], wants[0], tol=1e-12)
check("  counted as vacuous, not as validated", (st["vacuous"], st["validated"]),
      (1, 3))

print("\n=== 3. the validation bites, in two layers ===")
# price, vega and gamma FEED the rate recovery, so corrupting one shifts that
# node's rate estimate away from its siblings' and the intra-smile spread check
# fires. The whole smile is refused, which is the right blast radius: a smile
# whose nodes disagree about r is not a smile this script can price at all.
base, _ = smile(100.0, 30 / 365.0, 0.05, (0.85, 0.95, 1.0, 1.10))
for greek in ("price", "vega", "gamma"):
    rows = [dict(r) for r in base]
    rows[2][greek] = rows[2][greek] * 1.001       # 10 bp on the ATM node
    results, st = call_prices_for_group(rows)
    check(f"a 0.1% error in {greek} refuses the whole smile",
          (st["r"], sum(1 for r in results if r[2] is not None)), (None, 4))
    check("  because the nodes now disagree on the rate",
          "disagree" in results[2][2], True)
    check("  by far more than the tolerance",
          st["spread"] > MAX_RATE_SPREAD * 100, True)

# theta is NOT a rate input, so the smile's rate survives intact and the error
# is caught one layer down, by per-node reproduction. Only that node is lost.
rows = [dict(r) for r in base]
rows[2]["theta"] *= 1.001
results, st = call_prices_for_group(rows)
close("a corrupt theta leaves the smile's rate intact", st["r"], 0.05, 1e-10)
check("  the bad node is refused", "theta" in (results[2][2] or ""), True)
check("  and the other three still get values",
      all(r[1] is not None for i, r in enumerate(results) if i != 2), True)
# Theta is the sharpest check available: it depends on r EXPLICITLY, not only
# through disc, so it pins the rate rather than just the discount factor.
rows = [dict(r) for r in base]
rows[2]["theta"] *= 1.000001
results, _ = call_prices_for_group(rows)
check("theta catches even a 1e-6 relative error", results[2][2] is not None,
      True)

print("\n=== 4. a group that is not one smile is refused ===")
mixed = [r for r, _ in [node(100.0, 95.0, 30 / 365.0, 0.30, 0.05),
                        node(100.0, 105.0, 30 / 365.0, 0.30, 0.08)]]
r, info = recover_rate(mixed)
check("two different rates in one group -> refusal", r, None)
check("  and it says the nodes disagree", "disagree" in info["reason"], True)
check("  spread exceeds the tolerance", info["spread"] > MAX_RATE_SPREAD, True)
two_fwd = [node(100.0, 95.0, 30 / 365.0, 0.30, 0.05)[0],
           node(101.0, 95.0, 30 / 365.0, 0.30, 0.05)[0]]
r, info = recover_rate(two_fwd)
check("two forwards in one group -> refusal", r, None)
check("  and it says so", "not a single smile" in info["reason"], True)

print("\n=== 5. refusals rather than guesses ===")
one, _ = node(100.0, 95.0, 30 / 365.0, 0.30, 0.05)
res, _ = call_prices_for_group([{**one, "price": None}])
check("NULL price -> NULL call", res[0][1], None)
check("  reason names it", "never priced" in res[0][2], True)
for missing in ("forward", "strike", "iv", "dte_actual"):
    r, info = recover_rate([{**one, missing: None}])
    check(f"missing {missing} -> no rate", r, None)
r, info = recover_rate([{**one, "dte_actual": 0.0}])
check("T = 0 -> refusal, not a divide-by-zero", r, None)
r, info = recover_rate([{**one, "iv": -0.1}])
check("negative sigma -> refusal", r, None)
# A discount factor implying a ~40% rate is not a discount factor.
insane = dict(one)
for g in ("price", "vega", "gamma"):
    insane[g] *= 0.9
r, info = recover_rate([insane])
check("an out-of-band rate is refused", r, None)
check("  and reports the rate it found",
      "sane band" in info["reason"] or "implausible" in info["reason"], True)

print("\n=== 6. r = 0 is a real case, not a sentinel ===")
# surface_config sets R_MIN = 0.0, so a fit can legitimately land at zero.
rows, wants = smile(100.0, 90 / 365.0, 0.0, (0.9, 1.0, 1.1), 0.22)
results, st = call_prices_for_group(rows)
close("zero rate recovers", st["r"], 0.0, tol=1e-12)
close("  ATM call value", results[1][1], wants[1], tol=1e-12)
check("  and at F = K the call equals the put",
      abs(results[1][1] - rows[1]["price"]) < 1e-12, True)

print("\n=== 7. a single well-conditioned node is enough ===")
one, want = node(100.0, 92.0, 30 / 365.0, 0.28, 0.045)
results, st = call_prices_for_group([one])
close("rate from one node", st["r"], 0.045, tol=1e-10)
close("  call price", results[0][1], want, tol=1e-12)
check("  three independent candidates agreed",
      len(disc_candidates(one)), 3)
# vega and gamma NULL: the price route alone must carry it.
res, st = call_prices_for_group([{**one, "vega": None, "gamma": None,
                                  "theta": None}])
close("price alone recovers the rate", st["r"], 0.045, tol=1e-10)
close("  and the call price", res[0][1], want, tol=1e-12)

print("\n" + "=" * 70)
print(f"PASSED {len(PASS)} / {len(PASS) + len(FAIL)}")
if FAIL:
    for f in FAIL[:15]:
        print("  -", f)
    sys.exit(1)
print("ALL GREEN")
