"""Universe filter tests: the price band and the spread floor.

    python -m scalp.tests.test_universe_filters      (exit 1 on any failure)

Synthetic rows only — no API, no database, nothing written. classify() and
spread_gate() are pure functions of their arguments, which is what makes the
whole filter testable without either.

The property under test that matters most is REVERSIBILITY. An excluded symbol
is not fetched, so it is not measured, so it cannot produce the number that
would let it back in. Every test below that mentions a re-test is guarding that
one-way door, and the door is the reason the floor can be set aggressively.
"""
import sys
import zlib
from datetime import date, timedelta

import pandas as pd

from scalp import config
from scalp.update_universe import classify, spread_gate

PASS, FAIL = [], []


def check(name, got, want):
    ok = got == want
    (PASS if ok else FAIL).append(name)
    print(f"  [{'ok  ' if ok else 'FAIL'}] {name:<58} got={got!r} want={want!r}")


TODAY = date(2026, 9, 4)
N = config.UNIVERSE_SPREAD_RETEST_DAYS


def _phase_day(symbol, day=TODAY):
    """A date whose ordinal lands on this symbol's staggered re-test slot."""
    phase = zlib.crc32(symbol.encode()) % N
    return day + timedelta(days=(phase - day.toordinal()) % N)


def _quiet_day(symbol, day=TODAY):
    """A date that is NOT this symbol's slot, so the threshold decides."""
    d = _phase_day(symbol, day) + timedelta(days=1)
    return d


def mkrow(symbol, close, volume):
    return {"symbol": symbol, "close": close, "volume": volume}


def market(*rows):
    return pd.DataFrame(list(rows))


print("=== 1. the price band ===")
# $50 floor: a $60 name is now in. Under the old $100 floor it was invisible.
rows = classify(market(mkrow("SIXTY", 60.0, 5_000_000)), pd.DataFrame(), TODAY)
check("a $60 name at $300M qualifies", rows[0]["qualified"], True)

# A name that neither qualifies nor was previously known carries NO row at
# all -- classify deliberately does not file the whole market every night.
# So "excluded" is an empty result here, not a row with qualified=False.
rows = classify(market(mkrow("CHEAP", 45.0, 10_000_000)), pd.DataFrame(), TODAY)
check("a $45 name is still below the floor", rows, [])

# Dollar volume is unchanged: $60 x 1M = $60M, under the $100M floor.
rows = classify(market(mkrow("THIN", 60.0, 1_000_000)), pd.DataFrame(), TODAY)
check("$60M dollar volume still fails", rows, [])

check("exit price is below the entry floor",
      config.UNIVERSE_EXIT_PRICE < config.UNIVERSE_MIN_PRICE, True)
check("exit dollar volume is below the entry floor",
      config.UNIVERSE_EXIT_DOLLAR_VOL < config.UNIVERSE_MIN_DOLLAR_VOL, True)

# $200M x 1M shares = $200M, exactly at the floor: in.
rows = classify(market(mkrow("EDGE", 200.0, 1_000_000)), pd.DataFrame(), TODAY)
check("a name exactly at the dollar-volume floor qualifies",
      rows[0]["qualified"], True)
rows = classify(market(mkrow("EDGE", 199.0, 1_000_000)), pd.DataFrame(), TODAY)
check("...and just under it does not", rows, [])


print("\n=== 1b. the exit thresholds must track the entry floors ===")
# Retention by hysteresis is INDEFINITE: `incumbent` is (qualified OR
# retained), so a retained name is an incumbent again the next night, forever,
# with stickiness playing no part. That makes a stale exit constant not merely
# inconsistent but load-bearing -- it silently neuters a raised entry floor,
# because every existing member between the old exit and the new floor is
# carried indefinitely and the floor only ever blocks new entrants.
#
# Guarded as a RATIO rather than a pair of numbers so this still holds after
# the next threshold change.
check("exit price ratio is sane", 0.5 <= config.UNIVERSE_EXIT_PRICE_RATIO < 1.0,
      True)
check("exit dollar volume ratio is sane",
      0.5 <= config.UNIVERSE_EXIT_DOLLAR_VOL_RATIO < 1.0, True)

# The behavioural form: a name doing half the entry floor must eventually
# leave, and must not be carried forever by its own retained status.
half = config.UNIVERSE_MIN_DOLLAR_VOL / 2.0
px = max(config.UNIVERSE_MIN_PRICE, 60.0)
_prior = pd.DataFrame([{
    "symbol": "TAIL", "qualified": True, "retained": False,
    "first_entered": TODAY - timedelta(days=200),
    "sticky_until": TODAY - timedelta(days=100)}])     # sticky long expired
_mkt = market(mkrow("TAIL", px, half / px))
_in = []
_day = TODAY
for _ in range(5):
    _r = classify(_mkt, _prior, _day)[0]
    _in.append(bool(_r["qualified"] or _r["retained"]))
    _prior, _day = pd.DataFrame([_r]), _day + timedelta(days=1)
check("a name at half the dollar-volume floor is dropped", any(_in), False)

# The hysteresis cushion has to protect the band the floor just admitted. At
# the old constant 85 an incumbent at $55 would have been dropped instantly --
# the band with the least cushion would have been the newly-admitted one.
prior = pd.DataFrame([{
    "symbol": "WOBBLE", "qualified": True, "retained": False,
    "first_entered": TODAY - timedelta(days=5),
    "sticky_until": None}])
rows = classify(market(mkrow("WOBBLE", 47.0, 3_000_000)), prior, TODAY)
check("an incumbent at $47 is retained, not dropped", rows[0]["retained"], True)


print("\n=== 2. the spread floor ===")
tight, wide = 2.0, 12.0
q = _quiet_day("AMZN")

check("a 2 bps name is excluded",
      spread_gate("AMZN", {"AMZN": (tight, q - timedelta(days=1))}, q)[1], True)
check("a 12 bps name is kept",
      spread_gate("AMZN", {"AMZN": (wide, q - timedelta(days=1))}, q)[1], False)
check("...and the reason is recorded",
      spread_gate("AMZN", {"AMZN": (wide, q - timedelta(days=1))}, q)[2], "wide")

# The boundary is inclusive: a name AT the floor is tradable, not excluded.
at = config.UNIVERSE_MIN_SPREAD_BPS
check("a name exactly at the floor is kept",
      spread_gate("AMZN", {"AMZN": (at, q - timedelta(days=1))}, q)[1], False)
check("...and just under it is not",
      spread_gate("AMZN", {"AMZN": (at - 0.01, q - timedelta(days=1))}, q)[1],
      True)


print("\n=== 3. REVERSIBILITY — exclusion is never a one-way door ===")
# No measurement at all: a new entrant gets one session before being judged.
check("an unmeasured symbol is included", spread_gate("NEW", {}, TODAY)[1], False)
check("...and says why", spread_gate("NEW", {}, TODAY)[2], "unmeasured")

# The load-bearing case. A tight name whose measurement has gone stale comes
# back REGARDLESS of that measurement -- the re-test is checked before the
# threshold, so a 2 bps name is re-measured on schedule forever.
stale = {"AMZN": (tight, q - timedelta(days=N))}
check("a stale tight name returns for re-test", spread_gate("AMZN", stale, q)[1],
      False)
check("...and says why", spread_gate("AMZN", stale, q)[2], "retest")

# Same name, same tight score, on its own staggered slot.
p = _phase_day("AMZN")
check("a fresh tight name returns on its phase night",
      spread_gate("AMZN", {"AMZN": (tight, p - timedelta(days=1))}, p)[1], False)

# The property in the aggregate: over one full cycle every excluded symbol is
# re-tested at least once. This is the test that would fail if the stagger and
# the age backstop ever disagreed.
syms = [f"SYM{i}" for i in range(200)]
seen, loads = set(), []
for k in range(N):
    day = TODAY + timedelta(days=k)
    lookup = {s: (tight, day - timedelta(days=1)) for s in syms}
    back = [s for s in syms if not spread_gate(s, lookup, day)[1]]
    loads.append(len(back))
    seen.update(back)
check(f"every symbol re-tested within {N} days", len(seen), len(syms))
check("no night carries the whole cohort", max(loads) < len(syms), True)
check("the re-test load is spread, not pulsed", max(loads) - min(loads) <= 20,
      True)


print("\n=== 4. the floor is a veto, not an entry term ===")
# The bug this shape avoids: folded into `qualified`, the floor would exclude
# nothing on the night it was applied, because hysteresis and a live 30-day
# sticky window would both still carry the name.
qm = _quiet_day("MSFT")
prior = pd.DataFrame([{
    "symbol": "MSFT", "qualified": True, "retained": False,
    "first_entered": qm - timedelta(days=90),
    "sticky_until": qm + timedelta(days=25)}])
rows = classify(market(mkrow("MSFT", 400.0, 30_000_000)), prior, qm,
                {"MSFT": (tight, qm - timedelta(days=1))})
r = rows[0]
check("a tight incumbent still qualifies on price/volume", r["qualified"], True)
check("...but is excluded by the floor", r["spread_excluded"], True)
check("...and its stickiness is left intact", r["sticky_until"] is not None, True)

# An empty lookup must be inert, so the floor cannot silently exclude the world
# on a database that has not computed the metric yet.
rows = classify(market(mkrow("MSFT", 400.0, 30_000_000)), prior, qm, {})
check("an empty lookup excludes nothing", rows[0]["spread_excluded"], False)
rows = classify(market(mkrow("MSFT", 400.0, 30_000_000)), prior, qm)
check("an omitted lookup excludes nothing", rows[0]["spread_excluded"], False)


print(f"\n{'=' * 62}")
print(f"PASSED {len(PASS)} / {len(PASS) + len(FAIL)}")
if FAIL:
    print("FAILED:")
    for f in FAIL:
        print("  -", f)
    sys.exit(1)
print("ALL GREEN")
