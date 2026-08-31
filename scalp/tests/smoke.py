"""Execute every entry point and the whole metric path. No socket, no database.

    python -m scalp.tests.smoke

Three tiers, cheapest first, each catching what the one before it cannot:

  1. CROSS-MODULE REFERENCES (scalp.tests.check_references) — stdlib only, so
     it runs even where pandas is not installed. Catches a call to a function
     that does not exist.

  2. IMPORT AND ARGPARSE — runs `--help` on every script in a subprocess. That
     executes all module-level code and builds the full argument parser, so an
     import error, a bad default, or a malformed argparse call fails here
     rather than on the VPS.

  3. THE METRIC PATH ON A SYNTHETIC FRAME — builds a small trade_quote-shaped
     DataFrame and runs prepare -> compute_window -> compute_buckets ->
     cross_source_metrics, plus the quality guard. This is the tier that
     executes code rather than inspecting it, and it is where a runtime error
     in the metric layer shows up.

WHAT THIS IS FOR. Three bugs have reached the VPS as code that was written,
byte-compiled, pushed and never run: `td.today_et` never defined,
`profile_compute` calling a deleted function, and `free_space_gb` crashing on
a path that does not exist yet. py_compile passes all three, because it checks
syntax and stops.

The synthetic frame deliberately includes the awkward cases rather than a
clean tape: duplicate timestamps, a crossed quote, an excluded condition code,
an odd lot below the tiered round lot, a TRF print, and a separate flat tape
for the near-zero-noise ratio guard. A smoke test on tidy data proves only
that the happy path runs.
"""
from __future__ import annotations

import subprocess
import sys
from datetime import date

SCRIPTS = [
    "scalp.update_universe",
    "scalp.fetch",
    "scalp.compute",
    "scalp.prune",
    "scalp.profile_compute",
    "scalp.quality",
    "scalp.step0.s0_availability",
    "scalp.step0.s1_venue_check",
    "scalp.step0.s2_one_day",
    "scalp.step0.s3_multiday_timing",
    "scalp.step0.s4_conditions",
    "scalp.step0.s5_quote_emission",
    "scalp.step0.s6_session_bounds",
    "scalp.step0.s7_quote_sizing",
]

SESSION = date(2026, 8, 28)

# A size that lands between the tiered round lot (40 at ~$330) and a fixed 100.
# The odd-lot assertions are meaningless without one.
TIER_PROBE_SIZE = 60
TIER_PROBE_PRICE = 330.13


def rule(title: str) -> None:
    print()
    print("-" * 70)
    print(title)
    print("-" * 70)


# --- tier 1 ------------------------------------------------------------------

def tier_references() -> int:
    rule("1. cross-module references")
    from scalp.tests import check_references
    problems = check_references.check()
    if problems:
        for problem in problems:
            print(f"  {problem}")
        print(f"\n  {len(problems)} unresolved reference(s)")
        return len(problems)
    print("  ok — every module.attr resolves")
    return 0


# --- tier 2 ------------------------------------------------------------------

def tier_help() -> int:
    rule("2. import + argparse, via --help")
    failures = 0
    for module in SCRIPTS:
        proc = subprocess.run([sys.executable, "-m", module, "--help"],
                              capture_output=True, text=True, timeout=180)
        if proc.returncode == 0:
            print(f"  ok    {module}")
        else:
            failures += 1
            tail = (proc.stderr or proc.stdout).strip().splitlines()
            print(f"  FAIL  {module}")
            for line in tail[-4:]:
                print(f"          {line}")
    return failures


# --- tier 3 ------------------------------------------------------------------

def synthetic_trade_quote():
    """A trade_quote-shaped frame containing the awkward cases on purpose."""
    import pandas as pd

    base = pd.Timestamp(f"{SESSION.isoformat()} 09:30:00")
    rows = []

    def add(offset_ms, price, size, bid, ask, exch=3, cond=0, ext1=255):
        stamp = base + pd.Timedelta(milliseconds=offset_ms)
        rows.append({
            "trade_timestamp": stamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
            "price": price, "size": size, "exchange": exch,
            "condition": cond, "ext_condition1": ext1,
            "ext_condition2": 255, "ext_condition3": 255, "ext_condition4": 255,
            "bid": bid, "ask": ask, "bid_size": 40, "ask_size": 40,
            "bid_condition": 0, "ask_condition": 0,
        })

    # A normal morning at an FDX-like price, so the round lot is 40, not 100.
    for i in range(400):
        drift = 330.00 + (i % 7) * 0.01
        add(i * 1_500, drift, 100 if i % 3 else 10, drift - 0.05, drift + 0.05)

    # Duplicate timestamps — the collapse path.
    add(600_000, 330.10, 200, 330.05, 330.15)
    add(600_000, 330.11, 300, 330.06, 330.16)

    # A crossed quote, which spread_metrics must drop and report.
    add(660_000, 330.20, 100, 330.30, 330.10)

    # An excluded condition code (96 DERIVATIVE), well off the mid.
    add(720_000, 335.00, 500, 330.05, 330.15, cond=96)

    # A restatement (51) that must not be counted as an arrival.
    add(780_000, 330.12, 900, 330.07, 330.17, cond=51)

    # An odd lot under the 40-share tiered round lot.
    add(840_000, 330.13, 5, 330.08, 330.18)

    # THE TIER PROBE. At ~$330 the round lot is 40, so 60 shares is a ROUND
    # lot under the tiered rule and an ODD lot under a fixed 100. Without a
    # print in the 40-99 band the two rules are arithmetically identical on
    # this fixture and the assertion below cannot fail, whatever the code
    # does. That is exactly how it passed vacuously the first time.
    add(870_000, 330.13, TIER_PROBE_SIZE, 330.08, 330.18)

    # A TRF print, for off_exchange_share.
    add(900_000, 330.14, 150, 330.09, 330.19, exch=57)

    return pd.DataFrame(rows)


def flat_trade_quote():
    """A near-zero-noise tape — the SGOV/BOXX case that produced 7e11 ratios."""
    import pandas as pd

    base = pd.Timestamp(f"{SESSION.isoformat()} 09:30:00")
    rows = []
    for i in range(300):
        stamp = base + pd.Timedelta(seconds=i * 2)
        rows.append({
            "trade_timestamp": stamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
            "price": 100.00, "size": 100, "exchange": 3,
            "condition": 0, "ext_condition1": 255, "ext_condition2": 255,
            "ext_condition3": 255, "ext_condition4": 255,
            "bid": 99.99, "ask": 100.01, "bid_size": 40, "ask_size": 40,
            "bid_condition": 0, "ask_condition": 0,
        })
    return pd.DataFrame(rows)


def tier_metrics() -> int:
    rule("3. metric path on a synthetic frame")
    import math

    from scalp import compute, config, metrics, quality

    failures = 0

    def check(label: str, condition: bool, detail: str = "") -> None:
        nonlocal failures
        if condition:
            print(f"  ok    {label}")
        else:
            failures += 1
            print(f"  FAIL  {label}  {detail}")

    df, cols = compute.prepare(synthetic_trade_quote(), SESSION)
    start, end = compute.session_bounds(SESSION)

    check("prepare attached condition flags",
          metrics.FLAG_EXCLUDED in df.columns)

    daily = metrics.compute_window(df, cols, start, end)
    prov = daily.pop("_provenance", {})

    check("daily row produced metrics", len(daily) > 40, f"got {len(daily)}")
    check("spread_bps_tw is finite",
          math.isfinite(daily.get("spread_bps_tw", float("nan"))),
          str(daily.get("spread_bps_tw")))
    check("trades_per_min > 0", daily.get("trades_per_min", 0) > 0)
    check("round lot resolved to 40 at ~$330",
          daily.get("round_lot_size") == 40, str(daily.get("round_lot_size")))
    # --- odd lot: prove the metric consumes the tier ----------------------
    # Three separate checks, so a failure says WHICH thing broke rather than
    # only that two numbers matched.
    window = metrics.slice_window(df, cols.time, start, end)
    kept = window[~metrics.excluded_mask(window, cols.condition_cols)]
    kept_sizes = kept[cols.size]
    lot = daily.get("round_lot_size")
    band = kept_sizes[(kept_sizes >= lot) & (kept_sizes < 100)]

    check(f"FIXTURE exercises the tier: a print in the {lot:.0f}-99 band",
          len(band) > 0,
          f"no size between {lot:.0f} and 99 in the fixture, so size<{lot:.0f} "
          f"and size<100 select the same rows and the checks below cannot "
          f"fail whatever the code does")

    expected_tiered = float((kept_sizes < lot).mean())
    expected_sub100 = float((kept_sizes < 100).mean())

    check(f"odd_lot_share counts size<{lot:.0f}, the tiered round lot",
          abs(daily.get("odd_lot_share", -1) - expected_tiered) < 1e-9,
          f"got {daily.get('odd_lot_share')}, tier-based expects "
          f"{expected_tiered:.6f}; a fixed-100 rule would give "
          f"{expected_sub100:.6f} — if it matches the latter the metric is "
          f"ignoring round_lot_size")

    check("sub_100_share counts size<100, unchanged",
          abs(daily.get("sub_100_share", -1) - expected_sub100) < 1e-9,
          f"got {daily.get('sub_100_share')}, expected {expected_sub100:.6f}")

    check(f"the {TIER_PROBE_SIZE}-share print is a ROUND lot at "
          f"${TIER_PROBE_PRICE:.2f} but sub-100",
          daily.get("odd_lot_share") < daily.get("sub_100_share"),
          f"odd_lot_share={daily.get('odd_lot_share')} is not below "
          f"sub_100_share={daily.get('sub_100_share')}; the "
          f"{TIER_PROBE_SIZE}-share print should be excluded from the first "
          f"and included in the second")
    check("off_exchange_share > 0 (TRF print present)",
          daily.get("off_exchange_share", 0) > 0)
    check("excluded prints were dropped", daily.get("rows_excluded", 0) >= 2,
          str(daily.get("rows_excluded")))
    check("crossed quote reported, not silently kept",
          daily.get("crossed_locked_share", 0) > 0)

    check("provenance produced", bool(prov))
    check("provenance breaks out per code",
          any(k.startswith("dropped_condition_") for k in prov))
    check("retained share <= 1", prov.get("trade_retained_share", 1) <= 1.0)

    buckets = metrics.compute_buckets(df, cols, start, end,
                                      config.INTRADAY_BUCKET_MINUTES)
    check("intraday buckets produced", len(buckets) > 1, str(len(buckets)))
    check("buckets carry no provenance",
          all("_provenance" not in b for b in buckets))

    # The near-zero-noise guard.
    flat_df, flat_cols = compute.prepare(flat_trade_quote(), SESSION)
    flat = metrics.compute_window(flat_df, flat_cols, start, end)
    flat.pop("_provenance", None)
    ratios = [v for k, v in flat.items() if k.startswith("ratio_")]
    huge = [v for v in ratios
            if isinstance(v, float) and math.isfinite(v) and abs(v) > 1e6]
    check("flat tape produces no absurd ratio", not huge,
          f"{len(huge)} ratio(s) above 1e6 — the SGOV case is back")

    # cross_source_metrics with the quote side absent, then present.
    check("cross_source with no quote data returns nothing",
          metrics.cross_source_metrics(daily, None) == {})
    cross = metrics.cross_source_metrics(
        daily, {"quote_records": 1000, "window_minutes": 390})
    check("quotes_per_trade computed when both sides present",
          math.isfinite(cross.get("quotes_per_trade", float("nan"))))

    # The restatement guard, on the same frame.
    audit = quality.audit_symbol_day(
        synthetic_trade_quote(), size_col="size", price_col="price",
        exchange_col="exchange", time_col="trade_timestamp",
        condition_cols=["condition", "ext_condition1"])
    check("quality audit runs", audit.get("rows", 0) > 0)

    return failures


def main() -> None:
    failures = tier_references()

    try:
        import pandas  # noqa: F401
    except ImportError:
        rule("2. and 3. SKIPPED")
        print("  pandas is not installed here, so the import and metric tiers")
        print("  cannot run. Tier 1 above is the part that runs anywhere, and")
        print("  it is the one that catches a call to a function that does not")
        print("  exist. Run the full check where the pipeline's deps live.")
        sys.exit(1 if failures else 0)

    failures += tier_help()
    failures += tier_metrics()

    print()
    if failures:
        print(f"SMOKE TEST FAILED — {failures} problem(s)")
        sys.exit(1)
    print("SMOKE TEST PASSED")


if __name__ == "__main__":
    main()
