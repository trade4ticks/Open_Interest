"""
test_spx_catalog.py — the metric catalog, offline. Runs the build's hard
checks against simulated live tables (one correct, one with an uncatalogued
column) and spot-checks rows the specs single out.

Run:  python test_spx_catalog.py     (exit 1 on any failure)
"""
from __future__ import annotations

import sys

import lib.spx_catalog as C
import lib.spx_features as F
import lib.spx_price as P
import lib.spx_table_b as B

FAILS: list[str] = []


def ok(cond, label: str) -> None:
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
    if not cond:
        FAILS.append(label)


def _base(c: str) -> str:
    """d1_X_change / d1_X_pct_change / d5_... / zscore_X_3m -> X."""
    if c.startswith("zscore_"):
        return c[len("zscore_"):-len("_3m")]
    c = c[3:]
    return c[:-len("_pct_change")] if c.endswith("_pct_change") else c[:-len("_change")]


def main() -> int:
    rows = C.catalog_rows()
    by = {(r["table_name"], r["metric"]): r for r in rows}

    print("build hard checks against matching live tables")
    live = {t: set(cols) | {"built_at"} for t, cols in C.TABLE_COLUMNS.items()}
    chk = F.Checker()
    C.check_catalog(rows, chk, live)
    ok(not chk.failed, "all pass")

    print("\n... and against a table with an uncatalogued column")
    live[P.TABLE_P5] = live[P.TABLE_P5] | {"new_metric"}
    chk2 = F.Checker()
    C.check_catalog(rows, chk2, live)
    ok(len(chk2.failed) == 1 and P.TABLE_P5 in chk2.failed[0], "exactly that table fails")

    print("\ncounts")
    ok(len(rows) == 85 + 49 + 15 + 32, f"{len(rows)} rows = 85 table A + 49 table B + 15 + 32 price")
    ok(sum(r["surfaced"] for r in rows) == 155, "155 surfaced")

    print("\nrows the specs single out")
    for m in F.UNSURFACED_LEVELS_A + ["total_oi"]:
        r = by[(F.TABLE_A, m)]
        ok(not r["surfaced"] and r["family"] in ("oi", "volume", "cross"), f"{m}: not surfaced, family {r['family']}")
    ok("year over year" in by[(F.TABLE_A, "vol_oi_ratio_call")]["description"]
       and "call volume" in by[(F.TABLE_A, "vol_oi_ratio_call")]["description"],
       "an unsurfaced level keeps its formula and gets the not-surfaced reason")
    ok("zero overlap" in by[(F.TABLE_A, "pct_oi_top5_strikes")]["description"], "zero-overlap note on pct_oi_top5_strikes")
    ok("NOT the volume counterpart" not in by[(F.TABLE_A, "pct_oi_front_expiry")]["description"]
       and "NOT the OI counterpart" in by[(F.TABLE_A, "pct_oi_front_expiry")]["description"],
       "front-expiry asymmetry text carried")
    ok("T-2's close to T-1's close" in by[(F.TABLE_A, "d1_total_oi_pct_change")]["description"],
       "d1_total_oi_pct_change says it spans T-2 -> T-1")
    lvl = {(r["table_name"], r["metric"]): r["family"] for r in rows}
    deriv_ok = all(r["family"] == lvl[(r["table_name"], _base(r["metric"]))] for r in rows
                   if r["metric"].startswith(("d1_", "d5_", "zscore_")))
    ok(deriv_ok, "every d1 / d5 / z column takes its level's family")
    ok(not {r["family"] for r in rows} & {"oi_change", "volume_change"}, "no oi_change / volume_change")
    ok(by[(P.TABLE_PD, "pct_up_days_20d")]["family"] == by[(P.TABLE_P5, "rsi_14")]["family"] == "price_momentum"
       and by[(P.TABLE_P5, "atr_normalized_ret_5d")]["family"] == "price_momentum",
       "pct_up_days_20d with rsi_14 in price_momentum; atr_normalized_ret_5d stays there")
    ok(all(by[(F.TABLE_A, m)]["family"] == "ingredient" and not by[(F.TABLE_A, m)]["surfaced"]
           for m in F.INGREDIENTS_A), "13 §3.2 ingredients")
    ok(by[(B.TABLE_B, "zscore_pct_vol_above_spot_3m")]["family"] == "volume_spot"
       and by[(B.TABLE_B, "max_oi_strike_put_div_spot")]["family"] == "oi_spot", "table B families")
    dn = by[(P.TABLE_P5, "donchian_pos_20d")]["description"]
    ok("outside [0, 1] are normal" in dn and "-0.79 to 1.50" in dn, "donchian says out-of-range is normal")
    ok("positive" in by[(P.TABLE_P5, "pct_from_52w_high")]["description"], "52w high says it can go positive")
    ok("C_{T-5}" in by[(P.TABLE_P5, "atr_normalized_ret_5d")]["description"], "atr_normalized_ret_5d says C_{T-5}")
    ok("ONE step" in by[(P.TABLE_P5, "rsi_14")]["description"], "rsi_14 states the one-step rule")
    ok((P.TABLE_PD, "d1_streak_change") not in by and (P.TABLE_P5, "d5_macd_hist_change") not in by,
       "§5 exclusions have no rows")
    ok("same bar" in by[(P.TABLE_P5, "d1_rsi_14_change")]["description"], "5-minute d1 is same-bar")
    ok(not any(r["metric"] in ("trade_date", "quote_time", "vol_session", "ohlc_session") for r in rows),
       "no key / provenance columns catalogued")

    print(f"\n{'ALL PASS' if not FAILS else f'{len(FAILS)} FAILURE(S)'}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
