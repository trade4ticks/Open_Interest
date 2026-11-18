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

    print("\ndisplay fields: form / base_column")
    A, P5, PD = F.TABLE_A, P.TABLE_P5, P.TABLE_PD
    for (t, m), want in {
        (A, "d1_oi_put_call_ratio_change"): ("chg_d", "oi_put_call_ratio"),
        (A, "d5_weighted_avg_dte_oi_change"): ("chg_1w", "weighted_avg_dte_oi"),
        (A, "zscore_total_oi_3m"): ("z", "total_oi"),
        (A, "d1_total_oi_pct_change"): ("chg_d", "total_oi"),
        (A, "d5_total_oi_pct_change"): ("chg_1w", "total_oi"),
        (A, "pct_oi_spxw"): ("level", None),
        (A, "call_oi"): ("level", None),
        (B.TABLE_B, "zscore_oi_weighted_all_0_30d_div_spot_3m"): ("z", "oi_weighted_all_0_30d_div_spot"),
        (PD, "d5_pct_up_days_20d_change"): ("chg_1w", "pct_up_days_20d"),
        (P5, "d1_atr_normalized_ret_5d_change"): ("chg_d", "atr_normalized_ret_5d"),
    }.items():
        r = by[(t, m)]
        ok((r["form"], r["base_column"]) == want, f"{m}: {r['form']}, {r['base_column']}")
    ok({r["form"] for r in rows} == set(C.FORMS), "all four forms occur, nothing else")
    ok(sum(r["form"] == "level" for r in rows) == 31 + 13 + 5 + 10, "levels: 31 A + 13 B + 5 + 10")

    print("\ndisplay fields: units")
    for (t, m), want in {
        (A, "pct_oi_0_30d"): "pct", (A, "d1_total_oi_pct_change"): "pct", (A, "d5_total_oi_pct_change"): "pct",
        (A, "total_oi"): "int", (A, "zscore_total_oi_3m"): "z_score",
        (A, "d1_weighted_avg_dte_oi_change"): "days", (A, "weighted_avg_dte_vol"): "days",
        (A, "net_new_oi_div_vol"): "ratio", (A, "d5_vol_oi_ratio_put_change"): "ratio",
        (B.TABLE_B, "max_oi_strike_put_div_spot"): "ratio", (B.TABLE_B, "pct_vol_above_spot"): "pct",
        (PD, "streak"): "int", (PD, "adx_14"): "index_0_100", (PD, "ma20_slope_5d"): "pct",
        (PD, "pct_up_days_20d"): "pct", (PD, "rv_ratio_5d_20d"): "ratio",
        (P5, "rsi_14"): "index_0_100", (P5, "d1_rsi_14_change"): "index_0_100", (P5, "macd_hist"): "pct",
        (P5, "donchian_pos_20d"): "pct", (P5, "pct_from_52w_low"): "pct", (P5, "atr_normalized_ret_5d"): "ratio",
        (P5, "zscore_rsi_14_3m"): "z_score", (P5, "spot"): "price", (P5, "atr_14d"): "price",
    }.items():
        ok(by[(t, m)]["units"] == want, f"{m}: {by[(t, m)]['units']}")
    ok(all(r["units"] == "z_score" for r in rows if r["form"] == "z"), "every z-score is z_score")
    ok(all(r["units"] == by[(r["table_name"], r["base_column"])]["units"] for r in rows
           if r["form"] in ("chg_d", "chg_1w") and r["base_column"] != "total_oi"),
       "every change inherits its level's units (total_oi aside)")
    ok({r["units"] for r in rows if r["surfaced"]} <= set(C.UNITS) - {"vol_decimal", "slope", "price", "log_return",
                                                                        "corr", "date", "time"},
       "surfaced units drawn only from the user's list")

    print("\n... a metric with no units fails the build")
    saved = C.UNITS_LEVEL.pop("streak")
    try:
        chk3 = F.Checker()
        C.check_catalog(C.catalog_rows(), chk3)
    finally:
        C.UNITS_LEVEL["streak"] = saved
    ok(any("surfaced metric has units" in f and "streak" in f for f in chk3.failed), "streak without units: FAIL")

    print("\n... a base_column that is not in the table fails the build")
    bad_rows = [dict(r) for r in rows]
    next(r for r in bad_rows if r["metric"] == "d1_rsi_14_change")["base_column"] = "rsi_99"
    chk4 = F.Checker()
    C.check_catalog(bad_rows, chk4)
    ok(len(chk4.failed) == 1 and "base_column" in chk4.failed[0], "exactly the base_column check fails")

    print("\ndisplay fields: formula")
    ok(by[(A, "oi_put_call_ratio_0_30d")]["formula"].startswith(
        "Σ(OI WHERE type='P' AND 0≤DTE≤30) / Σ(OI WHERE type='C' AND 0≤DTE≤30)"), "§3.1 text, verbatim")
    ok(by[(B.TABLE_B, "pct_oi_within_5pct")]["formula"] == "Σ(OI WHERE |strike/spot − 1| ≤ 0.05) / total_oi",
       "§4.1 text, the escaped | unescaped")
    ok("C_{T-5}" in by[(P5, "atr_normalized_ret_5d")]["formula"]
       and "C_{T-6}" not in by[(P5, "atr_normalized_ret_5d")]["formula"], "atr_normalized_ret_5d: C_{T-5}, as corrected")
    ok("exactly one step" in by[(P5, "macd_hist")]["formula"], "macd_hist carries §4.2's one-step rule")
    ok(by[(A, "d5_total_oi_pct_change")]["formula"] == "total_oi_T / total_oi_{T-5} − 1", "total_oi pct change")
    ok("same bar of day" in by[(P5, "zscore_rsi_14_3m")]["formula"]
       and "same bar" not in by[(PD, "zscore_adx_14_3m")]["formula"], "z formula: bar vs session")
    ok(all(r["formula"] and "`" not in r["formula"] and "\\|" not in r["formula"] for r in rows),
       "every row has a formula, no markdown left in it")

    print(f"\n{'ALL PASS' if not FAILS else f'{len(FAILS)} FAILURE(S)'}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
