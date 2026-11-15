"""
lib/spx_catalog.py — the metric catalog (OI spec §9 "table C", price spec §8)
for all four SPX tables: spx_oi_daily, spx_oi_5min, spx_price_daily,
spx_price_5min. Written by build_spx_catalog.py to `spx_metric_catalog`.

One row per stored metric column: (metric, table_name, description, family,
surfaced). Key and provenance columns (trade_date, quote_time, vol_session,
ohlc_session) are not metrics and get no row.

SURFACED: everything except table A's raw total_oi, the nine levels in
lib.spx_features.UNSURFACED_LEVELS_A, and the stored-only ingredients
(table A §3.2, table B's spot, the price tables' spot and atr_14d).

FAMILIES
  Every d1 / d5 / z column takes its level's family: the column prefix
  already says what it is, so the family never repeats that (the spec's
  oi_change / volume_change were dropped by the user on 2026-11-15).
  OI tables: oi, volume, cross (table A); oi_spot, volume_spot (table B).
  Price tables: price_trend, price_range, price_vol, price_momentum.
  Stored-only columns in all four tables: ingredient.
"""
from __future__ import annotations

import lib.spx_features as F
import lib.spx_price as P
import lib.spx_table_b as B

TABLE_C = "spx_metric_catalog"
CATALOG_COLUMNS = ["metric", "table_name", "description", "family", "surfaced"]

FAMILIES_OI = {"oi", "volume", "cross", "oi_spot", "volume_spot", "ingredient"}
FAMILIES_PRICE = {"price_trend", "price_range", "price_vol", "price_momentum", "ingredient"}

# --- Table A -----------------------------------------------------------------------------

FAMILY_A = {
    "total_oi": "oi", "oi_put_call_ratio": "oi", "oi_put_call_ratio_0_30d": "oi",
    "weighted_avg_dte_oi": "oi", "pct_oi_front_expiry": "oi", "pct_oi_0_30d": "oi",
    "pct_oi_top5_strikes": "oi", "pct_oi_spxw": "oi",
    "vol_put_call_ratio": "volume", "weighted_avg_dte_vol": "volume", "pct_vol_front_expiry": "volume",
    "pct_vol_0_30d": "volume", "pct_vol_top5_strikes": "volume", "pct_vol_spxw": "volume",
    "vol_oi_ratio_all": "cross", "vol_oi_ratio_call": "cross", "vol_oi_ratio_put": "cross",
    "net_new_oi_div_vol": "cross",
}

_A_TIMING = ("Row T: open interest is the book as of T-1's close (published on T); volume is session "
             "T-1's trading.")
BASE_A = {
    "total_oi": "Total SPX + SPXW open interest, all strikes and expiries. Not surfaced: it grows from "
                "about 10M to 17M contracts over 2020-2026, so the level is a proxy for the year; use "
                "its percentage changes and z-score.",
    "oi_put_call_ratio": "Put open interest / call open interest, whole book.",
    "oi_put_call_ratio_0_30d": "Put / call open interest, expiries 0-30 days out (DTE from T).",
    "vol_put_call_ratio": "Put volume / call volume in session T-1.",
    "weighted_avg_dte_oi": "Open-interest-weighted average days to expiry of the book (DTE from T).",
    "weighted_avg_dte_vol": "Volume-weighted average days to expiry of session T-1's trading (DTE from T-1).",
    "pct_oi_0_30d": "Share of open interest expiring 0-30 days out (DTE from T).",
    "pct_vol_0_30d": "Share of session T-1's volume in contracts 0-30 days from expiry (DTE from T-1).",
    "pct_oi_top5_strikes": "Share of open interest in the 5 strikes holding the most, strikes summed "
                           "across both roots and all expiries.",
    "pct_vol_top5_strikes": "Share of session T-1's volume in the 5 most-traded strikes, summed across "
                            "both roots and all expiries.",
    "pct_oi_spxw": "Share of open interest in the SPXW root (weeklies and dailies) rather than SPX.",
    "pct_vol_spxw": "Share of session T-1's volume in the SPXW root rather than SPX.",
    "vol_oi_ratio_all": "Session T-1's volume / open interest at T-1's close: turnover of the book.",
    "vol_oi_ratio_call": "Session T-1's call volume / call open interest at T-1's close.",
    "vol_oi_ratio_put": "Session T-1's put volume / put open interest at T-1's close.",
    "net_new_oi_div_vol": "Change in total open interest from T-2's close to T-1's close / session T-1's "
                          "volume: the net share of T-1's trading that opened positions.",
    # §3.2 ingredients
    "call_oi": "Call open interest, whole book. Ingredient.",
    "put_oi": "Put open interest, whole book. Ingredient.",
    "total_vol": "Total volume of session T-1, both roots. Ingredient.",
    "call_vol": "Call volume of session T-1. Ingredient.",
    "put_vol": "Put volume of session T-1. Ingredient.",
    "oi_weighted_all": "Open-interest-weighted average strike, all contracts. Ingredient of table B.",
    "oi_weighted_call": "Open-interest-weighted average strike, calls. Ingredient of table B.",
    "oi_weighted_put": "Open-interest-weighted average strike, puts. Ingredient of table B.",
    "oi_weighted_all_0_30d": "Open-interest-weighted average strike, expiries 0-30 days out. Ingredient of table B.",
    "vol_weighted_all": "Volume-weighted average strike of session T-1, all contracts. Ingredient of table B.",
    "vol_weighted_call": "Volume-weighted average strike of session T-1, calls. Ingredient of table B.",
    "vol_weighted_put": "Volume-weighted average strike of session T-1, puts. Ingredient of table B.",
    "max_oi_strike_put": "Strike of the put with the most open interest (ties to the lowest strike). "
                         "Ingredient of table B.",
}
# Descriptions in lib.spx_features.CATALOG_DESCRIPTIONS that replace the base text outright.
_A_FULL_OVERRIDE = {"pct_oi_front_expiry", "pct_vol_front_expiry", "d1_total_oi_pct_change",
                    "d5_total_oi_pct_change"}

# --- Table B -----------------------------------------------------------------------------

_SPOT = "the bar's spot (SPX open of that 5-minute bar)"
BASE_B = {
    "oi_weighted_all_div_spot": f"Open-interest-weighted average strike / {_SPOT}.",
    "oi_weighted_call_div_spot": f"Open-interest-weighted average call strike / {_SPOT}.",
    "oi_weighted_put_div_spot": f"Open-interest-weighted average put strike / {_SPOT}.",
    "oi_weighted_all_0_30d_div_spot": f"Open-interest-weighted average strike, expiries 0-30 days out, / {_SPOT}.",
    "vol_weighted_all_div_spot": f"Volume-weighted average strike of session T-1 / {_SPOT}.",
    "vol_weighted_call_div_spot": f"Volume-weighted average call strike of session T-1 / {_SPOT}.",
    "vol_weighted_put_div_spot": f"Volume-weighted average put strike of session T-1 / {_SPOT}.",
    "max_oi_strike_put_div_spot": f"Strike of the put with the most open interest (ties to the lowest) / {_SPOT}.",
    "pct_oi_within_5pct": f"Share of open interest at strikes within 5% of {_SPOT}.",
    "pct_vol_within_5pct": f"Share of session T-1's volume at strikes within 5% of {_SPOT}.",
    "pct_oi_above_spot": f"Share of open interest at strikes above {_SPOT}.",
    "pct_vol_above_spot": f"Share of session T-1's volume at strikes above {_SPOT}.",
}
_B_TIMING = ("Open interest and volume are row T's daily book (T-1's close / session T-1); only the "
             "price moves within the day. NULL on a bar whose price is unknown.")
SPOT_DESC = ("SPX open of the 5-minute bar (index_ohlc.spx_open), the price every spot-relative value "
             "on the row uses; NULL where index_ohlc has no usable price. Ingredient.")

# --- Price tables ----------------------------------------------------------------------------

FAMILY_PRICE = {
    "ma20_slope_5d": "price_trend", "adx_14": "price_trend",
    "pct_from_ma20": "price_trend", "pct_from_ma50": "price_trend",
    "pct_from_52w_high": "price_range", "pct_from_52w_low": "price_range", "donchian_pos_20d": "price_range",
    "rv_ratio_5d_20d": "price_vol",
    # pct_up_days_20d sits next to rsi_14: both read the up-vs-down balance, one
    # unweighted and one weighted by move size, so check one isn't repeating the other.
    "pct_up_days_20d": "price_momentum", "rsi_14": "price_momentum",
    "streak": "price_momentum", "macd_hist": "price_momentum",
    # A return scaled by ATR: volatility is the denominator, not the subject.
    "atr_normalized_ret_5d": "price_momentum",
}
_PD_TIMING = "As of the prior session's close; identical at every moment of the day."
BASE_PD = {
    "ma20_slope_5d": "20-session average close over T-20..T-1 / the same average five sessions earlier "
                     "(T-25..T-6), minus 1.",
    "pct_up_days_20d": "Share of the 20 sessions T-20..T-1 that closed up. Discrete: moves in steps of 0.05.",
    "rv_ratio_5d_20d": "5-session realised volatility / 20-session realised volatility, both ending T-1 "
                       "(sample sd of daily log returns); above 1 = recent volatility running hot.",
    "adx_14": "Wilder's ADX over the 14 sessions ending T-1, recursion seeded at 2017; 0-100, trend "
              "strength regardless of direction.",
    "streak": "Consecutive same-direction closes ending T-1: +4 = four straight up closes, -2 = two "
              "straight down, 0 = unchanged close. A counter: level only, no derivatives.",
}
_P5_TIMING = ("P = the bar's SPX open; every other input is from completed sessions through T-1. NULL on "
              "a bar whose price is unknown.")
BASE_P5 = {
    "pct_from_ma20": "P / the 20-session average close (T-20..T-1), minus 1.",
    "pct_from_ma50": "P / the 50-session average close (T-50..T-1), minus 1.",
    "pct_from_52w_high": "P / the highest close of the 252 sessions T-252..T-1, minus 1. Usually <= 0, but "
                         "it goes positive when SPX trades above that high intraday: a real new high, "
                         "not a bug, never clamped.",
    "pct_from_52w_low": "P / the lowest close of the 252 sessions T-252..T-1, minus 1. Usually >= 0, but "
                        "it goes negative when SPX trades below that low intraday: real, never clamped.",
    "donchian_pos_20d": "Position of P in the prior 20 sessions' range: (P - lowest low) / (highest high - "
                        "lowest low) over T-20..T-1. Values modestly outside [0, 1] are normal and mean SPX "
                        "is trading through its prior 20-day range intraday (2020-2026: -0.79 to 1.50, "
                        "p05 0.04, p95 1.05); not a bug, never clamped.",
    "atr_normalized_ret_5d": "Five-session return to P, (P / C_{T-5} - 1), divided by atr_14d / C_{T-1}: "
                             "the move in units of daily range. The ATR is fixed for the day; the return "
                             "runs live with P.",
    "rsi_14": "Wilder RSI(14) as if today closed at P: the smoothed gain/loss state as of T-1's close plus "
              "ONE step using P. Each bar is an independent what-if, not a running update.",
    "macd_hist": "(MACD - signal) / P, MACD 12/26/9, as if today closed at P: the EMA state as of T-1's "
                 "close plus ONE step using P. Divided by P so it compares across price levels.",
}
INGREDIENT_P5 = {
    "spot": "SPX open of the 5-minute bar (index_ohlc.spx_open): P, the price every value on the row uses; "
            "NULL where index_ohlc has no usable price. Ingredient.",
    "atr_14d": "Average true range over the 14 sessions T-14..T-1, in index points; fixed for the day. "
               "Ingredient of atr_normalized_ret_5d.",
}

# --- Derivative wording ------------------------------------------------------------------------

def _derivative(m: str, kind: str, grain: str) -> str:
    """kind in d1 / d5 / z; grain 'daily' or 'bar'."""
    if grain == "daily":
        if kind == "z":
            return (f"z-score of {m} against its last 60 sessions inclusive (sample sd); NULL until 60 "
                    f"observations exist, or if the window is constant.")
        n = int(kind[1])
        return f"Change in {m} versus {'the prior session' if n == 1 else 'five sessions earlier'} (absolute)."
    if kind == "z":
        return (f"z-score of {m} against its last 60 readings at the same bar of day (sessions without a "
                f"reading at that bar are skipped; sample sd); NULL until 60 readings exist.")
    n = int(kind[1])
    return (f"Change in {m} versus the same bar {'one session' if n == 1 else 'five sessions'} earlier "
            f"(absolute); NULL if that session has no reading at this bar.")


def _row(metric, table, desc, family, surfaced) -> dict:
    return {"metric": metric, "table_name": table, "description": " ".join(desc.split()),
            "family": family, "surfaced": bool(surfaced)}


def _a_level_desc(m: str) -> str:
    cd = F.CATALOG_DESCRIPTIONS.get(m)
    if cd and m in _A_FULL_OVERRIDE:
        return cd
    base = BASE_A[m]
    return f"{base} {cd}" if cd else base


def catalog_rows() -> list[dict]:
    rows = []
    # Table A
    surfaced_a = set(F.SURFACED_A)
    for m in F.CORE_A:
        fam = FAMILY_A[m]
        rows.append(_row(m, F.TABLE_A, _a_level_desc(m) + " " + _A_TIMING, fam, m in surfaced_a))
        for d in F.derivative_names(m):
            if d in F.CATALOG_DESCRIPTIONS and d in _A_FULL_OVERRIDE:
                desc = F.CATALOG_DESCRIPTIONS[d]
            else:
                kind = "z" if d.startswith("zscore_") else d[:2]
                desc = _derivative(m, kind, "daily")
                if kind != "z":
                    desc += " Row T describes session T-1, so this compares T-1 with " + \
                            ("T-2." if kind == "d1" else "T-6.")
            rows.append(_row(d, F.TABLE_A, desc, fam, d in surfaced_a))
    for m in F.INGREDIENTS_A:
        rows.append(_row(m, F.TABLE_A, BASE_A[m], "ingredient", False))

    # Table B
    rows.append(_row("spot", B.TABLE_B, SPOT_DESC, "ingredient", False))
    for m in B.CORE_B:
        fam = "oi_spot" if m in B.OI_SIDE_B else "volume_spot"
        rows.append(_row(m, B.TABLE_B, BASE_B[m] + " " + _B_TIMING, fam, True))
        for kind in ("d1", "d5", "z"):
            rows.append(_row(P.deriv_name(m, kind), B.TABLE_B, _derivative(m, kind, "bar"), fam, True))

    # Price, daily
    for m in P.CORE_PD:
        fam = FAMILY_PRICE[m]
        rows.append(_row(m, P.TABLE_PD, BASE_PD[m] + " " + _PD_TIMING, fam, True))
        for kind in P.DERIV_PD[m]:
            desc = _derivative(m, kind, "daily")
            if kind != "z":
                desc += " Each row is as of the prior session's close."
            rows.append(_row(P.deriv_name(m, kind), P.TABLE_PD, desc, fam, True))

    # Price, 5-minute
    for m in ("spot", "atr_14d"):
        rows.append(_row(m, P.TABLE_P5, INGREDIENT_P5[m], "ingredient", False))
    for m in P.CORE_P5:
        fam = FAMILY_PRICE[m]
        rows.append(_row(m, P.TABLE_P5, BASE_P5[m] + " " + _P5_TIMING, fam, True))
        for kind in P.DERIV_P5[m]:
            rows.append(_row(P.deriv_name(m, kind), P.TABLE_P5, _derivative(m, kind, "bar"), fam, True))
    return rows


# Columns of each table that are keys or provenance, not metrics.
NON_METRIC = {
    F.TABLE_A: {"trade_date", "vol_session"},
    B.TABLE_B: {"trade_date", "quote_time", "vol_session"},
    P.TABLE_PD: {"trade_date", "ohlc_session"},
    P.TABLE_P5: {"trade_date", "quote_time", "ohlc_session"},
}
TABLE_COLUMNS = {F.TABLE_A: F.COLUMNS_A, B.TABLE_B: B.COLUMNS_B, P.TABLE_PD: P.COLUMNS_PD, P.TABLE_P5: P.COLUMNS_P5}
EXPECTED_SURFACED = {F.TABLE_A: 62, B.TABLE_B: 48, P.TABLE_PD: 15, P.TABLE_P5: 30}


def check_catalog(rows: list[dict], chk: F.Checker, db_columns: dict | None = None) -> None:
    """Hard checks. db_columns = {table: set of column names in the live table}, if available."""
    keys = [(r["table_name"], r["metric"]) for r in rows]
    chk.check(len(keys) == len(set(keys)), f"one catalog row per (table, metric): {len(keys)} rows, "
                                           f"{len(keys) - len(set(keys))} duplicate(s)")
    for t, cols in TABLE_COLUMNS.items():
        want = set(cols) - NON_METRIC[t]
        have = {m for tt, m in keys if tt == t}
        chk.check(have == want, f"{t}: catalog covers exactly its {len(want)} stored metric columns "
                                f"(missing {sorted(want - have)}, extra {sorted(have - want)})")
        n_s = sum(1 for r in rows if r["table_name"] == t and r["surfaced"])
        chk.check(n_s == EXPECTED_SURFACED[t], f"{t}: {n_s} surfaced (spec: {EXPECTED_SURFACED[t]})")
        if db_columns is not None:
            live = db_columns.get(t)
            if live is None:
                chk.check(False, f"{t}: table not found in the destination database")
            else:
                expect = set(cols) | {"built_at"}
                chk.check(live == expect, f"{t}: live table's columns are exactly the catalogued metrics plus "
                                          f"keys (missing {sorted(expect - live)}, extra {sorted(live - expect)})")
    oi_t = {F.TABLE_A, B.TABLE_B}
    bad_f = [(r["table_name"], r["metric"], r["family"]) for r in rows
             if r["family"] not in (FAMILIES_OI if r["table_name"] in oi_t else FAMILIES_PRICE)]
    chk.check(not bad_f, f"every family is one the specs name ({len(bad_f)} not)" + "".join(f"\n    {x}" for x in bad_f[:5]))
    ing = [r for r in rows if r["family"] == "ingredient"]
    chk.check(all(not r["surfaced"] for r in ing), f"no ingredient is surfaced ({len(ing)} ingredients)")
    unsurf = {(F.TABLE_A, m) for m in F.UNSURFACED_LEVELS_A} | {(F.TABLE_A, "total_oi")}
    wrong = [k for k in unsurf if any(r["table_name"] == k[0] and r["metric"] == k[1] and r["surfaced"] for r in rows)]
    chk.check(not wrong, f"total_oi and the nine UNSURFACED_LEVELS_A are not surfaced ({len(wrong)} are)")
    bad_d = [k for k, r in zip(keys, rows) if not r["description"] or "\n" in r["description"]]
    chk.check(not bad_d, f"every description is one non-empty line ({len(bad_d)} not)")
    n = sum(r["surfaced"] for r in rows)
    chk.check(n == sum(EXPECTED_SURFACED.values()), f"{n} surfaced metrics in all (110 OI + 45 price = 155)")
