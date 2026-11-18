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

DISPLAY FIELDS (read by the user's spx_analysis_dashboard, in its vocabulary)
  form         level | chg_d | chg_1w | z, derived mechanically from the name
               (form_and_base).
  base_column  the level a chg_d / chg_1w / z column is derived from, NULL
               for a level; always a metric of the same table.
  units        one of UNITS. A change inherits its level's units, except
               total_oi (a count) whose changes are percentages; every z-score
               is z_score.
  formula      the spec's formula text: OI spec §3.1 / §3.2 / §4.1, price spec
               §3 / §4 (and §4.2 for rsi_14 / macd_hist); derivatives from §5.
"""
from __future__ import annotations

import re

import lib.spx_features as F
import lib.spx_price as P
import lib.spx_table_b as B

TABLE_C = "spx_metric_catalog"
CATALOG_COLUMNS = ["metric", "table_name", "description", "family", "surfaced",
                   "form", "base_column", "units", "formula"]

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

# --- Display fields: form, base_column, units, formula ---------------------------------------------

FORMS = ("level", "chg_d", "chg_1w", "z")
# The dashboard's ten, plus pct / days / index_0_100 (added by the user for this catalog).
UNITS = ("vol_decimal", "z_score", "slope", "ratio", "price", "log_return", "corr", "int", "date", "time",
         "pct", "days", "index_0_100")


def form_and_base(m: str) -> tuple[str, str | None]:
    """The column name decides the form, mechanically."""
    if m in ("d1_total_oi_pct_change", "d5_total_oi_pct_change"):
        return ("chg_d" if m[1] == "1" else "chg_1w"), "total_oi"
    g = re.fullmatch(r"d([15])_(.+)_change", m)
    if g:
        return ("chg_d" if g.group(1) == "1" else "chg_1w"), g.group(2)
    g = re.fullmatch(r"zscore_(.+)_3m", m)
    if g:
        return "z", g.group(1)
    return "level", None


# Units of every level column (surfaced or not), by the user's list. Shares (pct_*) by rule below.
UNITS_LEVEL = {
    # pct: shares and price-relative fractions
    "pct_up_days_20d": "pct", "donchian_pos_20d": "pct", "ma20_slope_5d": "pct", "macd_hist": "pct",
    # ratio
    "oi_put_call_ratio": "ratio", "oi_put_call_ratio_0_30d": "ratio", "vol_put_call_ratio": "ratio",
    "vol_oi_ratio_all": "ratio", "vol_oi_ratio_call": "ratio", "vol_oi_ratio_put": "ratio",
    "net_new_oi_div_vol": "ratio", "rv_ratio_5d_20d": "ratio", "atr_normalized_ret_5d": "ratio",
    # days, 0-100 indexes, counters
    "weighted_avg_dte_oi": "days", "weighted_avg_dte_vol": "days",
    "rsi_14": "index_0_100", "adx_14": "index_0_100",
    "streak": "int",
    # stored-only: contract counts, and strikes / prices / ATR in index points
    "total_oi": "int", "call_oi": "int", "put_oi": "int", "total_vol": "int", "call_vol": "int", "put_vol": "int",
    "oi_weighted_all": "price", "oi_weighted_call": "price", "oi_weighted_put": "price",
    "oi_weighted_all_0_30d": "price", "vol_weighted_all": "price", "vol_weighted_call": "price",
    "vol_weighted_put": "price", "max_oi_strike_put": "price", "spot": "price", "atr_14d": "price",
}


def level_units(m: str) -> str | None:
    if m in UNITS_LEVEL:
        return UNITS_LEVEL[m]
    if m.startswith("pct_"):                 # every pct_* share, pct_from_*
        return "pct"
    if m.endswith("_div_spot"):
        return "ratio"
    return None                              # a gap: fails the build


def units_of(m: str) -> str | None:
    form, base = form_and_base(m)
    if form == "level":
        return level_units(m)
    if form == "z":
        return "z_score"
    if base == "total_oi":                   # a count whose changes are percentages
        return "pct"
    return level_units(base)


# The specs' formula text. OI spec §3.1 (table A) and §3.2 (ingredients), §4.1 (table B);
# price spec §3 (daily), §4 and §4.2 (5-minute). Backticks dropped; a definition the spec
# gives under its table (DTE, front_exp, rv_n, log_ret, atr_14d, P) is appended where the
# formula uses it.
_DTE = "DTE = expiration − trade_date"
_FRONT = "front_exp = MIN(expiration WHERE DTE ≥ 0), computed across both roots"
_STRIKES = "strike concentration is computed per strike across both roots and all expirations"
_LOGRET = "log_ret_t = LN(C_t / C_{t-1})"
_P = "P = that bar's spx_open"
_ATR = "atr_14d = mean(TR_t, t ∈ T-14..T-1), TR_t = MAX(H_t − L_t, |H_t − C_{t-1}|, |L_t − C_{t-1}|)"
_ONE_STEP = "take the smoothed state as of T-1's close, frozen, and apply exactly one step using P as \"today's close\""

FORMULA_LEVEL = {
    # OI spec §3.1
    "total_oi": "Σ(OI)",
    "oi_put_call_ratio": "put_oi / call_oi",
    "oi_put_call_ratio_0_30d": f"Σ(OI WHERE type='P' AND 0≤DTE≤30) / Σ(OI WHERE type='C' AND 0≤DTE≤30); {_DTE}",
    "vol_put_call_ratio": "total_put_vol / total_call_vol",
    "weighted_avg_dte_oi": f"Σ(DTE·OI) / Σ(OI); {_DTE}",
    "weighted_avg_dte_vol": f"Σ(DTE·volume) / Σ(volume); {_DTE}",
    "pct_oi_front_expiry": f"Σ(OI WHERE expiration = front_exp) / total_oi; {_FRONT}; {_DTE}",
    "pct_vol_front_expiry": f"Σ(volume WHERE expiration = front_exp) / total_vol; {_FRONT}; {_DTE}",
    "pct_oi_0_30d": f"Σ(OI WHERE 0≤DTE≤30) / total_oi; {_DTE}",
    "pct_vol_0_30d": f"Σ(volume WHERE 0≤DTE≤30) / total_vol; {_DTE}",
    "pct_oi_top5_strikes": f"Σ(OI of the 5 strikes with largest per-strike Σ OI) / total_oi; {_STRIKES}",
    # the spec's cell reads "same, on volume"
    "pct_vol_top5_strikes": f"Σ(volume of the 5 strikes with largest per-strike Σ volume) / total_vol; {_STRIKES}",
    "pct_oi_spxw": "Σ(OI WHERE root='SPXW') / total_oi",
    "pct_vol_spxw": "Σ(volume WHERE root='SPXW') / total_vol",
    "vol_oi_ratio_all": "total_vol / total_oi",
    "vol_oi_ratio_call": "total_call_vol / call_oi",
    "vol_oi_ratio_put": "total_put_vol / put_oi",
    "net_new_oi_div_vol": "d1_total_oi_change / total_vol",
    # OI spec §3.2 (the weighted strikes are the spec's; the five counts follow its notation)
    "call_oi": "Σ(OI WHERE type='C')",
    "put_oi": "Σ(OI WHERE type='P')",
    "total_vol": "Σ(volume)",
    "call_vol": "Σ(volume WHERE type='C')",
    "put_vol": "Σ(volume WHERE type='P')",
    "oi_weighted_all": "Σ(strike·OI) / Σ(OI)",
    "oi_weighted_call": "Σ(strike·OI WHERE type='C') / Σ(OI WHERE type='C')",
    "oi_weighted_put": "Σ(strike·OI WHERE type='P') / Σ(OI WHERE type='P')",
    "oi_weighted_all_0_30d": f"oi_weighted_all restricted to 0≤DTE≤30; {_DTE}",
    "vol_weighted_all": "Σ(strike·volume) / Σ(volume)",
    "vol_weighted_call": "Σ(strike·volume WHERE type='C') / Σ(volume WHERE type='C')",
    "vol_weighted_put": "Σ(strike·volume WHERE type='P') / Σ(volume WHERE type='P')",
    "max_oi_strike_put": "strike of the put with the largest per-strike Σ OI; ties resolve to the lowest strike",
    # OI spec §4.1
    "oi_weighted_all_div_spot": "oi_weighted_all / spot",
    "oi_weighted_call_div_spot": "oi_weighted_call / spot",
    "oi_weighted_put_div_spot": "oi_weighted_put / spot",
    "oi_weighted_all_0_30d_div_spot": "oi_weighted_all_0_30d / spot",
    "vol_weighted_all_div_spot": "vol_weighted_all / spot",
    "vol_weighted_call_div_spot": "vol_weighted_call / spot",
    "vol_weighted_put_div_spot": "vol_weighted_put / spot",
    "max_oi_strike_put_div_spot": "max_oi_strike_put / spot",
    "pct_oi_within_5pct": "Σ(OI WHERE |strike/spot − 1| ≤ 0.05) / total_oi",
    "pct_vol_within_5pct": "Σ(volume WHERE |strike/spot − 1| ≤ 0.05) / total_vol",
    "pct_oi_above_spot": "Σ(OI WHERE strike > spot) / total_oi",
    "pct_vol_above_spot": "Σ(volume WHERE strike > spot) / total_vol",
    # spot (tables B and 5-minute price): price spec §4, the definition table B shares
    "spot": _P,
    # price spec §3
    "ma20_slope_5d": "mean(C, T-20..T-1) / mean(C, T-25..T-6) − 1",
    "pct_up_days_20d": f"count(log_ret > 0 over T-20..T-1) / 20; {_LOGRET}",
    "rv_ratio_5d_20d": ("rv_5d / rv_20d; rv_n = stdev(log_ret over the n sessions ending T-1) × √252, sample "
                        f"standard deviation; {_LOGRET}"),
    "adx_14": "Wilder's ADX over the 14 sessions ending T-1",
    "streak": ("consecutive same-direction closes ending T-1; positive for up, negative for down, so +4 means "
               "four straight up closes"),
    # price spec §4
    "pct_from_ma20": f"P / mean(C, T-20..T-1) − 1; {_P}",
    "pct_from_ma50": f"P / mean(C, T-50..T-1) − 1; {_P}",
    "pct_from_52w_high": f"P / MAX(C, T-252..T-1) − 1; {_P}",
    "pct_from_52w_low": f"P / MIN(C, T-252..T-1) − 1; {_P}",
    "donchian_pos_20d": f"(P − MIN(L, T-20..T-1)) / (MAX(H, T-20..T-1) − MIN(L, T-20..T-1)); {_P}",
    # the spec printed C_{T-6}; corrected by the user to C_{T-5} (lib.spx_price.RET_BASE_BACK = 5)
    "atr_normalized_ret_5d": f"(P / C_{{T-5}} − 1) / (atr_14d / C_{{T-1}}); {_ATR}; {_P}",
    "rsi_14": f"Wilder RSI, period 14 (§4.2): {_ONE_STEP}; {_P}",
    "macd_hist": f"(MACD − signal) / P; MACD = EMA12 − EMA26, signal = EMA9(MACD) (§4.2): {_ONE_STEP}; {_P}",
    "atr_14d": _ATR,
}


def formula_of(m: str, grain: str) -> str | None:
    """grain 'daily' or 'bar'. Derivatives from §5 of both specs."""
    form, b = form_and_base(m)
    if form == "level":
        return FORMULA_LEVEL.get(m)
    bar = ", same bar of day" if grain == "bar" else ""
    if form == "z":
        win = "60 readings at the same bar of day" if grain == "bar" else "60 trailing sessions inclusive of T"
        return f"({b}_T − μ₆₀[{b}]) / σ₆₀[{b}]; {win}, sample standard deviation"
    n = 1 if form == "chg_d" else 5
    if b == "total_oi":
        return f"total_oi_T / total_oi_{{T-{n}}} − 1"
    return f"{b}_T − {b}_{{T-{n}}}{bar}"


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
    form, base = form_and_base(metric)
    grain = "bar" if table in (B.TABLE_B, P.TABLE_P5) else "daily"
    return {"metric": metric, "table_name": table, "description": " ".join(desc.split()),
            "family": family, "surfaced": bool(surfaced),
            "form": form, "base_column": base, "units": units_of(metric), "formula": formula_of(metric, grain)}


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

    # Display fields
    bad = [k for k, r in zip(keys, rows) if r["form"] not in FORMS]
    chk.check(not bad, f"every form is one of {', '.join(FORMS)} ({len(bad)} not)" + "".join(f"\n    {x}" for x in bad[:5]))
    bad = [k for k, r in zip(keys, rows) if (r["form"] == "level") != (r["base_column"] is None)]
    chk.check(not bad, f"base_column is NULL exactly on levels ({len(bad)} not)" + "".join(f"\n    {x}" for x in bad[:5]))
    have = set(keys)
    bad = [(t, m, r["base_column"]) for (t, m), r in zip(keys, rows)
           if r["base_column"] is not None and (t, r["base_column"]) not in have]
    chk.check(not bad, f"every non-level row's base_column is a metric of the same table ({len(bad)} not)"
              + "".join(f"\n    {x}" for x in bad[:5]))
    bad = [k for k, r in zip(keys, rows) if r["surfaced"] and not r["units"]]
    chk.check(not bad, f"every surfaced metric has units ({len(bad)} without)" + "".join(f"\n    {x}" for x in bad[:10]))
    bad = [k for k, r in zip(keys, rows) if not r["units"]]
    chk.check(not bad, f"every stored-only metric has units too ({len(bad)} without)" + "".join(f"\n    {x}" for x in bad[:10]))
    bad = [(k, r["units"]) for k, r in zip(keys, rows) if r["units"] and r["units"] not in UNITS]
    chk.check(not bad, f"every units value is in the dashboard vocabulary plus pct / days / index_0_100 "
                       f"({len(bad)} not)" + "".join(f"\n    {x}" for x in bad[:5]))
    bad = [k for k, r in zip(keys, rows) if not r["formula"] or "\n" in r["formula"]]
    chk.check(not bad, f"every row has a one-line formula ({len(bad)} without)" + "".join(f"\n    {x}" for x in bad[:10]))
