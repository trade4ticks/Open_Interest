"""
test_spx_price_5min.py — spx_price_5min compute on a synthetic index_ohlc,
with no database. The mock is test_spx_price's (a row for every calendar day
09:30-16:00, zero-filled closed days, padded early closes, a priced 16:00 bar),
over a longer span so the 52-week window fills.

Covers spec §9 for the intraday table:
  * §4.2 RECURSION: two bars of one day with the same P give identical rsi_14
    and macd_hist whatever happens between them, and equal ONE step from the
    T-1 state; one frozen step with P = C_T reproduces the T-close state
  * hand-computed rsi (period 3), macd_hist (2 / 3 / 2) and donchian_pos_20d
  * every level's window offsets against explicit slicing, incl. C_{T-5}
  * lookahead: T's high / low / close rewritten -> every row of T unchanged;
    later opens rewritten -> earlier bars unchanged; the build's probe passes
  * §4.1: planted breakouts leave donchian > 1, < 0 and the 52-week metrics
    past 0, unclamped, and the report counts them
  * an unknown price: row exists, levels NULL; same-bar d1 next session NULL
  * grid 78 / 42, no 16:00; z gate; one-day build == full build; hard checks
  * the independent verify path agrees bar for bar

Run:  python test_spx_price_5min.py     (exit 1 on any failure)
"""
from __future__ import annotations

import math
import statistics
import sys
from datetime import date, time
from fractions import Fraction as Q

import numpy as np
import pandas as pd

import lib.spx_features as F
import lib.spx_price as P
import test_spx_price as TP
import verify_spx_price_5min as V5
from lib.market_hours import get_trading_days

FAILS: list[str] = []


def ok(cond, label: str) -> None:
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
    if not cond:
        FAILS.append(label)


def close(a, b, label: str, rel: float = 1e-12) -> None:
    good = a is not None and not pd.isna(a) and math.isclose(float(a), float(b), rel_tol=rel, abs_tol=1e-14)
    ok(good, f"{label}: got {a!r}, expected {b!r}")


def isnull(v) -> bool:
    return v is None or pd.isna(v)


SEED = date(2023, 1, 1)
LAST = date(2025, 9, 30)
SESSIONS = get_trading_days(SEED, LAST)
TI = 560                                    # target day; 300+ clean sessions behind it
T = SESSIONS[TI]
T_UP, T_DOWN = SESSIONS[TI + 10], SESSIONS[TI + 12]
UNK_BAR = time(11, 0)                       # T's 11:00 open is NaN
SAME_A, SAME_B, WILD = time(10, 0), time(14, 0), time(12, 0)
HALF = date(2024, 11, 29)

TP.SEED, TP.LAST, TP.SESSIONS = SEED, LAST, SESSIONS
TP.NAN_DAY, TP.NONE_DAY, TP.ZERO_HIGH_DAY = SESSIONS[620], SESSIONS[640], SESSIONS[120]


def mock() -> pd.DataFrame:
    bars = TP.mock_bars()
    at = lambda d, q: (bars["trade_date"] == d) & (bars["quote_time"] == q)
    x = float(bars.loc[at(T, SAME_A), "spx_open"].iloc[0])
    bars.loc[at(T, SAME_B), "spx_open"] = x                      # same P twice on T
    bars.loc[at(T, WILD), "spx_open"] = x * 1.3                  # something wild in between
    bars.loc[at(T, UNK_BAR), "spx_open"] = float("nan")
    top = bars.loc[bars["trade_date"] < T_UP, "spx_close"].max()
    bot = bars.loc[(bars["trade_date"] < T_DOWN) & (bars["spx_close"] > 0), "spx_close"].min()
    bars.loc[at(T_UP, time(10, 30)), "spx_open"] = top * 1.05     # through the 20-day and 52-week highs
    bars.loc[at(T_DOWN, time(10, 30)), "spx_open"] = bot * 0.95   # through the 20-day and 52-week lows
    return bars


def build(start, end, bars):
    return P.compute_price_5min(start, end, bars=bars, seed_from=SEED)


def main() -> int:
    bars = mock()
    res = build(SESSIONS[300], LAST, bars)
    full = res.full.set_index(["trade_date", "quote_time"])
    daily = res.daily.reset_index(drop=True)
    C, H, L = list(daily["close"]), list(daily["high"]), list(daily["low"])
    j = TI - 1                                   # T-1's position in res.daily (daily starts at SEED)
    ok(daily["trade_date"].iloc[j] == SESSIONS[TI - 1], "daily positions line up with SESSIONS")

    print("grid")
    per = res.frame.groupby("trade_date").size()
    short = per[per != 78]
    ok(short.eq(42).all() and HALF in short.index and len(short) == 4,
       f"78 bars a regular session, 42 on each early close ({', '.join(map(str, short.index))})")
    ok(not (res.frame["quote_time"] == time(16, 0)).any(), "no 16:00 bar")

    print("\n§4.2 recursion: one frozen step per bar")
    a, b = full.loc[(T, SAME_A)], full.loc[(T, SAME_B)]
    ok(a["spot"] == b["spot"], f"{SAME_A} and {SAME_B} on {T} carry the same P ({a['spot']:.4f})")
    ok(a["rsi_14"] == b["rsi_14"] and a["macd_hist"] == b["macd_hist"],
       f"identical rsi_14 ({a['rsi_14']:.10f}) and macd_hist ({a['macd_hist']:.3e}) despite a 30% jump at {WILD}")
    ag, al = P.wilder_rsi_state(C)
    ef, es, sg = P.macd_state(C)
    close(a["rsi_14"], P.rsi_step(ag[j], al[j], C[j], a["spot"]), "rsi_14 = ONE step from the T-1 state")
    close(a["macd_hist"], P.macd_hist_step(ef[j], es[j], sg[j], a["spot"]), "macd_hist = ONE step from the T-1 state")
    close(P.rsi_step(ag[j], al[j], C[j], C[j + 1]), 100 * ag[j + 1] / (ag[j + 1] + al[j + 1]),
          "frozen step with P = C_T reproduces RSI at T's close")
    m2 = ef[j + 1] - es[j + 1]
    close(P.macd_hist_step(ef[j], es[j], sg[j], C[j + 1]), (m2 - sg[j + 1]) / C[j + 1],
          "frozen step with P = C_T reproduces the MACD histogram at T's close", rel=1e-9)
    ran = full.loc[T, "rsi_14"].dropna()
    ok(ran.nunique() > 10, f"rsi_14 still varies across T's bars with P ({ran.nunique()} distinct values)")

    print("\nhand-computed fixtures")
    # RSI(3): closes 44 45 44.5 46 45 47 -> changes +1 -.5 +1.5 -1 +2.
    # seed ag = (1+0+1.5)/3 = 5/6, al = .5/3 = 1/6; then 5/9, 4/9; then 28/27, 8/27.
    # P = 46.5: ag' = (2*28/27)/3 = 56/81, al' = (2*8/27 + .5)/3 = 59/162 -> 100*112/171 = 11200/171
    # P = 48  : ag' = (56/27 + 1)/3 = 83/81, al' = 16/81 -> 8300/99
    g3, l3 = P.wilder_rsi_state([44, 45, 44.5, 46, 45, 47], 3)
    close(g3[5], 28 / 27, "RSI(3) avg gain after 5 changes")
    close(l3[5], 8 / 27, "RSI(3) avg loss after 5 changes")
    r = P.rsi_step(g3[5], l3[5], 47, np.array([46.5, 48.0]), 3)
    close(r[0], 11200 / 171, "RSI(3) one step, P = 46.5")
    close(r[1], 8300 / 99, "RSI(3) one step, P = 48")
    # MACD(2, 3, 2): closes 10 11 12 11 13 14. EMA2 21/2 23/2 67/6 223/18 727/54; EMA3 11 11 12 13;
    # MACD 1/2 1/6 7/18 25/54; signal 1/3 10/27 35/81. P = 13: EMA2' = 1066/81... hist = -5/702.
    f2, s3, g2 = P.macd_state([10, 11, 12, 11, 13, 14], 2, 3, 2)
    close(f2[5], float(Q(727, 54)), "MACD(2,3,2) EMA2 state")
    close(g2[5], float(Q(35, 81)), "MACD(2,3,2) signal state")
    ok(np.isnan(g2[:3]).all() and not np.isnan(g2[3]), "signal first defined at close #4 (slow 3 + sig 2 - 1)")
    hst = P.macd_hist_step(f2[5], s3[5], g2[5], np.array([13.0, 15.0]), 2, 3, 2)
    close(hst[0], -5 / 702, "MACD(2,3,2) hist, P = 13")
    close(hst[1], 1 / 810, "MACD(2,3,2) hist, P = 15")
    st = pd.DataFrame({k: [np.nan] * 3 for k in ["ma20", "ma50", "hi252", "lo252", "c_base", "atr_14d", "c_prev",
                                                  "rsi_ag", "rsi_al", "ema_fast", "ema_slow", "macd_sig"]})
    st["hh20"], st["ll20"] = 12.0, 7.0
    dn = P.bar_levels(st, [11.5, 13.0, 6.0])["donchian_pos_20d"]
    ok(list(np.round(dn, 12)) == [0.9, 1.2, -0.2], f"donchian (P - 7) / (12 - 7) at 11.5, 13, 6: {list(dn)}")

    print("\nlevels at a bar of T against explicit slicing")
    q = time(10, 30)
    row = full.loc[(T, q)]
    p = row["spot"]
    close(row["pct_from_ma20"], p / statistics.fmean(C[j - 19:j + 1]) - 1, "pct_from_ma20: C[T-20..T-1]")
    close(row["pct_from_ma50"], p / statistics.fmean(C[j - 49:j + 1]) - 1, "pct_from_ma50: C[T-50..T-1]")
    close(row["pct_from_52w_high"], p / max(C[j - 251:j + 1]) - 1, "pct_from_52w_high: C[T-252..T-1]")
    close(row["pct_from_52w_low"], p / min(C[j - 251:j + 1]) - 1, "pct_from_52w_low: C[T-252..T-1]")
    hh, ll = max(H[j - 19:j + 1]), min(L[j - 19:j + 1])
    close(row["donchian_pos_20d"], (p - ll) / (hh - ll), "donchian_pos_20d: H, L over T-20..T-1")
    tr = [max(H[k] - L[k], abs(H[k] - C[k - 1]), abs(L[k] - C[k - 1])) for k in range(j - 13, j + 1)]
    close(row["atr_14d"], statistics.fmean(tr), "atr_14d: mean TR over T-14..T-1")
    close(row["atr_normalized_ret_5d"], (p / C[j - 4] - 1) / (statistics.fmean(tr) / C[j]),
          "atr_normalized_ret_5d: (P / C_{T-5} - 1) / (atr / C_{T-1})")
    ok(row["ohlc_session"] == SESSIONS[TI - 1], "ohlc_session = T-1")

    print("\nlookahead")
    trap = bars.copy()
    trap.loc[trap["trade_date"] == T, ["spx_high", "spx_low", "spx_close"]] *= 1.5
    rt = build(T, T, trap).frame.set_index("quote_time")
    base = full.loc[T]
    same = all(P._same(base.at[qq, c], rt.at[qq, c]) for qq in base.index for c in P.COLUMNS_P5[2:])
    ok(same, "T's high / low / close x1.5 -> all 78 rows of T unchanged")
    trap2 = bars.copy()
    trap2.loc[(trap2["trade_date"] == T) & (trap2["quote_time"] > time(13, 0)), "spx_open"] *= 0.7
    r2 = build(T, T, trap2).frame.set_index("quote_time")
    ok(all(P._same(base.at[qq, c], r2.at[qq, c]) for qq in base.index if qq <= time(13, 0)
           for c in P.COLUMNS_P5[2:]), "later opens rewritten -> bars through 13:00 unchanged")
    nb = SESSIONS[TI + 1]
    rn = build(nb, nb, trap).frame
    ok(not np.allclose(rn["pct_from_ma20"].dropna(), full.loc[nb, "pct_from_ma20"].dropna()),
       "the next session's rows DO change (they use T's close)")
    bad = P.lookahead_probe_5min(bars, [T, SESSIONS[TI + 40], HALF], res.frame, seed_from=SEED)
    ok(not bad, f"lookahead_probe_5min passes ({len(bad)} mismatches)" + (f": {bad[:3]}" if bad else ""))

    print("\none-day build reproduces the full build")
    one = build(T, T, bars).frame.set_index("quote_time")
    ok(all(P._same(one.at[qq, c], base.at[qq, c]) for qq in base.index for c in P.COLUMNS_P5[2:]),
       f"build({T}, {T}) == full-build rows")

    print("\nunknown price")
    u = full.loc[(T, UNK_BAR)]
    ok(isnull(u["spot"]) and u[P.CORE_P5].isna().all() and not isnull(u["atr_14d"]),
       f"{T} {UNK_BAR}: row exists, all 8 levels NULL, day state (atr_14d) still stored")
    ok(isnull(full.loc[(SESSIONS[TI + 1], UNK_BAR), "d1_rsi_14_change"])
       and not isnull(full.loc[(SESSIONS[TI + 1], time(11, 5)), "d1_rsi_14_change"]),
       "next session's same-bar d1 is NULL at 11:00 only")
    ok(isnull(u["zscore_rsi_14_3m"]), "no z-score on the unknown bar")

    print("\n§4.1: out of the textbook range, not clamped")
    up, down = full.loc[(T_UP, time(10, 30))], full.loc[(T_DOWN, time(10, 30))]
    ok(up["donchian_pos_20d"] > 1 and up["pct_from_52w_high"] > 0,
       f"breakout bar: donchian {up['donchian_pos_20d']:.3f} > 1, pct_from_52w_high {up['pct_from_52w_high']:.4f} > 0")
    ok(down["donchian_pos_20d"] < 0 and down["pct_from_52w_low"] < 0,
       f"breakdown bar: donchian {down['donchian_pos_20d']:.3f} < 0, pct_from_52w_low {down['pct_from_52w_low']:.4f} < 0")
    rep = P.textbook_report(res.frame)
    yr = T_UP.year
    ok(rep[("donchian_pos_20d", yr)][0] >= 2 and rep[("pct_from_52w_high", yr)][0] >= 1
       and rep[("pct_from_52w_low", yr)][0] >= 1, "report counts them")

    print("\nz gate and hard checks")
    ff = res.full[res.full["spot"].notna()].sort_values(["quote_time", "trade_date"])
    exact = True
    for m in P.Z_P5:
        grp = ff.groupby("quote_time")[m]
        cnt = grp.transform(lambda x: x.notna().astype(int).rolling(P.Z_WINDOW, min_periods=1).sum())
        sd = grp.transform(lambda x: x.rolling(P.Z_WINDOW).std())
        exact &= bool((ff[f"zscore_{m}_3m"].notna() == ((cnt >= P.Z_WINDOW) & (sd > 0))).all())
    ok(exact, "z non-NULL exactly where the bar's last 60 readings hold 60 levels")
    chk = F.Checker()
    P.check_price_5min(res.frame, sessions=SESSIONS[300:], chk=chk, full=res.full, daily=res.daily)
    ok(not chk.failed, "check_price_5min passes on the synthetic build")

    print("\nindependent verify path agrees with the build, bar for bar")
    raw = bars.copy()
    for c in P.PRICE_COLS:
        raw[c] = raw[c].where(raw[c] > 0)
    orig = P.SEED_FROM
    P.SEED_FROM = SEED
    try:
        for d in (T, T_UP, SESSIONS[SESSIONS.index(HALF) + 1], HALF):
            day = raw[raw["trade_date"] == d]
            opens = {qq: (None if pd.isna(v) else float(v)) for qq, v in zip(day["quote_time"], day["spx_open"])}
            mine = V5.recompute_day(d, raw[raw["trade_date"] < d], opens).set_index("quote_time")
            built = full.loc[d]
            diffs = [(qq, c) for qq in built.index for c in P.COLUMNS_P5[2:]
                     if not V5.VD._same(built.at[qq, c], mine.at[qq, c])]
            ok(len(mine) == len(built) and not diffs,
               f"{d}: {len(built)} bars x {len(P.COLUMNS_P5) - 2} columns agree"
               + (f"; differ: {diffs[:5]}" if diffs else ""))
    finally:
        P.SEED_FROM = orig

    print(f"\n{'ALL PASS' if not FAILS else f'{len(FAILS)} FAILURE(S)'}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
