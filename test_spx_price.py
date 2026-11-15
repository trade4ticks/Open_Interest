"""
test_spx_price.py — spx_price_daily compute on a synthetic index_ohlc, with
no database.

The mock is laid out like the real table (spec §7.1): a row for every
calendar day 09:30-16:00, zero-filled on weekends and holidays, early closes
padded with zeros after 12:55, and a 16:00 bar carrying a price that must
never be read. Planted: a session whose 15:55 close is NaN, a session with no
usable bar, and a bar with a zero high.

Covers: daily OHLC from the grid (15:55 / 12:55 close, never 16:00); every
level's window offsets against explicit slicing; ADX hand-computed on a
period-3 fixture; streak; the lookahead trap (session T rewritten, row T
unchanged); a one-day build equal to the full build; the z-score gate; NULL
handling and the ADX reset after a gap; the build's hard checks; the
independent verify path agreeing with the build.

Run:  python test_spx_price.py     (exit 1 on any failure)
"""
from __future__ import annotations

import math
import statistics
import sys
from datetime import date, datetime, time, timedelta
from fractions import Fraction as Q

import numpy as np
import pandas as pd

import lib.spx_features as F
import lib.spx_price as P
import verify_spx_price_daily as V
from lib.market_hours import get_trading_days, session_bounds

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


SEED = date(2024, 1, 1)
LAST = date(2025, 9, 30)
SESSIONS = get_trading_days(SEED, LAST)
NAN_I = 200                          # its 15:55 close is NaN
NONE_I = 300                         # no usable bar at all
ZERO_HIGH_I = 120                    # one bar's high is 0
NAN_DAY, NONE_DAY, ZERO_HIGH_DAY = SESSIONS[NAN_I], SESSIONS[NONE_I], SESSIONS[ZERO_HIGH_I]
TI = 380                             # target row, clean 61-session window behind it
T = SESSIONS[TI]
HALF = date(2024, 11, 29)
ALL_BARS = [(datetime.combine(date.min, time(9, 30)) + i * timedelta(minutes=5)).time() for i in range(79)]


def mock_bars() -> pd.DataFrame:
    rng = np.random.default_rng(7)
    bounds = session_bounds(SEED, LAST)
    rows, px = [], 4700.0
    d = SEED
    while d <= LAST:
        if d not in bounds:
            rows += [(d, f"{q:%H:%M:%S}", 0.0, 0.0, 0.0, 0.0) for q in ALL_BARS]
        else:
            close_t = bounds[d][1].time()
            for q in ALL_BARS:
                if q >= close_t and q != time(16, 0):
                    rows.append((d, f"{q:%H:%M:%S}", 0.0, 0.0, 0.0, 0.0))   # half-day padding
                    continue
                o = px
                c = o * math.exp(rng.normal(0, 0.0012))
                h, l = max(o, c) * (1 + abs(rng.normal(0, 0.0004))), min(o, c) * (1 - abs(rng.normal(0, 0.0004)))
                if q == time(16, 0):
                    o = h = l = c = px * 1.07          # the after-close bar: must never be read
                else:
                    px = c
                rows.append((d, f"{q:%H:%M:%S}", o, h, l, c))
            px *= math.exp(rng.normal(0, 0.003))       # overnight gap
        d += timedelta(days=1)
    df = pd.DataFrame(rows, columns=["trade_date", "quote_time", *P.PRICE_COLS])
    key = lambda d, q: (df["trade_date"] == d) & (df["quote_time"] == q)
    df.loc[key(NAN_DAY, "15:55:00"), "spx_close"] = float("nan")
    df.loc[df["trade_date"] == NONE_DAY, P.PRICE_COLS] = 0.0
    df.loc[key(ZERO_HIGH_DAY, "11:00:00"), "spx_high"] = 0.0
    return P.prepare_ohlc(df)


def build(start, end, bars):
    return P.compute_price_daily(start, end, bars=bars, seed_from=SEED)


def main() -> int:
    bars = mock_bars()
    res = build(SESSIONS[1], LAST, bars)
    full, daily = res.full.set_index("trade_date"), res.daily.set_index("trade_date")

    print("daily OHLC from the grid")
    g = bars[bars["trade_date"] == T].set_index("quote_time")
    close(daily.at[T, "close"], g.at[time(15, 55), "spx_close"], "close = 15:55 bar's close")
    ok(daily.at[T, "close"] != g.at[time(16, 0), "spx_close"], "16:00 bar never used as close")
    grid_t = [q for q in ALL_BARS if q < time(16, 0)]
    close(daily.at[T, "high"], g.loc[grid_t, "spx_high"].max(), "high = max over 78 grid bars")
    close(daily.at[T, "low"], g.loc[grid_t, "spx_low"].min(), "low = min over 78 grid bars")
    close(daily.at[T, "open"], g.at[time(9, 30), "spx_open"], "open = 09:30 bar's open")
    gh = bars[bars["trade_date"] == HALF].set_index("quote_time")
    close(daily.at[HALF, "close"], gh.at[time(12, 55), "spx_close"], f"early close {HALF}: close = 12:55 bar")
    ok(int(daily.at[HALF, "n_bars"]) == 42, f"early close {HALF}: 42 grid bars")
    ok(isnull(daily.at[NAN_DAY, "close"]) and not isnull(daily.at[NAN_DAY, "high"]),
       f"{NAN_DAY}: NaN 15:55 close -> NULL daily close, high still known (no borrowing from 15:50)")
    ok(daily.loc[NONE_DAY, ["open", "high", "low", "close"]].isna().all(), f"{NONE_DAY}: all-zero session -> no OHLC")
    ok(int(daily.at[ZERO_HIGH_DAY, "n_high"]) == 77, f"{ZERO_HIGH_DAY}: zero high is unknown (77 of 78)")
    rep = P.daily_ohlc_report(res.daily)
    ok(rep["no_ohlc"] == [NONE_DAY], "report lists the no-OHLC session")
    ok(set(rep["partial"]) == {NAN_DAY, ZERO_HIGH_DAY}, "report lists both partial sessions")

    print("\nlevels at row T against explicit slicing (sessions T-1 back)")
    C = list(res.daily["close"])
    i = TI                                     # position of T in SESSIONS == in res.daily
    ok(full.at[T, "ohlc_session"] == SESSIONS[i - 1], "row T's ohlc_session is T-1")
    close(full.at[T, "ma20_slope_5d"],
          statistics.fmean(C[i - 20:i]) / statistics.fmean(C[i - 25:i - 5]) - 1, "ma20_slope_5d: C[T-20..T-1] / C[T-25..T-6]")
    rets = [math.log(C[j] / C[j - 1]) for j in range(i - 20, i)]
    close(full.at[T, "pct_up_days_20d"], sum(r > 0 for r in rets) / 20, "pct_up_days_20d over T-20..T-1")
    close(full.at[T, "rv_ratio_5d_20d"],
          (statistics.stdev(rets[-5:]) * math.sqrt(252)) / (statistics.stdev(rets) * math.sqrt(252)),
          "rv_ratio_5d_20d (sample sd, sqrt 252 both sides)", rel=1e-10)
    k, sg = i - 1, np.sign(C[i - 1] - C[i - 2])
    while np.sign(C[k] - C[k - 1]) == sg:
        k -= 1
    close(full.at[T, "streak"], sg * (i - 1 - k), "streak, signed run of closes ending T-1")

    print("\nADX hand-computed, period 3 (exact fractions; table in the comment)")
    # t  H     L     C     TR  +DM  -DM  TR3     +DM3      -DM3     DX            ADX
    # 0  10    8     9
    # 1  11    9     10.5  2   1    0
    # 2  12    10    11    2   1    0
    # 3  11.5  9.5   10    2   0    1/2  6       2         1/2      60
    # 4  11    8     8.5   3   0    3/2  7       4/3       11/6     300/19
    # 5  10    7     9.5   3   0    1    23/3    8/9       20/9     300/7         5260/133
    # 6  12    9     11.5  3   2    0    73/9    70/27     40/27    300/11        155620/4389
    # 7  13    11    12    2   1    0    200/27  221/81    80/81    14100/301     3174860/80883
    # 8  12.5  10.5  11    2   0    1/2  562/81  442/243   563/486  32100/1447    11784389140/351113103
    # 9  14    11    13.5  3   3/2  0    1853/243 3955/1458 563/729 282900/5081   219082859279380/5352017029029
    H = [10, 11, 12, 11.5, 11, 10, 12, 13, 12.5, 14]
    L = [8, 9, 10, 9.5, 8, 7, 9, 11, 10.5, 11]
    Cc = [9, 10.5, 11, 10, 8.5, 9.5, 11.5, 12, 11, 13.5]
    a = P.wilder_adx(H, L, Cc, n=3)
    ok(np.isnan(a[:5]).all(), "ADX(3) NULL before 2n-1 = 5 sessions of TR")
    for t, q in [(5, Q(5260, 133)), (6, Q(155620, 4389)), (7, Q(3174860, 80883)),
                 (8, Q(11784389140, 351113103)), (9, Q(219082859279380, 5352017029029))]:
        close(a[t], float(q), f"ADX(3) at t={t}")
    b = V._adx_by_segments(H, L, Cc, n=3)
    ok(all(isnull(b[t]) if np.isnan(a[t]) else math.isclose(a[t], b[t], rel_tol=1e-12) for t in range(10)),
       "verify's segment ADX agrees on the fixture")
    a2 = P.wilder_adx(H[:4] + [float("nan")] + H[5:], L, Cc, n=3)
    ok(np.isnan(a2).all(), "a missing high at t=4 resets ADX(3): t=4,5 unusable, re-seeding from t=6 "
                           "needs through t=10, so no value at all")

    print("\nstreak")
    s = P.streak_series([np.nan, 0.1, 0.2, -0.1, -0.1, -0.3, 0.0, 0.2, np.nan, 0.1, 0.1, -0.2])
    exp = [None, None, None, -1, -2, -3, 0, 1, None, None, None, -1]
    ok(all((isnull(x) and e is None) or x == e for x, e in zip(s, exp)),
       f"streak: {list(s)} (NULL until the sign changes after a gap)")

    print("\nLOOKAHEAD trap: every price of session T multiplied by 1.5")
    trap = bars.copy()
    trap.loc[trap["trade_date"] == T, P.PRICE_COLS] *= 1.5
    rt = build(SESSIONS[1], LAST, trap).full.set_index("trade_date")
    ok(all(P._same(full.at[T, c], rt.at[T, c]) for c in P.COLUMNS_PD[1:]), "row T unchanged")
    nxt = SESSIONS[TI + 1]
    ok(not all(P._same(full.at[nxt, c], rt.at[nxt, c]) for c in P.CORE_PD), "row T+1 changes (it uses T)")
    ok(not P.lookahead_probe(bars, [SESSIONS[100], T, NAN_DAY, SESSIONS[NAN_I + 1]], res.frame, seed_from=SEED),
       "lookahead_probe: rows rebuilt from bars before their date match, incl. the day after a NaN close")

    print("\none-day build reproduces the full build")
    one = build(T, T, bars).frame
    ok(len(one) == 1 and all(P._same(one[c].iloc[0], full.at[T, c]) for c in P.COLUMNS_PD[1:]),
       f"build({T}, {T}) row == full-build row")

    print("\nNULL handling around the NaN close and the empty session")
    pos = {d: n for n, d in enumerate(SESSIONS)}
    def lv(off, m, base=NAN_I):         # value on the row whose ohlc_session is SESSIONS[base + off]
        return full.at[SESSIONS[base + off + 1], m]
    ok(not isnull(lv(0, "adx_14")), "ADX known on the row of the NaN-close session (needs only its H/L)")
    ok(all(isnull(lv(o, "adx_14")) for o in range(1, 28)) and not isnull(lv(28, "adx_14")),
       "ADX resets: NULL for the 27 sessions after, back on the 28th")
    ok(all(isnull(lv(o, "ma20_slope_5d")) for o in range(0, 25)) and not isnull(lv(25, "ma20_slope_5d")),
       "ma20_slope_5d NULL while the NaN close is in either window, back 25 sessions later")
    ok(all(isnull(lv(o, "pct_up_days_20d")) for o in range(0, 21)) and not isnull(lv(21, "pct_up_days_20d")),
       "pct_up_days_20d NULL for the 21 returns that touch it")
    ok(SESSIONS[NONE_I + 1] in full.index and full.loc[SESSIONS[NONE_I + 1], P.CORE_PD].isna().all(),
       "row after the empty session exists, every level NULL")

    print("\nz-score gate and hard checks")
    ff = res.full.reset_index(drop=True)
    exact = True
    for m in P.Z_PD:
        cnt = ff[m].notna().astype(int).rolling(P.Z_WINDOW, min_periods=1).sum()
        z = ff[f"zscore_{m}_3m"]
        sd = ff[m].rolling(P.Z_WINDOW).std()
        exact &= bool(((z.notna()) == ((cnt >= P.Z_WINDOW) & (sd > 0))).all())
    ok(exact, "z non-NULL exactly where the 60-row window holds 60 levels")
    first_z = ff.loc[ff["zscore_adx_14_3m"].notna(), "trade_date"].min()
    ok(pos[first_z] == P.LEVEL_HISTORY + P.Z_WINDOW - 1,
       f"first ADX z-score on row {first_z}, session #{pos[first_z]} (28 sessions to seed ADX + 59 more)")
    chk = F.Checker()
    P.check_price_daily(res.frame, sessions=SESSIONS[1:], chk=chk, full=res.full, daily=res.daily)
    ok(not chk.failed, "check_price_daily passes on the synthetic build")

    print("\nindependent verify path agrees with the build")
    raw = bars.copy()
    for c in P.PRICE_COLS:
        raw[c] = raw[c].where(raw[c] > 0)
    orig = P.SEED_FROM
    V.P.SEED_FROM = SEED
    try:
        for d in (T, SESSIONS[NAN_I + 30], SESSIONS[NONE_I + 90], SESSIONS[-1]):
            mine = V.recompute_row(d, raw[raw["trade_date"] < d])
            diffs = [c for c in P.COLUMNS_PD if not V._same(full.at[d, c] if c != "trade_date" else d, mine[c])]
            ok(not diffs, f"{d}: {len(P.COLUMNS_PD)} columns agree" + (f"; differ: {diffs}" if diffs else ""))
    finally:
        V.P.SEED_FROM = orig

    print(f"\n{'ALL PASS' if not FAILS else f'{len(FAILS)} FAILURE(S)'}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
