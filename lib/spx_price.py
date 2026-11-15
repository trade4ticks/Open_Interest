"""
lib/spx_price.py — SPX price metrics from public.index_ohlc (spec: SPX price
metrics build specification). Phase 1: spx_price_daily.

Pure compute, like lib/spx_features.py and lib/spx_table_b.py:
build_spx_price_daily.py writes, verify_spx_price_daily.py and
test_spx_price.py check.

SOURCE
------
public.index_ohlc through SPX_INDEX_OHLC_DSN, read-only. Nothing else.

DAILY OHLC IS DERIVED FROM THE GRID (spec §1.1)
-----------------------------------------------
The grid is lib.spx_table_b.expected_grid: the NYSE calendar's real session
hours, 78 bars 09:30..15:55, 42 bars 09:30..12:55 on an early close, never the
16:00 bar. Per price column, NULL, NaN, zero and negative are unknown.

    open  = spx_open  of the session's first grid bar (09:30)
    close = spx_close of the session's LAST grid bar (15:55, or 12:55)
    high  = MAX(spx_high) over the session's grid bars with a known high
    low   = MIN(spx_low)  over the session's grid bars with a known low

An unknown first/last bar leaves open/close NULL; nothing is interpolated or
borrowed from a neighbouring bar. A session with no usable bar has no daily
OHLC at all. daily_ohlc_report() lists both kinds.

TIMING (spec §2)
----------------
Every metric is computed per SESSION s from sessions <= s, then placed on the
row of the next NYSE session: row T carries the value as of T-1's close, and
the stored ohlc_session column says which session that was. check_price_daily
fails unless ohlc_session is the prior NYSE session for every row, and
lookahead_probe() recomputes rows from bars strictly before trade_date.

RECURSION SEED
--------------
ADX (and in phase 2 RSI / MACD) are recursive, so a value depends on where the
recursion started. It always starts at SEED_FROM, index_ohlc's first day,
whatever --start is: every build reads the whole history, a one-day build
equals the same row of a full build, and a later backfill of 2026 cannot move
a 2021 value. A session whose inputs are missing resets the recursion, which
then re-seeds from scratch (no value across a gap, never a short window).

WINDOWS
-------
A rolling metric is NULL unless every input in its window is known: a missing
close blanks every window it falls in rather than shortening it.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, time, timedelta

import duckdb
import numpy as np
import pandas as pd

import lib.spx_features as F
import lib.spx_table_b as B
from lib.market_hours import get_trading_days, next_trading_day

TABLE_PD = "spx_price_daily"
SEED_FROM = date(2017, 1, 1)
PRICE_COLS = ["spx_open", "spx_high", "spx_low", "spx_close"]
Z_WINDOW = F.Z_WINDOW
ADX_N = 14

CORE_PD = ["ma20_slope_5d", "pct_up_days_20d", "rv_ratio_5d_20d", "adx_14", "streak"]

# Spec §5 — not uniform, by design. Exclusions are reasoned, not oversights.
DERIV_PD = {
    "ma20_slope_5d":   ("d1", "z"),         # no d5: already compares windows five days apart
    "pct_up_days_20d": ("d5", "z"),         # no d1: moves by -0.05 / 0 / +0.05
    "rv_ratio_5d_20d": ("d1", "d5", "z"),
    "adx_14":          ("d1", "d5", "z"),
    "streak":          (),                  # a counter: level only
}


def deriv_name(m: str, kind: str) -> str:
    return f"zscore_{m}_3m" if kind == "z" else f"{kind}_{m}_change"


DERIVATIVES_PD = [deriv_name(m, k) for m in CORE_PD for k in DERIV_PD[m]]
SURFACED_PD = CORE_PD + DERIVATIVES_PD
STORED_ONLY_PD = ["ohlc_session"]
COLUMNS_PD = ["trade_date", "ohlc_session", *CORE_PD, *DERIVATIVES_PD]
Z_PD = [m for m in CORE_PD if "z" in DERIV_PD[m]]

assert len(CORE_PD) == 5 and len(SURFACED_PD) == 15

# Completed sessions a row needs behind it before every level can exist:
# ma20_slope_5d 25 closes; ADX 28 (14 TR from the 2nd session, then 14 DX).
LEVEL_HISTORY = 2 * ADX_N


# --- Reading -------------------------------------------------------------------

def load_ohlc(dsn: str, lo: date, hi: date) -> pd.DataFrame:
    """Every index_ohlc row in [lo, hi], unfiltered: [trade_date, quote_time,
    spx_open, spx_high, spx_low, spx_close]. Unknown values are classified in
    daily_ohlc, not here."""
    import psycopg2
    conn = psycopg2.connect(dsn)
    try:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, quote_time, spx_open, spx_high, spx_low, spx_close "
                        "FROM public.index_ohlc WHERE trade_date BETWEEN %s AND %s", (lo, hi))
            rows = cur.fetchall()
    finally:
        conn.close()
    return prepare_ohlc(pd.DataFrame(rows, columns=["trade_date", "quote_time", *PRICE_COLS]))


def prepare_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    df = B.prepare_spot(df)              # dates, quote_time normalised, duplicates fail
    for c in PRICE_COLS[1:]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    return df


# --- Daily OHLC from the grid --------------------------------------------------------

def daily_ohlc(bars: pd.DataFrame, sessions: list[date]) -> pd.DataFrame:
    """One row per session in `sessions` (all of them, even with no data):
    [trade_date, open, high, low, close, n_bars, n_high, n_low]."""
    grid = B.expected_grid(sessions)
    m = grid.merge(bars[["trade_date", "quote_time", *PRICE_COLS]],
                   on=["trade_date", "quote_time"], how="left")
    m = m.sort_values(["trade_date", "quote_time"]).reset_index(drop=True)
    for c in PRICE_COLS:
        m[c] = m[c].where(m[c] > 0)               # NULL / NaN / zero / negative -> unknown
    g = m.groupby("trade_date", sort=True)
    first = m.drop_duplicates("trade_date", keep="first").set_index("trade_date")
    last = m.drop_duplicates("trade_date", keep="last").set_index("trade_date")
    out = pd.DataFrame({
        "open": first["spx_open"],
        "high": g["spx_high"].max(),
        "low": g["spx_low"].min(),
        "close": last["spx_close"],
        "n_bars": g.size(),
        "n_high": g["spx_high"].count(),
        "n_low": g["spx_low"].count(),
    })
    out = out.reindex(pd.Index(sessions, name="trade_date"))
    out["n_bars"] = out["n_bars"].fillna(0).astype(int)
    return out.reset_index()


def daily_ohlc_report(daily: pd.DataFrame, label: str = "") -> dict:
    """Sessions with no usable daily OHLC, and sessions with part of it. Never fails."""
    d = daily
    none = d[d[["open", "high", "low", "close"]].isna().all(axis=1)]
    partial = d[~d.index.isin(none.index) & (
        d[["open", "high", "low", "close"]].isna().any(axis=1)
        | (d["n_high"] < d["n_bars"]) | (d["n_low"] < d["n_bars"]))]
    incons = d[(d["high"] < d["low"]) | (d["close"] > d["high"]) | (d["close"] < d["low"])
               | (d["open"] > d["high"]) | (d["open"] < d["low"])]
    print(f"  daily OHLC{label}: {len(d)} sessions {d['trade_date'].min()} → {d['trade_date'].max()}; "
          f"no usable bar: {len(none)}; partial: {len(partial)}; high/low/open/close inconsistent: {len(incons)}")
    for _, r in none.iterrows():
        print(f"    {r['trade_date']}  NO DAILY OHLC — no usable bar in the grid "
              f"[{B.KNOWN_GAPS.get(r['trade_date'], 'NEW — not a known gap')}]")
    for _, r in partial.iterrows():
        miss = [c for c in ("open", "high", "low", "close") if pd.isna(r[c])]
        print(f"    {r['trade_date']}  partial: high from {int(r['n_high'])}/{int(r['n_bars'])} bars, "
              f"low from {int(r['n_low'])}/{int(r['n_bars'])}"
              + (f"; NULL {', '.join(miss)}" if miss else "")
              + f"  [{B.KNOWN_GAPS.get(r['trade_date'], 'NEW — not a known gap')}]")
    for _, r in incons.head(20).iterrows():
        print(f"    {r['trade_date']}  inconsistent: O {r['open']} H {r['high']} L {r['low']} C {r['close']}")
    return {"no_ohlc": list(none["trade_date"]), "partial": list(partial["trade_date"]),
            "inconsistent": list(incons["trade_date"])}


# --- Indicators (per session, as of that session's close) -------------------------------

def wilder_adx(high, low, close, n: int = ADX_N) -> np.ndarray:
    """Wilder's ADX. Per session t >= 1 with H_t, L_t, H_{t-1}, L_{t-1}, C_{t-1} known:
        TR  = max(H_t - L_t, |H_t - C_{t-1}|, |L_t - C_{t-1}|)
        up  = H_t - H_{t-1},  dn = L_{t-1} - L_t
        +DM = up if up > dn and up > 0 else 0;  -DM = dn if dn > up and dn > 0 else 0
    TR_n, +DM_n, -DM_n: the sum of the first n values, then X_n <- X_n - X_n/n + x.
    +DI = 100 +DM_n / TR_n, -DI likewise, DX = 100 |+DI - -DI| / (+DI + -DI).
    ADX: the mean of the first n DX, then ADX <- (ADX (n-1) + DX) / n.
    A missing input, or +DI + -DI = 0, resets everything; it re-seeds from scratch."""
    H, L, C = (np.asarray(x, dtype=float) for x in (high, low, close))
    out = np.full(len(H), np.nan)
    acc_tr, acc_p, acc_m, acc_dx = [], [], [], []
    s_tr = s_p = s_m = adx = None

    def reset():
        nonlocal s_tr, s_p, s_m, adx
        acc_tr.clear(), acc_p.clear(), acc_m.clear(), acc_dx.clear()
        s_tr = s_p = s_m = adx = None

    for t in range(1, len(H)):
        if np.isnan([H[t], L[t], H[t - 1], L[t - 1], C[t - 1]]).any():
            reset()
            continue
        tr = max(H[t] - L[t], abs(H[t] - C[t - 1]), abs(L[t] - C[t - 1]))
        up, dn = H[t] - H[t - 1], L[t - 1] - L[t]
        pdm = up if (up > dn and up > 0) else 0.0
        mdm = dn if (dn > up and dn > 0) else 0.0
        if s_tr is None:
            acc_tr.append(tr), acc_p.append(pdm), acc_m.append(mdm)
            if len(acc_tr) < n:
                continue
            s_tr, s_p, s_m = sum(acc_tr), sum(acc_p), sum(acc_m)
        else:
            s_tr = s_tr - s_tr / n + tr
            s_p = s_p - s_p / n + pdm
            s_m = s_m - s_m / n + mdm
        if s_tr <= 0:
            reset()
            continue
        pdi, mdi = 100.0 * s_p / s_tr, 100.0 * s_m / s_tr
        if pdi + mdi <= 0:
            reset()
            continue
        dx = 100.0 * abs(pdi - mdi) / (pdi + mdi)
        if adx is None:
            acc_dx.append(dx)
            if len(acc_dx) < n:
                continue
            adx = sum(acc_dx) / n
        else:
            adx = (adx * (n - 1) + dx) / n
        out[t] = adx
    return out


def streak_series(log_ret) -> np.ndarray:
    """+k after k straight up closes, -k after k straight down closes, 0 on an
    unchanged close. NULL where the run's start is not known: after a missing
    return, until the sign next changes (the run may have begun before the gap)."""
    r = np.asarray(log_ret, dtype=float)
    out = np.full(len(r), np.nan)
    prev_sign, run, known = None, 0, False
    for i, x in enumerate(r):
        if np.isnan(x):
            prev_sign, run, known = None, 0, False
            continue
        s = 1 if x > 0 else (-1 if x < 0 else 0)
        if s == 0:
            prev_sign, run, known = 0, 0, True
            out[i] = 0.0
            continue
        if prev_sign is not None and s == prev_sign:
            run += 1
        else:
            known = prev_sign is not None
            run = 1
        prev_sign = s
        if known:
            out[i] = float(s * run)
    return out


def session_metrics(daily: pd.DataFrame) -> pd.DataFrame:
    """CORE_PD per session, as of that session's close, from sessions <= it.
    `daily` must hold every NYSE session in order (positional windows = sessions)."""
    c, h, l = daily["close"], daily["high"], daily["low"]
    r = np.log(c / c.shift(1))
    ma20 = c.rolling(20, min_periods=20).mean()
    up = (r > 0).astype(float).where(r.notna())
    rv5 = r.rolling(5, min_periods=5).std(ddof=1)        # x sqrt(252) cancels in the ratio
    rv20 = r.rolling(20, min_periods=20).std(ddof=1)
    return pd.DataFrame({
        "ohlc_session": daily["trade_date"],
        "ma20_slope_5d": ma20 / ma20.shift(5) - 1.0,
        "pct_up_days_20d": up.rolling(20, min_periods=20).sum() / 20.0,
        "rv_ratio_5d_20d": rv5 / rv20.where(rv20 > 0),
        "adx_14": wilder_adx(h, l, c, ADX_N),
        "streak": streak_series(r),
    })


def _derivatives_pd(levels: pd.DataFrame) -> pd.DataFrame:
    """Spec §5 derivatives over the contiguous session spine: d1/d5 = level
    minus the level 1/5 rows earlier; z = lib.spx_features.zscore_sql."""
    cols = []
    for m in CORE_PD:
        for k in DERIV_PD[m]:
            if k == "z":
                cols.append(F.zscore_sql(m))
            else:
                n = int(k[1])
                cols.append(f"{m} - LAG({m}, {n}) OVER w AS {deriv_name(m, k)}")
    con = duckdb.connect(database=":memory:")
    try:
        con.register("lv_df", levels)
        con.execute("CREATE TABLE levels AS SELECT * REPLACE (CAST(trade_date AS DATE) AS trade_date, "
                    "CAST(ohlc_session AS DATE) AS ohlc_session) FROM lv_df")
        out = con.execute("SELECT *,\n       " + ",\n       ".join(cols) + "\nFROM levels\n"
                          f"WINDOW w AS (ORDER BY trade_date),\n       {F.W60_SQL}\n"
                          "ORDER BY trade_date").df()
    finally:
        con.close()
    for c in ("trade_date", "ohlc_session"):
        out[c] = pd.to_datetime(out[c]).dt.date
    return out


# --- Compute -------------------------------------------------------------------

@dataclass
class PriceDaily:
    frame: pd.DataFrame          # rows in [start, end_eff], COLUMNS_PD
    full: pd.DataFrame           # every row from the seed on
    daily: pd.DataFrame          # derived daily OHLC per session, seed .. last input session
    start: date
    end_requested: date
    end_eff: date
    data_last: date              # last session with a usable daily close
    lead_in_sessions: int        # completed sessions available before start
    notes: list = field(default_factory=list)


@dataclass
class _Inputs:
    daily: pd.DataFrame          # daily OHLC of every input session, in order
    inputs: list                 # input sessions: seed .. the session before end_eff
    rows: list                   # the row each input's close feeds: inputs[1:] + [end_eff]
    end_eff: date
    data_last: date
    notes: list


def _input_spine(start: date, end: date, bars: pd.DataFrame, seed_from: date,
                 complete_through: date | None) -> _Inputs:
    """Rows run to the session after the last COMPLETE session, which is the
    last one with a usable close (a session still trading has no 15:55 close
    yet, so its partial high / low never feed a row). `complete_through`
    overrides that, for the lookahead probes, which cut the bars at a day."""
    notes = []
    sess_all = get_trading_days(seed_from, end)
    if not sess_all:
        raise SystemExit(f"no NYSE session in {seed_from} → {end}")
    bars = bars[(bars["trade_date"] >= seed_from) & (bars["trade_date"] <= end)]
    daily_all = daily_ohlc(bars, sess_all)
    known_close = daily_all.loc[daily_all["close"].notna(), "trade_date"]
    if known_close.empty:
        raise SystemExit("index_ohlc has no session with a usable close in range")
    data_last = complete_through or known_close.max()
    nxt = next_trading_day(data_last)
    cap = min(end, nxt)
    end_eff = max(s for s in [*sess_all, nxt] if s <= cap)
    if nxt < end:
        notes.append(f"--end {end} clamped to {end_eff}: the last session with a usable close in "
                     f"index_ohlc is {data_last}, and row T needs T-1's close")
    if end_eff < start:
        raise SystemExit(f"nothing to build: --start {start} is after {end_eff}")
    inputs = [s for s in sess_all if s < end_eff]
    daily = daily_all[daily_all["trade_date"].isin(set(inputs))].reset_index(drop=True)
    return _Inputs(daily=daily, inputs=inputs, rows=[*inputs[1:], end_eff], end_eff=end_eff,
                   data_last=data_last, notes=notes)


def compute_price_daily(start: date, end: date, *, bars: pd.DataFrame,
                        seed_from: date = SEED_FROM, complete_through: date | None = None) -> PriceDaily:
    """`bars` = load_ohlc(dsn, seed_from, end) (or a mock in the same shape).
    Row range and completeness rule: _input_spine."""
    inp = _input_spine(start, end, bars, seed_from, complete_through)
    sm = session_metrics(inp.daily)
    # Each session's close feeds the row of the next NYSE session; the last input's is end_eff.
    sm.insert(0, "trade_date", inp.rows)
    full = _derivatives_pd(sm)[COLUMNS_PD]
    frame = full[(full["trade_date"] >= start) & (full["trade_date"] <= inp.end_eff)].reset_index(drop=True)
    lead = sum(1 for s in inp.inputs if s < start)
    return PriceDaily(frame=frame, full=full, daily=inp.daily, start=start, end_requested=end,
                      end_eff=inp.end_eff, data_last=inp.data_last, lead_in_sessions=lead, notes=inp.notes)


def lookahead_probe(bars: pd.DataFrame, days: list[date], frame: pd.DataFrame,
                    seed_from: date = SEED_FROM) -> list[tuple]:
    """Recompute each row of `days` from bars strictly before that trade_date
    and compare with `frame`. [(day, column, built, probe)] for every mismatch."""
    bad = []
    built = frame.set_index("trade_date")
    for d in days:
        prev = get_trading_days(d - timedelta(days=10), d - timedelta(days=1))[-1]
        r = compute_price_daily(d, d, bars=bars[bars["trade_date"] < d], seed_from=seed_from,
                                complete_through=prev).frame
        if len(r) != 1 or r["trade_date"].iloc[0] != d:
            bad.append((d, "row", "present", "absent"))
            continue
        for c in COLUMNS_PD[1:]:
            a, b = built.at[d, c], r[c].iloc[0]
            if not _same(a, b):
                bad.append((d, c, a, b))
    return bad


def _same(a, b, rel: float = 1e-12, abs_: float = 1e-14) -> bool:
    na, nb = a is None or pd.isna(a), b is None or pd.isna(b)
    if na or nb:
        return na and nb
    if isinstance(a, date) or isinstance(b, date):
        return a == b
    return math.isclose(float(a), float(b), rel_tol=rel, abs_tol=abs_)


# --- Checks ----------------------------------------------------------------------

def check_price_daily(df: pd.DataFrame, *, sessions: list[date], chk: F.Checker,
                      full: pd.DataFrame | None = None, daily: pd.DataFrame | None = None) -> None:
    df = df.sort_values("trade_date").reset_index(drop=True)

    have = list(df["trade_date"])
    chk.check(have == sessions and df["trade_date"].is_unique,
              f"exactly one row per NYSE session: {len(have)} rows, {len(sessions)} sessions")

    cal = get_trading_days(sessions[0] - timedelta(days=15), sessions[-1])
    prev = {cal[i]: cal[i - 1] for i in range(1, len(cal))}
    src = df["ohlc_session"]
    leak = ~(src.notna() & (src < df["trade_date"]) & (src == df["trade_date"].map(prev)))
    chk.check(not leak.any(),
              f"LOOKAHEAD: every row's ohlc_session (the last session whose close, high or low it uses) "
              f"is strictly earlier than its trade_date and is the prior NYSE session — {len(df)} rows, "
              f"{int(leak.sum())} violations" + F._show(df, leak, ["trade_date", "ohlc_session"]))

    def bound(c, ok_fn, txt):
        s = df[c].dropna()
        chk.check(bool(ok_fn(s).all()), f"{c} {txt}  [min {s.min():.6g}, max {s.max():.6g}]"
                  if len(s) else f"{c} {txt}  [no values]")
    bound("pct_up_days_20d", lambda s: (s >= 0) & (s <= 1), "in [0, 1]")
    bound("adx_14", lambda s: (s >= 0) & (s <= 100), "in [0, 100]")
    bound("rv_ratio_5d_20d", lambda s: s > 0, "> 0")
    bound("streak", lambda s: s == np.round(s), "is a whole number")

    # A level is missing only for a reason: some session in its window lacks daily OHLC.
    if daily is not None:
        dd = daily.set_index("trade_date")
        okday = dd[["high", "low", "close"]].notna().all(axis=1)
        seq = list(dd.index)
        pos = {d: i for i, d in enumerate(seq)}
        hist = 60
        clean = []
        for s in df["ohlc_session"]:
            i = pos.get(s)
            clean.append(i is not None and i >= hist and bool(okday.iloc[i - hist:i + 1].all()))
        clean = pd.Series(clean, index=df.index)
        gap = clean & df[CORE_PD].isna().any(axis=1)
        chk.check(not gap.any(),
                  f"every row whose {hist + 1} input sessions all have a daily high, low and close carries "
                  f"all 5 levels ({int(gap.sum())} without, of {int(clean.sum())} such rows)"
                  + F._show(df, gap, ["trade_date", *CORE_PD]))

    if full is not None:
        f = full.sort_values("trade_date").reset_index(drop=True)
        in_out = f["trade_date"].isin(set(df["trade_date"]))
        bad = []
        for m in Z_PD:
            cnt = f[m].notna().astype(int).rolling(Z_WINDOW, min_periods=1).sum()
            if (in_out & f[f"zscore_{m}_3m"].notna() & (cnt < Z_WINDOW)).any():
                bad.append(m)
        chk.check(not bad, "z-scores NULL wherever the 60-session window holds < 60 observations"
                  + (f": violated by {bad}" if bad else ""))



# =====================================================================================
# Phase 2 — spx_price_5min: one row per (trade_date, quote_time), P = the bar's spx_open
# =====================================================================================
#
# Every row combines a FROZEN day state (computed from completed sessions through
# T-1, identical for all bars of T) with the bar's own P. Nothing from T but P
# enters: not T's high, low or close, not another bar's price. RSI and MACD take
# the smoothed state as of T-1's close and apply exactly ONE step with P as
# "today's close" (spec §4.2) — each bar is an independent "what if today closed
# here", never a running update across the day.

TABLE_P5 = "spx_price_5min"
RSI_N = 14
MACD_FAST, MACD_SLOW, MACD_SIG = 12, 26, 9
RET_BASE_BACK = 5            # atr_normalized_ret_5d: P / C_{T-5} - 1. P lives in session T, so T-5 -> T
                             # is five sessions (the spec's C_{T-6} came from the factor ret_5d, where
                             # both ends are completed closes). Denominator stays atr_14d / C_{T-1}.

CORE_P5 = ["pct_from_ma20", "pct_from_ma50", "pct_from_52w_high", "pct_from_52w_low",
           "donchian_pos_20d", "atr_normalized_ret_5d", "rsi_14", "macd_hist"]
DERIV_P5 = {                 # spec §5, exact
    "pct_from_ma20":         ("d1", "d5", "z"),
    "pct_from_ma50":         ("d1", "d5", "z"),
    "pct_from_52w_high":     ("d1", "d5", "z"),
    "pct_from_52w_low":      ("d1", "d5", "z"),
    "donchian_pos_20d":      ("d1", "d5", "z"),
    "atr_normalized_ret_5d": ("d1", "z"),          # no d5: already a 5-day window
    "rsi_14":                ("d1", "d5", "z"),
    "macd_hist":             ("d1", "z"),          # no d5: EMAs of 12 / 26 / 9 sessions
}
DERIVATIVES_P5 = [deriv_name(m, k) for m in CORE_P5 for k in DERIV_P5[m]]
SURFACED_P5 = CORE_P5 + DERIVATIVES_P5
STORED_ONLY_P5 = ["spot", "ohlc_session", "atr_14d"]
COLUMNS_P5 = ["trade_date", "quote_time", "spot", "ohlc_session", "atr_14d", *CORE_P5, *DERIVATIVES_P5]
Z_P5 = [m for m in CORE_P5 if "z" in DERIV_P5[m]]
assert len(CORE_P5) == 8 and len(SURFACED_P5) == 30 and len(SURFACED_PD) + len(SURFACED_P5) == 45

# Completed sessions behind a row before every level can exist: the 52-week window.
LEVEL_HISTORY_P5 = 252
# Spec §4.1: textbook ranges these may leave on a live price. Reported, never clamped.
TEXTBOOK = {"donchian_pos_20d": (0.0, 1.0), "pct_from_52w_high": (None, 0.0), "pct_from_52w_low": (0.0, None)}


def wilder_rsi_state(close, n: int = RSI_N) -> tuple[np.ndarray, np.ndarray]:
    """Wilder's average gain / loss as of each session's close. Changes need two
    consecutive known closes; the first n changes seed with their simple mean,
    then avg <- (avg (n-1) + x) / n. A missing close resets; re-seeds from scratch."""
    c = np.asarray(close, dtype=float)
    ag_out, al_out = np.full(len(c), np.nan), np.full(len(c), np.nan)
    gains, losses, ag, al = [], [], None, None
    for t in range(1, len(c)):
        if np.isnan(c[t]) or np.isnan(c[t - 1]):
            gains, losses, ag, al = [], [], None, None
            continue
        d = c[t] - c[t - 1]
        g, l = max(d, 0.0), max(-d, 0.0)
        if ag is None:
            gains.append(g), losses.append(l)
            if len(gains) < n:
                continue
            ag, al = sum(gains) / n, sum(losses) / n
        else:
            ag, al = (ag * (n - 1) + g) / n, (al * (n - 1) + l) / n
        ag_out[t], al_out[t] = ag, al
    return ag_out, al_out


def rsi_step(ag, al, c_prev, p, n: int = RSI_N):
    """ONE Wilder step from the frozen state (ag, al as of T-1, close C_{T-1})
    with p as today's close. RSI = 100 ag' / (ag' + al'); NULL if both are 0."""
    d = np.asarray(p, dtype=float) - c_prev
    ag2 = (ag * (n - 1) + np.maximum(d, 0.0)) / n
    al2 = (al * (n - 1) + np.maximum(-d, 0.0)) / n
    tot = ag2 + al2
    return 100.0 * ag2 / np.where(tot > 0, tot, np.nan)


def _ema_seeded(x: list, n: int) -> list:
    """EMA over a gap-free run: seeded with the SMA of the first n, then
    e <- e + 2/(n+1) (x - e). None before the seed."""
    out, e, a = [None] * len(x), None, 2.0 / (n + 1)
    for i, v in enumerate(x):
        if i == n - 1:
            e = sum(x[:n]) / n
        elif i >= n:
            e = e + a * (v - e)
        out[i] = e
    return out


def macd_state(close, fast: int = MACD_FAST, slow: int = MACD_SLOW, sig: int = MACD_SIG):
    """(EMA_fast, EMA_slow, signal) as of each session's close. Each EMA seeds
    with the SMA of its first n closes; MACD = EMA_fast - EMA_slow from the
    slow seed on; signal seeds with the SMA of the first `sig` MACD values.
    A missing close resets all three; they re-seed over the next run."""
    c = np.asarray(close, dtype=float)
    ef, es, sg = (np.full(len(c), np.nan) for _ in range(3))
    t = 0
    while t < len(c):
        if np.isnan(c[t]):
            t += 1
            continue
        run = []
        while t < len(c) and not np.isnan(c[t]):
            run.append(t)
            t += 1
        x = [float(c[i]) for i in run]
        f, s = _ema_seeded(x, fast), _ema_seeded(x, slow)
        first = slow - 1
        g = [None] * len(x)
        if len(x) >= first + sig:
            g[first:] = _ema_seeded([a - b for a, b in zip(f[first:], s[first:])], sig)
        for k, i in enumerate(run):
            if g[k] is not None:
                ef[i], es[i], sg[i] = f[k], s[k], g[k]
    return ef, es, sg


def macd_hist_step(ef, es, sg, p, fast: int = MACD_FAST, slow: int = MACD_SLOW, sig: int = MACD_SIG):
    """ONE step of all three EMAs from the frozen T-1 state with p as today's
    close; returns (MACD' - signal') / p."""
    p = np.asarray(p, dtype=float)
    ef2 = ef + 2.0 / (fast + 1) * (p - ef)
    es2 = es + 2.0 / (slow + 1) * (p - es)
    m2 = ef2 - es2
    sg2 = sg + 2.0 / (sig + 1) * (m2 - sg)
    return (m2 - sg2) / p


def session_state(daily: pd.DataFrame) -> pd.DataFrame:
    """Per session s, everything a bar on the NEXT session needs, as of s's close."""
    c, h, l = daily["close"], daily["high"], daily["low"]
    pc = c.shift(1)
    tr = np.maximum(np.maximum(h - l, (h - pc).abs()), (l - pc).abs())   # NaN if any input is NaN
    ag, al = wilder_rsi_state(c, RSI_N)
    ef, es, sg = macd_state(c)
    return pd.DataFrame({
        "ohlc_session": daily["trade_date"],
        "c_prev": c,                                              # C_{T-1}
        "ma20": c.rolling(20, min_periods=20).mean(),
        "ma50": c.rolling(50, min_periods=50).mean(),
        "hi252": c.rolling(252, min_periods=252).max(),
        "lo252": c.rolling(252, min_periods=252).min(),
        "hh20": h.rolling(20, min_periods=20).max(),
        "ll20": l.rolling(20, min_periods=20).min(),
        "c_base": c.shift(RET_BASE_BACK - 1),                     # C_{T-5}
        "atr_14d": tr.rolling(14, min_periods=14).mean(),
        "rsi_ag": ag, "rsi_al": al,
        "ema_fast": ef, "ema_slow": es, "macd_sig": sg,
    })


def bar_levels(st: pd.DataFrame, p) -> dict:
    """CORE_P5 for bars with price p against their day's frozen state `st`
    (aligned row for row). NaN p -> NaN everywhere."""
    p = np.asarray(p, dtype=float)
    rng = (st["hh20"] - st["ll20"]).to_numpy()
    return {
        "pct_from_ma20": p / st["ma20"].to_numpy() - 1.0,
        "pct_from_ma50": p / st["ma50"].to_numpy() - 1.0,
        "pct_from_52w_high": p / st["hi252"].to_numpy() - 1.0,
        "pct_from_52w_low": p / st["lo252"].to_numpy() - 1.0,
        "donchian_pos_20d": (p - st["ll20"].to_numpy()) / np.where(rng > 0, rng, np.nan),
        "atr_normalized_ret_5d": (p / st["c_base"].to_numpy() - 1.0)
                                 / (st["atr_14d"].to_numpy() / st["c_prev"].to_numpy()),
        "rsi_14": rsi_step(st["rsi_ag"].to_numpy(), st["rsi_al"].to_numpy(), st["c_prev"].to_numpy(), p),
        "macd_hist": macd_hist_step(st["ema_fast"].to_numpy(), st["ema_slow"].to_numpy(),
                                    st["macd_sig"].to_numpy(), p),
    }


@dataclass
class Price5:
    frame: pd.DataFrame          # rows in [start, end_eff], COLUMNS_P5
    full: pd.DataFrame           # every row from the seed's next session on
    grid: pd.DataFrame           # out-range grid with spot_status (for B.spot_report)
    daily: pd.DataFrame
    start: date
    end_eff: date
    data_last: date
    lead_in_sessions: int        # completed sessions before start
    lead_in_min_readings: int    # fewest known 52w readings any bar has before start (want >= 60)
    notes: list = field(default_factory=list)


def compute_price_5min(start: date, end: date, *, bars: pd.DataFrame, seed_from: date = SEED_FROM,
                       complete_through: date | None = None) -> Price5:
    inp = _input_spine(start, end, bars, seed_from, complete_through)
    st = session_state(inp.daily)
    st.insert(0, "trade_date", inp.rows)

    grid = B.classify_spot(B.expected_grid(inp.rows), bars[["trade_date", "quote_time", "spx_open"]])
    lv = grid.merge(st, on="trade_date", how="left", validate="many_to_one")
    lv = lv.sort_values(["trade_date", "quote_time"]).reset_index(drop=True)
    for m, v in bar_levels(lv, lv["spot"]).items():
        lv[m] = v
    lv["sess_n"] = lv["trade_date"].map({d: i for i, d in enumerate(inp.rows)})
    lv = lv[["trade_date", "quote_time", "spot", "ohlc_session", "atr_14d", "sess_n", *CORE_P5]]

    full = B.run_same_bar_derivatives(lv, DERIV_P5)
    full["trade_date"] = pd.to_datetime(full["trade_date"]).dt.date
    full["ohlc_session"] = pd.to_datetime(full["ohlc_session"]).dt.date
    full["quote_time"] = [B.normalize_quote_time(v) for v in full["quote_time"]]
    full = full.sort_values(["trade_date", "quote_time"]).reset_index(drop=True)[COLUMNS_P5]

    inr = (full["trade_date"] >= start) & (full["trade_date"] <= inp.end_eff)
    frame = full[inr].reset_index(drop=True)
    pre = full[(full["trade_date"] < start) & full["spot"].notna() & full["pct_from_52w_high"].notna()]
    out_sessions = [d for d in inp.rows if d >= start]
    reg = B.expected_grid(out_sessions)["quote_time"].unique()
    counts = pre.groupby("quote_time").size().reindex(reg, fill_value=0)
    return Price5(frame=frame, full=full, grid=grid[grid["trade_date"] >= start].reset_index(drop=True),
                  daily=inp.daily, start=start, end_eff=inp.end_eff, data_last=inp.data_last,
                  lead_in_sessions=sum(1 for s in inp.inputs if s < start),
                  lead_in_min_readings=int(counts.min()) if len(counts) else 0, notes=inp.notes)


def lookahead_probe_5min(bars: pd.DataFrame, days: list[date], frame: pd.DataFrame,
                         cut: time = time(12, 0), seed_from: date = SEED_FROM) -> list[tuple]:
    """For each day d: rebuild from bars strictly before d plus ONLY the
    spx_open of d's bars up to `cut` (d's high, low and close blanked, later
    opens blanked). Every row of d at or before `cut` must match `frame`."""
    bad = []
    built = frame.set_index(["trade_date", "quote_time"])
    for d in days:
        prev = get_trading_days(d - timedelta(days=10), d - timedelta(days=1))[-1]
        today = bars[bars["trade_date"] == d].copy()
        today[["spx_high", "spx_low", "spx_close"]] = np.nan
        today.loc[today["quote_time"] > cut, "spx_open"] = np.nan
        cutbars = pd.concat([bars[bars["trade_date"] < d], today], ignore_index=True)
        r = compute_price_5min(d, d, bars=cutbars, seed_from=seed_from, complete_through=prev).frame
        r = r[r["quote_time"] <= cut]
        if r.empty:
            bad.append((d, None, "rows", "present", "absent"))
        for row in r.to_dict("records"):
            key = (d, row["quote_time"])
            if key not in built.index:
                bad.append((d, row["quote_time"], "row", "absent", "present"))
                continue
            for c in COLUMNS_P5[2:]:
                if not _same(built.at[key, c], row[c]):
                    bad.append((d, row["quote_time"], c, built.at[key, c], row[c]))
    return bad


def check_price_5min(df: pd.DataFrame, *, sessions: list[date], chk: F.Checker,
                     full: pd.DataFrame | None = None, daily: pd.DataFrame | None = None) -> None:
    df = df.sort_values(["trade_date", "quote_time"]).reset_index(drop=True)

    exp = B.expected_grid(sessions)
    have = df[["trade_date", "quote_time"]]
    dup = int(have.duplicated().sum())
    missing = exp.merge(have, how="left", indicator=True).query("_merge == 'left_only'")
    extra = have.merge(exp, how="left", indicator=True).query("_merge == 'left_only'")
    per = exp.groupby("trade_date").size()
    half = per[per < 78]
    chk.check(dup == 0 and missing.empty and extra.empty,
              f"bar grid exact: {len(have)} rows = {len(exp)} calendar bars over {len(sessions)} sessions "
              f"({len(half)} early close(s) with {sorted(set(half.tolist()))} bars; no 16:00 bar); "
              f"{len(missing)} missing, {len(extra)} extra, {dup} duplicate"
              + ("" if missing.empty else "\n" + missing.head(5).to_string(index=False))
              + ("" if extra.empty else "\n" + extra.head(5).to_string(index=False)))

    cal = get_trading_days(sessions[0] - timedelta(days=15), sessions[-1])
    prev = {cal[i]: cal[i - 1] for i in range(1, len(cal))}
    src = df["ohlc_session"]
    leak = ~(src.notna() & (src < df["trade_date"]) & (src == df["trade_date"].map(prev)))
    chk.check(not leak.any(),
              f"LOOKAHEAD: every bar's ohlc_session (the last session whose close, high or low it uses) is "
              f"strictly earlier than its trade_date and is the prior NYSE session — {len(df)} rows, "
              f"{int(leak.sum())} violations" + F._show(df, leak, ["trade_date", "quote_time", "ohlc_session"]))

    unknown = df["spot"].isna()
    leaked = unknown & df[CORE_P5].notna().any(axis=1)
    chk.check(not leaked.any(), f"price NULL-guard: all 8 levels NULL on the {int(unknown.sum())} bar(s) with "
                                f"unknown spx_open ({int(leaked.sum())} violate)")

    if daily is not None:
        dd = daily.set_index("trade_date")
        okday = dd[["high", "low", "close"]].notna().all(axis=1).to_numpy()
        pos = {d: i for i, d in enumerate(dd.index)}
        clean_s = {}
        for s in df["ohlc_session"].unique():
            i = pos.get(s)
            clean_s[s] = i is not None and i >= LEVEL_HISTORY_P5 and bool(okday[i - LEVEL_HISTORY_P5:i + 1].all())
        clean = ~unknown & df["ohlc_session"].map(clean_s).fillna(False).astype(bool)
        gap = clean & df[CORE_P5].isna().any(axis=1)
        chk.check(not gap.any(),
                  f"every bar with a known price whose {LEVEL_HISTORY_P5 + 1} input sessions all have a daily "
                  f"high, low and close carries all 8 levels ({int(gap.sum())} without, of {int(clean.sum())})"
                  + F._show(df, gap, ["trade_date", "quote_time", *CORE_P5]))

    s = df["spot"].dropna()
    chk.check(bool((s > 0).all()), f"spot > 0 wherever known  [min {s.min():.2f}]" if len(s) else "spot: none")
    s = df["rsi_14"].dropna()
    chk.check(bool(((s >= 0) & (s <= 100)).all()),
              f"rsi_14 in [0, 100]  [min {s.min():.6g}, max {s.max():.6g}]" if len(s) else "rsi_14: no values")
    s = df["atr_14d"].dropna()
    chk.check(bool((s > 0).all()), f"atr_14d > 0 wherever defined  [min {s.min():.6g}]" if len(s) else "atr_14d: none")

    # §4.2 on real data: within a session, bars with the same P must give the same RSI and MACD.
    k = df[df["spot"].notna()]
    g = k.groupby(["trade_date", "spot"])
    n_groups = int((g.size() > 1).sum())
    viol = int(((g["rsi_14"].nunique(dropna=False) > 1) | (g["macd_hist"].nunique(dropna=False) > 1)).sum())
    chk.check(viol == 0, f"RECURSION: bars of one session with the same price have identical rsi_14 and "
                         f"macd_hist — {n_groups} repeated-price group(s), {viol} violate")

    if full is not None:
        f = full[full["spot"].notna()].sort_values(["quote_time", "trade_date"])
        in_out = f["trade_date"] >= sessions[0]
        bad = []
        for m in Z_P5:
            cnt = (f[m].notna().astype(int).groupby(f["quote_time"])
                   .transform(lambda x: x.rolling(Z_WINDOW, min_periods=1).sum()))
            if (in_out & f[f"zscore_{m}_3m"].notna() & (cnt < Z_WINDOW)).any():
                bad.append(m)
        chk.check(not bad, "z-scores NULL wherever the bar's 60-reading window holds < 60 observations"
                  + (f": violated by {bad}" if bad else ""))


def textbook_report(df: pd.DataFrame) -> dict:
    """Spec §4.1: bars outside the textbook range, by year. Never fails; nothing is clamped."""
    year = pd.to_datetime(df["trade_date"]).dt.year
    out = {}
    print(f"  {'metric':<20}{'year':>6}{'bars':>9}{'outside':>9}{'share':>8}{'sessions':>10}  textbook range")
    for m, (lo, hi) in TEXTBOOK.items():
        v = df[m]
        outside = pd.Series(False, index=df.index)
        if lo is not None:
            outside |= v < lo
        if hi is not None:
            outside |= v > hi
        rng = f"[{'-inf' if lo is None else lo}, {'+inf' if hi is None else hi}]"
        for y in sorted(year.unique()):
            sel = year == y
            n = int(v[sel].notna().sum())
            o = int(outside[sel].sum())
            ns = df.loc[sel & outside, "trade_date"].nunique()
            print(f"  {m:<20}{y:>6}{n:>9}{o:>9}{(o / n if n else 0):>8.1%}{ns:>10}  {rng}")
            out[(m, y)] = (o, n, ns)
        n_all = int(v.notna().sum())
        print(f"  {m:<20}{'all':>6}{n_all:>9}{int(outside.sum()):>9}"
              f"{(outside.sum() / n_all if n_all else 0):>8.1%}{df.loc[outside, 'trade_date'].nunique():>10}")
    return out
