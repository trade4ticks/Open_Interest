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
from datetime import date, timedelta

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
              f"[{B.KNOWN_GAPS.get(r['trade_date'], 'not a known gap')}]")
    for _, r in partial.iterrows():
        miss = [c for c in ("open", "high", "low", "close") if pd.isna(r[c])]
        print(f"    {r['trade_date']}  partial: high from {int(r['n_high'])}/{int(r['n_bars'])} bars, "
              f"low from {int(r['n_low'])}/{int(r['n_bars'])}"
              + (f"; NULL {', '.join(miss)}" if miss else "")
              + f"  [{B.KNOWN_GAPS.get(r['trade_date'], 'not a known gap')}]")
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


def compute_price_daily(start: date, end: date, *, bars: pd.DataFrame,
                        seed_from: date = SEED_FROM, complete_through: date | None = None) -> PriceDaily:
    """`bars` = load_ohlc(dsn, seed_from, end) (or a mock in the same shape).

    Rows run to the session after the last COMPLETE session, which is the last
    one with a usable close (a session still trading has no 15:55 close yet, so
    its partial high / low never feed a row). `complete_through` overrides
    that, for lookahead_probe, which cuts the bars at a day boundary."""
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
    # Row T needs only T-1: rows run to the session after the last usable close.
    nxt = next_trading_day(data_last)
    cap = min(end, nxt)
    end_eff = max(s for s in [*sess_all, nxt] if s <= cap)
    if nxt < end:
        notes.append(f"--end {end} clamped to {end_eff}: the last session with a usable close in "
                     f"index_ohlc is {data_last}, and row T needs T-1's close")
    if end_eff < start:
        raise SystemExit(f"nothing to build: --start {start} is after {end_eff}")

    inputs = [s for s in sess_all if s < end_eff]          # every session a row in range may use
    daily = daily_all[daily_all["trade_date"].isin(set(inputs))].reset_index(drop=True)
    sm = session_metrics(daily)
    # Each session's close feeds the row of the next NYSE session; the last input's is end_eff.
    sm.insert(0, "trade_date", [*inputs[1:], end_eff])
    full = _derivatives_pd(sm)[COLUMNS_PD]
    frame = full[(full["trade_date"] >= start) & (full["trade_date"] <= end_eff)].reset_index(drop=True)
    lead = sum(1 for s in inputs if s < start)
    return PriceDaily(frame=frame, full=full, daily=daily, start=start, end_requested=end,
                      end_eff=end_eff, data_last=data_last, lead_in_sessions=lead, notes=notes)


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
