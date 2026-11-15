"""
verify_spx_price_daily.py — checks on spx_price_daily as written.

    python verify_spx_price_daily.py --start 20200102 --end 20261113
    python verify_spx_price_daily.py --start 20200102 --end 20261113 --recompute-date 20261026

HARD checks (exit 1 on failure):
  * exactly one row per NYSE session in range
  * LOOKAHEAD: ohlc_session is the prior NYSE session on every row
  * pct_up_days_20d in [0, 1], adx_14 in [0, 100], rv_ratio_5d_20d > 0,
    streak a whole number
  * every row whose 61 input sessions have a daily high, low and close
    carries all 5 levels
  * z-scores NULL until the window holds 60 observations
  * INDEPENDENT RECOMPUTE of one row, every stored column: index_ohlc read with
    its own SQL test for zero / NaN / NULL and only for trade_date < T (so a
    match also proves no lookahead), bars from pandas_market_calendars
    directly, every metric in plain Python loops, ADX smoothed segment by
    segment, d1 / d5 / z rebuilt by hand

REPORTED (never fail the run):
  * sessions with no usable daily OHLC, or only part of it
  * every column's min / median / max / NULL count by year
  * every column ranked by how little it moves
"""
from __future__ import annotations

import argparse
import math
import statistics
import sys
from datetime import date

import pandas as pd
import pandas_market_calendars as mcal
import psycopg2

import config
import lib.spx_features as F
import lib.spx_price as P
import lib.spx_table_b as B
from build_spx_features import _parse_day
from lib.market_hours import get_trading_days

REL, ABS = 1e-9, 1e-12


def section(t: str) -> None:
    print(f"\n=== {t} ===")


def load_db(start: date, end: date) -> pd.DataFrame:
    if not config.SPX_FEATURES_DSN:
        raise SystemExit("SPX_FEATURES_DSN is not set")
    with psycopg2.connect(config.SPX_FEATURES_DSN) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(P.COLUMNS_PD)} FROM {P.TABLE_PD} "
                    "WHERE trade_date BETWEEN %s AND %s ORDER BY trade_date", (start, end))
        df = pd.DataFrame(cur.fetchall(), columns=P.COLUMNS_PD)
    for c in P.COLUMNS_PD[2:]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    return df


# --- independent recompute -------------------------------------------------------------

def _bars_independent(before: date) -> pd.DataFrame:
    """index_ohlc rows with trade_date < `before`; unknown prices NULLed in SQL."""
    conn = psycopg2.connect(config.SPX_INDEX_OHLC_DSN)
    try:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cols = ", ".join(f"CASE WHEN {c} IS NULL OR {c} = 'NaN'::float8 OR {c} <= 0 THEN NULL ELSE {c} END"
                             for c in P.PRICE_COLS)
            cur.execute(f"SELECT trade_date, quote_time, {cols} FROM public.index_ohlc "
                        "WHERE trade_date >= %s AND trade_date < %s", (P.SEED_FROM, before))
            df = pd.DataFrame(cur.fetchall(), columns=["trade_date", "quote_time", *P.PRICE_COLS])
    finally:
        conn.close()
    return df


def _schedule(lo: date, hi: date) -> list[tuple]:
    """[(session, [bar start times])] straight from pandas_market_calendars."""
    s = mcal.get_calendar("NYSE").schedule(start_date=lo, end_date=hi)
    out = []
    for ts, row in s.iterrows():
        o = row["market_open"].tz_convert("America/New_York")
        c = row["market_close"].tz_convert("America/New_York")
        bars, t = [], o
        while t < c:
            bars.append(t.time())
            t += pd.Timedelta(minutes=5)
        out.append((ts.date(), bars))
    return out


def _num(v):
    return None if v is None or (isinstance(v, float) and math.isnan(v)) else float(v)


def _mean(xs):
    return None if any(x is None for x in xs) else sum(xs) / len(xs)


def _adx_by_segments(H, L, C, n=P.ADX_N):
    """Wilder's ADX written as passes over maximal runs of usable sessions."""
    m = len(H)
    usable = [False] + [None not in (H[t], L[t], H[t - 1], L[t - 1], C[t - 1]) for t in range(1, m)]
    out = [None] * m
    t = 1
    while t < m:
        if not usable[t]:
            t += 1
            continue
        seg = []
        while t < m and usable[t]:
            seg.append(t)
            t += 1
        tr = [max(H[k] - L[k], abs(H[k] - C[k - 1]), abs(L[k] - C[k - 1])) for k in seg]
        pdm, mdm = [], []
        for k in seg:
            up, dn = H[k] - H[k - 1], L[k - 1] - L[k]
            pdm.append(up if up > dn and up > 0 else 0.0)
            mdm.append(dn if dn > up and dn > 0 else 0.0)
        if len(seg) < n:
            continue
        st, sp, sm = sum(tr[:n]), sum(pdm[:n]), sum(mdm[:n])
        dx = []
        for i in range(n - 1, len(seg)):
            if i >= n:
                st, sp, sm = st - st / n + tr[i], sp - sp / n + pdm[i], sm - sm / n + mdm[i]
            pdi, mdi = 100 * sp / st, 100 * sm / st
            dx.append((seg[i], 100 * abs(pdi - mdi) / (pdi + mdi)))
        if len(dx) < n:
            continue
        adx = sum(v for _, v in dx[:n]) / n
        out[dx[n - 1][0]] = adx
        for k, v in dx[n:]:
            adx = (adx * (n - 1) + v) / n
            out[k] = adx
    return out


def independent_levels(before: date, raw: pd.DataFrame | None = None):
    """Levels for every row up to and including row `before`, from bars strictly
    earlier. Returns (row dates, {metric: [values]}, ohlc sessions)."""
    raw = _bars_independent(before) if raw is None else raw
    px = {}
    for r in raw.itertuples(index=False):
        d = pd.Timestamp(r.trade_date).date()
        q = B.normalize_quote_time(r.quote_time)
        px[(d, q)] = tuple(_num(v) for v in (r.spx_open, r.spx_high, r.spx_low, r.spx_close))
    sched = [(d, bars) for d, bars in _schedule(P.SEED_FROM, before) if d < before]
    H, L, C = [], [], []
    for d, bars in sched:
        rows = [px.get((d, q), (None,) * 4) for q in bars]
        hs = [x[1] for x in rows if x[1] is not None]
        ls = [x[2] for x in rows if x[2] is not None]
        H.append(max(hs) if hs else None)
        L.append(min(ls) if ls else None)
        C.append(rows[-1][3])
    m = len(C)
    R = [None] + [math.log(C[j] / C[j - 1]) if None not in (C[j], C[j - 1]) else None for j in range(1, m)]
    ma20 = [_mean(C[j - 19:j + 1]) if j >= 19 else None for j in range(m)]
    lv = {k: [None] * m for k in P.CORE_PD}
    for j in range(m):
        if j >= 5 and None not in (ma20[j], ma20[j - 5]):
            lv["ma20_slope_5d"][j] = ma20[j] / ma20[j - 5] - 1
        if j >= 19 and None not in R[j - 19:j + 1]:
            lv["pct_up_days_20d"][j] = sum(1 for x in R[j - 19:j + 1] if x > 0) / 20
            s20 = statistics.stdev(R[j - 19:j + 1])
            lv["rv_ratio_5d_20d"][j] = statistics.stdev(R[j - 4:j + 1]) / s20 if s20 > 0 else None
        # streak: walk back from j
        if R[j] is not None:
            sg = (R[j] > 0) - (R[j] < 0)
            if sg == 0:
                lv["streak"][j] = 0.0
            else:
                k = j
                while k >= 0 and R[k] is not None and ((R[k] > 0) - (R[k] < 0)) == sg:
                    k -= 1
                lv["streak"][j] = float(sg * (j - k)) if k >= 0 and R[k] is not None else None
    lv["adx_14"] = _adx_by_segments(H, L, C)
    # Session j's close feeds the row of session j+1 (the last feeds `before`).
    rows = [d for d, _ in sched[1:]] + [before]
    return rows, lv, [d for d, _ in sched]


def recompute_row(day: date, raw: pd.DataFrame | None = None) -> dict:
    rows, lv, src = independent_levels(day, raw)
    j = len(rows) - 1
    out = {"trade_date": day, "ohlc_session": src[j]}
    for m in P.CORE_PD:
        x = lv[m]
        out[m] = x[j]
        for k in P.DERIV_PD[m]:
            if k == "z":
                w = x[j - P.Z_WINDOW + 1:j + 1] if j >= P.Z_WINDOW - 1 else []
                if len(w) == P.Z_WINDOW and None not in w:
                    sd = statistics.stdev(w)
                    out[P.deriv_name(m, k)] = (x[j] - statistics.fmean(w)) / sd if sd > 0 else None
                else:
                    out[P.deriv_name(m, k)] = None
            else:
                n = int(k[1])
                out[P.deriv_name(m, k)] = (x[j] - x[j - n]) if j >= n and None not in (x[j], x[j - n]) else None
    return out


def _same(a, b) -> bool:
    na, nb = a is None or pd.isna(a), b is None or pd.isna(b)
    if na or nb:
        return na and nb
    if isinstance(a, date) or isinstance(b, date):
        return a == b
    return math.isclose(float(a), float(b), rel_tol=REL, abs_tol=ABS)


def check_recompute(db: pd.DataFrame, day: date, chk: F.Checker) -> None:
    section(f"Independent recompute of {day} (index_ohlc bars before {day} only)")
    row = db[db["trade_date"] == day]
    if row.empty:
        chk.check(False, f"{day} is not stored")
        return
    stored = row.iloc[0].to_dict()
    mine = recompute_row(day)
    diffs = [(c, stored[c], mine[c]) for c in P.COLUMNS_PD if not _same(stored[c], mine[c])]
    for c in P.COLUMNS_PD:
        print(f"  {c:<32}{str(stored[c]):>26}{str(mine[c]):>26}{'  <-- DIFF' if not _same(stored[c], mine[c]) else ''}")
    chk.check(not diffs, f"independent recompute of {day} matches every stored column "
                         f"({len(P.COLUMNS_PD)} compared, {len(diffs)} differ); bars on or after {day} were never "
                         f"read, so the match also confirms no lookahead")


# --- reports -------------------------------------------------------------------------------

def reports(db: pd.DataFrame) -> None:
    year = pd.to_datetime(db["trade_date"]).dt.year
    cols = P.COLUMNS_PD[2:]
    section("Report: min / median / max / NULLs by year (every column)")
    print(f"{'column':<34}{'year':>6}{'min':>14}{'median':>14}{'max':>14}{'nulls':>7}{'rows':>6}")
    for c in cols:
        for y, s in db[c].groupby(year):
            print(f"{c:<34}{y:>6}{s.min():>14.6g}{s.median():>14.6g}{s.max():>14.6g}"
                  f"{int(s.isna().sum()):>7}{len(s):>6}")

    section("Report: every column by how little it moves (all years)")
    print("  p05-p95 = the spread a threshold can work with; distinct / at-mode = how discrete it is")
    rows = []
    for c in cols:
        s = db[c].dropna()
        if s.empty:
            continue
        p05, p50, p95 = s.quantile([0.05, 0.5, 0.95])
        mode_share = s.round(10).value_counts(normalize=True).iloc[0]
        rows.append((c, p05, p50, p95, p95 - p05, s.round(10).nunique(), mode_share))
    print(f"{'column':<34}{'p05':>12}{'median':>12}{'p95':>12}{'p95-p05':>12}{'distinct':>10}{'at mode':>9}")
    for c, a, m, b, w, k, ms in sorted(rows, key=lambda t: (t[5], t[4])):
        print(f"{c:<34}{a:>12.5g}{m:>12.5g}{b:>12.5g}{w:>12.5g}{k:>10}{ms:>9.1%}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--start", type=_parse_day, required=True)
    p.add_argument("--end", type=_parse_day, required=True)
    p.add_argument("--recompute-date", type=_parse_day, help="row rebuilt independently (default: last stored)")
    p.add_argument("--no-reports", action="store_true")
    args = p.parse_args()
    if not config.SPX_INDEX_OHLC_DSN:
        p.error("SPX_INDEX_OHLC_DSN is not set")

    chk = F.Checker()
    db = load_db(args.start, args.end)
    if db.empty:
        section(f"{P.TABLE_PD}: no rows in {args.start} → {args.end}")
        return 1
    end = db["trade_date"].max()
    section(f"{P.TABLE_PD}: {len(db):,} rows in {args.start} → {end}")
    sessions = get_trading_days(args.start, end)

    bars = P.load_ohlc(config.SPX_INDEX_OHLC_DSN, P.SEED_FROM, end)
    daily = P.daily_ohlc(bars, [s for s in get_trading_days(P.SEED_FROM, end) if s < end])
    P.check_price_daily(db, sessions=sessions, chk=chk, daily=daily)

    # z gating, judged from each column's 60th stored row (earlier rows depend on unstored history).
    nth = pd.Series(range(len(db)), index=db.index)
    bad = []
    for m in P.Z_PD:
        cnt = db[m].notna().astype(int).rolling(P.Z_WINDOW, min_periods=1).sum()
        if ((nth >= P.Z_WINDOW - 1) & db[f"zscore_{m}_3m"].notna() & (cnt < P.Z_WINDOW)).any():
            bad.append(m)
    chk.check(not bad, f"z-scores NULL until the window holds 60 observations (judged from the "
                       f"{P.Z_WINDOW}th stored row)" + (f"; violated by {bad}" if bad else ""))

    section("Daily OHLC report (sessions feeding the stored rows)")
    feed = daily[(daily["trade_date"] >= db["ohlc_session"].min())].reset_index(drop=True)
    P.daily_ohlc_report(feed)

    check_recompute(db, args.recompute_date or end, chk)
    if not args.no_reports:
        reports(db)

    section("Result")
    if chk.failed:
        print(f"{len(chk.failed)} HARD CHECK(S) FAILED:")
        for f in chk.failed:
            print(f"  - {f.splitlines()[0]}")
        return 1
    print("All hard checks pass.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
