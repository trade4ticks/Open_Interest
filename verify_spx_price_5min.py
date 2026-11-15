"""
verify_spx_price_5min.py — checks on spx_price_5min as written.

    python verify_spx_price_5min.py --start 20200102 --end 20261113
    python verify_spx_price_5min.py --start 20200102 --end 20261113 --recompute-date 20261026

HARD checks (exit 1 on failure):
  * the bar grid is exactly the NYSE calendar's (78 / 42 bars, no 16:00 bar)
  * LOOKAHEAD: ohlc_session is the prior NYSE session on every bar
  * price NULL-guard; stored NULL-price bars are exactly index_ohlc's unknown bars
  * every bar with a known price and 253 clean input sessions has all 8 levels
  * rsi_14 in [0, 100], atr_14d > 0, spot > 0
  * RECURSION: bars of one session with the same price have identical rsi_14
    and macd_hist (spec §4.2)
  * z-scores NULL until a bar's window holds 60 readings
  * INDEPENDENT RECOMPUTE of every bar of one day, every stored column: daily
    OHLC from index_ohlc rows before that day (own SQL zero / NaN / NULL test,
    bars from pandas_market_calendars), and from the day itself ONLY spx_open
    — so a match also proves no close, high or low of the day was used. RSI
    and MACD states, rolling windows and same-bar d1 / d5 / z in plain Python.

REPORTED (never fail the run):
  * spec §4.1: bars outside the textbook range by year
  * sessions with no usable daily OHLC, or only part of it
  * every column's min / median / max / NULL count by year; discreteness ranking
"""
from __future__ import annotations

import argparse
import statistics
import sys
from datetime import date

import pandas as pd
import psycopg2

import config
import lib.spx_features as F
import lib.spx_price as P
import lib.spx_table_b as B
import verify_spx_price_daily as VD
from build_spx_features import _parse_day
from lib.market_hours import get_trading_days

ROWS_BACK = 120          # rows of history recomputed so every bar has 60 readings for its z-score


def section(t: str) -> None:
    print(f"\n=== {t} ===")


def load_db(start: date, end: date) -> pd.DataFrame:
    if not config.SPX_FEATURES_DSN:
        raise SystemExit("SPX_FEATURES_DSN is not set")
    with psycopg2.connect(config.SPX_FEATURES_DSN) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(P.COLUMNS_P5)} FROM {P.TABLE_P5} "
                    "WHERE trade_date BETWEEN %s AND %s ORDER BY trade_date, quote_time", (start, end))
        df = pd.DataFrame(cur.fetchall(), columns=P.COLUMNS_P5)
    for c in ["spot", "atr_14d", *P.CORE_P5, *P.DERIVATIVES_P5]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    df["quote_time"] = [B.normalize_quote_time(v) for v in df["quote_time"]]
    return df


# --- independent recompute -------------------------------------------------------------

def _opens_on(day: date) -> dict:
    """{quote_time: spx_open or None} for `day` — the open column ONLY."""
    conn = psycopg2.connect(config.SPX_INDEX_OHLC_DSN)
    try:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute("""SELECT quote_time, CASE WHEN spx_open IS NULL OR spx_open = 'NaN'::float8
                                                    OR spx_open <= 0 THEN NULL ELSE spx_open END
                           FROM public.index_ohlc WHERE trade_date = %s""", (day,))
            rows = cur.fetchall()
    finally:
        conn.close()
    return {B.normalize_quote_time(q): VD._num(None if v is None else float(v)) for q, v in rows}


def _rsi_states(C, j0=0, n=P.RSI_N):
    """Per session j >= j0: (avg gain, avg loss) or None, rebuilt for each j from
    the start of its run of known closes."""
    out = [None] * len(C)
    for j in range(j0, len(C)):
        # length of the run of consecutive known changes ending at j
        k = j
        while k >= 1 and C[k] is not None and C[k - 1] is not None:
            k -= 1
        changes = [C[i] - C[i - 1] for i in range(k + 1, j + 1)]
        if len(changes) < n:
            continue
        ag = statistics.fmean(max(d, 0.0) for d in changes[:n])
        al = statistics.fmean(max(-d, 0.0) for d in changes[:n])
        for d in changes[n:]:
            ag = (ag * (n - 1) + max(d, 0.0)) / n
            al = (al * (n - 1) + max(-d, 0.0)) / n
        out[j] = (ag, al)
    return out


def _macd_states(C, j0=0, fast=P.MACD_FAST, slow=P.MACD_SLOW, sig=P.MACD_SIG):
    """Per session j >= j0: (ema_fast, ema_slow, signal) or None, rebuilt for
    each j from the start of its run of known closes."""
    out = [None] * len(C)
    run = []
    for j, c in enumerate(C):
        if c is None:
            run = []
            continue
        run.append(c)
        if j < j0 or len(run) < slow + sig - 1:
            continue
        def ema_path(xs, n):
            e = sum(xs[:n]) / n
            path = [e]
            for x in xs[n:]:
                e += 2.0 / (n + 1) * (x - e)
                path.append(e)
            return path                       # path[i] = EMA at xs[n - 1 + i]
        ef = ema_path(run, fast)
        es = ema_path(run, slow)
        macd = [ef[i + slow - fast] - es[i] for i in range(len(es))]   # aligned from run[slow - 1]
        sg = ema_path(macd, sig)
        out[j] = (ef[-1], es[-1], sg[-1])
    return out


def recompute_day(day: date, raw: pd.DataFrame | None = None, opens_today: dict | None = None) -> pd.DataFrame:
    raw = VD._bars_independent(day) if raw is None else raw
    opens_today = _opens_on(day) if opens_today is None else opens_today
    sched, H, L, C = VD.daily_series(day, raw)
    opens = {}
    for r in raw.itertuples(index=False):
        opens[(pd.Timestamp(r.trade_date).date(), B.normalize_quote_time(r.quote_time))] = VD._num(r.spx_open)
    j0 = max(0, len(C) - ROWS_BACK - 1)
    rsi_s, macd_s = _rsi_states(C, j0), _macd_states(C, j0)
    TR = [None] + [max(H[k] - L[k], abs(H[k] - C[k - 1]), abs(L[k] - C[k - 1]))
                   if None not in (H[k], L[k], C[k - 1]) else None for k in range(1, len(C))]

    def win(xs, j, n):
        w = xs[j - n + 1:j + 1] if j >= n - 1 else []
        return w if len(w) == n and None not in w else None

    today_bars = VD._schedule(day, day)[0][1]
    row_list = [(sched[k + 1][0], sched[k + 1][1], k) for k in range(len(sched) - 1)] + [(day, today_bars, len(sched) - 1)]
    row_list = row_list[-ROWS_BACK:]
    levels = []
    for d, bars, j in row_list:
        w20, w50, w252 = win(C, j, 20), win(C, j, 50), win(C, j, 252)
        h20, l20, tr14 = win(H, j, 20), win(L, j, 20), win(TR, j, 14)
        atr = statistics.fmean(tr14) if tr14 else None
        cb = C[j - (P.RET_BASE_BACK - 1)] if j >= P.RET_BASE_BACK - 1 else None
        for q in bars:
            p = opens_today.get(q) if d == day else opens.get((d, q))
            r = {"trade_date": d, "quote_time": q, "spot": p, "ohlc_session": sched[j][0], "atr_14d": atr}
            for m in P.CORE_P5:
                r[m] = None
            if p is not None:
                if w20:
                    r["pct_from_ma20"] = p / statistics.fmean(w20) - 1
                if w50:
                    r["pct_from_ma50"] = p / statistics.fmean(w50) - 1
                if w252:
                    r["pct_from_52w_high"] = p / max(w252) - 1
                    r["pct_from_52w_low"] = p / min(w252) - 1
                if h20 and l20 and max(h20) > min(l20):
                    r["donchian_pos_20d"] = (p - min(l20)) / (max(h20) - min(l20))
                if atr and cb is not None and C[j] is not None:
                    r["atr_normalized_ret_5d"] = (p / cb - 1) / (atr / C[j])
                if rsi_s[j] is not None and C[j] is not None:
                    ag, al = rsi_s[j]
                    n = P.RSI_N
                    g, lo = max(p - C[j], 0.0), max(C[j] - p, 0.0)
                    ag2, al2 = (ag * (n - 1) + g) / n, (al * (n - 1) + lo) / n
                    r["rsi_14"] = 100 * ag2 / (ag2 + al2) if ag2 + al2 > 0 else None
                if macd_s[j] is not None:
                    ef, es, sg = macd_s[j]
                    ef2 = ef + 2 / (P.MACD_FAST + 1) * (p - ef)
                    es2 = es + 2 / (P.MACD_SLOW + 1) * (p - es)
                    sg2 = sg + 2 / (P.MACD_SIG + 1) * ((ef2 - es2) - sg)
                    r["macd_hist"] = ((ef2 - es2) - sg2) / p
            levels.append(r)
    order = {d: i for i, (d, _, _) in enumerate(row_list)}
    by_bar = {(r["trade_date"], r["quote_time"]): r for r in levels}
    out = []
    for r in [x for x in levels if x["trade_date"] == day]:
        q, i = r["quote_time"], order[day]
        hist = [x for x in levels if x["quote_time"] == q and x["spot"] is not None and order[x["trade_date"]] <= i]
        o = dict(r)
        for m in P.CORE_P5:
            for k in P.DERIV_P5[m]:
                name = P.deriv_name(m, k)
                if k == "z":
                    w = [x[m] for x in hist[-P.Z_WINDOW:]]
                    if r["spot"] is not None and len(w) == P.Z_WINDOW and None not in w:
                        sd = statistics.stdev(w)
                        o[name] = (r[m] - statistics.fmean(w)) / sd if sd > 0 else None
                    else:
                        o[name] = None
                else:
                    back = row_list[i - int(k[1])][0] if i >= int(k[1]) else None
                    prev = by_bar.get((back, q))
                    o[name] = (r[m] - prev[m]) if prev is not None and None not in (r[m], prev[m]) else None
        out.append(o)
    return pd.DataFrame(out)[P.COLUMNS_P5]


def check_recompute(db: pd.DataFrame, day: date, chk: F.Checker) -> None:
    section(f"Independent recompute of every bar of {day} (earlier sessions + that day's spx_open only)")
    stored = db[db["trade_date"] == day].set_index("quote_time")
    if stored.empty:
        chk.check(False, f"{day} is not stored")
        return
    mine = recompute_day(day).set_index("quote_time")
    diffs = []
    for q in sorted(set(stored.index) | set(mine.index)):
        if q not in stored.index or q not in mine.index:
            diffs.append((q, "row", q in stored.index, q in mine.index))
            continue
        for c in P.COLUMNS_P5:
            if c == "quote_time":
                continue
            a, b = stored.at[q, c], mine.at[q, c]
            if not VD._same(a, b):
                diffs.append((q, c, a, b))
    for q, c, a, b in diffs[:20]:
        print(f"  DIFF {q} {c}: stored {a!r}, recomputed {b!r}")
    chk.check(not diffs, f"independent recompute of {day}: {len(stored)} bars x {len(P.COLUMNS_P5) - 1} columns "
                         f"match ({len(diffs)} differ); the day's high, low and close were never read")


# --- main -----------------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--start", type=_parse_day, required=True)
    p.add_argument("--end", type=_parse_day, required=True)
    p.add_argument("--recompute-date", type=_parse_day, help="day rebuilt independently (default: last stored)")
    p.add_argument("--no-reports", action="store_true")
    args = p.parse_args()
    if not config.SPX_INDEX_OHLC_DSN:
        p.error("SPX_INDEX_OHLC_DSN is not set")

    chk = F.Checker()
    db = load_db(args.start, args.end)
    if db.empty:
        section(f"{P.TABLE_P5}: no rows in {args.start} → {args.end}")
        return 1
    end = db["trade_date"].max()
    section(f"{P.TABLE_P5}: {len(db):,} rows in {args.start} → {end}")
    sessions = get_trading_days(args.start, end)

    bars = P.load_ohlc(config.SPX_INDEX_OHLC_DSN, P.SEED_FROM, end)
    daily = P.daily_ohlc(bars, [s for s in get_trading_days(P.SEED_FROM, end) if s < end])
    P.check_price_5min(db, sessions=sessions, chk=chk, daily=daily)

    k = db[db["spot"].notna()].sort_values(["quote_time", "trade_date"])
    nth = k.groupby("quote_time").cumcount()
    bad = []
    for m in P.Z_P5:
        cnt = (k[m].notna().astype(int).groupby(k["quote_time"])
               .transform(lambda x: x.rolling(P.Z_WINDOW, min_periods=1).sum()))
        if ((nth >= P.Z_WINDOW - 1) & k[f"zscore_{m}_3m"].notna() & (cnt < P.Z_WINDOW)).any():
            bad.append(m)
    chk.check(not bad, f"z-scores NULL until a bar's window holds 60 readings (judged from each bar's "
                       f"{P.Z_WINDOW}th stored reading)" + (f"; violated by {bad}" if bad else ""))

    grid = B.classify_spot(B.expected_grid(sessions), bars[["trade_date", "quote_time", "spx_open"]])
    stored_unknown = set(map(tuple, db.loc[db["spot"].isna(), ["trade_date", "quote_time"]].to_numpy()))
    grid_unknown = set(map(tuple, grid.loc[grid["spot_status"] != "ok", ["trade_date", "quote_time"]].to_numpy()))
    chk.check(stored_unknown == grid_unknown,
              f"stored NULL-price bars are exactly index_ohlc's unknown bars "
              f"({len(stored_unknown)} stored, {len(grid_unknown)} in index_ohlc today)")

    section("Spec §4.1: bars outside the textbook range (expected; never clamped)")
    P.textbook_report(db)
    section("Daily OHLC report (sessions feeding the stored rows)")
    P.daily_ohlc_report(daily[daily["trade_date"] >= db["ohlc_session"].min()].reset_index(drop=True))

    check_recompute(db, args.recompute_date or end, chk)
    if not args.no_reports:
        VD.reports(db, ["spot", "atr_14d", *P.CORE_P5, *P.DERIVATIVES_P5])

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
