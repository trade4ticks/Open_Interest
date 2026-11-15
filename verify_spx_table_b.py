"""
verify_spx_table_b.py — checks on table B (spx_oi_5min) as written.

    python verify_spx_table_b.py --start 20200102 --end 20261113
    python verify_spx_table_b.py --start 20260801 --end 20261113 --recompute-date 20261026
    python verify_spx_table_b.py --record-regression 20261026     # once, then commit the fixture
    python verify_spx_table_b.py --start 20260801 --end 20261113 --regress

HARD checks (exit 1 on failure):
  * the bar grid is exactly the NYSE calendar's: 78 bars a regular session,
    fewer on an early close, never NULL-padded, no duplicates
  * LOOKAHEAD from the stored vol_session
  * spot NULL-guard: every level NULL where spot is unknown, and only there
  * every pct_* in [0, 1]; spot > 0 wherever known
  * z-scores NULL until a bar's window holds 60 readings
  * INDEPENDENT RECOMPUTE of every bar of one day, in pandas straight from the
    parquet files and index_ohlc: its own SQL test for zero / NaN / NULL, bars
    from pandas_market_calendars directly, volume session = the latest one in
    the store before T, same-bar d1 / d5 / z rebuilt by hand
  * --regress: the compute for the fixture's day matches
    fixtures/spx_oi_5min_regression.json

REPORTED (never fail the run):
  * the spot report (bars of unknown spot by session and reason, early closes,
    index_ohlc rows outside the grid)
  * every column's min / median / max / NULL count by year
  * the level metrics ranked by how little they move
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import psycopg2

import config
import lib.spx_features as F
import lib.spx_table_b as B
import verify_spx_features as VA
from build_spx_features import _parse_day
from lib.market_hours import get_trading_days

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "spx_oi_5min_regression.json"
REL, ABS = 1e-9, 1e-10


def section(t: str) -> None:
    print(f"\n=== {t} ===")


def load_db(start: date, end: date) -> pd.DataFrame:
    if not config.SPX_FEATURES_DSN:
        raise SystemExit("SPX_FEATURES_DSN is not set")
    with psycopg2.connect(config.SPX_FEATURES_DSN) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(B.COLUMNS_B)} FROM {B.TABLE_B} "
                    "WHERE trade_date BETWEEN %s AND %s ORDER BY trade_date, quote_time", (start, end))
        df = pd.DataFrame(cur.fetchall(), columns=B.COLUMNS_B)
    for c in B.COLUMNS_B[4:] + ["spot"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    df["quote_time"] = [B.normalize_quote_time(v) for v in df["quote_time"]]
    return df


# --- independent recompute -------------------------------------------------------------

def _spot_independent(lo: date, hi: date) -> pd.DataFrame:
    """Zero / NaN / NULL classified in SQL, not by lib.spx_table_b."""
    conn = psycopg2.connect(config.SPX_INDEX_OHLC_DSN)
    try:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT trade_date, quote_time,
                       CASE WHEN spx_open IS NULL OR spx_open = 'NaN'::float8 OR spx_open <= 0
                            THEN NULL ELSE spx_open END
                FROM public.index_ohlc WHERE trade_date BETWEEN %s AND %s""", (lo, hi))
            df = pd.DataFrame(cur.fetchall(), columns=["trade_date", "quote_time", "spot"])
    finally:
        conn.close()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["quote_time"] = [B.normalize_quote_time(v) for v in df["quote_time"]]
    df["spot"] = pd.to_numeric(df["spot"], errors="coerce").astype(float)
    return df.set_index(["trade_date", "quote_time"])["spot"]


def _bars(day: date) -> list:
    s = mcal.get_calendar("NYSE").schedule(start_date=day, end_date=day)
    o = s.iloc[0]["market_open"].tz_convert("America/New_York")
    c = s.iloc[0]["market_close"].tz_convert("America/New_York")
    out, t = [], o
    while t < c:
        out.append(t.time())
        t += pd.Timedelta(minutes=5)
    return out


def recompute_day(day: date) -> pd.DataFrame:
    lo = day - timedelta(days=170)
    oi = VA._read(config.SPX_OI_RAW_DIR, lo, day,
                  ["trade_date", "expiration", "strike", "option_type", "open_interest"])
    oi = oi[oi["expiration"] >= oi["trade_date"]]          # expired contracts are not positions
    vol = VA._read(config.SPX_VOLUME_DIR, lo - timedelta(days=10), day - timedelta(days=1),
                   ["trade_date", "expiration", "strike", "option_type", "volume"])
    oi_days = sorted(oi["trade_date"].unique())
    vol_days = sorted(vol["trade_date"].unique())
    T = pd.Timestamp(day)
    sess = oi_days[:oi_days.index(T) + 1][-(B.LEAD_IN_SESSIONS_B + 1):]
    spot = _spot_independent(sess[0].date(), day)

    rows = []
    for d in sess:
        go = oi[oi["trade_date"] == d]
        o = VA._side(go, "open_interest", d)
        so = go.groupby("strike")["open_interest"].sum()
        earlier = [s for s in vol_days if s < d]
        vs = earlier[-1] if earlier else None
        if vs is not None:
            gv = vol[vol["trade_date"] == vs]
            v = VA._side(gv, "volume", vs)
            sv = gv.groupby("strike")["volume"].sum()
        for q in _bars(d.date()):
            px = spot.get((d.date(), q), np.nan)
            r = {"trade_date": d.date(), "quote_time": q, "spot": px,
                 "vol_session": vs.date() if vs is not None else None}
            if not np.isnan(px):
                k = so.index.to_numpy(float)
                r.update(
                    oi_weighted_all_div_spot=o["w_all"] / px, oi_weighted_call_div_spot=o["w_call"] / px,
                    oi_weighted_put_div_spot=o["w_put"] / px, oi_weighted_all_0_30d_div_spot=o["w_all_0_30"] / px,
                    max_oi_strike_put_div_spot=o["max_put"] / px,
                    pct_oi_within_5pct=so[np.abs(k / px - 1.0) <= 0.05].sum() / so.sum(),
                    pct_oi_above_spot=so[k > px].sum() / so.sum(),
                )
                if vs is not None:
                    kv = sv.index.to_numpy(float)
                    r.update(
                        vol_weighted_all_div_spot=v["w_all"] / px, vol_weighted_call_div_spot=v["w_call"] / px,
                        vol_weighted_put_div_spot=v["w_put"] / px,
                        pct_vol_within_5pct=sv[np.abs(kv / px - 1.0) <= 0.05].sum() / sv.sum(),
                        pct_vol_above_spot=sv[kv > px].sum() / sv.sum(),
                    )
            rows.append(r)
    lv = pd.DataFrame(rows)
    for m in B.CORE_B:
        if m not in lv:
            lv[m] = np.nan
    sess_dates = [d.date() for d in sess]
    pos = {d: i for i, d in enumerate(sess_dates)}
    idx = lv.set_index(["trade_date", "quote_time"])

    out = []
    for q in _bars(day):
        r = idx.loc[(day, q)].to_dict()
        r.update(trade_date=day, quote_time=q)
        i = pos[day]
        for m in B.CORE_B:
            for n in (1, 5):
                key = (sess_dates[i - n], q) if i - n >= 0 else None
                prev = idx[m].get(key, np.nan) if key else np.nan
                r[f"d{n}_{m}_change"] = r[m] - prev if not (pd.isna(r[m]) or pd.isna(prev)) else np.nan
            if pd.isna(r["spot"]):
                r[f"zscore_{m}_3m"] = np.nan
                continue
            hist = lv[(lv["quote_time"] == q) & lv["spot"].notna() & (lv["trade_date"] <= day)]
            w = hist.sort_values("trade_date")[m].iloc[-B.Z_WINDOW:]
            if len(w) == B.Z_WINDOW and w.notna().all():
                sd = w.std(ddof=1)
                r[f"zscore_{m}_3m"] = (w.iloc[-1] - w.mean()) / sd if sd else np.nan
            else:
                r[f"zscore_{m}_3m"] = np.nan
        out.append(r)
    return pd.DataFrame(out)


def check_recompute(db: pd.DataFrame, day: date, chk: F.Checker) -> None:
    section(f"Independent recompute of every bar of {day} vs stored rows")
    stored = db[db["trade_date"] == day].set_index("quote_time")
    if stored.empty:
        chk.check(False, f"{day}: no stored rows")
        return
    ind = recompute_day(day).set_index("quote_time")
    bad = []
    missing = sorted(set(ind.index) ^ set(stored.index))
    for q in ind.index.intersection(stored.index):
        for c in B.COLUMNS_B[2:]:
            if not VA._same(stored.at[q, c], ind.at[q, c]):
                bad.append((q, c, stored.at[q, c], ind.at[q, c]))
    for q, c, a, b in bad[:20]:
        print(f"    {q:%H:%M} {c:<40} stored {a!r:<24} recomputed {b!r}")
    chk.check(not bad and not missing,
              f"{day}: {len(ind)} bars x {len(B.COLUMNS_B) - 2} columns compared (rel {REL:g}), "
              f"{len(bad)} differ, {len(missing)} bar(s) on one side only")


# --- regression ----------------------------------------------------------------------------

def _compute_day(day: date) -> pd.DataFrame:
    from db import get_connection
    from lib.split_factors import load_splits
    with get_connection() as conn:
        splits = {r: load_splits(conn, r) for r in F.ROOTS}
    res = B.compute_table_b(day, day, oi_store=config.SPX_OI_RAW_DIR, vol_store=config.SPX_VOLUME_DIR,
                            splits=splits,
                            spot_source=lambda lo, hi: B.load_spot(config.SPX_INDEX_OHLC_DSN, lo, hi))
    if res.frame.empty:
        raise SystemExit(f"{day}: compute produced no rows")
    return res.frame


def _js(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    if isinstance(v, (np.floating, np.integer)):
        return None if np.isnan(v) else float(v)
    return v


def record_regression(day: date) -> int:
    f = _compute_day(day)
    rows = [{c: _js(v) for c, v in r.items()} for r in f[B.COLUMNS_B].to_dict("records")]
    FIXTURE.parent.mkdir(exist_ok=True)
    FIXTURE.write_text(json.dumps({"date": day.isoformat(),
                                   "recorded": datetime.now().isoformat(timespec="seconds"),
                                   "rows": rows}, indent=1) + "\n")
    print(f"Recorded {len(rows)} bars x {len(B.COLUMNS_B)} columns for {day} to {FIXTURE}. Commit it.")
    return 0


def check_regression(chk: F.Checker) -> None:
    section("Regression against stored expected values")
    if not FIXTURE.exists():
        chk.check(False, f"no fixture at {FIXTURE}; run --record-regression DATE first")
        return
    fx = json.loads(FIXTURE.read_text())
    got = _compute_day(date.fromisoformat(fx["date"]))
    got = {r["quote_time"].isoformat(): r for r in got[B.COLUMNS_B].to_dict("records")}
    bad = []
    for e in fx["rows"]:
        g = got.get(e["quote_time"])
        if g is None:
            bad.append((e["quote_time"], "row", "present", "missing"))
            continue
        for c, ev in e.items():
            gv = g[c]
            same = (_js(gv) == ev) if c in ("trade_date", "quote_time", "vol_session") else VA._same(gv, ev)
            if not same:
                bad.append((e["quote_time"], c, ev, gv))
    for q, c, e, g in bad[:20]:
        print(f"    {q} {c:<40} expected {e!r:<24} got {g!r}")
    chk.check(not bad and len(got) == len(fx["rows"]),
              f"{fx['date']}: {len(fx['rows'])} bars match the fixture recorded {fx['recorded']}"
              + (f"; {len(bad)} differ" if bad else ""))


# --- reports -----------------------------------------------------------------------------------

def reports(db: pd.DataFrame) -> None:
    year = pd.to_datetime(db["trade_date"]).dt.year
    section("Report: min / median / max / NULLs by year (every column)")
    print(f"{'column':<46}{'year':>6}{'min':>14}{'median':>14}{'max':>14}{'nulls':>8}{'rows':>8}")
    for c in ["spot"] + B.CORE_B + B.DERIVATIVES_B:
        for y, s in db[c].groupby(year):
            print(f"{c:<46}{y:>6}{s.min():>14.6g}{s.median():>14.6g}{s.max():>14.6g}"
                  f"{int(s.isna().sum()):>8}{len(s):>8}")

    section("Report: level metrics by how little they move (p05-p95 width / |median|, all years)")
    rows = []
    for c in B.CORE_B:
        s = db[c].dropna()
        if s.empty:
            continue
        p05, p50, p95 = s.quantile([0.05, 0.5, 0.95])
        within = db[c].groupby(year).apply(lambda x: x.quantile(0.95) - x.quantile(0.05)).median()
        rows.append((c, p05, p50, p95, (p95 - p05) / abs(p50) if p50 else np.inf, within))
    print(f"{'metric':<34}{'p05':>12}{'median':>12}{'p95':>12}{'width/|med|':>13}{'med yr width':>14}")
    for c, a, m, b, r, w in sorted(rows, key=lambda t: t[4]):
        print(f"{c:<34}{a:>12.5g}{m:>12.5g}{b:>12.5g}{r:>13.4f}{w:>14.5g}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--start", type=_parse_day)
    p.add_argument("--end", type=_parse_day)
    p.add_argument("--recompute-date", type=_parse_day,
                   help="day whose bars are rebuilt independently (default: last stored day)")
    p.add_argument("--regress", action="store_true")
    p.add_argument("--record-regression", type=_parse_day, metavar="DATE")
    p.add_argument("--no-reports", action="store_true")
    args = p.parse_args()
    if not config.SPX_INDEX_OHLC_DSN:
        p.error("SPX_INDEX_OHLC_DSN is not set")
    if args.record_regression:
        return record_regression(args.record_regression)
    if not args.start or not args.end:
        p.error("--start and --end are required")

    chk = F.Checker()
    end = min(args.end, min(F.store_last_date(config.SPX_OI_RAW_DIR, r) for r in F.ROOTS))
    db = load_db(args.start, end)
    section(f"{B.TABLE_B}: {len(db):,} rows in {args.start} → {end}")
    if db.empty:
        chk.check(False, "no rows")
        return 1
    sessions = get_trading_days(args.start, end)
    B.check_table_b(db, sessions=sessions, chk=chk)

    # z gating: conclusive once a bar has 60 stored readings, or from the first
    # row if the range starts at the OI store's first session.
    oi_first = min(F.store_first_date(config.SPX_OI_RAW_DIR, r) for r in F.ROOTS)
    from_start = db["trade_date"].min() <= oi_first
    k = db[db["spot"].notna()].sort_values(["quote_time", "trade_date"])
    nth = k.groupby("quote_time").cumcount()
    bad = []
    for m in B.CORE_B:
        cnt = (k[m].notna().astype(int).groupby(k["quote_time"])
               .transform(lambda x: x.rolling(B.Z_WINDOW, min_periods=1).sum()))
        judge = pd.Series(True, index=k.index) if from_start else (nth >= B.Z_WINDOW - 1)
        if (judge & k[f"zscore_{m}_3m"].notna() & (cnt < B.Z_WINDOW)).any():
            bad.append(m)
    chk.check(not bad, "z-scores NULL until a bar's window holds 60 readings"
              + ("" if from_start else f" (judged from each bar's {B.Z_WINDOW}th stored reading)")
              + (f"; violated by {bad}" if bad else ""))

    section("Spot report")
    spot = B.load_spot(config.SPX_INDEX_OHLC_DSN, args.start, end)
    grid = B.classify_spot(B.expected_grid(sessions), spot)
    B.spot_report(grid, spot, sessions)
    stored_unknown = set(map(tuple, db.loc[db["spot"].isna(), ["trade_date", "quote_time"]].to_numpy()))
    grid_unknown = set(map(tuple, grid.loc[grid["spot_status"] != "ok", ["trade_date", "quote_time"]].to_numpy()))
    chk.check(stored_unknown == grid_unknown,
              f"stored NULL-spot bars are exactly index_ohlc's unknown bars "
              f"({len(stored_unknown)} stored, {len(grid_unknown)} in index_ohlc today)")

    check_recompute(db, args.recompute_date or db["trade_date"].max(), chk)
    if args.regress:
        check_regression(chk)
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
