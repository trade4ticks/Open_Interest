"""
verify_spx_features.py — checks on table A (spx_oi_daily) as written.

    python verify_spx_features.py --start 20200102 --end 20261113
    python verify_spx_features.py --start 20260801 --end 20261113 --recompute-date 20261026
    python verify_spx_features.py --record-regression 20261026     # once, then commit the fixture
    python verify_spx_features.py --start 20260801 --end 20261113 --regress

HARD checks (exit 1 on failure):
  * exactly one row per NYSE session in range, each with OI from both roots
  * LOOKAHEAD, from the stored vol_session: every row's volume session is
    strictly earlier than its trade_date and is the prior NYSE session
  * every pct_* in [0, 1]; pct_oi_spxw / pct_vol_spxw in (0, 1);
    weighted_avg_dte_oi > 0; both put/call ratios > 0
  * z-scores NULL until 60 observations exist
  * INDEPENDENT RECOMPUTE: one day's full row rebuilt in pandas straight from
    the parquet files — no DuckDB, no build_features SQL, no calendar: the
    volume session is the latest one in the volume store before T — and
    diffed column by column against the stored row
  * --regress: the compute for the fixture's date matches the stored expected
    values in fixtures/spx_oi_daily_regression.json. This is what fails if a
    change to build_features.OI_FEATURES_SQL alters SPX.

REPORTED (never fail the run):
  * every date where weighted_avg_dte_oi <= weighted_avg_dte_vol (reliably
    true, but a thin session with a large LEAP block can flip it)
  * every metric's min / median / max / NULL count by year
  * the level metrics ranked by how little they move
  * pct_vol_0_30d by year
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
import psycopg2
import pyarrow.parquet as pq

import config
import lib.spx_features as F
from lib.market_hours import get_trading_days

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "spx_oi_daily_regression.json"
REL, ABS = 1e-9, 1e-10


def _parse_day(s: str) -> date:
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    raise argparse.ArgumentTypeError(f"bad date {s!r}; use YYYYMMDD")


def section(t: str) -> None:
    print(f"\n=== {t} ===")


def _same(a, b) -> bool:
    na, nb = a is None or pd.isna(a), b is None or pd.isna(b)
    if na or nb:
        return na and nb
    if isinstance(a, date) or isinstance(b, date):
        return a == b
    return math.isclose(float(a), float(b), rel_tol=REL, abs_tol=ABS)


def load_db(start: date, end: date) -> pd.DataFrame:
    if not config.SPX_FEATURES_DSN:
        raise SystemExit("SPX_FEATURES_DSN is not set")
    with psycopg2.connect(config.SPX_FEATURES_DSN) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(F.COLUMNS_A)} FROM {F.TABLE_A} "
                    "WHERE trade_date BETWEEN %s AND %s ORDER BY trade_date", (start, end))
        df = pd.DataFrame(cur.fetchall(), columns=F.COLUMNS_A)
    for c in F.COLUMNS_A[2:]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    return df


# --- independent recompute (pandas, straight from parquet) ----------------------

def _read(store: Path, lo: date, hi: date, cols: list[str]) -> pd.DataFrame:
    frames = []
    for root in F.ROOTS:
        for y in range(lo.year, hi.year + 1):
            p = store / root / f"{y}.parquet"
            if p.exists():
                d = pq.read_table(p, columns=cols,
                                  filters=[("trade_date", ">=", lo), ("trade_date", "<=", hi)]).to_pandas()
                d["root"] = root
                frames.append(d)
    df = pd.concat(frames, ignore_index=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["expiration"] = pd.to_datetime(df["expiration"])
    return df


def _side(g: pd.DataFrame, qty: str, day: pd.Timestamp) -> dict:
    """Aggregates for one day of one store; dte measured from `day`."""
    q = g[qty].astype(float)
    dte = (g["expiration"] - day).dt.days
    call, put = g["option_type"] == "C", g["option_type"] == "P"
    tot = q.sum()
    in30 = dte.between(0, 30)
    front = g.loc[dte >= 0, "expiration"].min()
    per_strike = q.groupby(g["strike"]).sum()
    return dict(
        tot=tot, call=q[call].sum(), put=q[put].sum(),
        pc_0_30=q[put & in30].sum() / q[call & in30].sum() if q[call & in30].sum() else np.nan,
        wdte=(dte * q).sum() / tot,
        front=q[g["expiration"] == front].sum() / tot,
        in30=q[in30].sum() / tot,
        top5=per_strike.nlargest(5).sum() / tot,
        spxw=q[g["root"] == "SPXW"].sum() / tot,
        w_all=(g["strike"] * q).sum() / tot,
        w_call=(g["strike"] * q)[call].sum() / q[call].sum(),
        w_put=(g["strike"] * q)[put].sum() / q[put].sum(),
        w_all_0_30=(g["strike"] * q)[in30].sum() / q[in30].sum() if q[in30].sum() else np.nan,
        max_put=_max_strike(g[put], q[put]),
    )


def _max_strike(g: pd.DataFrame, q: pd.Series) -> float:
    s = q.groupby(g["strike"]).sum()
    return float(s[s == s.max()].index.min())


def recompute(day: date) -> dict:
    lo = day - timedelta(days=130)
    oi = _read(config.SPX_OI_RAW_DIR, lo, day,
               ["trade_date", "expiration", "strike", "option_type", "open_interest"])
    vol = _read(config.SPX_VOLUME_DIR, lo - timedelta(days=10), day - timedelta(days=1),
                ["trade_date", "expiration", "strike", "option_type", "volume"])
    oi_days = sorted(oi["trade_date"].unique())
    vol_days = sorted(vol["trade_date"].unique())
    T = pd.Timestamp(day)
    if T not in set(oi_days):
        raise SystemExit(f"{day}: no OI in the store")
    i_T = oi_days.index(T)
    need = oi_days[max(0, i_T - F.Z_WINDOW):i_T + 1]   # 61 sessions: z window + one for net_new

    rows = []
    prev_total = None
    for d in need:
        o = _side(oi[oi["trade_date"] == d], "open_interest", d)
        earlier = [s for s in vol_days if s < d]
        vs = earlier[-1] if earlier else None
        v = _side(vol[vol["trade_date"] == vs], "volume", vs) if vs is not None else None
        r = dict(
            trade_date=d.date(), vol_session=vs.date() if vs is not None else None,
            total_oi=o["tot"], call_oi=o["call"], put_oi=o["put"],
            oi_put_call_ratio=o["put"] / o["call"], oi_put_call_ratio_0_30d=o["pc_0_30"],
            weighted_avg_dte_oi=o["wdte"], pct_oi_front_expiry=o["front"],
            pct_oi_0_30d=o["in30"], pct_oi_top5_strikes=o["top5"], pct_oi_spxw=o["spxw"],
            oi_weighted_all=o["w_all"], oi_weighted_call=o["w_call"], oi_weighted_put=o["w_put"],
            oi_weighted_all_0_30d=o["w_all_0_30"], max_oi_strike_put=o["max_put"],
        )
        if v:
            r.update(
                total_vol=v["tot"], call_vol=v["call"], put_vol=v["put"],
                vol_put_call_ratio=v["put"] / v["call"], weighted_avg_dte_vol=v["wdte"],
                pct_vol_front_expiry=v["front"], pct_vol_0_30d=v["in30"],
                pct_vol_top5_strikes=v["top5"], pct_vol_spxw=v["spxw"],
                vol_weighted_all=v["w_all"], vol_weighted_call=v["w_call"], vol_weighted_put=v["w_put"],
                vol_oi_ratio_all=v["tot"] / o["tot"], vol_oi_ratio_call=v["call"] / o["call"],
                vol_oi_ratio_put=v["put"] / o["put"],
                net_new_oi_div_vol=(o["tot"] - prev_total) / v["tot"] if prev_total is not None else np.nan,
            )
        prev_total = o["tot"]
        rows.append(r)
    lv = pd.DataFrame(rows)

    out = lv.iloc[-1].to_dict()
    n = len(lv)
    for m in F.CORE_A:
        x = lv[m].astype(float) if m in lv else pd.Series([np.nan] * n)
        x1 = x.iloc[-2] if n >= 2 else np.nan
        x5 = x.iloc[-6] if n >= 6 else np.nan
        if m == "total_oi":
            out["d1_total_oi_pct_change"] = (x.iloc[-1] - x1) / x1
            out["d5_total_oi_pct_change"] = (x.iloc[-1] - x5) / x5
        else:
            out[f"d1_{m}_change"] = x.iloc[-1] - x1
            out[f"d5_{m}_change"] = x.iloc[-1] - x5
        w = x.iloc[-F.Z_WINDOW:]
        if len(w) == F.Z_WINDOW and w.notna().all():
            sd = w.std(ddof=1)
            out[f"zscore_{m}_3m"] = (w.iloc[-1] - w.mean()) / sd if sd else np.nan
        else:
            out[f"zscore_{m}_3m"] = np.nan
    return out


def check_recompute(db: pd.DataFrame, day: date, chk: F.Checker) -> None:
    section(f"Independent recompute of {day} (pandas from parquet) vs stored row")
    row = db[db["trade_date"] == day]
    if row.empty:
        chk.check(False, f"{day}: no stored row to compare")
        return
    stored = row.iloc[0].to_dict()
    ind = recompute(day)
    bad = []
    for c in F.COLUMNS_A[1:]:
        if not _same(stored.get(c), ind.get(c)):
            bad.append((c, stored.get(c), ind.get(c)))
    for c, a, b in bad:
        print(f"    {c:<40} stored {a!r:<26} recomputed {b!r}")
    chk.check(not bad, f"{day}: {len(F.COLUMNS_A) - 1} columns compared "
                       f"(rel {REL:g}), {len(bad)} differ; volume session {ind.get('vol_session')}")


# --- regression fixture ------------------------------------------------------------

def _compute_day(day: date) -> dict:
    from db import get_connection
    from lib.split_factors import load_splits
    with get_connection() as conn:
        splits = {r: load_splits(conn, r) for r in F.ROOTS}
    res = F.compute_table_a(day, day, oi_store=config.SPX_OI_RAW_DIR,
                            vol_store=config.SPX_VOLUME_DIR, splits=splits)
    if res.frame.empty or res.frame.iloc[0]["trade_date"] != day:
        raise SystemExit(f"{day}: compute produced no row")
    return res.frame.iloc[0][F.COLUMNS_A].to_dict()


def _jsonable(v):
    if v is None or (isinstance(v, float) and math.isnan(v)) or v is pd.NA:
        return None
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating, float)):
        return None if np.isnan(v) else float(v)
    return v


def record_regression(day: date) -> int:
    vals = {c: _jsonable(v) for c, v in _compute_day(day).items()}
    FIXTURE.parent.mkdir(exist_ok=True)
    FIXTURE.write_text(json.dumps({"date": day.isoformat(),
                                   "recorded": datetime.now().isoformat(timespec="seconds"),
                                   "values": vals}, indent=1) + "\n")
    print(f"Recorded {len(vals)} values for {day} to {FIXTURE}. Commit it.")
    return 0


def check_regression(chk: F.Checker) -> None:
    section("Regression against stored expected values")
    if not FIXTURE.exists():
        chk.check(False, f"no fixture at {FIXTURE}; run --record-regression DATE first")
        return
    fx = json.loads(FIXTURE.read_text())
    day = date.fromisoformat(fx["date"])
    got = _compute_day(day)
    bad = []
    for c, exp in fx["values"].items():
        g = got.get(c)
        if c in ("trade_date", "vol_session"):
            ok = (g.isoformat() if isinstance(g, date) else g) == exp
        else:
            ok = _same(g, exp)
        if not ok:
            bad.append((c, exp, g))
    missing = set(F.COLUMNS_A) - set(fx["values"])
    for c, e, g in bad:
        print(f"    {c:<40} expected {e!r:<26} got {g!r}")
    chk.check(not bad and not missing,
              f"{day}: {len(fx['values'])} values match the fixture recorded {fx['recorded']}"
              + (f"; {len(bad)} differ" if bad else "")
              + (f"; columns not in fixture: {sorted(missing)}" if missing else ""))


# --- reports -------------------------------------------------------------------------

def reports(db: pd.DataFrame) -> None:
    year = pd.to_datetime(db["trade_date"]).dt.year
    cols = F.COLUMNS_A[2:]   # every stored column, surfaced or not

    section("Report: min / median / max / NULLs by year (every column)")
    print(f"{'column':<44}{'year':>6}{'min':>14}{'median':>14}{'max':>14}{'nulls':>7}{'rows':>6}")
    for c in cols:
        g = db[c].groupby(year)
        for y, s in g:
            print(f"{c:<44}{y:>6}{s.min():>14.6g}{s.median():>14.6g}{s.max():>14.6g}"
                  f"{int(s.isna().sum()):>7}{len(s):>6}")

    section("Report: level metrics by how little they move (p05-p95 width / |median|, all years)")
    rows = []
    for c in F.CORE_A:
        s = db[c].dropna()
        if s.empty:
            continue
        p05, p50, p95 = s.quantile([0.05, 0.5, 0.95])
        within = db[c].groupby(year).apply(lambda x: x.quantile(0.95) - x.quantile(0.05)).median()
        rows.append((c, p05, p50, p95, (p95 - p05) / abs(p50) if p50 else np.inf, within))
    print(f"{'metric':<28}{'p05':>12}{'median':>12}{'p95':>12}{'width/|med|':>13}{'med yr width':>14}")
    for c, a, m, b, r, w in sorted(rows, key=lambda t: t[4]):
        print(f"{c:<28}{a:>12.5g}{m:>12.5g}{b:>12.5g}{r:>13.4f}{w:>14.5g}")

    section("Report: pct_vol_0_30d by year")
    s = db["pct_vol_0_30d"]
    print(f"{'year':>6}{'n':>6}{'min':>10}{'p05':>10}{'median':>10}{'p95':>10}{'max':>10}")
    for y, x in s.groupby(year):
        x = x.dropna()
        if x.empty:
            continue
        q = x.quantile([0.05, 0.5, 0.95])
        print(f"{y:>6}{len(x):>6}{x.min():>10.4f}{q[0.05]:>10.4f}{q[0.5]:>10.4f}{q[0.95]:>10.4f}{x.max():>10.4f}")

    section("Report: weighted_avg_dte, OI vs volume, median by year")
    print(f"{'year':>6}{'dte_oi':>10}{'dte_vol':>10}")
    for y in sorted(year.unique()):
        m = year == y
        print(f"{y:>6}{db.loc[m, 'weighted_avg_dte_oi'].median():>10.2f}"
              f"{db.loc[m, 'weighted_avg_dte_vol'].median():>10.2f}")


# --- main --------------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--start", type=_parse_day)
    p.add_argument("--end", type=_parse_day)
    p.add_argument("--recompute-date", type=_parse_day,
                   help="day to rebuild independently (default: last stored row in range)")
    p.add_argument("--regress", action="store_true", help="also check against the stored fixture")
    p.add_argument("--record-regression", type=_parse_day, metavar="DATE",
                   help="compute DATE and write the regression fixture, then exit")
    p.add_argument("--no-reports", action="store_true")
    args = p.parse_args()

    if args.record_regression:
        return record_regression(args.record_regression)
    if not args.start or not args.end:
        p.error("--start and --end are required")

    chk = F.Checker()
    oi_last = min(F.store_last_date(config.SPX_OI_RAW_DIR, r) for r in F.ROOTS)
    end = min(args.end, oi_last)
    db = load_db(args.start, end)
    section(f"{F.TABLE_A}: {len(db)} rows in {args.start} → {end}")
    if db.empty:
        chk.check(False, "no rows")
        return 1

    vol_first = max(F.store_first_date(config.SPX_VOLUME_DIR, r) for r in F.ROOTS)
    F.check_table_a(db, sessions=get_trading_days(args.start, end), vol_first=vol_first, chk=chk)

    # z gating. Rows near the start of a later range had lead-in, so the
    # 60-row window is only conclusive from the 60th stored row on — or from
    # the first if the range begins at the start of the OI store.
    oi_first = min(F.store_first_date(config.SPX_OI_RAW_DIR, r) for r in F.ROOTS)
    from_start = db["trade_date"].iloc[0] <= oi_first
    bad = []
    for m in F.CORE_A:
        cnt = db[m].notna().astype(int).rolling(F.Z_WINDOW, min_periods=1).sum()
        judge = pd.Series(True, index=db.index) if from_start else (db.index >= F.Z_WINDOW - 1)
        if (judge & db[f"zscore_{m}_3m"].notna() & (cnt < F.Z_WINDOW)).any():
            bad.append(m)
    chk.check(not bad, "z-scores NULL until 60 observations"
              + (" (range starts at the store's first session: all rows judged)" if from_start
                 else f" (judged from row {F.Z_WINDOW})")
              + (f"; violated by {bad}" if bad else ""))
    if from_start:
        first59 = db.iloc[:F.Z_WINDOW - 1][[f"zscore_{m}_3m" for m in F.CORE_A]]
        chk.check(bool(first59.isna().all().all()),
                  f"every z-score NULL on the first {F.Z_WINDOW - 1} sessions of the store")

    check_recompute(db, args.recompute_date or db["trade_date"].iloc[-1], chk)
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
