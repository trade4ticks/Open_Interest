"""
lib/spx_table_b.py — table B (spx_oi_5min): spot-relative SPX/SPXW OI and
volume metrics, one row per (trade_date, quote_time) 5-minute bar.

Pure compute, like lib/spx_features.py: build_spx_table_b.py writes,
verify_spx_table_b.py and test_spx_table_b.py check.

INPUTS PER ROW
--------------
* The day-level book for T, exactly as table A sees it: OI labelled T, volume
  of session T-1 (lib.spx_features.compute_table_a with keep_strikes=True, so
  the per-strike sums come out of the same `oi` / `vol` views, and the same
  lookahead rule, as every table A metric).
* spot = public.index_ohlc.spx_open of that bar — the bar's OPEN. A bar
  stamped 14:45 spans 14:45-14:50; its open is known to any entry inside it.

THE GRID (spec §7.1)
--------------------
Built from the NYSE calendar and that session's real hours, never from the
rows index_ohlc happens to hold: bars start at the open and step 5 minutes
while bar start < close. 78 bars on a regular session (09:30 .. 15:55), 42 on
a 13:00 early close. index_ohlc's 16:00 row (a bar after the close),
weekend/holiday zero rows and half-day afternoon padding are never read into
the grid. NULL, NaN, zero and negative spx_open are all "unknown".

UNKNOWN SPOT
------------
Every spot-dependent level on that bar is NULL, never 0, and so are its d1,
d5 and z. The row still exists: the grid is complete whether or not a price
was printed.

DERIVATIVES, SAME BAR OF DAY
----------------------------
* d1 / d5: the level minus the level at the SAME quote_time 1 / 5 NYSE
  sessions earlier. NULL if that session has no reading at that bar (a 13:00
  close has no 14:45 bar; an unknown spot is no reading). Never spans.
* z: over the 60 most recent READINGS at that quote_time — rows whose spot is
  known — so a half-day afternoon or a missing price drops out of the window
  rather than blanking that bar's z-score for the next 59 sessions. Otherwise
  the factor construction: COUNT >= 60 gate, AVG / STDDEV_SAMP, NULLIF.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta

import duckdb
import numpy as np
import pandas as pd

import lib.spx_features as F
from lib.market_hours import get_trading_days, session_bounds

TABLE_B = "spx_oi_5min"
BAR = timedelta(minutes=5)
Z_WINDOW = F.Z_WINDOW
# Readings per bar, not sessions: half-day afternoons are missing from the
# window, so read enough sessions that every bar still has 60 before --start.
LEAD_IN_SESSIONS_B = 75

CORE_B = [
    "oi_weighted_all_div_spot",
    "oi_weighted_call_div_spot",
    "oi_weighted_put_div_spot",
    "oi_weighted_all_0_30d_div_spot",
    "vol_weighted_all_div_spot",
    "vol_weighted_call_div_spot",
    "vol_weighted_put_div_spot",
    "max_oi_strike_put_div_spot",
    "pct_oi_within_5pct",
    "pct_vol_within_5pct",
    "pct_oi_above_spot",
    "pct_vol_above_spot",
]
OI_SIDE_B = [c for c in CORE_B if c.startswith(("oi_", "max_oi", "pct_oi"))]
VOL_SIDE_B = [c for c in CORE_B if c not in OI_SIDE_B]
DERIVATIVES_B = [d for m in CORE_B for d in (f"d1_{m}_change", f"d5_{m}_change", f"zscore_{m}_3m")]
SURFACED_B = CORE_B + DERIVATIVES_B
STORED_ONLY_B = ["spot"]
COLUMNS_B = ["trade_date", "quote_time", "spot", "vol_session", *CORE_B, *DERIVATIVES_B]

assert len(CORE_B) == 12 and len(SURFACED_B) == 48
assert len(OI_SIDE_B) == 7 and len(VOL_SIDE_B) == 5
assert not any(c.endswith(("_pc", "_co")) for c in COLUMNS_B)

# Table-A ingredient each ratio divides by spot.
_DIV_SPOT = {
    "oi_weighted_all_div_spot": "oi_weighted_all",
    "oi_weighted_call_div_spot": "oi_weighted_call",
    "oi_weighted_put_div_spot": "oi_weighted_put",
    "oi_weighted_all_0_30d_div_spot": "oi_weighted_all_0_30d",
    "vol_weighted_all_div_spot": "vol_weighted_all",
    "vol_weighted_call_div_spot": "vol_weighted_call",
    "vol_weighted_put_div_spot": "vol_weighted_put",
    "max_oi_strike_put_div_spot": "max_oi_strike_put",
}

# Intra-session gaps measured by the user (spec §7.1). Anything else is NEW.
KNOWN_GAPS = {
    date(2020, 3, 9): "COVID circuit breaker",
    date(2020, 3, 12): "COVID circuit breaker",
    date(2020, 3, 16): "COVID circuit breaker",
    date(2020, 3, 18): "COVID circuit breaker",
    date(2019, 6, 14): "known, unexplained",
    date(2019, 8, 13): "known, unexplained",
}
CIRCUIT_BREAKERS = {d for d, why in KNOWN_GAPS.items() if "circuit" in why}


# --- Spot ----------------------------------------------------------------------

def normalize_quote_time(v) -> time:
    """index_ohlc.quote_time to datetime.time, whatever its column type."""
    if isinstance(v, datetime):
        return v.time().replace(microsecond=0)
    if isinstance(v, time):
        return v.replace(microsecond=0, tzinfo=None)
    if isinstance(v, timedelta):
        secs = int(v.total_seconds())
        return time(secs // 3600, (secs % 3600) // 60, secs % 60)
    if isinstance(v, (int, np.integer)):          # 930, 1545
        return time(int(v) // 100, int(v) % 100)
    if isinstance(v, str):
        s = v.strip()
        for fmt in ("%H:%M:%S", "%H:%M", "%H%M"):
            try:
                return datetime.strptime(s, fmt).time()
            except ValueError:
                pass
    raise SystemExit(f"index_ohlc.quote_time value {v!r} ({type(v).__name__}) not understood")


def load_spot(dsn: str, lo: date, hi: date) -> pd.DataFrame:
    """[trade_date, quote_time, spx_open] for every index_ohlc row in [lo, hi],
    unfiltered: classification of zero / NaN / NULL happens in classify_spot."""
    import psycopg2
    conn = psycopg2.connect(dsn)
    try:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, quote_time, spx_open FROM public.index_ohlc "
                        "WHERE trade_date BETWEEN %s AND %s", (lo, hi))
            rows = cur.fetchall()
    finally:
        conn.close()
    df = pd.DataFrame(rows, columns=["trade_date", "quote_time", "spx_open"])
    return prepare_spot(df)


def prepare_spot(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["quote_time"] = [normalize_quote_time(v) for v in df["quote_time"]]
    df["spx_open"] = pd.to_numeric(df["spx_open"], errors="coerce").astype(float)
    dup = df.duplicated(["trade_date", "quote_time"], keep=False)
    if dup.any():
        raise SystemExit("index_ohlc has duplicate (trade_date, quote_time) rows:\n"
                         + df[dup].sort_values(["trade_date", "quote_time"]).head(10).to_string(index=False))
    return df


def _status(v) -> str:
    if v is None:
        return "no_row"
    if isinstance(v, float) and math.isnan(v):
        return "nan_or_null"
    if v == 0:
        return "zero"
    if v < 0:
        return "negative"
    return "ok"


def expected_grid(sessions: list[date]) -> pd.DataFrame:
    """[trade_date, quote_time] for every bar of every session, from the
    calendar's real open and close."""
    if not sessions:
        return pd.DataFrame(columns=["trade_date", "quote_time"])
    bounds = session_bounds(min(sessions), max(sessions))
    want = set(sessions)
    rows = []
    for d, (o, c) in sorted(bounds.items()):
        if d not in want:
            continue
        t = o
        while t < c:
            rows.append((d, t.time()))
            t += BAR
    return pd.DataFrame(rows, columns=["trade_date", "quote_time"])


def classify_spot(grid: pd.DataFrame, spot_df: pd.DataFrame) -> pd.DataFrame:
    """grid + [spot (float, NaN = unknown), spot_status]."""
    m = grid.merge(spot_df, on=["trade_date", "quote_time"], how="left", indicator=True)
    raw = [None if ind == "left_only" else (float("nan") if pd.isna(v) else float(v))
           for v, ind in zip(m["spx_open"], m["_merge"])]
    m["spot_status"] = [_status(v) for v in raw]
    m["spot"] = [v if s == "ok" else np.nan for v, s in zip(raw, m["spot_status"])]
    return m[["trade_date", "quote_time", "spot", "spot_status"]]


# --- Compute -------------------------------------------------------------------

@dataclass
class TableB:
    frame: pd.DataFrame          # rows in [start, end_eff], COLUMNS_B
    full: pd.DataFrame           # incl. lead-in sessions
    grid: pd.DataFrame           # out-range grid with spot_status
    start: date
    end_eff: date
    lead_in_sessions: int        # lead-in sessions read
    lead_in_min_readings: int    # fewest known readings any bar has before --start (want >= 60)
    table_a: F.TableA
    spot_df: pd.DataFrame        # index_ohlc rows read, for spot_report
    total_mismatch: list = field(default_factory=list)   # (date, which, per-strike total, table A total)
    notes: list = field(default_factory=list)


def compute_table_b(start: date, end: date, *, oi_store, vol_store,
                    splits: dict[str, pd.DataFrame], spot_source) -> TableB:
    """spot_source(lo, hi) -> prepared index_ohlc rows (load_spot / prepare_spot);
    called once for the whole span including lead-in."""
    oi_last = min(F.store_last_date(oi_store, r) for r in F.ROOTS)
    end_eff = min(end, oi_last)
    notes = []
    if end_eff < end:
        notes.append(f"end capped at {end_eff}: last OI trade_date present in both roots")
    out_sessions = get_trading_days(start, end_eff)
    if not out_sessions:
        raise SystemExit(f"no NYSE sessions in {start} .. {end_eff}")
    back = get_trading_days(start - timedelta(days=LEAD_IN_SESSIONS_B * 2 + 30), start - timedelta(days=1))
    oi_first = min(F.store_first_date(oi_store, r) for r in F.ROOTS)
    lead = [d for d in back[-LEAD_IN_SESSIONS_B:] if d >= oi_first]
    spine = lead + out_sessions

    A = F.compute_table_a(spine[0], end_eff, oi_store=oi_store, vol_store=vol_store,
                          splits=splits, keep_strikes=True)
    a = A.full.set_index("trade_date")

    spot_df = spot_source(spine[0], spine[-1])
    grid = classify_spot(expected_grid(spine), spot_df)

    so = {d: (g["strike"].to_numpy(float), g["oi"].to_numpy(float))
          for d, g in A.strike_oi.groupby("trade_date")}
    sv = {d: (g["strike"].to_numpy(float), g["vol"].to_numpy(float))
          for d, g in A.strike_vol.groupby("feature_date")}

    parts = []
    mismatch = []
    for d, g in grid.groupby("trade_date", sort=True):
        s = g["spot"].to_numpy(float)
        known = ~np.isnan(s)
        out = {"trade_date": g["trade_date"].to_numpy(), "quote_time": g["quote_time"].to_numpy(),
               "spot": s, "vol_session": [a.at[d, "vol_session"] if d in a.index else None] * len(g)}
        arow = a.loc[d] if d in a.index else None
        for m, ing in _DIV_SPOT.items():
            num = np.nan if arow is None or pd.isna(arow[ing]) else float(arow[ing])
            out[m] = np.where(known, num / np.where(known, s, 1.0), np.nan)
        for side, book, total_col in (("oi", so.get(d), "total_oi"), ("vol", sv.get(d), "total_vol")):
            within = np.full(len(g), np.nan)
            above = np.full(len(g), np.nan)
            if book is not None and known.any():
                k, w = book
                tot = w.sum()
                if arow is not None and not pd.isna(arow[total_col]) and \
                        not math.isclose(tot, float(arow[total_col]), rel_tol=1e-12):
                    mismatch.append((d, side, tot, float(arow[total_col])))
                sk = s[known]
                in5 = (np.abs(k[None, :] / sk[:, None] - 1.0) <= 0.05).astype(float)
                abv = (k[None, :] > sk[:, None]).astype(float)
                within[known] = (in5 @ w) / tot
                above[known] = (abv @ w) / tot
            out[f"pct_{side}_within_5pct"] = within
            out[f"pct_{side}_above_spot"] = above
        parts.append(pd.DataFrame(out))
    lv = pd.concat(parts, ignore_index=True)
    lv["sess_n"] = lv["trade_date"].map({d: i for i, d in enumerate(spine)})

    full = _derivatives(lv)
    full["trade_date"] = pd.to_datetime(full["trade_date"]).dt.date
    full["vol_session"] = pd.to_datetime(full["vol_session"]).dt.date
    full["quote_time"] = [normalize_quote_time(v) for v in full["quote_time"]]
    full = full.sort_values(["trade_date", "quote_time"]).reset_index(drop=True)[COLUMNS_B]

    frame = full[full["trade_date"] >= start].reset_index(drop=True)
    lead_rows = full[(full["trade_date"] < start) & full["spot"].notna() & full["pct_oi_within_5pct"].notna()]
    reg_bars = expected_grid(out_sessions)["quote_time"].unique()
    counts = lead_rows.groupby("quote_time").size().reindex(reg_bars, fill_value=0)
    return TableB(frame=frame, full=full, grid=grid[grid["trade_date"] >= start].reset_index(drop=True),
                  start=start, end_eff=end_eff, lead_in_sessions=len(lead),
                  lead_in_min_readings=int(counts.min()) if len(counts) else 0,
                  table_a=A, spot_df=spot_df, total_mismatch=mismatch, notes=notes)


def _derivatives(lv: pd.DataFrame) -> pd.DataFrame:
    con = duckdb.connect(database=":memory:")
    con.register("lv_df", lv)
    con.execute("CREATE TABLE lv AS SELECT * REPLACE (CAST(trade_date AS DATE) AS trade_date, "
                "CAST(quote_time AS TIME) AS quote_time) FROM lv_df")
    dcols, zcols = [], []
    for m in CORE_B:
        dcols.append(f"c.{m} - p1.{m} AS d1_{m}_change")
        dcols.append(f"c.{m} - p5.{m} AS d5_{m}_change")
        zcols.append(f"CASE WHEN COUNT({m}) OVER w >= {Z_WINDOW} THEN ({m} - AVG({m}) OVER w)"
                     f" / NULLIF(STDDEV_SAMP({m}) OVER w, 0) ELSE NULL END AS zscore_{m}_3m")
    sql = f"""
    WITH d AS (
        SELECT c.*, {", ".join(dcols)}
        FROM lv c
        LEFT JOIN lv p1 ON p1.quote_time = c.quote_time AND p1.sess_n = c.sess_n - 1
        LEFT JOIN lv p5 ON p5.quote_time = c.quote_time AND p5.sess_n = c.sess_n - 5
    ),
    z AS (
        SELECT trade_date, quote_time, {", ".join(zcols)}
        FROM lv
        WHERE spot IS NOT NULL
        WINDOW w AS (PARTITION BY quote_time ORDER BY trade_date
                     ROWS BETWEEN {Z_WINDOW - 1} PRECEDING AND CURRENT ROW)
    )
    SELECT * FROM d LEFT JOIN z USING (trade_date, quote_time)
    """
    out = con.execute(sql).df()
    con.close()
    return out


# --- Checks & reports ------------------------------------------------------------

def check_table_b(df: pd.DataFrame, *, sessions: list[date], chk: F.Checker,
                  full: pd.DataFrame | None = None) -> None:
    df = df.sort_values(["trade_date", "quote_time"]).reset_index(drop=True)

    exp = expected_grid(sessions)
    have = df[["trade_date", "quote_time"]]
    dup = have.duplicated().sum()
    missing = exp.merge(have, how="left", indicator=True).query("_merge == 'left_only'")
    extra = have.merge(exp, how="left", indicator=True).query("_merge == 'left_only'")
    per = exp.groupby("trade_date").size()
    half = per[per < 78]
    chk.check(dup == 0 and missing.empty and extra.empty,
              f"bar grid exact: {len(have)} rows = {len(exp)} calendar bars over {len(sessions)} sessions "
              f"({len(half)} early-close session(s) with {sorted(set(half.tolist()))} bars); "
              f"{len(missing)} missing, {len(extra)} extra, {dup} duplicate"
              + ("" if missing.empty else "\n" + missing.head(5).to_string(index=False))
              + ("" if extra.empty else "\n" + extra.head(5).to_string(index=False)))

    cal = get_trading_days(sessions[0] - timedelta(days=15), sessions[-1])
    prev = {cal[i]: cal[i - 1] for i in range(1, len(cal))}
    vs = df["vol_session"]
    has = vs.notna()
    leak = has & ~((vs < df["trade_date"]) & (vs == df["trade_date"].map(prev)))
    chk.check(not leak.any(),
              f"LOOKAHEAD: every row's volume session is strictly earlier than its trade_date and is the "
              f"prior NYSE session — {int(has.sum())} rows checked, {int(leak.sum())} violations")

    unknown = df["spot"].isna()
    leaked = unknown & df[CORE_B].notna().any(axis=1)
    chk.check(not leaked.any(),
              f"spot NULL-guard: all 12 levels NULL on the {int(unknown.sum())} bar(s) with unknown spot "
              f"({int(leaked.sum())} violate)")
    gap_oi = ~unknown & df[OI_SIDE_B].isna().any(axis=1)
    chk.check(not gap_oi.any(), f"every bar with a known spot has all 7 OI-side levels ({int(gap_oi.sum())} without)"
              + F._show(df, gap_oi, ["trade_date", "quote_time", "spot"]))
    gap_v = ~unknown & has & df[VOL_SIDE_B].isna().any(axis=1)
    chk.check(not gap_v.any(), f"every bar with a known spot and volume has all 5 volume-side levels "
                               f"({int(gap_v.sum())} without)")
    s = df["spot"].dropna()
    chk.check(bool((s > 0).all()), f"spot > 0 wherever known  [min {s.min():.2f}]" if len(s) else "spot: none known")
    for c in [c for c in CORE_B if c.startswith("pct_")]:
        s = df[c].dropna()
        chk.check(bool(((s >= 0) & (s <= 1)).all()),
                  f"{c} in [0, 1]  [min {s.min():.6g}, max {s.max():.6g}]" if len(s) else f"{c}: no values")

    if full is not None:
        f = full[full["spot"].notna()].sort_values(["quote_time", "trade_date"])
        bad = []
        in_out = f["trade_date"] >= sessions[0]
        for m in CORE_B:
            cnt = (f[m].notna().astype(int).groupby(f["quote_time"])
                   .transform(lambda x: x.rolling(Z_WINDOW, min_periods=1).sum()))
            if (in_out & f[f"zscore_{m}_3m"].notna() & (cnt < Z_WINDOW)).any():
                bad.append(m)
        chk.check(not bad, "z-scores NULL wherever the bar's 60-reading window holds < 60 observations"
                  + (f": violated by {bad}" if bad else ""))


def _ranges(times: list) -> str:
    """[09:30, 09:35, 09:40, 11:00] -> '09:30-09:40, 11:00' (5-minute steps)."""
    times = sorted(times)
    out, a, b = [], times[0], times[0]
    for t in times[1:]:
        if (datetime.combine(date.min, t) - datetime.combine(date.min, b)) == BAR:
            b = t
        else:
            out.append(f"{a:%H:%M}" if a == b else f"{a:%H:%M}-{b:%H:%M}")
            a = b = t
    out.append(f"{a:%H:%M}" if a == b else f"{a:%H:%M}-{b:%H:%M}")
    return ", ".join(out)


# index_ohlc's own layout: a row for every calendar day, 09:30 through 16:00.
_OHLC_DAY = [(datetime.combine(date.min, time(9, 30)) + i * BAR).time() for i in range(79)]


def truncated_days(spot_df: pd.DataFrame, lo: date, hi: date, sessions: list[date]) -> list[tuple]:
    """Days in [lo, hi] where index_ohlc has no ROW (not a zero or NaN row) for
    some of its usual 09:30-16:00 bars. [(day, rows, missing ranges, kind)]."""
    sp = spot_df[(spot_df["trade_date"] >= lo) & (spot_df["trade_date"] <= hi)]
    have = sp.groupby("trade_date")["quote_time"].apply(set).to_dict()
    sess = set(sessions)
    bounds = session_bounds(lo, hi)
    out = []
    d = lo
    while d <= hi:
        got = have.get(d, set())
        miss = [t for t in _OHLC_DAY if t not in got]
        if miss:
            if d not in sess:
                kind = "closed day"
            elif bounds[d][1].time() < time(16, 0):
                kind = f"session, early close {bounds[d][1]:%H:%M}"
            else:
                kind = "session"
            out.append((d, len(got), _ranges(miss), kind))
        d += timedelta(days=1)
    return out


def spot_report(grid: pd.DataFrame, spot_df: pd.DataFrame, sessions: list[date]) -> dict:
    """Per-session account of dropped bars (spec §7.1). Never fails the run."""
    per = grid.groupby("trade_date").size()
    st = grid["spot_status"].value_counts()
    print(f"  grid: {len(grid)} bars over {len(per)} sessions; spot known on {int(st.get('ok', 0))}; "
          f"unknown: " + (", ".join(f"{k} {v}" for k, v in st.items() if k != "ok") or "none"))

    half = per[per < 78]
    if len(half):
        print("  early-close sessions (fewer bars by design, not dropped): "
              + ", ".join(f"{d} ({n} bars)" for d, n in half.items()))

    bad = grid[grid["spot_status"] != "ok"]
    new = []
    if len(bad):
        print("  sessions with bars of unknown spot:")
        for d, g in bad.groupby("trade_date"):
            label = KNOWN_GAPS.get(d, "NEW — not one of the six measured gaps")
            if d not in KNOWN_GAPS:
                new.append(d)
            times = "; ".join(f"{why}: {_ranges(list(gg['quote_time']))}"
                              for why, gg in g.groupby("spot_status"))
            print(f"    {d}  {len(g)} bar(s)  [{label}]  {times}")
    cb = sorted(d for d in CIRCUIT_BREAKERS if d in set(grid["trade_date"]))
    if cb:
        print("  NOTE: on " + ", ".join(map(str, cb)) + " the market-wide circuit breakers left bars with no "
              "price. A trade entered inside those bars gets NULL spot-relative metrics, and the same bar's "
              "d1 one session later and d5 five sessions later are NULL too. These are the days most worth "
              "studying; the spot-free table A metrics are unaffected.")

    lo, hi = min(sessions), max(sessions)
    sp = spot_df[(spot_df["trade_date"] >= lo) & (spot_df["trade_date"] <= hi)]
    valid = sp["spx_open"].notna() & (sp["spx_open"] > 0)
    in_grid = sp.merge(grid[["trade_date", "quote_time"]], how="left", indicator=True)["_merge"].eq("both").to_numpy()
    trading = sp["trade_date"].isin(set(sessions)).to_numpy()
    off_day = sp[valid.to_numpy() & ~trading]
    at_1600 = sp[valid.to_numpy() & trading & ~in_grid & (sp["quote_time"] == time(16, 0)).to_numpy()]
    off_hours = sp[valid.to_numpy() & trading & ~in_grid & (sp["quote_time"] != time(16, 0)).to_numpy()]
    print(f"  index_ohlc rows not read into the grid: {int((~in_grid).sum())} "
          f"(16:00 bar with a price, excluded as after the close: {len(at_1600)})")
    if len(off_day):
        print(f"  ! {len(off_day)} priced row(s) on days the NYSE calendar says were closed: "
              + ", ".join(sorted({str(d) for d in off_day['trade_date']})[:10]))
    if len(off_hours):
        print(f"  ! {len(off_hours)} priced row(s) outside the calendar's session hours (e.g. a half-day "
              f"afternoon carrying prices): "
              + ", ".join(sorted({str(d) for d in off_hours['trade_date']})[:10]))
    trunc = truncated_days(spot_df, lo, hi, sessions)
    if trunc:
        print(f"  index_ohlc days with ROWS MISSING (no row at all, as distinct from zero / NaN rows), "
              f"{len(trunc)} day(s):")
        for d, n, rng, kind in trunc:
            print(f"    {d}  {n} of 79 rows  [{kind}]  missing {rng}")
    else:
        print("  index_ohlc days with rows missing: none — every day has all 79 rows 09:30-16:00")
    return {"unknown": int(len(bad)), "new_gap_sessions": new, "priced_off_day": len(off_day),
            "priced_off_hours": len(off_hours), "half_days": len(half),
            "truncated_days": [t[0] for t in trunc]}
