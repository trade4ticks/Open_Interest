"""
test_spx_table_b.py — table B compute on a synthetic store and a synthetic
index_ohlc, with no database.

index_ohlc is mocked the way §7.1 describes the real one: a row for every
calendar day 09:30-16:00, zero-filled on weekends, a 13:00 early close padded
with zeros to 16:00, a NaN bar, a missing bar, a zero bar inside a session,
and a priced 16:00 bar. quote_time is text, to exercise the normaliser.

Hand-computed: one bar's spot-relative levels and its same-bar d1.
Also: the grid comes from the calendar (78 / 42 bars), unknown spot gives
NULL levels on a row that still exists, d1 / d5 never span a missing bar,
the z window skips missing readings, the z-score gate is exact, and a one-day
build reproduces the full-range rows.

Run:  python test_spx_table_b.py     (exit 1 on any failure)
"""
from __future__ import annotations

import math
import sys
import tempfile
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pandas as pd

import lib.spx_features as F
import lib.spx_table_b as B
from lib.market_hours import get_trading_days
from test_spx_features import generic_oi, generic_vol

FAILS: list[str] = []


def ok(cond, label: str) -> None:
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
    if not cond:
        FAILS.append(label)


def close(a, b, label: str, rel: float = 1e-12) -> None:
    good = a is not None and not pd.isna(a) and math.isclose(float(a), float(b), rel_tol=rel, abs_tol=1e-15)
    ok(good, f"{label}: got {a!r}, expected {b!r}")


SESSIONS = get_trading_days(date(2025, 8, 1), date(2025, 12, 31))
HALF = date(2025, 11, 28)
HALF_DAYS = {HALF, date(2025, 12, 24)}
AFTER_HALF = SESSIONS[SESSIONS.index(HALF) + 1]
TI = SESSIONS.index(date(2025, 12, 10))
T, P = SESSIONS[TI], SESSIONS[TI - 1]
BAR_T = time(10, 30)
NAN_DAY, NAN_BAR = SESSIONS[TI - 20], time(10, 0)
MISSING_DAY, MISSING_BAR = SESSIONS[TI - 15], time(11, 0)
ZERO_DAY, ZERO_BAR = SESSIONS[TI - 10], time(12, 0)
TRUNC_DAY, TRUNC_FROM = SESSIONS[TI - 30], time(11, 35)   # morning rows, no afternoon rows


def spot_rows() -> pd.DataFrame:
    rows = []
    d = SESSIONS[0] - timedelta(days=3)
    idx = {s: i for i, s in enumerate(SESSIONS)}
    while d <= SESSIONS[-1]:
        t = datetime.combine(d, time(9, 30))
        k = 0
        while t.time() <= time(16, 0):
            q = t.time()
            if d not in idx:
                px = 0.0                                    # weekend / holiday zero fill
            elif d in HALF_DAYS and q >= time(13, 0):
                px = 0.0                                    # half-day afternoon padding
            else:
                px = 5000.0 + 40 * math.sin(idx[d] / 3.0) + 0.7 * k + 3 * math.cos(k + idx[d])
            if (d, q) == (T, BAR_T):
                px = 5200.0
            if (d, q) == (P, BAR_T):
                px = 4900.0
            if (d, q) == (NAN_DAY, NAN_BAR):
                px = float("nan")
            if (d, q) == (ZERO_DAY, ZERO_BAR):
                px = 0.0
            if (d, q) != (MISSING_DAY, MISSING_BAR) and not (d == TRUNC_DAY and q >= TRUNC_FROM):
                rows.append((d, q.strftime("%H:%M"), px))
            t += timedelta(minutes=5)
            k += 1
        d += timedelta(days=1)
    return B.prepare_spot(pd.DataFrame(rows, columns=["trade_date", "quote_time", "spx_open"]))


def build_store(base: Path) -> None:
    oi, vol = [], []
    for i, s in enumerate(SESSIONS):
        oi += generic_oi(i, s)
        vol += generic_vol(i, s)
    oi_df = pd.DataFrame(oi, columns=["root", "trade_date", "expiration", "strike", "option_type", "open_interest"])
    vol_df = pd.DataFrame(vol, columns=["root", "trade_date", "expiration", "strike", "option_type", "volume", "trade_count"])
    for df, sub in ((oi_df, "oi"), (vol_df, "vol")):
        for root, g in df.groupby("root"):
            dd = base / sub / root
            dd.mkdir(parents=True)
            g.drop(columns="root").to_parquet(dd / "2025.parquet", index=False)


def hand_levels(i_oi: int, d: date, i_vol: int, s_vol: date, spot: float) -> dict:
    o = generic_oi(i_oi, d)
    v = generic_vol(i_vol, s_vol)
    def side(rows, qi):
        tot = sum(r[qi] for r in rows)
        w = lambda pred: sum(r[qi] for r in rows if pred(r))
        return dict(
            tot=tot,
            w_all=sum(r[3] * r[qi] for r in rows) / tot,
            w_call=sum(r[3] * r[qi] for r in rows if r[4] == "C") / w(lambda r: r[4] == "C"),
            w_put=sum(r[3] * r[qi] for r in rows if r[4] == "P") / w(lambda r: r[4] == "P"),
            within=w(lambda r: abs(r[3] / spot - 1) <= 0.05) / tot,
            above=w(lambda r: r[3] > spot) / tot,
        )
    so, sv = side(o, 5), side(v, 5)
    # 0-30d OI: only the SPXW rows (expiry s+3); SPX rows expire at s+45.
    o30 = [r for r in o if (r[2] - d).days <= 30]
    w30 = sum(r[3] * r[5] for r in o30) / sum(r[5] for r in o30)
    puts = {}
    for r in o:
        if r[4] == "P":
            puts[r[3]] = puts.get(r[3], 0) + r[5]
    max_put = min(k for k, x in puts.items() if x == max(puts.values()))
    return {
        "oi_weighted_all_div_spot": so["w_all"] / spot,
        "oi_weighted_call_div_spot": so["w_call"] / spot,
        "oi_weighted_put_div_spot": so["w_put"] / spot,
        "oi_weighted_all_0_30d_div_spot": w30 / spot,
        "vol_weighted_all_div_spot": sv["w_all"] / spot,
        "vol_weighted_call_div_spot": sv["w_call"] / spot,
        "vol_weighted_put_div_spot": sv["w_put"] / spot,
        "max_oi_strike_put_div_spot": max_put / spot,
        "pct_oi_within_5pct": so["within"], "pct_vol_within_5pct": sv["within"],
        "pct_oi_above_spot": so["above"], "pct_vol_above_spot": sv["above"],
    }


def main() -> int:
    no_splits = {r: pd.DataFrame(columns=["trade_date", "splits"]) for r in F.ROOTS}
    spot = spot_rows()
    src = lambda lo, hi: spot[(spot["trade_date"] >= lo) & (spot["trade_date"] <= hi)]
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        build_store(base)
        kw = dict(oi_store=base / "oi", vol_store=base / "vol", splits=no_splits, spot_source=src)

        print("\n--- full range ---")
        res = B.compute_table_b(SESSIONS[0], SESSIONS[-1], **kw)
        df = res.frame
        ok(list(df.columns) == B.COLUMNS_B, "columns = COLUMNS_B")
        ok(len(B.SURFACED_B) == 48, "48 surfaced columns")
        ok(not res.total_mismatch, "per-strike totals equal table A totals")
        per = df.groupby("trade_date").size()
        ok(per.drop(list(HALF_DAYS)).eq(78).all() and per[list(HALF_DAYS)].eq(42).all(),
           f"78 bars a session, 42 on each of {sorted(HALF_DAYS)}")
        ok(df["quote_time"].min() == time(9, 30) and df["quote_time"].max() == time(15, 55), "bars 09:30 .. 15:55")

        print(f"\n--- hand-computed bar {T} {BAR_T} (spot 5200), volume session {P} ---")
        r = df[(df["trade_date"] == T) & (df["quote_time"] == BAR_T)].iloc[0]
        exp = hand_levels(TI, T, TI - 1, P, 5200.0)
        for m in B.CORE_B:
            close(r[m], exp[m], m)
        ok(r["vol_session"] == P, f"vol_session = {r['vol_session']}")
        ok(r["pct_oi_above_spot"] == 0.0, "nothing above 5200: pct_oi_above_spot is 0, not NULL")
        rp = hand_levels(TI - 1, P, TI - 2, SESSIONS[TI - 2], 4900.0)
        for m in ("pct_oi_within_5pct", "pct_oi_above_spot", "oi_weighted_all_div_spot", "vol_weighted_put_div_spot"):
            close(r[f"d1_{m}_change"], exp[m] - rp[m], f"d1_{m}_change (same bar, prior session)")

        print("\n--- unknown spot: row exists, every level NULL ---")
        for d, q, why in ((NAN_DAY, NAN_BAR, "NaN"), (MISSING_DAY, MISSING_BAR, "no row"),
                          (ZERO_DAY, ZERO_BAR, "zero")):
            row = df[(df["trade_date"] == d) & (df["quote_time"] == q)]
            ok(len(row) == 1 and pd.isna(row.iloc[0]["spot"]) and row.iloc[0][B.CORE_B].isna().all()
               and row.iloc[0][B.DERIVATIVES_B].isna().all(), f"{why} bar {d} {q}: row present, all NULL")
            nxt = SESSIONS[SESSIONS.index(d) + 1]
            rn = df[(df["trade_date"] == nxt) & (df["quote_time"] == q)].iloc[0]
            ok(pd.isna(rn["d1_pct_oi_above_spot_change"]) and pd.notna(rn["zscore_pct_oi_above_spot_3m"]),
               f"next session {nxt} {q}: d1 NULL (never spans), z set (window skips the gap)")
        other = df[(df["trade_date"] == NAN_DAY) & (df["quote_time"] == time(10, 5))].iloc[0]
        ok(other[B.CORE_B].notna().all(), "neighbouring bar on the same day unaffected")

        print("\n--- half day ---")
        r = df[(df["trade_date"] == AFTER_HALF) & (df["quote_time"] == time(14, 45))].iloc[0]
        ok(pd.isna(r["d1_pct_oi_above_spot_change"]) and pd.notna(r["zscore_pct_oi_above_spot_3m"]),
           f"{AFTER_HALF} 14:45: d1 NULL (no 14:45 bar on {HALF}), z set")
        r = df[(df["trade_date"] == AFTER_HALF) & (df["quote_time"] == time(10, 0))].iloc[0]
        ok(pd.notna(r["d1_pct_oi_above_spot_change"]), f"{AFTER_HALF} 10:00: d1 set ({HALF} had a 10:00 bar)")

        print("\n--- z-score gating, same bar of day ---")
        z = df[df["quote_time"] == time(9, 30)].sort_values("trade_date")["zscore_pct_oi_above_spot_3m"]
        ok(z.iloc[:59].isna().all() and pd.notna(z.iloc[59]), "09:30 z: NULL for sessions 1-59, set at 60")
        zv = df[df["quote_time"] == time(9, 30)].sort_values("trade_date")["zscore_vol_weighted_all_div_spot_3m"]
        ok(zv.iloc[:60].isna().all() and pd.notna(zv.iloc[60]), "09:30 volume z: set at 61 (no volume on day 1)")
        g = df[(df["quote_time"] == BAR_T) & df["spot"].notna()].sort_values("trade_date")
        w = g["pct_oi_above_spot"].iloc[g["trade_date"].tolist().index(T) - 59:g["trade_date"].tolist().index(T) + 1]
        close(g[g["trade_date"] == T]["zscore_pct_oi_above_spot_3m"].iloc[0],
              (w.iloc[-1] - w.mean()) / w.std(ddof=1) if w.std(ddof=1) else float("nan"),
              f"z at {T} {BAR_T} vs pandas over its 60 readings", rel=1e-9)

        print("\n--- hard checks ---")
        chk = F.Checker()
        B.check_table_b(df, sessions=SESSIONS, chk=chk, full=res.full)
        ok(not chk.failed, "check_table_b passes")

        print("\n--- spot report ---")
        rep = B.spot_report(res.grid, res.spot_df, SESSIONS)
        ok(set(rep["new_gap_sessions"]) == {NAN_DAY, MISSING_DAY, ZERO_DAY, TRUNC_DAY},
           f"planted gaps reported as NEW: {rep['new_gap_sessions']}")
        ok(rep["priced_off_day"] == 0 and rep["priced_off_hours"] == 0, "no priced rows off the calendar")
        ok(rep["half_days"] == 2, "two early closes")
        ok(set(rep["truncated_days"]) == {MISSING_DAY, TRUNC_DAY},
           f"days with rows missing: {rep['truncated_days']}")
        tr = df[(df["trade_date"] == TRUNC_DAY)]
        ok(len(tr) == 78 and tr[tr["quote_time"] >= TRUNC_FROM]["spot"].isna().all()
           and tr[tr["quote_time"] < TRUNC_FROM]["spot"].notna().all(),
           f"{TRUNC_DAY}: all 78 rows kept, spot NULL from {TRUNC_FROM:%H:%M}")

        print("\n--- a one-day build with lead-in reproduces the full-range rows ---")
        one = B.compute_table_b(T, T, **kw)
        a = one.frame.set_index("quote_time")
        b = df[df["trade_date"] == T].set_index("quote_time")
        diff = [(q, c) for q in b.index for c in B.COLUMNS_B[2:]
                if not ((pd.isna(a.at[q, c]) and pd.isna(b.at[q, c])) or
                        (not pd.isna(a.at[q, c]) and not pd.isna(b.at[q, c]) and
                         (a.at[q, c] == b.at[q, c] if not isinstance(b.at[q, c], float)
                          else math.isclose(a.at[q, c], b.at[q, c], rel_tol=1e-9, abs_tol=1e-12))))]
        ok(not diff, f"78 bars x {len(B.COLUMNS_B) - 2} columns match" + (f"; differ: {diff[:5]}" if diff else ""))
        ok(one.lead_in_min_readings >= 60, f"lead-in: {one.lead_in_sessions} sessions, "
                                           f"min {one.lead_in_min_readings} readings per bar")

        print("\n--- verify_spx_table_b's independent recompute agrees with the build ---")
        import config
        import verify_spx_table_b as VB
        config.SPX_OI_RAW_DIR, config.SPX_VOLUME_DIR = base / "oi", base / "vol"

        def fake_spot(lo, hi):   # stands in for the SQL CASE on index_ohlc
            s = spot[(spot["trade_date"] >= lo) & (spot["trade_date"] <= hi)].copy()
            s["spot"] = s["spx_open"].where(s["spx_open"].notna() & (s["spx_open"] > 0))
            return s.set_index(["trade_date", "quote_time"])["spot"]
        VB._spot_independent = fake_spot
        for d in (T, SESSIONS[SESSIONS.index(NAN_DAY) + 1], AFTER_HALF, HALF):
            chk3 = F.Checker()
            VB.check_recompute(df, d, chk3)
            ok(not chk3.failed, f"recompute matches build on {d}")

        print("\n--- the LOOKAHEAD check fires on a shifted frame (the [FAIL]s below are expected) ---")
        sh = df.copy()
        sh["vol_session"] = sh["trade_date"]
        chk2 = F.Checker()
        B.check_table_b(sh, sessions=SESSIONS, chk=chk2)
        ok(any(f.startswith("LOOKAHEAD") for f in chk2.failed), "LOOKAHEAD fails a shifted frame")

    print(f"\n{'ALL PASS' if not FAILS else f'{len(FAILS)} FAILURE(S)'}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
