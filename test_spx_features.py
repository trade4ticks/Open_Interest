"""
test_spx_features.py — table A compute on a synthetic store, with no database.

Builds a tiny SPX + SPXW OI / volume store over ~105 real NYSE sessions in a
temp directory and runs lib.spx_features.compute_table_a on it. One session,
T, has hand-built rows whose metric values are written out below as
arithmetic, not captured from a run.

The tests that matter most:

  * LOOKAHEAD. Volume labelled T (T's own session) is planted with 1,000,000
    contracts. If it reaches row T, total_vol is wrong by three orders of
    magnitude. It must appear on row T+1 instead.
  * Volume DTE is measured from the volume session: T-1's 0DTE trades count
    as DTE 0, so they sit in the 0-30d bucket and the front expiry.
  * Z-scores are NULL until the window holds 60 observations — exactly.
  * A one-day build (start = end = T) with lead-in read from the store
    reproduces the full-range row for T.
  * check_table_a fails a frame whose volume session equals its trade_date.

Run:  python test_spx_features.py     (exit 1 on any failure)
"""
from __future__ import annotations

import math
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

import lib.spx_features as F
from lib.market_hours import get_trading_days

FAILS: list[str] = []


def ok(cond: bool, label: str) -> None:
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
    if not cond:
        FAILS.append(label)


def close(a, b, label: str, rel: float = 1e-12) -> None:
    good = (a is not None and b is not None and not pd.isna(a)
            and math.isclose(float(a), float(b), rel_tol=rel, abs_tol=1e-12))
    ok(good, f"{label}: got {a!r}, expected {b!r}")


SESSIONS = get_trading_days(date(2026, 3, 2), date(2026, 7, 31))
TI = 69
T, P = SESSIONS[TI], SESSIONS[TI - 1]
E30, E60 = T + timedelta(days=30), T + timedelta(days=60)


def generic_oi(i: int, s: date) -> list[tuple]:
    return [
        ("SPX",  s, s + timedelta(days=45), 5000.0, "C", 1000 + 10 * i + (i % 7) * 3),
        ("SPX",  s, s + timedelta(days=45), 4800.0, "P", 1500 + (i % 5) * 20),
        ("SPXW", s, s + timedelta(days=3),  5050.0, "C", 400 + (i % 3) * 15),
        ("SPXW", s, s + timedelta(days=3),  4950.0, "P", 600 + (i % 4) * 10),
    ]


def generic_vol(i: int, s: date) -> list[tuple]:
    return [
        ("SPXW", s, s,                      5000.0, "C", 900 + (i % 6) * 30, 90),
        ("SPXW", s, s,                      4950.0, "P", 1100 + (i % 4) * 25, 110),
        ("SPX",  s, s + timedelta(days=45), 5000.0, "C", 50 + i % 3, 5),
        ("SPX",  s, s + timedelta(days=45), 4800.0, "P", 70 + (i % 2) * 5, 7),
    ]


def build_store(base: Path) -> None:
    oi, vol = [], []
    for i, s in enumerate(SESSIONS):
        if s == T:
            oi += [
                ("SPXW", T, T,   5000.0, "C", 100),
                ("SPXW", T, T,   4900.0, "P", 300),
                ("SPX",  T, E30, 5100.0, "C", 200),
                ("SPX",  T, E60, 4900.0, "P", 400),
            ] + [("SPX", T, E60, float(k), "C", 10) for k in range(5200, 5800, 100)] + [
                # Expired before T (vendor purge lag / post-opex): not positions. Must be dropped,
                # or every hand-computed value below moves.
                ("SPX",  T, P, 5000.0, "C", 50_000),
                ("SPXW", T, P, 4000.0, "P", 80_000),
            ]
            # THE TRAP: T's own session volume. Belongs to row T+1, never T.
            vol += [("SPXW", T, T, 5000.0, "C", 999_000, 1), ("SPX", T, E30, 5100.0, "P", 1_000, 1)]
        elif s == P:
            oi += generic_oi(i, s)
            vol += [
                ("SPXW", P, P,   5000.0, "C", 500, 50),
                ("SPXW", P, P,   4950.0, "P", 300, 30),
                ("SPX",  P, E30, 4900.0, "P", 200, 20),
            ]
        else:
            oi += generic_oi(i, s)
            vol += generic_vol(i, s)
    oi_df = pd.DataFrame(oi, columns=["root", "trade_date", "expiration", "strike", "option_type", "open_interest"])
    vol_df = pd.DataFrame(vol, columns=["root", "trade_date", "expiration", "strike", "option_type", "volume", "trade_count"])
    for df, sub in ((oi_df, "oi"), (vol_df, "vol")):
        for root, g in df.groupby("root"):
            d = base / sub / root
            d.mkdir(parents=True)
            g.drop(columns="root").to_parquet(d / "2026.parquet", index=False)


def main() -> int:
    no_splits = {r: pd.DataFrame(columns=["trade_date", "splits"]) for r in F.ROOTS}
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        build_store(base)
        kw = dict(oi_store=base / "oi", vol_store=base / "vol", splits=no_splits)

        print("\n--- full range ---")
        full = F.compute_table_a(SESSIONS[0], SESSIONS[-1], **kw)
        df = full.frame.set_index("trade_date")
        ok(list(full.frame.columns[:len(F.COLUMNS_A)]) == F.COLUMNS_A, "column order = COLUMNS_A")
        ok(len(F.SURFACED_A) == 62 and len(F.STORED_ONLY_A) == 23, "62 surfaced + 23 stored-only")
        ok(full.bad_split_factors == 0, "split factors all 1.0")

        r = df.loc[T]
        print(f"\n--- hand-computed row T = {T} (volume session P = {P}) ---")
        ok(r["vol_session"] == P, f"vol_session = {r['vol_session']}")
        close(r["total_oi"], 1060, "total_oi (two expired contracts, 130,000 OI, dropped)")
        ok(r["oi_rows_expired_dropped"] == 2 and r["oi_rows_neg_dte"] == 0,
           f"diagnostics: {r['oi_rows_expired_dropped']} expired rows dropped, {r['oi_rows_neg_dte']} reach a metric")
        close(r["oi_expired_share_dropped"], 130_000 / 131_060, "oi_expired_share_dropped")
        close(r["call_oi"], 360, "call_oi")
        close(r["put_oi"], 700, "put_oi")
        close(r["oi_put_call_ratio"], 700 / 360, "oi_put_call_ratio")
        close(r["oi_put_call_ratio_0_30d"], 300 / 300, "oi_put_call_ratio_0_30d")
        close(r["weighted_avg_dte_oi"], (30 * 200 + 60 * 400 + 60 * 60) / 1060, "weighted_avg_dte_oi")
        close(r["pct_oi_front_expiry"], 400 / 1060, "pct_oi_front_expiry")
        close(r["pct_oi_0_30d"], 600 / 1060, "pct_oi_0_30d")
        close(r["pct_oi_top5_strikes"], (700 + 200 + 100 + 10 + 10) / 1060, "pct_oi_top5_strikes")
        close(r["pct_oi_spxw"], 400 / 1060, "pct_oi_spxw")
        close(r["oi_weighted_all"], (5000 * 100 + 4900 * 300 + 5100 * 200 + 4900 * 400 + 10 * 32700) / 1060, "oi_weighted_all")
        close(r["oi_weighted_call"], (5000 * 100 + 5100 * 200 + 10 * 32700) / 360, "oi_weighted_call")
        close(r["oi_weighted_put"], 4900, "oi_weighted_put")
        close(r["oi_weighted_all_0_30d"], (5000 * 100 + 4900 * 300 + 5100 * 200) / 600, "oi_weighted_all_0_30d")
        close(r["max_oi_strike_put"], 4900, "max_oi_strike_put")

        dte_x = (E30 - P).days
        close(r["total_vol"], 1000, "total_vol (trap not leaked)")
        close(r["call_vol"], 500, "call_vol")
        close(r["put_vol"], 500, "put_vol")
        close(r["vol_put_call_ratio"], 1.0, "vol_put_call_ratio")
        close(r["weighted_avg_dte_vol"], 200 * dte_x / 1000, "weighted_avg_dte_vol (DTE from vol session)")
        close(r["pct_vol_front_expiry"], 0.8, "pct_vol_front_expiry (P's own 0DTE)")
        close(r["pct_vol_0_30d"], 0.8, "pct_vol_0_30d (0DTE in bucket; E30 is 31+ days from P)")
        close(r["pct_vol_top5_strikes"], 1.0, "pct_vol_top5_strikes")
        close(r["pct_vol_spxw"], 0.8, "pct_vol_spxw")
        close(r["vol_oi_ratio_all"], 1000 / 1060, "vol_oi_ratio_all")
        close(r["vol_oi_ratio_call"], 500 / 360, "vol_oi_ratio_call")
        close(r["vol_oi_ratio_put"], 500 / 700, "vol_oi_ratio_put")
        close(r["vol_weighted_all"], (5000 * 500 + 4950 * 300 + 4900 * 200) / 1000, "vol_weighted_all")
        close(r["vol_weighted_put"], (4950 * 300 + 4900 * 200) / 500, "vol_weighted_put")

        oi_p = sum(x[5] for x in generic_oi(TI - 1, P))
        close(r["net_new_oi_div_vol"], (1060 - oi_p) / 1000, "net_new_oi_div_vol")
        close(r["d1_total_oi_pct_change"], (1060 - oi_p) / oi_p, "d1_total_oi_pct_change")
        g = generic_oi(TI - 1, P)
        pc_p = sum(x[5] for x in g if x[4] == "P") / sum(x[5] for x in g if x[4] == "C")
        close(r["d1_oi_put_call_ratio_change"], 700 / 360 - pc_p, "d1_oi_put_call_ratio_change")
        close(r["d5_pct_oi_spxw_change"], 400 / 1060 - df.iloc[TI - 5]["pct_oi_spxw"], "d5_pct_oi_spxw_change")

        w = df["oi_put_call_ratio"].iloc[TI - 59:TI + 1]
        close(r["zscore_oi_put_call_ratio_3m"], (w.iloc[-1] - w.mean()) / w.std(ddof=1),
              "zscore_oi_put_call_ratio_3m vs pandas (ddof=1)", rel=1e-9)

        print("\n--- the trap lands on T+1 ---")
        r1 = df.loc[SESSIONS[TI + 1]]
        ok(r1["vol_session"] == T, f"row T+1 vol_session = {r1['vol_session']}")
        close(r1["total_vol"], 1_000_000, "row T+1 total_vol")

        print("\n--- z-score gating ---")
        z_oi = df["zscore_oi_put_call_ratio_3m"]
        ok(z_oi.iloc[:59].isna().all() and pd.notna(z_oi.iloc[59]),
           "OI z: NULL for sessions 1-59, set at session 60")
        z_v = df["zscore_vol_put_call_ratio_3m"]
        ok(z_v.iloc[:60].isna().all() and pd.notna(z_v.iloc[60]),
           "volume z: first row has no prior-session volume, so set at session 61")
        z_n = df["zscore_net_new_oi_div_vol_3m"]
        ok(z_n.iloc[:60].isna().all() and pd.notna(z_n.iloc[60]), "net_new_oi_div_vol z: set at session 61")

        print("\n--- hard checks on the full range ---")
        chk = F.Checker()
        F.check_table_a(full.frame, sessions=SESSIONS, vol_first=full.vol_first, chk=chk, full=full.full)
        ok(not chk.failed, "check_table_a passes on clean synthetic data")

        print("\n--- check_table_a catches a one-day shift ---")
        shifted = full.frame.copy()
        shifted["vol_session"] = shifted["trade_date"]
        chk2 = F.Checker()
        F.check_table_a(shifted, sessions=SESSIONS, vol_first=full.vol_first, chk=chk2)
        ok(any(f.startswith("LOOKAHEAD") for f in chk2.failed), "LOOKAHEAD check fails a shifted frame")

        print("\n--- one-day build with lead-in reproduces the full-range row ---")
        one = F.compute_table_a(T, T, **kw)
        ok(len(one.frame) == 1 and one.lead_in_used == 60, f"1 row, lead-in used {one.lead_in_used}/60")
        a, b = one.frame.iloc[0], full.frame.set_index("trade_date").loc[T]
        diffs = [c for c in F.COLUMNS_A[1:]
                 if not ((pd.isna(a[c]) and pd.isna(b[c]))
                         or (not pd.isna(a[c]) and not pd.isna(b[c])
                             and (a[c] == b[c] if not isinstance(a[c], float)
                                  else math.isclose(a[c], b[c], rel_tol=1e-9, abs_tol=1e-12))))]
        ok(not diffs, f"all {len(F.COLUMNS_A) - 1} columns match" + (f"; differ: {diffs}" if diffs else ""))

        print("\n--- verify_spx_features' independent pandas recompute agrees with the build ---")
        import config
        import verify_spx_features as V
        config.SPX_OI_RAW_DIR, config.SPX_VOLUME_DIR = base / "oi", base / "vol"
        db = full.frame[F.COLUMNS_A].copy()
        for c in F.COLUMNS_A[2:]:
            db[c] = pd.to_numeric(db[c]).astype(float)
        for d in (T, SESSIONS[TI + 1], SESSIONS[-1]):
            chk3 = F.Checker()
            V.check_recompute(db, d, chk3)
            ok(not chk3.failed, f"recompute matches build on {d}")
        V.reports(db)

    print(f"\n{'ALL PASS' if not FAILS else f'{len(FAILS)} FAILURE(S)'}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
