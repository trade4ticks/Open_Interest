"""
verify_surface_fixes.py — attribute surface changes to a specific fix.

Read-only. No database, no writes. Loads one (ticker, date, snapshot) chain
from the parquet stores, runs build_snapshot under several flag combinations
and diffs the results, so a change in output can be traced to the fix that
caused it rather than to "the re-surface".

Combinations run:
    baseline    all three off — what the current stored surface came from
    +direct     fix 1 only, direct expiry match
    +cboe       fix 2 only, CBOE forward
    +knots      fix 3 only, fixed-knot spline
    all         all three on — what the re-surface will produce

Reported per combination:
    tenors      how many TARGET_DTES produced any surface row (fix 1 should
                raise this; the F 7 DTE case currently produces none)
    fwd method  share by forward_method (fix 2 should collapse spot_fallback)
    butterfly   flagged expiries / usable expiries (fix 3 should cut this)
    skew        iv at 25-delta put and ATM for a chosen tenor, and the chord
                slope between them — the ADBE degeneracy is visible here

The flags are read into surface_fit's namespace at import, so they are
overridden there rather than in surface_config; setting the config module
alone would have no effect on an already-imported surface_fit.

Usage:
    python verify_surface_fixes.py --ticker ADBE --date 20260102 --snapshot 0945
    python verify_surface_fixes.py --ticker F --date 20260102 --tenor 7
    python verify_surface_fixes.py --ticker SPY --date 20260102 --source intraday
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime

import pandas as pd

import lib.surface_fit as SF
from lib.clean_chain import clean_chain
from lib.surface_config import TARGET_DTES

COMBOS = [
    ("baseline", dict(DIRECT_EXPIRY_MATCH=False, CBOE_FORWARD=False,
                      FIXED_KNOT_SPLINE=False)),
    ("+direct",  dict(DIRECT_EXPIRY_MATCH=True,  CBOE_FORWARD=False,
                      FIXED_KNOT_SPLINE=False)),
    ("+cboe",    dict(DIRECT_EXPIRY_MATCH=False, CBOE_FORWARD=True,
                      FIXED_KNOT_SPLINE=False)),
    ("+knots",   dict(DIRECT_EXPIRY_MATCH=False, CBOE_FORWARD=False,
                      FIXED_KNOT_SPLINE=True)),
    ("all",      dict(DIRECT_EXPIRY_MATCH=True,  CBOE_FORWARD=True,
                      FIXED_KNOT_SPLINE=True)),
]


def load_chain(ticker: str, trade_date, snapshot: str, source: str):
    """One snapshot's raw chain from whichever store holds it."""
    if source == "intraday":
        from lib.chain_intraday_store import session_path
        p = session_path(ticker, trade_date)
        if not p.exists():
            raise SystemExit(f"no intraday file: {p}")
        df = pd.read_parquet(p)
    elif source == "live":
        from lib.chain_live_store import read_cycle
        df = read_cycle(ticker, trade_date, snapshot)
        if df.empty:
            raise SystemExit(f"no live cycle for {ticker} {trade_date} {snapshot}")
    else:
        from lib.chain_snapshot_store import year_path
        p = year_path(ticker, trade_date.year)
        if not p.exists():
            raise SystemExit(f"no snapshots file: {p}")
        df = pd.read_parquet(p)
        df = df[df["trade_date"] == trade_date]

    df = df[df["snapshot"].astype(str) == snapshot]
    if df.empty:
        raise SystemExit(f"no rows for {ticker} {trade_date} snapshot={snapshot}")
    return df


def apply_flags(flags: dict) -> None:
    for name, value in flags.items():
        setattr(SF, name, value)


def summarise(res: dict, tenor: int) -> dict:
    surface = res.get("surface", [])
    diags = [d for d in res.get("diagnostics", []) if not d.get("skipped")]

    tenors = sorted({r["dte"] for r in surface})
    methods: dict[str, int] = {}
    for d in diags:
        m = d.get("forward_method") or "?"
        methods[m] = methods.get(m, 0) + 1
    bf = sum(1 for d in diags if d.get("butterfly_arb_flag"))

    iv25 = iv_atm = None
    for r in surface:
        if r["dte"] == tenor and int(r.get("put_delta", 0)) == 25:
            iv25 = r.get("iv")
    for a in res.get("atm", []):
        if a["dte"] == tenor:
            iv_atm = a.get("atm_iv")
    return {"tenors": tenors, "n_rows": len(surface), "methods": methods,
            "bf": bf, "n_fits": len(diags), "iv25": iv25, "iv_atm": iv_atm}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ticker", required=True)
    ap.add_argument("--date", required=True, help="YYYYMMDD")
    ap.add_argument("--snapshot", default="0945")
    ap.add_argument("--source", default="snapshots",
                    choices=("snapshots", "intraday", "live"))
    ap.add_argument("--tenor", type=int, default=21,
                    help="tenor to report 25-delta vs ATM skew for")
    args = ap.parse_args()

    trade_date = datetime.strptime(args.date, "%Y%m%d").date()
    ticker = args.ticker.upper()

    raw = load_chain(ticker, trade_date, args.snapshot, args.source)
    df = clean_chain(raw)
    print(f"{ticker} {trade_date} {args.snapshot}  ({args.source})")
    print(f"  {len(raw):,} raw rows, {df['expiration'].nunique()} expirations\n")

    saved = {n: getattr(SF, n) for n in
             ("DIRECT_EXPIRY_MATCH", "CBOE_FORWARD", "FIXED_KNOT_SPLINE")}
    rows = []
    try:
        for label, flags in COMBOS:
            apply_flags(flags)
            res = SF.build_snapshot(df, ticker, trade_date, args.snapshot)
            rows.append((label, summarise(res, args.tenor)))
    finally:
        apply_flags(saved)

    base = rows[0][1]
    print(f"  {'combo':<10}{'tenors':>8}{'rows':>7}{'butterfly':>11}"
          f"{'spot_fb':>9}{'cboe':>7}{'pcp':>6}")
    for label, s in rows:
        tot = max(s["n_fits"], 1)
        fb = s["methods"].get("spot_fallback", 0)
        cb = s["methods"].get("cboe_atm", 0)
        pc = s["methods"].get("pcp", 0)
        print(f"  {label:<10}{len(s['tenors']):>8}{s['n_rows']:>7}"
              f"{s['bf']:>6}/{s['n_fits']:<4}{100*fb/tot:>8.0f}%"
              f"{100*cb/tot:>6.0f}%{100*pc/tot:>5.0f}%")

    print(f"\n  tenors present, baseline: {base['tenors']}")
    gained = sorted(set(rows[-1][1]["tenors"]) - set(base["tenors"]))
    lost = sorted(set(base["tenors"]) - set(rows[-1][1]["tenors"]))
    print(f"  gained with all fixes:    {gained or 'none'}")
    print(f"  LOST with all fixes:      {lost or 'none'}"
          + ("   <-- investigate" if lost else ""))

    print(f"\n  {args.tenor}d skew, 25-delta put vs ATM:")
    print(f"  {'combo':<10}{'iv_25p':>12}{'iv_atm':>12}{'difference':>14}")
    for label, s in rows:
        a, b = s["iv25"], s["iv_atm"]
        d = (f"{a - b:+.3e}" if None not in (a, b) else "-")
        print(f"  {label:<10}{a if a is None else f'{a:.8f}':>12}"
              f"{b if b is None else f'{b:.8f}':>12}{d:>14}")
    print("\n  A difference at 1e-7 means the fitted vertex sits at k_25/2 and "
          "the\n  chord across a symmetric pair has zero slope — not "
          "extrapolation.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
