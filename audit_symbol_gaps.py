"""
audit_symbol_gaps.py — how much history the ticker-rename gap has already cost.

READ-ONLY. Diagnoses, changes nothing. This is the damage report for a defect
that exists in the EXISTING stores (daily_features, the OI parquet store,
chain_eod, chain_snapshots), not in the new equity_1min store.

The defect
----------
Every fetcher in this repo requests a ticker under its CURRENT symbol for all
history. META was FB until 2022-06-09, so a request for META in 2019 returns
nothing — not an error. fetch_oi.py writes no rows; fetch_chain_eod.py and
fetch_chain_snapshots.py enumerate no expirations and write no session; the
completeness audit's coverage_blocks then classifies the whole pre-rename
stretch as "never requested, not missing" and reports it as fine.

Every layer behaves correctly and the data is silently absent. No audit in this
repo can see it, because none of them know what the symbol used to be.

Why the train window is the thing to measure
--------------------------------------------
lib/bin_compute sets TRAIN_TEST_CUTOFF_DEFAULT = 2024-01-01, so trade_date <
2024-01-01 is the training half, and TT_MIN_TRAIN_DEFAULT = 500 requires 500
valid pre-cutoff observations before tt_bins will bin a (ticker, metric) at
all. A ticker renamed in mid-2022 has at most ~400 trading days before the
cutoff — under the threshold. train_test_series then returns all-zero bins and
build_bin_tables logs it as a "thin ticker".

So the cost is not only missing rows: the ticker is silently dropped from every
tt_bins-based analysis while still appearing in the table. That is the number
this report exists to put in front of you.

Usage:
    python audit_symbol_gaps.py
    python audit_symbol_gaps.py --tickers META,LCID,SOFI
    python audit_symbol_gaps.py --no-vendor      # offline; uses --known-renames
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
from datetime import date, datetime
from pathlib import Path

from lib.market_hours import get_trading_days

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

TRAIN_CUTOFF = date(2024, 1, 1)     # lib.bin_compute.TRAIN_TEST_CUTOFF_DEFAULT
TT_MIN_TRAIN = 500                  # lib.bin_compute.TT_MIN_TRAIN_DEFAULT


def first_daily_features_date(conn, ticker: str) -> date | None:
    with conn.cursor() as cur:
        cur.execute("SELECT MIN(trade_date) FROM daily_features "
                    "WHERE ticker = %s", (ticker,))
        r = cur.fetchone()
    return r[0] if r and r[0] else None


def first_ohlc_date(conn, ticker: str) -> date | None:
    with conn.cursor() as cur:
        cur.execute("SELECT MIN(trade_date) FROM underlying_ohlc "
                    "WHERE ticker = %s", (ticker,))
        r = cur.fetchone()
    return r[0] if r and r[0] else None


def first_store_date(list_years_fn, year_path_fn, ticker: str) -> date | None:
    """Earliest trade_date in a per-ticker/per-year parquet store."""
    import pyarrow.parquet as pq
    best = None
    for y in list_years_fn(ticker):
        try:
            tbl = pq.read_table(year_path_fn(ticker, y), columns=["trade_date"])
        except Exception:
            continue
        for td in tbl.column("trade_date").to_pylist():
            if td is not None and (best is None or td < best):
                best = td
    return best


def train_days_lost(first_data: date | None, history_start: date) -> tuple:
    """(trading days present pre-cutoff, trading days missing pre-cutoff)."""
    if first_data is None:
        present = 0
    else:
        lo = max(first_data, history_start)
        present = len(get_trading_days(lo, TRAIN_CUTOFF)) if lo < TRAIN_CUTOFF else 0
    total = len(get_trading_days(history_start, TRAIN_CUTOFF))
    return present, max(0, total - present)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Damage report for the ticker-rename gap (read-only).")
    ap.add_argument("--tickers", help="comma-separated; default = OI store universe")
    ap.add_argument("--history-start", default="20190101",
                    help="YYYYMMDD: the date your history is supposed to begin "
                         "(default 20190101)")
    ap.add_argument("--no-vendor", action="store_true",
                    help="skip the Polygon events lookup; only tickers named in "
                         "--known-renames are treated as renamed")
    ap.add_argument("--known-renames", default="",
                    help="comma-separated TICKER=FORMER[:YYYY-MM-DD] pairs to "
                         "seed the report without a vendor lookup, e.g. "
                         "META=FB:2022-06-09")
    ap.add_argument("--out", default="audit_symbol_gaps.csv")
    args = ap.parse_args()

    history_start = datetime.strptime(args.history_start, "%Y%m%d").date()

    from db import get_connection
    from lib.parquet_store import list_tickers as list_oi_tickers

    tickers = ([t.strip().upper() for t in args.tickers.split(",") if t.strip()]
               if args.tickers else list_oi_tickers())
    if not tickers:
        raise SystemExit("No tickers to audit.")

    print("=== ticker-rename damage report (read-only) ===")
    print(f"Universe      : {len(tickers)} tickers")
    print(f"History start : {history_start}")
    print(f"Train cutoff  : {TRAIN_CUTOFF} "
          f"(tt_bins needs >= {TT_MIN_TRAIN} pre-cutoff obs)\n")

    # --- who was renamed -----------------------------------------------------
    renamed: dict = {}
    classifications: dict = {}
    prior_names: dict = {}
    for pair in (args.known_renames or "").split(","):
        pair = pair.strip()
        if not pair or "=" not in pair:
            continue
        tk, rest = pair.split("=", 1)
        former, _, when = rest.partition(":")
        d = None
        if when:
            try:
                d = datetime.strptime(when, "%Y-%m-%d").date()
            except ValueError:
                pass
        renamed[tk.strip().upper()] = (former.strip().upper(), d)

    if not args.no_vendor:
        from lib.polygon_symbols import build_all
        print("Resolving symbol history from Polygon ...", end=" ", flush=True)
        symbol_map = build_all(tickers)
        n = 0
        for t, h in symbol_map.items():
            if not h.renamed:
                continue
            n += 1
            former = h.former_symbols[0] if h.former_symbols else ""
            # The canonical symbol's interval start is the rename date.
            rename_date = None
            for s, _e, sym in h.intervals:
                if sym.upper() == t.upper():
                    rename_date = s
                    break
            renamed.setdefault(t, (former, rename_date))
            classifications[t] = h.classification
            prior_names[t] = h.prior_names.get(former, "")
        print(f"OK ({n} with a symbol change)")

    if not renamed:
        print("\nNo renamed tickers identified. Either the universe genuinely "
              "has none, or the vX events endpoint is not entitled — in which "
              "case seed known cases with --known-renames.")
        return 0

    # --- measure -------------------------------------------------------------
    from lib.parquet_store import list_years as oi_list_years, year_path as oi_year_path
    try:
        from lib.chain_store import list_years as ch_list_years, year_path as ch_year_path
        have_chain = True
    except Exception:
        have_chain = False

    rows: list = []
    with get_connection() as conn:
        for t in sorted(renamed):
            former, rename_date = renamed[t]
            f_df = first_daily_features_date(conn, t)
            f_ohlc = first_ohlc_date(conn, t)
            try:
                f_oi = first_store_date(oi_list_years, oi_year_path, t)
            except Exception as exc:
                log.warning("  %s: OI store read failed — %s", t, exc)
                f_oi = None
            f_ch = None
            if have_chain:
                try:
                    f_ch = first_store_date(ch_list_years, ch_year_path, t)
                except Exception:
                    f_ch = None

            present, missing = train_days_lost(f_df, history_start)
            rows.append({
                "ticker": t,
                "former_symbol": former,
                # A de-SPAC or bankruptcy gap may be CORRECT — the former
                # symbol was a different entity, so its absence under this
                # ticker is right. Only a rebrand gap is unambiguously a defect.
                "classification": classifications.get(t, "UNKNOWN"),
                "former_name": prior_names.get(t, ""),
                "rename_date": rename_date.isoformat() if rename_date else "",
                "first_daily_features": f_df.isoformat() if f_df else "NONE",
                "first_underlying_ohlc": f_ohlc.isoformat() if f_ohlc else "NONE",
                "first_oi_store": f_oi.isoformat() if f_oi else "NONE",
                "first_chain_eod": f_ch.isoformat() if f_ch else ("NONE" if have_chain else ""),
                "train_days_present": present,
                "train_days_missing": missing,
                "pct_train_missing": (f"{100.0 * missing / (present + missing):.0f}%"
                                      if (present + missing) else ""),
                "below_tt_min_train": "YES" if present < TT_MIN_TRAIN else "no",
            })

    rows.sort(key=lambda r: -r["train_days_missing"])

    out = Path(args.out)
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print(f"\n{'=' * 100}")
    print("RENAME DAMAGE — first available row per store")
    print(f"{'=' * 100}")
    print(f"  {'ticker':<7}{'former':<7}{'class':<19}{'daily_features':<15}"
          f"{'oi_store':<12}{'train_miss':>11}{'%':>6}  tt_bins")
    for r in rows:
        print(f"  {r['ticker']:<7}{r['former_symbol']:<7}"
              f"{r['classification'][:18]:<19}"
              f"{r['first_daily_features']:<15}{r['first_oi_store']:<12}"
              f"{r['train_days_missing']:>11}{r['pct_train_missing']:>6}  "
              f"{'DROPPED' if r['below_tt_min_train'] == 'YES' else 'ok'}")

    rebrands = [r for r in rows if r["classification"] == "RENAME"]
    if rebrands:
        print(f"\n  {len(rebrands)} of these are REBRANDS — same company "
              f"throughout, so the missing")
        print("  history is unambiguously a defect: " +
              ", ".join(r["ticker"] for r in rebrands))
    shells = [r for r in rows
              if r["classification"] in ("LIKELY_SPAC_SHELL", "LIKELY_BANKRUPTCY")]
    if shells:
        print(f"\n  {len(shells)} look like de-SPACs / bankruptcy relistings. "
              f"For these the gap may be")
        print("  CORRECT — the former symbol was a different entity, so its "
              "absence under this")
        print("  ticker is right, and the listing simply is young: " +
              ", ".join(r["ticker"] for r in shells))

    dropped = [r for r in rows if r["below_tt_min_train"] == "YES"]
    print(f"\n  {len(rows)} renamed ticker(s); "
          f"{len(dropped)} fall below tt_bins' {TT_MIN_TRAIN}-observation "
          f"minimum and are")
    print("  silently binned as all-zero — present in tt_bins, absent from any "
          "result built on it.")
    if dropped:
        print("    " + ", ".join(r["ticker"] for r in dropped))
    print(f"\n  CSV: {out.resolve()}")
    print("\n  Note: underlying_ohlc comes from yfinance, which resolves "
          "renames internally,")
    print("  so a ticker can have full OHLC history while its OI and chain "
          "history start")
    print("  at the rename date. Compare the two columns above — a gap "
          "between them is")
    print("  the signature of this defect rather than a genuinely young "
          "listing.")
    return 1 if dropped else 0


if __name__ == "__main__":
    sys.exit(main())
