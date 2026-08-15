"""
audit_equity_1min.py — completeness audit of the equity_1min store.

Read-only and fully offline: unlike audit_chain_snapshots.py it needs no vendor
call, because what SHOULD be there is fully determined by the exchange calendar
rather than by a per-session expiration list.

This exists because the fetcher's manifest records what it BELIEVES it fetched.
That is the right structure for resumability — it keys on exactly the unit that
fails — but it is bookkeeping, not evidence. The only way to know the store is
complete is to re-derive coverage from the bars themselves and diff it against
the calendar, which is what this does. Manifest drift (a chunk recorded `ok`
whose rows never landed) is invisible to the fetcher and visible here.

Checks per (ticker, session):

  1. SESSION MISSING     — a trading day inside a FETCHED BLOCK with no bars at
                           all. Blocks are runs of stored sessions split on
                           gaps > --max-gap trading days, so months never
                           requested are reported as unfetched rather than
                           flagged missing. Same treatment as the chain audit.
  2. REGULAR INCOMPLETE  — regular-hours bars below --regular-ratio of the
                           minutes the session actually had. Calendar-aware:
                           390 on a normal day, 210 on a 13:00 early close, so
                           the ~15 half-days per decade are not false alarms.
  3. ROWCOUNT LOW        — total bars far below the median of adjacent
                           sessions. Catches partial loss that check 2 cannot,
                           e.g. regular hours intact but extended hours gone.
  4. NO EXTENDED         — zero premarket AND zero after-hours bars on a
                           session whose neighbours have them. Informational:
                           thin names genuinely do not trade extended hours.

Severity is the max of the checks that fired, so the CSV sorts worst-first.

A bar exists only where a trade happened, so none of these are proofs — an
illiquid ticker legitimately misses regular-hours minutes. They are smoke
alarms, deliberately loose, same as the sibling audit.

Usage:
    python audit_equity_1min.py
    python audit_equity_1min.py --tickers SPY,AAPL --out audit.csv
    python audit_equity_1min.py --start 20190101 --end 20261231
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from config import EQUITY_1MIN_DIR
from lib.chain_fetch_common import log_path, setup_file_logging
from lib.equity_1min_store import (
    MANIFEST_FAILED, MANIFEST_OK,
    list_tickers, read_manifest, session_counts,
)
from lib.market_hours import get_trading_days, regular_minutes
from lib.parquet_store import list_tickers as list_oi_tickers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SEV_SESSION_MISSING    = 4
SEV_REGULAR_INCOMPLETE = 3
SEV_ROWCOUNT_LOW       = 2
SEV_NO_EXTENDED        = 1
SEV_OK                 = 0
SEV_NAME = {
    4: "SESSION_MISSING",
    3: "REGULAR_INCOMPLETE",
    2: "ROWCOUNT_LOW",
    1: "NO_EXTENDED",
    0: "OK",
}

REGULAR_RATIO_DEFAULT = 0.95   # regular bars / expected regular minutes
ROWCOUNT_LOW_RATIO    = 0.5
ADJACENT_WINDOW       = 5


def coverage_blocks(present: list, all_days: list, max_gap: int) -> list:
    """Split stored sessions into contiguously-fetched blocks.

    Without this the audit would assume everything between a ticker's first and
    last stored session was requested, and flag every trading day in a month
    that was never fetched as SESSION_MISSING.
    """
    pos = {d: i for i, d in enumerate(all_days)}
    idxs = sorted(pos[d] for d in present if d in pos)
    if not idxs:
        return []
    blocks: list = []
    start_i = prev = idxs[0]
    for i in idxs[1:]:
        if i - prev > max_gap:
            blocks.append((start_i, prev))
            start_i = i
        prev = i
    blocks.append((start_i, prev))
    return blocks


def audit_ticker(ticker: str, start: date | None, end: date | None,
                 expected_regular: dict, regular_ratio: float,
                 max_gap: int = 5) -> tuple:
    counts = session_counts(ticker)
    if counts.empty:
        log.warning("  %s: no rows in store", ticker)
        return [], 0

    counts["trade_date"] = pd.to_datetime(counts["trade_date"]).dt.date
    if start is not None:
        counts = counts[counts["trade_date"] >= start]
    if end is not None:
        counts = counts[counts["trade_date"] <= end]
    if counts.empty:
        return [], 0

    wide = (counts.pivot_table(index="trade_date", columns="session",
                               values="bars", aggfunc="sum", fill_value=0)
            .reset_index())
    for c in ("premarket", "regular", "after", "other"):
        if c not in wide.columns:
            wide[c] = 0
    wide["total"] = wide[["premarket", "regular", "after", "other"]].sum(axis=1)

    present_sessions = sorted(wide["trade_date"].tolist())
    lo = start or present_sessions[0]
    hi = end or present_sessions[-1]
    all_days = get_trading_days(lo, hi)
    present_set = set(present_sessions)

    if start is not None and end is not None:
        expected_days = all_days
        n_unfetched = 0
    else:
        blocks = coverage_blocks(present_sessions, all_days, max_gap)
        expected_days = [d for (a, b) in blocks for d in all_days[a:b + 1]]
        n_unfetched = len(all_days) - len(expected_days)

    by_date = {r["trade_date"]: r for _, r in wide.iterrows()}

    ordered = [(d, by_date[d]["total"]) for d in present_sessions]
    median_adj: dict = {}
    for i, (d, _) in enumerate(ordered):
        lo_i = max(0, i - ADJACENT_WINDOW)
        hi_i = min(len(ordered), i + ADJACENT_WINDOW + 1)
        neighbours = [c for j, (_, c) in enumerate(ordered)
                      if lo_i <= j < hi_i and j != i]
        median_adj[d] = float(pd.Series(neighbours).median()) if neighbours else 0.0

    # Does this ticker trade extended hours at all? If it never does, flagging
    # every session for having none is noise, not signal.
    ext_totals = [by_date[d]["premarket"] + by_date[d]["after"]
                  for d in present_sessions]
    trades_extended = (pd.Series(ext_totals).median() > 0) if ext_totals else False

    out: list = []
    for d in expected_days:
        if d not in present_set:
            out.append({
                "ticker": ticker, "trade_date": d.isoformat(),
                "severity": SEV_SESSION_MISSING,
                "issue": SEV_NAME[SEV_SESSION_MISSING],
                "bars_total": 0, "bars_premarket": 0, "bars_regular": 0,
                "bars_after": 0,
                "expected_regular": expected_regular.get(d, ""),
                "regular_ratio": "",
                "median_adjacent_total": "", "total_ratio": "",
            })
            continue

        row = by_date[d]
        sev = SEV_OK
        issues: list = []

        exp_reg = expected_regular.get(d)
        reg_ratio = None
        if exp_reg:
            reg_ratio = row["regular"] / exp_reg
            if reg_ratio < regular_ratio:
                sev = max(sev, SEV_REGULAR_INCOMPLETE)
                issues.append(SEV_NAME[SEV_REGULAR_INCOMPLETE])

        med = median_adj.get(d, 0.0)
        tot_ratio = (row["total"] / med) if med else None
        if tot_ratio is not None and tot_ratio < ROWCOUNT_LOW_RATIO:
            sev = max(sev, SEV_ROWCOUNT_LOW)
            issues.append(SEV_NAME[SEV_ROWCOUNT_LOW])

        if trades_extended and (row["premarket"] + row["after"]) == 0:
            sev = max(sev, SEV_NO_EXTENDED)
            issues.append(SEV_NAME[SEV_NO_EXTENDED])

        out.append({
            "ticker": ticker, "trade_date": d.isoformat(),
            "severity": sev,
            "issue": "+".join(issues) if issues else "OK",
            "bars_total": int(row["total"]),
            "bars_premarket": int(row["premarket"]),
            "bars_regular": int(row["regular"]),
            "bars_after": int(row["after"]),
            "expected_regular": exp_reg if exp_reg else "",
            "regular_ratio": f"{reg_ratio:.3f}" if reg_ratio is not None else "",
            "median_adjacent_total": f"{med:.0f}" if med else "",
            "total_ratio": f"{tot_ratio:.2f}" if tot_ratio is not None else "",
        })

    return out, n_unfetched


def write_detail_csv(rows: list, path: Path) -> None:
    cols = ["ticker", "trade_date", "severity", "issue", "bars_total",
            "bars_premarket", "bars_regular", "bars_after",
            "expected_regular", "regular_ratio", "median_adjacent_total",
            "total_ratio"]
    rows = sorted(rows, key=lambda r: (-r["severity"], r["ticker"],
                                       r["trade_date"]))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def write_summary_csv(rows: list, path: Path) -> list:
    agg: dict = defaultdict(lambda: {
        "sessions": 0, "sessions_missing": 0, "regular_incomplete": 0,
        "rowcount_low": 0, "no_extended": 0, "worst_severity": 0,
    })
    for r in rows:
        key = (r["ticker"], r["trade_date"][:7])
        a = agg[key]
        a["sessions"] += 1
        a["worst_severity"] = max(a["worst_severity"], r["severity"])
        if r["severity"] == SEV_SESSION_MISSING:
            a["sessions_missing"] += 1
        if SEV_NAME[SEV_REGULAR_INCOMPLETE] in str(r["issue"]):
            a["regular_incomplete"] += 1
        if SEV_NAME[SEV_ROWCOUNT_LOW] in str(r["issue"]):
            a["rowcount_low"] += 1
        if SEV_NAME[SEV_NO_EXTENDED] in str(r["issue"]):
            a["no_extended"] += 1

    cols = ["ticker", "year_month", "sessions", "sessions_missing",
            "regular_incomplete", "rowcount_low", "no_extended",
            "worst_severity", "worst_issue"]
    out = []
    for (tk, ym), a in agg.items():
        out.append({"ticker": tk, "year_month": ym, **a,
                    "worst_issue": SEV_NAME[a["worst_severity"]]})
    out.sort(key=lambda r: (-r["worst_severity"], -r["sessions_missing"],
                            r["ticker"], r["year_month"]))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(out)
    return out


def audit_manifests(tickers: list) -> list:
    """Chunks the fetcher itself recorded as failed — the known-unfetched work
    list, as opposed to the derived-from-data findings above."""
    out = []
    for tk in tickers:
        mf = read_manifest(tk)
        if mf.empty:
            continue
        bad = mf[mf["status"] == MANIFEST_FAILED]
        for _, r in bad.iterrows():
            out.append({"ticker": tk,
                        "chunk_start": str(r["chunk_start"]),
                        "chunk_end": str(r["chunk_end"]),
                        "note": str(r.get("note", ""))[:120]})
    return out


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Completeness audit of the equity_1min store (read-only).")
    ap.add_argument("--tickers", help="comma-separated; default = all in store")
    ap.add_argument("--start", help="YYYYMMDD; default = each ticker's earliest")
    ap.add_argument("--end", help="YYYYMMDD; default = each ticker's latest")
    ap.add_argument("--max-gap", type=int, default=5,
                    help="trading-day gap separating fetched blocks (default 5). "
                         "Ignored when --start and --end are both given.")
    ap.add_argument("--regular-ratio", type=float, default=REGULAR_RATIO_DEFAULT,
                    help=f"flag a session whose regular-hours bars fall below "
                         f"this fraction of the minutes the session actually "
                         f"had (default {REGULAR_RATIO_DEFAULT})")
    ap.add_argument("--universe", default=None,
                    help="comma-separated expected universe; default = OI store")
    ap.add_argument("--no-leading-empty", action="store_true",
                    help="skip the leading-empty (ticker-rename) report, which "
                         "makes one reference call per flagged ticker")
    ap.add_argument("--leading-empty-out",
                    default="equity_1min_leading_empty.csv")
    ap.add_argument("--out", default="audit_equity_1min.csv")
    ap.add_argument("--summary-out", default="audit_equity_1min_summary.csv")
    args = ap.parse_args()

    log_file = setup_file_logging("audit_equity_1min")
    print("=== equity_1min completeness audit (read-only, offline) ===")
    print(f"Store: {EQUITY_1MIN_DIR}")
    print(f"Log:   {log_file}\n")
    log.info("argv: %s", " ".join(sys.argv[1:]))

    if not EQUITY_1MIN_DIR.exists():
        raise SystemExit(f"Store does not exist: {EQUITY_1MIN_DIR}")

    in_store = list_tickers()
    tickers = ([t.strip().upper() for t in args.tickers.split(",") if t.strip()]
               if args.tickers else in_store)
    if not tickers:
        raise SystemExit(f"No tickers found under {EQUITY_1MIN_DIR}")

    if args.universe:
        universe = [t.strip().upper() for t in args.universe.split(",") if t.strip()]
    else:
        try:
            universe = list_oi_tickers()
        except Exception as exc:
            log.warning("could not read the OI-store universe: %s", exc)
            universe = []
    absent = sorted(set(universe) - set(in_store))
    if universe:
        print(f"Universe: {len(universe)} tickers | in equity_1min: "
              f"{len(in_store)} | ABSENT ENTIRELY: {len(absent)}")
        if absent:
            print("  " + ", ".join(absent))
            log.warning("tickers absent from equity_1min entirely: %s",
                        ", ".join(absent))
        print()

    start = datetime.strptime(args.start, "%Y%m%d").date() if args.start else None
    end = datetime.strptime(args.end, "%Y%m%d").date() if args.end else None

    # Calendar-aware expected regular-hours minutes, computed once for a range
    # that covers every ticker.
    cal_lo = start or date(2019, 1, 1)
    cal_hi = end or date.today()
    expected_regular = regular_minutes(cal_lo, cal_hi)

    t0 = time.monotonic()
    all_rows: list = []
    total_unfetched = 0
    with tqdm(total=len(tickers), unit="tk", ncols=90, desc="audit") as bar:
        for tk in tickers:
            try:
                rows_t, unfetched_t = audit_ticker(
                    tk, start, end, expected_regular, args.regular_ratio,
                    max_gap=args.max_gap)
                all_rows.extend(rows_t)
                total_unfetched += unfetched_t
            except Exception as exc:
                log.error("  FAIL %s: %s", tk, exc, exc_info=True)
            bar.update(1)

    if not all_rows:
        raise SystemExit("No sessions audited — store empty for these filters.")

    out_path = Path(args.out)
    sum_path = Path(args.summary_out)
    write_detail_csv(all_rows, out_path)
    summary = write_summary_csv(all_rows, sum_path)

    by_sev: dict = defaultdict(int)
    for r in all_rows:
        by_sev[r["severity"]] += 1

    print(f"\nAudited {len(all_rows):,} (ticker, session) cells across "
          f"{len(tickers)} tickers in {time.monotonic() - t0:.0f}s")
    if total_unfetched and not (start and end):
        print(f"Excluded {total_unfetched:,} trading day(s) outside any "
              f"fetched block — never requested, not missing.")
        print(f"  (blocks split on gaps > {args.max_gap} trading days; use "
              f"--start/--end to audit an explicit range instead)")

    print("\nBy severity:")
    for sev in sorted(SEV_NAME, reverse=True):
        n = by_sev.get(sev, 0)
        if n:
            print(f"  {SEV_NAME[sev]:<22}{n:>8,}  "
                  f"{100.0 * n / len(all_rows):>5.1f}%")

    failed_chunks = audit_manifests(tickers)
    if failed_chunks:
        print(f"\nManifest records {len(failed_chunks)} FAILED chunk(s) — "
              f"a plain re-run of fetch_equity_1min.py retries these:")
        for r in failed_chunks[:10]:
            print(f"  {r['ticker']:<8}{r['chunk_start']}..{r['chunk_end']}  "
                  f"{r['note']}")
        if len(failed_chunks) > 10:
            print(f"  ... and {len(failed_chunks) - 10} more")

    # Ticker renames: the defect every other check in this file accepts as
    # legitimate. Reported here too so it can be re-examined without refetching.
    if not args.no_leading_empty:
        try:
            from fetch_equity_1min import report_leading_empty
            report_leading_empty(tickers,
                                 out_csv=args.leading_empty_out)
        except Exception as exc:
            log.warning("leading-empty report failed: %s", exc)

    worst = [s for s in summary if s["worst_severity"] > 0][:10]
    if worst:
        print("\nWorst ticker-months:")
        print(f"  {'ticker':<8}{'month':<10}{'issue':<22}"
              f"{'sess_miss':>10}{'reg_incompl':>12}")
        for s in worst:
            print(f"  {s['ticker']:<8}{s['year_month']:<10}"
                  f"{s['worst_issue']:<22}{s['sessions_missing']:>10}"
                  f"{s['regular_incomplete']:>12}")

    print(f"\nDetail:  {out_path.resolve()}")
    print(f"Summary: {sum_path.resolve()}")
    print(f"Log:     {log_path()}")
    print("\nRecover missing data with:")
    print("  python fetch_equity_1min.py --repair --tickers <t>")

    if by_sev.get(SEV_SESSION_MISSING) or by_sev.get(SEV_REGULAR_INCOMPLETE) \
            or failed_chunks:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
