"""
audit_chain_snapshots.py — completeness audit of the chain_snapshots store.

Read-only. Answers one question: which (ticker, session) cells are incomplete,
and how badly. Run after every batch.

This exists because the fetcher's resumability is keyed at
(trade_date, snapshot_label) while point queries fail at
(trade_date, expiration, snapshot_label). A session that wrote SOME
expirations looks fully loaded and is skipped forever by a plain re-run. The
only way to know a store is complete is to re-derive what should be there and
diff it — which is what this does.

Checks per (ticker, session):

  1. SESSION MISSING      — a trading day inside a FETCHED BLOCK with no rows
                            at all. Blocks are runs of stored sessions split
                            on gaps > --max-gap trading days, so months you
                            never requested are reported as unfetched rather
                            than flagged as missing.
  2. SNAPSHOT MISSING     — session present but only one of '0945' / '1545'.
  3. EXPIRATIONS MISSING  — expirations the vendor lists for that session
                            (via /v3/option/history/eod, the same source the
                            fetcher enumerates from) that have no rows stored.
  4. ROW COUNT LOW        — rows far below the median of adjacent sessions.
                            Catches partial loss that the expiration check
                            cannot see, e.g. an expiration present but missing
                            most of its strikes.

Severity is the max of the checks that fired, so the CSV sorts worst-first.

Enumeration costs one vendor call per (ticker, session) and is what makes
check 3 possible. --no-enumerate skips it for a fast structural-only pass
(checks 1, 2 and 4), which needs no network at all.

Usage:
    python audit_chain_snapshots.py
    python audit_chain_snapshots.py --tickers SPY,JNJ --out audit.csv
    python audit_chain_snapshots.py --no-enumerate          # fast, offline
    python audit_chain_snapshots.py --start 20260601 --end 20260630
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm

from config import CHAIN_SNAPSHOTS_DIR
from lib.chain_fetch_common import log_path, setup_file_logging, track
from lib.chain_snapshot_store import SNAPSHOT_LABELS, list_tickers, list_years, year_path
from lib.market_hours import get_trading_days
from lib.parquet_store import list_tickers as list_oi_tickers
from lib.thetadata import (
    enumerate_expirations_eod,
    max_connections,
    set_max_connections,
    test_connection,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Ordered worst-first. The numeric value is what the CSV sorts on.
SEV_SESSION_MISSING   = 4
SEV_SNAPSHOT_MISSING  = 3
SEV_EXPIRATIONS_MISS  = 2
SEV_ROWCOUNT_LOW      = 1
SEV_OK                = 0
SEV_NAME = {
    4: "SESSION_MISSING",
    3: "SNAPSHOT_MISSING",
    2: "EXPIRATIONS_MISSING",
    1: "ROWCOUNT_LOW",
    0: "OK",
}

# A session with fewer than this fraction of the adjacent-session median row
# count is flagged. Chains genuinely change size day to day (expiries roll
# off), so this is deliberately loose — it is a smoke alarm, not a proof.
ROWCOUNT_LOW_RATIO = 0.5
ADJACENT_WINDOW = 5          # sessions either side for the median


# --- Store reading ----------------------------------------------------------

def read_index(ticker: str) -> pd.DataFrame:
    """(trade_date, snapshot, expiration) for every stored row of one ticker.

    Three columns only — the store is far too large to read whole, and these
    are all the audit needs.
    """
    frames = []
    for y in list_years(ticker):
        p = year_path(ticker, y)
        try:
            tbl = pq.read_table(p, columns=["trade_date", "snapshot", "expiration"])
        except Exception as exc:
            log.error("  %s/%d.parquet unreadable — %s", ticker, y, exc)
            continue
        frames.append(tbl.to_pandas())
    if not frames:
        return pd.DataFrame(columns=["trade_date", "snapshot", "expiration"])
    return pd.concat(frames, ignore_index=True)


# --- Per-ticker audit -------------------------------------------------------

def coverage_blocks(present: list[date], all_days: list[date],
                    max_gap: int) -> list[tuple[int, int]]:
    """Split stored sessions into contiguously-fetched blocks.

    Returns (start_idx, end_idx) pairs into `all_days`.

    Without this the audit assumed everything between a ticker's first and
    last stored session was requested, so every trading day in a month that
    was never fetched got flagged SESSION_MISSING. A run of dense sessions
    separated from the next by more than `max_gap` trading days is treated as
    a separate fetch; the space between blocks is UNFETCHED, not missing.
    """
    pos = {d: i for i, d in enumerate(all_days)}
    idxs = sorted(pos[d] for d in present if d in pos)
    if not idxs:
        return []
    blocks: list[tuple[int, int]] = []
    start_i = prev = idxs[0]
    for i in idxs[1:]:
        if i - prev > max_gap:
            blocks.append((start_i, prev))
            start_i = i
        prev = i
    blocks.append((start_i, prev))
    return blocks


def audit_ticker(ticker: str,
                 start: date | None,
                 end: date | None,
                 do_enumerate: bool,
                 max_gap: int = 5) -> tuple[list[dict], int]:
    idx = read_index(ticker)
    if idx.empty:
        log.warning("  %s: no rows in store", ticker)
        return [], 0

    if start is not None:
        idx = idx[idx["trade_date"] >= start]
    if end is not None:
        idx = idx[idx["trade_date"] <= end]
    if idx.empty:
        return [], 0

    present_sessions = sorted(idx["trade_date"].unique())
    lo = start or present_sessions[0]
    hi = end or present_sessions[-1]

    all_days = get_trading_days(lo, hi)
    present_set = set(present_sessions)

    if start is not None and end is not None:
        # An explicit range is a statement of intent: every session in it was
        # meant to be fetched, so every absent one is genuinely missing.
        expected = all_days
        n_unfetched = 0
    else:
        # Otherwise only audit inside contiguously-fetched blocks. Trading days
        # between blocks were never requested and are reported separately.
        blocks = coverage_blocks(present_sessions, all_days, max_gap)
        expected = [d for (a, b) in blocks for d in all_days[a:b + 1]]
        n_unfetched = len(all_days) - len(expected)

    rows_by_session = idx.groupby("trade_date").size().to_dict()
    exps_by_session = idx.groupby("trade_date")["expiration"].apply(set).to_dict()
    snaps_by_session = idx.groupby("trade_date")["snapshot"].apply(set).to_dict()

    # Adjacent-session median row count, for the partial-loss check.
    ordered = [(d, rows_by_session.get(d, 0)) for d in present_sessions]
    median_adj: dict[date, float] = {}
    for i, (d, _) in enumerate(ordered):
        lo_i = max(0, i - ADJACENT_WINDOW)
        hi_i = min(len(ordered), i + ADJACENT_WINDOW + 1)
        neighbours = [c for j, (_, c) in enumerate(ordered)
                      if lo_i <= j < hi_i and j != i]
        median_adj[d] = float(pd.Series(neighbours).median()) if neighbours else 0.0

    # Enumerate what SHOULD be there — for MISSING sessions too, not just
    # present ones. Without the missing ones a wholly-absent session reported
    # zero missing cells, which is why the session count and the cell count
    # could not be reconciled.
    enumerated: dict[date, set] = {}
    to_enum = [d for d in expected if d in present_set or d not in present_set]
    if do_enumerate and to_enum:
        def _enum(sess: date):
            with track(f"enum {ticker} {sess}", kind="enum"):
                return sess, enumerate_expirations_eod(ticker, sess, sess)

        with ThreadPoolExecutor(max_workers=max_connections()) as pool:
            futs = [pool.submit(_enum, d) for d in to_enum]
            for fut in as_completed(futs):
                try:
                    sess, exps = fut.result()
                except Exception as exc:
                    log.warning("  %s: enumeration failed — %s", ticker, exc)
                    continue
                enumerated[sess] = {e for e in exps if e >= sess}

    out: list[dict] = []

    for d in expected:
        if d not in present_set:
            # Every enumerated expiration is missing, for both labels — so the
            # cell cost of a wholly-absent session is now counted, and the
            # session count and cell count reconcile.
            enum_set = enumerated.get(d)
            n_enum = len(enum_set) if enum_set is not None else None
            out.append({
                "ticker": ticker, "trade_date": d.isoformat(),
                "severity": SEV_SESSION_MISSING,
                "issue": SEV_NAME[SEV_SESSION_MISSING],
                "snapshots_present": "", "rows": 0,
                "median_adjacent_rows": "", "row_ratio": "",
                "expirations_present": 0,
                "expirations_enumerated": n_enum if n_enum is not None else "",
                "expirations_missing": n_enum if n_enum is not None else "",
                "missing_expiration_sample": "|".join(
                    e.isoformat() for e in sorted(enum_set)[:5]) if enum_set else "",
            })
            continue

        sev = SEV_OK
        issues: list[str] = []

        snaps = snaps_by_session.get(d, set())
        if len(snaps) < len(SNAPSHOT_LABELS):
            sev = max(sev, SEV_SNAPSHOT_MISSING)
            issues.append(SEV_NAME[SEV_SNAPSHOT_MISSING])

        exps_present = exps_by_session.get(d, set())
        enum_set = enumerated.get(d)
        missing_exps: set = set()
        if enum_set:
            missing_exps = enum_set - exps_present
            if missing_exps:
                sev = max(sev, SEV_EXPIRATIONS_MISS)
                issues.append(SEV_NAME[SEV_EXPIRATIONS_MISS])

        rows = rows_by_session.get(d, 0)
        med = median_adj.get(d, 0.0)
        ratio = (rows / med) if med else None
        if ratio is not None and ratio < ROWCOUNT_LOW_RATIO:
            sev = max(sev, SEV_ROWCOUNT_LOW)
            issues.append(SEV_NAME[SEV_ROWCOUNT_LOW])

        out.append({
            "ticker": ticker, "trade_date": d.isoformat(),
            "severity": sev,
            "issue": "+".join(issues) if issues else "OK",
            "snapshots_present": "|".join(sorted(str(s) for s in snaps)),
            "rows": rows,
            "median_adjacent_rows": f"{med:.0f}" if med else "",
            "row_ratio": f"{ratio:.2f}" if ratio is not None else "",
            "expirations_present": len(exps_present),
            "expirations_enumerated": len(enum_set) if enum_set is not None else "",
            "expirations_missing": len(missing_exps) if enum_set is not None else "",
            "missing_expiration_sample": "|".join(
                e.isoformat() for e in sorted(missing_exps)[:5]),
        })

    return out, n_unfetched


# --- Summaries --------------------------------------------------------------

def write_detail_csv(rows: list[dict], path: Path) -> None:
    cols = ["ticker", "trade_date", "severity", "issue", "snapshots_present",
            "rows", "median_adjacent_rows", "row_ratio", "expirations_present",
            "expirations_enumerated", "expirations_missing",
            "missing_expiration_sample"]
    # Worst first, then by how much is missing, so the top of the file is the
    # work list.
    rows = sorted(
        rows,
        key=lambda r: (
            -r["severity"],
            -(int(r["expirations_missing"]) if str(r["expirations_missing"]).isdigit() else 0),
            r["ticker"], r["trade_date"],
        ),
    )
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def write_summary_csv(rows: list[dict], path: Path) -> None:
    agg: dict[tuple[str, str], dict] = defaultdict(lambda: {
        "sessions": 0, "sessions_missing": 0, "snapshot_missing": 0,
        "expirations_missing_sessions": 0, "missing_cells": 0,
        "rowcount_low": 0, "worst_severity": 0,
    })
    for r in rows:
        key = (r["ticker"], r["trade_date"][:7])
        a = agg[key]
        a["sessions"] += 1
        a["worst_severity"] = max(a["worst_severity"], r["severity"])
        if r["severity"] == SEV_SESSION_MISSING:
            a["sessions_missing"] += 1
        if SEV_NAME[SEV_SNAPSHOT_MISSING] in str(r["issue"]):
            a["snapshot_missing"] += 1
        if SEV_NAME[SEV_ROWCOUNT_LOW] in str(r["issue"]):
            a["rowcount_low"] += 1
        miss = r["expirations_missing"]
        if str(miss).isdigit() and int(miss) > 0:
            a["expirations_missing_sessions"] += 1
            # Each missing expiration is 2 cells (one per snapshot label).
            a["missing_cells"] += int(miss) * len(SNAPSHOT_LABELS)

    cols = ["ticker", "year_month", "sessions", "sessions_missing",
            "snapshot_missing", "expirations_missing_sessions",
            "missing_cells", "rowcount_low", "worst_severity", "worst_issue"]
    out = []
    for (tk, ym), a in agg.items():
        out.append({"ticker": tk, "year_month": ym, **a,
                    "worst_issue": SEV_NAME[a["worst_severity"]]})
    out.sort(key=lambda r: (-r["worst_severity"], -r["missing_cells"],
                            r["ticker"], r["year_month"]))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(out)
    return out


# --- Main -------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Completeness audit of the chain_snapshots store (read-only).")
    ap.add_argument("--tickers", help="comma-separated; default = all in store")
    ap.add_argument("--start", help="YYYYMMDD; default = each ticker's earliest")
    ap.add_argument("--end", help="YYYYMMDD; default = each ticker's latest")
    ap.add_argument("--max-gap", type=int, default=5,
                    help=("trading-day gap that separates one fetched block "
                          "from the next (default 5). Days BETWEEN blocks were "
                          "never requested and are reported as unfetched, not "
                          "missing. Ignored when --start and --end are given."))
    ap.add_argument("--universe", default=None,
                    help=("comma-separated expected ticker universe; default "
                          "= tickers in the OI store. Tickers in the universe "
                          "with no chain_snapshots data are listed."))
    ap.add_argument("--no-enumerate", action="store_true",
                    help="skip vendor enumeration — fast, offline, but cannot "
                         "detect missing expirations (checks 1, 2, 4 only)")
    ap.add_argument("--connections", type=int, default=None,
                    help="concurrent connections for enumeration (default 4)")
    ap.add_argument("--out", default="audit_chain_snapshots.csv",
                    help="detail CSV path")
    ap.add_argument("--summary-out", default="audit_chain_snapshots_summary.csv",
                    help="per-ticker-month summary CSV path")
    args = ap.parse_args()

    if args.connections is not None:
        set_max_connections(args.connections)

    log_file = setup_file_logging("audit_chain_snapshots")
    print("=== chain_snapshots completeness audit (read-only) ===")
    print(f"Store: {CHAIN_SNAPSHOTS_DIR}")
    print(f"Log:   {log_file}\n")
    log.info("argv: %s", " ".join(sys.argv[1:]))

    if not CHAIN_SNAPSHOTS_DIR.exists():
        raise SystemExit(f"Store does not exist: {CHAIN_SNAPSHOTS_DIR}")

    in_store = list_tickers()
    tickers = ([t.strip().upper() for t in args.tickers.split(",") if t.strip()]
               if args.tickers else in_store)
    if not tickers:
        raise SystemExit(f"No tickers found under {CHAIN_SNAPSHOTS_DIR}")

    # Which of the intended universe never made it into the store at all?
    # A ticker with no directory produces no rows and so would otherwise be
    # invisible to every check below.
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
        print(f"Universe: {len(universe)} tickers | in chain_snapshots: "
              f"{len(in_store)} | ABSENT ENTIRELY: {len(absent)}")
        if absent:
            print("  " + ", ".join(absent))
            log.warning("tickers absent from chain_snapshots entirely: %s",
                        ", ".join(absent))
        print()

    start = datetime.strptime(args.start, "%Y%m%d").date() if args.start else None
    end = datetime.strptime(args.end, "%Y%m%d").date() if args.end else None
    do_enum = not args.no_enumerate

    if do_enum:
        print("Checking ThetaData ...", end=" ", flush=True)
        if not test_connection():
            raise SystemExit("FAILED — terminal not reachable. Use "
                             "--no-enumerate for an offline structural audit.")
        print(f"OK ({max_connections()} connections)\n")
    else:
        print("Enumeration DISABLED — missing expirations will NOT be "
              "detected.\n")

    t0 = time.monotonic()
    all_rows: list[dict] = []
    total_unfetched = 0
    with tqdm(total=len(tickers), unit="tk", ncols=90, desc="audit") as bar:
        for tk in tickers:
            try:
                rows_t, unfetched_t = audit_ticker(tk, start, end, do_enum,
                                                   max_gap=args.max_gap)
                all_rows.extend(rows_t)
                total_unfetched += unfetched_t
            except Exception as exc:
                log.error("  FAIL %s: %s", tk, exc, exc_info=True)
            bar.update(1)

    if not all_rows:
        raise SystemExit("No sessions audited — store empty for the given filters.")

    out_path = Path(args.out)
    sum_path = Path(args.summary_out)
    write_detail_csv(all_rows, out_path)
    summary = write_summary_csv(all_rows, sum_path)

    # --- console report -----------------------------------------------------
    by_sev: dict[int, int] = defaultdict(int)
    for r in all_rows:
        by_sev[r["severity"]] += 1
    total_missing_cells = sum(s["missing_cells"] for s in summary)

    print(f"\nAudited {len(all_rows):,} (ticker, session) cells across "
          f"{len(tickers)} tickers in {time.monotonic() - t0:.0f}s")
    if total_unfetched and not (start and end):
        print(f"Excluded {total_unfetched:,} trading day(s) outside any "
              f"fetched block — never requested, not missing.")
        print(f"  (blocks split on gaps > {args.max_gap} trading days; "
              f"use --start/--end to audit an explicit range instead)")

    print("\nBy severity:")
    for sev in sorted(SEV_NAME, reverse=True):
        n = by_sev.get(sev, 0)
        if n:
            print(f"  {SEV_NAME[sev]:<22}{n:>8,}  "
                  f"{100.0 * n / len(all_rows):>5.1f}%")

    if do_enum:
        # Reconciliation: session count and cell count measure different
        # things, so show how each contributes rather than leaving the reader
        # to square two numbers that never had to match.
        cells_from_missing_sessions = sum(
            int(r["expirations_missing"]) * len(SNAPSHOT_LABELS)
            for r in all_rows
            if r["severity"] == SEV_SESSION_MISSING
            and str(r["expirations_missing"]).isdigit())
        cells_from_partial = total_missing_cells - cells_from_missing_sessions
        print(f"\nMissing (session, expiration, snapshot) cells: "
              f"{total_missing_cells:,}")
        print(f"  from wholly-missing sessions      {cells_from_missing_sessions:>10,}")
        print(f"  from partially-filled sessions    {cells_from_partial:>10,}")
        print("  Recover with:  python fetch_chain_snapshots.py --repair "
              "--tickers <t> --start <YYYYMMDD> --end <YYYYMMDD>")
    else:
        print("\n(missing-expiration count unavailable — run without "
              "--no-enumerate)")

    worst = [s for s in summary if s["worst_severity"] > 0][:10]
    if worst:
        print("\nWorst ticker-months:")
        print(f"  {'ticker':<8}{'month':<10}{'issue':<22}"
              f"{'sess_miss':>10}{'cells_miss':>12}")
        for s in worst:
            print(f"  {s['ticker']:<8}{s['year_month']:<10}"
                  f"{s['worst_issue']:<22}{s['sessions_missing']:>10}"
                  f"{s['missing_cells']:>12,}")

    print(f"\nDetail:  {out_path.resolve()}")
    print(f"Summary: {sum_path.resolve()}")
    print(f"Log:     {log_path()}")

    if by_sev.get(SEV_SESSION_MISSING) or by_sev.get(SEV_SNAPSHOT_MISSING) \
            or by_sev.get(SEV_EXPIRATIONS_MISS):
        raise SystemExit(1)      # non-zero so a batch script notices


if __name__ == "__main__":
    main()
