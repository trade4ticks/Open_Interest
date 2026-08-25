"""
zscore_migration_check.py — scope and preview the z-score redefinition.

Read-only. Nothing is written; run this BEFORE the recompute.

Two questions, in order:

  scope   how many equity_metrics_z rows exist per snapshot, so the size of
          the recompute is known rather than assumed
  diff    what the new definition does to ONE ticker at the baseline bucket

The second matters because 1545 is the bucket that "should not change". Its
BASELINE is unchanged — history at 1545 scored against 1545 is the same series
either way — but removing self-inclusion still moves every value there. Today's
reading used to sit inside its own mean and stdev, which inflates sigma and
pulls the score toward zero; excluding it makes scores slightly larger in
magnitude. That shift is the thing to eyeball before it touches 380 columns
across the whole history.

Away from 1545 the change is not a shift but a replacement, so a diff there
would compare a number against a mostly-NULL column and tell you little.

Usage:
    python zscore_migration_check.py --scope
    python zscore_migration_check.py --ticker SPY --diff
    python zscore_migration_check.py --ticker SPY --diff --limit 12
"""
from __future__ import annotations

import argparse
import sys

from db import get_connection
from lib.metrics_config import BASELINE_MIN_N, BASELINE_SNAPSHOT, Z_WINDOWS
from lib.metrics_store import zscore_rows

SCOPE_SQL = """
SELECT snapshot,
       count(*)                        AS z_rows,
       count(DISTINCT ticker)          AS tickers,
       count(DISTINCT trade_date)      AS days,
       min(trade_date)                 AS first_day,
       max(trade_date)                 AS last_day
FROM equity_metrics_z
GROUP BY snapshot
ORDER BY snapshot
"""


def scope(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SCOPE_SQL)
        rows = cur.fetchall()
    if not rows:
        print("  equity_metrics_z is empty — nothing to recompute.")
        return

    print(f"  {'snapshot':<10}{'z rows':>10}{'tickers':>9}{'days':>7}"
          f"   {'first':<12}{'last':<12}  effect")
    total = changed = 0
    for snap, n, tk, days, d0, d1 in rows:
        total += n
        # At the baseline bucket only self-inclusion changes; elsewhere the
        # window itself is replaced.
        effect = ("self-inclusion only" if snap == BASELINE_SNAPSHOT
                  else "REDEFINED (new window)")
        if snap != BASELINE_SNAPSHOT:
            changed += n
        print(f"  {snap:<10}{n:>10,}{tk:>9}{days:>7}   "
              f"{str(d0):<12}{str(d1):<12}  {effect}")
    print(f"\n  total z rows            {total:>10,}")
    print(f"  redefined (non-{BASELINE_SNAPSHOT})  {changed:>10,}")
    print(f"  shifted only ({BASELINE_SNAPSHOT})   {total - changed:>10,}")


def diff(conn, ticker: str, limit: int) -> None:
    """Recompute the baseline bucket under the new rules and diff, in memory."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT trade_date FROM equity_metrics "
            "WHERE ticker = %s AND snapshot = %s "
            "ORDER BY trade_date DESC LIMIT %s",
            (ticker, BASELINE_SNAPSHOT, limit))
        dates = sorted(r[0] for r in cur.fetchall())
    if not dates:
        print(f"  no {BASELINE_SNAPSHOT} metrics rows for {ticker}")
        return

    new_rows = {r["trade_date"]: r
                for r in zscore_rows(conn, ticker, BASELINE_SNAPSHOT, dates)}
    if not new_rows:
        print(f"  new definition produced no rows for {ticker} — most likely "
              f"fewer than {BASELINE_MIN_N} prior baseline observations")
        return

    # A representative column per window rather than all 380: the shift is
    # systematic, so one column shows it and 380 would bury it.
    probes = [f"iv_30d_atm_z_{w}" for w in Z_WINDOWS]
    cols = ", ".join(probes)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT trade_date, {cols} FROM equity_metrics_z "
            f"WHERE ticker = %s AND snapshot = %s AND trade_date = ANY(%s)",
            (ticker, BASELINE_SNAPSHOT, dates))
        old_rows = {r[0]: r[1:] for r in cur.fetchall()}

    print(f"  {ticker} at {BASELINE_SNAPSHOT} — stored vs recomputed\n")
    print(f"  {'trade_date':<12}" + "".join(
        f"{p + ' old':>20}{p + ' new':>20}{'delta':>10}" for p in probes[:1]))
    deltas = []
    for d in dates:
        new = new_rows.get(d)
        old = old_rows.get(d)
        if new is None:
            continue
        cells = f"  {str(d):<12}"
        for i, p in enumerate(probes[:1]):
            o = old[i] if old else None
            n = new.get(p)
            if o is not None and n is not None:
                deltas.append(n - o)
                cells += f"{o:>20.4f}{n:>20.4f}{n - o:>10.4f}"
            else:
                cells += f"{'-' if o is None else f'{o:.4f}':>20}"
                cells += f"{'-' if n is None else f'{n:.4f}':>20}{'-':>10}"
        print(cells)

    if deltas:
        mag = [abs(x) for x in deltas]
        print(f"\n  compared {len(deltas)} value(s)")
        print(f"    mean delta   {sum(deltas)/len(deltas):+.4f}")
        print(f"    max |delta|  {max(mag):.4f}")
        print(f"    |new| > |old| in {sum(1 for x in deltas if x)} of "
              f"{len(deltas)}  (expected: removing self-inclusion shrinks "
              f"sigma, so scores grow in magnitude)")
    else:
        print("\n  no comparable pairs — the stored column is NULL where the "
              "new one is not, which is itself the answer for this ticker")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scope", action="store_true",
                    help="row counts per snapshot")
    ap.add_argument("--diff", action="store_true",
                    help=f"recompute one ticker at {BASELINE_SNAPSHOT} and "
                         f"diff against the stored values")
    ap.add_argument("--ticker", default="SPY")
    ap.add_argument("--limit", type=int, default=10,
                    help="most recent N baseline dates to diff")
    args = ap.parse_args()
    if not (args.scope or args.diff):
        ap.print_help()
        return 1

    with get_connection() as conn:
        if args.scope:
            print("=== SCOPE ===")
            scope(conn)
        if args.diff:
            print(f"\n=== DIFF: {args.ticker} ===")
            diff(conn, args.ticker.upper(), args.limit)
    print("\nRead-only — nothing was written.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
