"""
daily_carryforward_audit.py — do the daily columns actually carry? Read-only.

The failure this exists to catch has a signature: a column populated at one
snapshot and NULL at every other one, on the same date, for the same tickers.
That is what spotvol_beta looked like on 2026-08-24 — 121/121 at 1545, 0/121 at
every intraday bucket — and it was invisible because the close, which is what
anyone eyeballs, was perfectly healthy.

Some families are daily by construction and must be flat across buckets:

    realized_vol   rv_*, rv_park_*, rv_gk_*, log_ret_*, downside_semivol_1m
    vrp            vrp_*, vrp_ratio_*
    spot_vol       vov_30d_1m, spotvol_beta_*, spotvol_r2_*
    calendar       day_of_week, days_to_monthly_opex, days_to_earnings

Everything else is a genuine per-snapshot reading and is expected to vary.

--- Two checks -------------------------------------------------------------

  COVERAGE  non-null share per (column, snapshot). A daily column whose
            coverage collapses away from the baseline bucket is the bug.

  FLATNESS  distinct values per (ticker, trade_date) across buckets. A daily
            column should have exactly ONE. More than one means it is being
            recomputed per snapshot from something snapshot-specific.

            The spot-vol columns are the documented exception: the baseline
            row IS the day's daily observation and sits inside its own window,
            while intraday buckets stop at T-1. So they legitimately show TWO
            distinct values per day — the carried-forward one and the close's.
            Three or more is a real problem.

Usage:
    python daily_carryforward_audit.py                     # most recent date
    python daily_carryforward_audit.py --date 2026-08-24
    python daily_carryforward_audit.py --date 2026-08-24 --all-columns
"""
from __future__ import annotations

import argparse
import sys
from datetime import date as _date

from db import get_connection
from lib.metrics_config import BASE_COLUMNS, BASELINE_SNAPSHOT

DAILY_FAMILIES = ("realized_vol", "vrp", "spot_vol", "calendar")

# Carried forward, but the baseline bucket includes its own observation, so two
# distinct values per day is correct for these and only these.
ASOF_SENSITIVE_FAMILY = "spot_vol"


def _daily_columns() -> list:
    return [c for c in BASE_COLUMNS if c.family in DAILY_FAMILIES]


def _latest_date(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT max(trade_date) FROM equity_metrics")
        return cur.fetchone()[0]


def _snapshots(conn, day) -> list:
    with conn.cursor() as cur:
        cur.execute("SELECT snapshot, count(*) FROM equity_metrics "
                    "WHERE trade_date = %s GROUP BY snapshot ORDER BY snapshot",
                    (day,))
        return cur.fetchall()


def coverage(conn, day, cols: list, snaps: list) -> dict:
    """{column: {snapshot: (non_null, total)}} in ONE pass over the date."""
    sel = ", ".join(f"count({c.name}) AS n_{i}" for i, c in enumerate(cols))
    with conn.cursor() as cur:
        cur.execute(f"SELECT snapshot, count(*), {sel} FROM equity_metrics "
                    f"WHERE trade_date = %s GROUP BY snapshot", (day,))
        rows = cur.fetchall()
    out = {c.name: {} for c in cols}
    for r in rows:
        snap, total = r[0], r[1]
        for i, c in enumerate(cols):
            out[c.name][snap] = (r[2 + i], total)
    return out


def flatness(conn, day, cols: list) -> dict:
    """{column: max distinct values seen for any one ticker on this date}."""
    sel = ", ".join(f"max(d_{i}) AS m_{i}" for i in range(len(cols)))
    inner = ", ".join(f"count(DISTINCT {c.name}) AS d_{i}"
                      for i, c in enumerate(cols))
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {sel} FROM (SELECT ticker, {inner} FROM equity_metrics "
            f"WHERE trade_date = %s GROUP BY ticker) s", (day,))
        r = cur.fetchone()
    return {c.name: r[i] for i, c in enumerate(cols)}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", help="YYYY-MM-DD (default: latest with metrics)")
    ap.add_argument("--all-columns", action="store_true",
                    help="list every daily column, not just the problems")
    args = ap.parse_args()

    with get_connection() as conn:
        day = (_date.fromisoformat(args.date) if args.date
               else _latest_date(conn))
        if day is None:
            print("  equity_metrics is empty.")
            return 1
        snaps = _snapshots(conn, day)
        if not snaps:
            print(f"  no equity_metrics rows on {day}")
            return 1

        cols = _daily_columns()
        print(f"=== daily carry-forward audit — {day} ===")
        print(f"  {len(cols)} daily column(s) across {len(snaps)} snapshot(s)")
        print(f"  baseline bucket: {BASELINE_SNAPSHOT}\n")

        cov = coverage(conn, day, cols, snaps)
        flat = flatness(conn, day, cols)

        intraday = [s for s, _ in snaps if s != BASELINE_SNAPSHOT]
        broken, uneven = [], []
        for c in cols:
            per = cov[c.name]
            base_n, base_t = per.get(BASELINE_SNAPSHOT, (0, 0))
            intra_n = sum(per.get(s, (0, 0))[0] for s in intraday)
            intra_t = sum(per.get(s, (0, 0))[1] for s in intraday)
            # The signature: healthy at the close, empty everywhere else.
            if base_n > 0 and intra_t > 0 and intra_n == 0:
                broken.append((c, base_n, base_t, intra_t))
            limit = 2 if c.family == ASOF_SENSITIVE_FAMILY else 1
            if (flat[c.name] or 0) > limit:
                uneven.append((c, flat[c.name], limit))

        if broken:
            print("  NOT CARRYING FORWARD — populated at the close, empty "
                  "intraday:")
            for c, bn, bt, it in broken:
                print(f"    {c.name:<24} {BASELINE_SNAPSHOT}: {bn}/{bt}"
                      f"    intraday: 0/{it}")
        else:
            print("  carry-forward: OK — no daily column is populated at the "
                  "close and empty intraday")

        if uneven:
            print("\n  VARIES ACROSS BUCKETS more than its as-of rule allows:")
            for c, n, limit in uneven:
                print(f"    {c.name:<24} up to {n} distinct value(s) per "
                      f"ticker/day (allowed {limit})")
        else:
            print("  flatness:      OK — every daily column holds one value "
                  "per ticker/day, two for spot_vol (its documented as-of "
                  "split)")

        if args.all_columns:
            print(f"\n  {'column':<24}{'family':<14}"
                  + "".join(f"{s:>7}" for s, _ in snaps))
            for c in cols:
                cells = ""
                for s, _ in snaps:
                    n, t = cov[c.name].get(s, (0, 0))
                    cells += f"{(f'{n}/{t}' if t else '-'):>7}"
                print(f"  {c.name:<24}{c.family:<14}{cells}")

    print("\nRead-only — nothing was written.")
    return 1 if (broken or uneven) else 0


if __name__ == "__main__":
    sys.exit(main())
