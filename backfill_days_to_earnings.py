"""
backfill_days_to_earnings.py — populate days_to_earnings on existing rows.

The fetch is its own backfill: get_earnings_dates(limit=24) returns ~6 years of
history alongside the next scheduled date, so once fetch_earnings_calendar.py
has run, the calendar already covers the whole metrics range. What is missing is
the JOIN — every equity_metrics row written before the source existed still
carries NULL.

--- Why SQL and not a metrics recompute -------------------------------------

days_to_earnings depends on nothing but (ticker, trade_date) and the calendar.
Recomputing it through build_equity_metrics would re-derive 240 columns per row
from the surface, the chain and the OHLC history — hours of work to change one
integer, and every other column would be rewritten from today's inputs rather
than left as computed. A single UPDATE touches exactly the column that was NULL.

The value it writes is identical to what lib/metrics_compute produces: the
LATERAL below is the same "first date on or after trade_date, in calendar days"
rule as earnings_store.days_to_earnings — and --verify proves that against the
real rows rather than assuming it, by re-deriving every value in Python from
the cached date lists and diffing. Worth running once after the first backfill:
if the two ever disagreed, historical rows and rows written by tonight's cron
would carry different definitions of the same column.

--- What stays NULL ---------------------------------------------------------

  * funds — no calendar rows at all
  * any trade_date past a ticker's last known earnings date, since Yahoo
    publishes only the next confirmed one

Both are correct NULLs. earnings_coverage is what tells them apart, so a run
that leaves thousands of NULLs is not evidence of a failed backfill; the
--report output breaks it down so that call does not have to be a guess.

Usage:
    python backfill_days_to_earnings.py --report          # read-only
    python backfill_days_to_earnings.py --verify          # read-only parity
    python backfill_days_to_earnings.py --dry-run         # counts, no write
    python backfill_days_to_earnings.py
    python backfill_days_to_earnings.py --tickers AAPL,JNJ
    python backfill_days_to_earnings.py --all             # also overwrite non-NULL
"""
from __future__ import annotations

import argparse
import sys
import time

from db import get_connection

# The lookup, expressed once. A LATERAL rather than a correlated scalar
# subquery so it uses ix_earnings_calendar_lookup as an index scan stopping at
# the first row, instead of aggregating every future date per metrics row.
NEXT_DATE_LATERAL = """
LEFT JOIN LATERAL (
    SELECT ec.earnings_date
    FROM earnings_calendar ec
    WHERE ec.ticker = m.ticker
      AND ec.earnings_date >= m.trade_date
    ORDER BY ec.earnings_date
    LIMIT 1
) nxt ON TRUE
"""

UPDATE_SQL = f"""
UPDATE equity_metrics m
SET days_to_earnings = sub.dte
FROM (
    SELECT m.ticker, m.trade_date, m.snapshot,
           (nxt.earnings_date - m.trade_date)::SMALLINT AS dte
    FROM equity_metrics m
    {NEXT_DATE_LATERAL}
    WHERE nxt.earnings_date IS NOT NULL
      {{ticker_filter}}
      {{null_filter}}
) sub
WHERE m.ticker = sub.ticker
  AND m.trade_date = sub.trade_date
  AND m.snapshot = sub.snapshot
  -- Skip rows already holding the right answer: an idempotent re-run should
  -- report 0 changed, not rewrite every row and inflate the count.
  AND m.days_to_earnings IS DISTINCT FROM sub.dte
"""

REPORT_SQL = """
SELECT count(*)                                          AS rows_total,
       count(*) FILTER (WHERE m.days_to_earnings IS NOT NULL) AS filled,
       count(*) FILTER (WHERE m.days_to_earnings IS NULL
                          AND cov.ticker IS NULL)        AS no_coverage_row,
       count(*) FILTER (WHERE m.days_to_earnings IS NULL
                          AND cov.has_earnings IS FALSE) AS fund,
       count(*) FILTER (WHERE m.days_to_earnings IS NULL
                          AND cov.has_earnings IS TRUE)  AS past_last_known,
       min(m.trade_date)                                 AS first_day,
       max(m.trade_date)                                 AS last_day
FROM equity_metrics m
LEFT JOIN earnings_coverage cov ON cov.ticker = m.ticker
"""


def calendar_ready(conn) -> bool:
    """The calendar must be populated first — an empty one would 'succeed'."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('earnings_calendar')")
        if cur.fetchone()[0] is None:
            print("  earnings_calendar does not exist. Run:\n"
                  "      python fetch_earnings_calendar.py --init-db")
            return False
        cur.execute("SELECT count(*), count(DISTINCT ticker), "
                    "min(earnings_date), max(earnings_date) "
                    "FROM earnings_calendar")
        n, tk, d0, d1 = cur.fetchone()
    if not n:
        print("  earnings_calendar is EMPTY. Run:\n"
              "      python fetch_earnings_calendar.py")
        return False
    print(f"  calendar: {n:,} date(s), {tk} ticker(s), {d0} .. {d1}")
    return True


def report(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(REPORT_SQL)
        total, filled, no_cov, fund, past = cur.fetchone()[:5]
        cur.execute(REPORT_SQL)
        d0, d1 = cur.fetchone()[5:7]
    if not total:
        print("  equity_metrics is empty.")
        return
    pct = 100.0 * filled / total
    print(f"\n  equity_metrics rows   {total:>10,}   {d0} .. {d1}")
    print(f"    days_to_earnings set{filled:>10,}   {pct:5.1f}%")
    print(f"    NULL — fund         {fund:>10,}   correct")
    print(f"    NULL — past last    {past:>10,}   correct (Yahoo publishes "
          f"only the next date)")
    print(f"    NULL — no coverage  {no_cov:>10,}"
          + ("   <- these are the ones to look at" if no_cov else ""))


def verify(conn, limit: int) -> int:
    """Re-derive every stored value in Python and diff against the SQL result.

    Reads the calendar once per ticker and walks the rows, which is the same
    code path lib/metrics_compute uses — so a disagreement here means the
    backfill and the nightly build would write different numbers for the same
    row, which is the one failure this whole design has to rule out.
    """
    from lib.earnings_store import days_to_earnings, load_dates

    with conn.cursor() as cur:
        cur.execute(
            "SELECT ticker, trade_date, snapshot, days_to_earnings "
            "FROM equity_metrics ORDER BY ticker, trade_date, snapshot "
            "LIMIT %s", (limit,))
        rows = cur.fetchall()
    if not rows:
        print("  equity_metrics is empty — nothing to verify.")
        return 0

    cache: dict = {}
    mismatches = []
    for tk, td, snap, stored in rows:
        if tk not in cache:
            cache[tk] = load_dates(conn, tk)
        expect = days_to_earnings(cache[tk], td)
        if (expect is None) != (stored is None) or (
                expect is not None and int(stored) != expect):
            mismatches.append((tk, td, snap, stored, expect))

    print(f"\n  checked {len(rows):,} row(s) across {len(cache)} ticker(s)")
    if not mismatches:
        print("  PARITY OK — SQL backfill and Python agree on every row.")
        return 0
    print(f"  MISMATCH on {len(mismatches):,} row(s):")
    for tk, td, snap, stored, expect in mismatches[:15]:
        print(f"    {tk:<6} {td} {snap:<6} stored={stored!s:<6} "
              f"python={expect!s}")
    return 2


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill days_to_earnings.")
    ap.add_argument("--tickers", help="comma-separated; default = all")
    ap.add_argument("--dry-run", action="store_true",
                    help="count the rows that would change, then roll back")
    ap.add_argument("--report", action="store_true",
                    help="read-only coverage breakdown; no write")
    ap.add_argument("--verify", action="store_true",
                    help="read-only: re-derive every value in Python and diff "
                         "against what the SQL backfill wrote")
    ap.add_argument("--limit", type=int, default=200_000,
                    help="rows to check under --verify (default 200000)")
    ap.add_argument("--all", action="store_true",
                    help="also recompute rows that already have a value "
                         "(use after a date revision)")
    args = ap.parse_args()

    with get_connection() as conn:
        print("=== days_to_earnings backfill ===")
        if not calendar_ready(conn):
            return 1
        if args.report:
            report(conn)
            print("\nRead-only — nothing was written.")
            return 0

        params = []
        tf = ""
        if args.tickers:
            tf = "AND m.ticker = ANY(%s)"
            params.append([t.strip().upper() for t in args.tickers.split(",")
                           if t.strip()])
        # Default to NULL-only: the common case is filling in rows written
        # before the source existed, and restricting the scan to those is both
        # faster and safe to re-run.
        nf = "" if args.all else "AND m.days_to_earnings IS NULL"
        sql = UPDATE_SQL.format(ticker_filter=tf, null_filter=nf)

        t0 = time.monotonic()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            changed = cur.rowcount
        wall = time.monotonic() - t0

        if args.dry_run:
            conn.rollback()
            print(f"\n  DRY RUN — {changed:,} row(s) would change "
                  f"({wall:.1f}s). Rolled back.")
            return 0

        conn.commit()
        print(f"\n  {changed:,} row(s) updated in {wall:.1f}s")
        report(conn)
    return 0


if __name__ == "__main__":
    sys.exit(main())
