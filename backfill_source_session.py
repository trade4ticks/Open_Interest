"""
backfill_source_session.py — One-time UPDATE to populate source_session
on every existing row of option_volume_daily and option_iv_daily.

source_session = previous_trading_day(trade_date), the inverse of the relabel
fetch_volume_eod.py / fetch_iv_chain.py apply at write time
(trade_date = next_trading_day(fetch_date)).

After this runs once, every pre-existing row has source_session set. New rows
written by the fetchers from now on populate source_session as part of the
upsert. Safe to re-run: only updates rows where source_session IS NULL.

Usage:
    python backfill_source_session.py
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas_market_calendars as mcal
import psycopg2.extras

from db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def _build_nyse_index(start: date, end: date) -> list[date]:
    """Sorted-asc list of NYSE trading days between start and end inclusive.
    Built directly via pandas_market_calendars (same source lib/market_hours.py
    uses)."""
    sched = mcal.get_calendar("NYSE").schedule(start_date=start, end_date=end)
    return [d.date() for d in sched.index]


def backfill_table(conn, table: str) -> int:
    """For every row with source_session IS NULL, set source_session to the
    trading day immediately before trade_date. Returns rows updated."""
    log.info("Scanning %s for rows where source_session IS NULL ...", table)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT MIN(trade_date), MAX(trade_date), COUNT(*) "
            f"FROM {table} WHERE source_session IS NULL"
        )
        min_td, max_td, n_null = cur.fetchone()

    if not n_null:
        log.info("  %s: 0 rows need backfill — done.", table)
        return 0

    # 30-day cushion before the earliest trade_date so the "previous trading
    # day" lookup never falls off the edge (worst case is a Mon after a
    # long-holiday weekend; 30 days is well beyond any NYSE gap).
    cal_start = min_td - timedelta(days=30)
    nyse_days = _build_nyse_index(cal_start, max_td)
    day_to_idx = {d: i for i, d in enumerate(nyse_days)}

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT DISTINCT trade_date FROM {table} WHERE source_session IS NULL"
        )
        distinct_tds = sorted(r[0] for r in cur.fetchall())

    log.info("  %s: %d null row(s) across %d distinct trade_date(s) (%s → %s)",
             table, n_null, len(distinct_tds), min_td, max_td)

    mapping: list[tuple[date, date]] = []   # (prev_session, trade_date) per UPDATE row
    unresolved: list[date] = []
    for td in distinct_tds:
        idx = day_to_idx.get(td)
        if idx is None:
            # trade_date isn't itself a NYSE trading day — shouldn't happen
            # given the relabel always lands on a trading day, but fall back
            # gracefully to the most recent trading day strictly before td.
            prev = next((d for d in reversed(nyse_days) if d < td), None)
        elif idx == 0:
            # td is the very first trading day in our index → no prior day
            # available even with the 30-day cushion. Should be impossible.
            prev = None
        else:
            prev = nyse_days[idx - 1]

        if prev is None:
            unresolved.append(td)
        else:
            mapping.append((prev, td))

    if unresolved:
        log.warning("  %s: %d trade_date(s) had no resolvable previous trading day: %s",
                    table, len(unresolved), unresolved[:5])

    if not mapping:
        log.info("  %s: nothing to apply after resolution.", table)
        return 0

    log.info("  %s: applying %d (trade_date → source_session) mappings ...",
             table, len(mapping))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            f"UPDATE {table} AS t "
            f"SET source_session = v.prev "
            f"FROM (VALUES %s) AS v(prev, td) "
            f"WHERE t.trade_date = v.td "
            f"  AND t.source_session IS NULL",
            mapping,
            template="(%s::date, %s::date)",
            page_size=500,
        )
        updated = cur.rowcount
    conn.commit()
    log.info("  %s: updated %d row(s).", table, updated)
    return updated


def main() -> None:
    print("=== backfill source_session on vol/IV tables ===\n")
    with get_connection() as conn:
        v_updated = backfill_table(conn, "option_volume_daily")
        i_updated = backfill_table(conn, "option_iv_daily")
    print(f"\nDone.")
    print(f"  option_volume_daily : {v_updated} row(s) updated")
    print(f"  option_iv_daily     : {i_updated} row(s) updated")


if __name__ == "__main__":
    main()
