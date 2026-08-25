"""
fetch_earnings_calendar.py — nightly earnings calendar refresh from yfinance.

One request per ticker. get_earnings_dates(limit=24) returns roughly 2020 to
the next scheduled date in that single call, so history and forward arrive
together and there is no separate backfill path — the first run IS the
backfill.

--- Why nightly ------------------------------------------------------------

Yahoo publishes only the NEXT confirmed date. A missed week means the date
passes with nothing behind it and days_to_earnings goes quietly NULL rather
than wrong, which is the failure mode the earnings_coverage_alert view exists
to surface.

--- Rate limiting ----------------------------------------------------------

Yahoo shut its documented public API down in 2017 and never replaced it;
yfinance uses reverse-engineered endpoints with NO published limit. What is
documented is the failure: heavy or parallel requests return 429 or empty
responses, and the block is per-IP and temporary.

So the pacing here is deliberately conservative rather than tuned to a number
that does not exist: serial requests, a fixed delay between them, and
exponential backoff on 429. At ~120 tickers this is minutes, not seconds, and
nothing downstream is waiting on it.

Usage:
    python fetch_earnings_calendar.py                  # all tickers
    python fetch_earnings_calendar.py --tickers AAPL,JNJ
    python fetch_earnings_calendar.py --delay 2.0      # slower, if 429s appear
    python fetch_earnings_calendar.py --init-db

Cron (nightly, well clear of the session):
    TZ=America/New_York 30 20 * * 1-5 \\
        /Open_Interest/.venv/bin/python /Open_Interest/fetch_earnings_calendar.py \\
        >> /Open_Interest/logs/earnings.log 2>&1
"""
from __future__ import annotations

import argparse
import logging
import random
import sys
import time

from db import get_connection
from lib.chain_fetch_common import log_path, setup_file_logging
from lib.earnings_store import (
    DEFAULT_LIMIT, STATUS_ERROR, STATUS_NONE, STATUS_OK,
    fetch_ticker, init_db, upsert_calendar, upsert_coverage,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("earnings")

# Serial, with a gap. Measured latency is 0.5-1.3s per call, so a 1.5s delay
# puts the effective rate near 25/minute — far below anything observed to
# trigger a block, and the whole universe still finishes inside five minutes.
DEFAULT_DELAY = 1.5
RETRY_ATTEMPTS = 4
RETRY_BASE = 5.0


def _is_rate_limited(err: str) -> bool:
    e = (err or "").lower()
    return "429" in e or "too many requests" in e or "rate limit" in e


def fetch_with_backoff(ticker: str, limit: int) -> tuple:
    """fetch_ticker plus exponential backoff on a rate-limit response.

    Only 429 is retried. An empty frame is a fund and a parse error is a bug;
    neither improves by waiting, and retrying them would multiply the request
    count against the one thing Yahoo actually penalises.
    """
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        rows, status, err = fetch_ticker(ticker, limit=limit)
        if status != STATUS_ERROR or not _is_rate_limited(err):
            return rows, status, err
        if attempt == RETRY_ATTEMPTS:
            return rows, status, err
        # Full jitter: a block is per-IP, so every ticker in the run is behind
        # the same wall and retrying them on the same schedule would rebuild
        # the burst that caused it.
        delay = random.uniform(0, RETRY_BASE * (2 ** (attempt - 1)))
        log.warning("  %s: rate limited, backing off %.1fs (attempt %d/%d)",
                    ticker, delay, attempt, RETRY_ATTEMPTS)
        time.sleep(delay)
    return [], STATUS_ERROR, "rate limited"


def universe() -> list:
    from lib.chain_snapshot_store import list_tickers as snap_tickers
    from lib.parquet_store import list_tickers as oi_tickers
    tk = sorted(set(snap_tickers()) | set(oi_tickers()))
    if not tk:
        raise SystemExit("no tickers found in the snapshots or OI store")
    return tk


def main() -> int:
    ap = argparse.ArgumentParser(description="Refresh the earnings calendar.")
    ap.add_argument("--tickers", help="comma-separated; default = all")
    ap.add_argument("--delay", type=float, default=DEFAULT_DELAY,
                    help=f"seconds between requests (default {DEFAULT_DELAY})")
    ap.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                    help=f"earnings periods per ticker (default {DEFAULT_LIMIT})")
    ap.add_argument("--init-db", action="store_true",
                    help="apply sql/10_earnings_calendar.sql and exit")
    args = ap.parse_args()

    log_file = setup_file_logging("fetch_earnings_calendar")
    print(f"=== earnings calendar refresh ===\nLog: {log_file}")

    with get_connection() as conn:
        if args.init_db:
            init_db(conn)
            print("earnings_calendar / earnings_coverage ready.")
            return 0

        tickers = ([t.strip().upper() for t in args.tickers.split(",")
                    if t.strip()] if args.tickers else universe())
        print(f"{len(tickers)} ticker(s), {args.delay:.1f}s between requests "
              f"(~{len(tickers) * args.delay / 60:.1f} min minimum)\n")

        t0 = time.monotonic()
        n_rows = n_ok = n_none = 0
        failed = []
        for i, tk in enumerate(tickers, 1):
            rows, status, err = fetch_with_backoff(tk, args.limit)
            try:
                n_rows += upsert_calendar(conn, rows)
                upsert_coverage(conn, tk, status, rows, err)
                conn.commit()
            except Exception as exc:                          # noqa: BLE001
                conn.rollback()
                log.error("  %s: WRITE FAILED — %s: %s", tk,
                          type(exc).__name__, exc)
                failed.append(tk)
                continue

            if status == STATUS_OK:
                n_ok += 1
            elif status == STATUS_NONE:
                # Not a failure. This is what an ETF returns, and treating it
                # as one would flag 28 tickers as broken every night until the
                # alert was ignored.
                n_none += 1
                log.info("  %s: no earnings (fund) — recorded, not failed", tk)
            else:
                failed.append(tk)
                log.error("  %s: %s", tk, err)

            if i % 20 == 0 or i == len(tickers):
                log.info("  progress: %d/%d", i, len(tickers))
            if i < len(tickers):
                time.sleep(args.delay)

        wall = time.monotonic() - t0

    print(f"\n{len(tickers)} ticker(s) in {wall:.0f}s "
          f"({wall / max(len(tickers), 1):.2f}s each)")
    print(f"  with earnings   {n_ok:>4}")
    print(f"  funds (no earn) {n_none:>4}")
    print(f"  FAILED          {len(failed):>4}"
          + (f"  — {', '.join(failed[:10])}" if failed else ""))
    print(f"  calendar rows   {n_rows:>4} upserted")
    print(f"\nLog: {log_path()}")
    print("Check earnings_coverage_alert for tickers needing attention.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
