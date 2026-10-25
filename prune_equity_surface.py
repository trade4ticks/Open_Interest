"""
prune_equity_surface.py — retention for equity_surface and its diagnostics.
DRY-RUN BY DEFAULT; nothing is changed without --delete.

    python prune_equity_surface.py                    # report only
    python prune_equity_surface.py --delete           # prune
    python prune_equity_surface.py --days 45          # a different window
    python prune_equity_surface.py --before 2026-09-25
    python prune_equity_surface.py --vacuum-full-diagnostics --delete

WHAT IS KEPT. The full 5-minute surface for SURFACE_RETENTION_DAYS (30).
Older than that, only RETAINED_SNAPSHOTS ('0945', '1545') — the twice-daily
slices the store held before the live capture began on 2026-08-24.

WHAT IT DOES, IN THIS ORDER — see lib/surface_retention.py for why:

  1. equity_surface_diagnostics: DELETE intraday rows older than the cutoff,
     one trade_date at a time, 50k rows per commit.
  2. equity_surface: every monthly partition whose WHOLE range is older than
     the cutoff is rewritten down to the kept slices (copy, then detach /
     drop / rename / attach in one transaction) and logged to
     equity_retention_log. A partition already free of intraday rows is
     logged as 'already_slim' without being rewritten. Skipped entirely if
     step 1 failed.
  3. VACUUM (ANALYZE) diagnostics, so the space step 1 freed is reused by
     tomorrow's inserts instead of extending the file.

Monthly, so a month's intraday is kept until the 1st of the month after it
turns 30 days old: 30-61 days, and the surface's space comes back once a
month. A DELETE would return none of it — see lib/surface_retention.py.

WHAT IT DOES NOT TOUCH. equity_atm, equity_metrics, equity_metrics_z: the
dashboard's intraday /series reads them at every bucket over its whole history
window. Nothing computed is lost: equity_metrics rows for a slimmed month stay
exactly as they were.

AFTER A PRUNE, build_equity_metrics.py batch skips purged (date, snapshot)
pairs, and build_equity_surface.py refuses --source intraday into a slimmed
month. Both read equity_retention_log.

--vacuum-full-diagnostics is a ONE-TIME shrink after the first purge. It takes
an ACCESS EXCLUSIVE lock that blocks the live pipeline's writes, so it refuses
to run on a weekday between 09:00 and 16:30 ET. Never from cron.

Cron (03:30 ET Tue-Sat, after each weekday session):

    30 3 * * 2-6 flock -n /tmp/equity_retention.lock \\
        /Open_Interest/.venv/bin/python /Open_Interest/prune_equity_surface.py \\
        --delete >> /Open_Interest/logs/equity_retention.log 2>&1
"""
from __future__ import annotations

import argparse
import shutil
import sys
import time
from datetime import date, datetime, time as dtime

from lib.market_hours import ET
from lib.surface_config import (
    RETAINED_SNAPSHOTS, RETENTION_MIN_DAYS, SURFACE_RETENTION_DAYS,
)
from lib import surface_retention as R


def _gb(b: int | None) -> str:
    return "?" if b is None else f"{b / 1024**3:.2f} GB"


def _root_free() -> str:
    # The root filesystem is the one that filled. Postgres runs in Docker, so
    # the server's data_directory is a container path and says nothing here.
    try:
        return _gb(shutil.disk_usage("/").free)
    except OSError:
        return "?"


def _in_session(now: datetime) -> bool:
    return now.weekday() < 5 and dtime(9, 0) <= now.time() <= dtime(16, 30)


def vacuum_full_diagnostics(conn, apply: bool) -> int:
    now = datetime.now(ET)
    size = R.relation_bytes(conn, R.DIAG_TABLE)
    print(f"\n{R.DIAG_TABLE}: {_gb(size)} on disk")
    print("VACUUM FULL rewrites it to its live size. Needs free space about")
    print("equal to that live size, and blocks every write until it finishes.")
    if not apply:
        print("\nDRY RUN — re-run with --delete to rewrite it.")
        return 0
    if _in_session(now):
        print(f"\nRefusing: {now:%a %H:%M} ET is inside 09:00-16:30 on a "
              f"weekday, and this would stall run_live_pipeline.py.")
        return 2
    # End this connection's transaction first. relation_bytes above took an
    # ACCESS SHARE lock on the table, held until commit, and VACUUM FULL —
    # on its own connection — needs ACCESS EXCLUSIVE. Left open, the two
    # wait on each other forever; Postgres sees no deadlock across sessions.
    conn.commit()
    t0 = time.monotonic()
    R.vacuum(R.DIAG_TABLE, full=True)
    after = R.relation_bytes(conn, R.DIAG_TABLE)
    print(f"\nrewritten in {time.monotonic() - t0:.0f}s: {_gb(size)} -> "
          f"{_gb(after)}")
    print(f"free on /  : {_root_free()}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--days", type=int, default=SURFACE_RETENTION_DAYS,
                    help=f"days of full intraday to keep "
                         f"(default {SURFACE_RETENTION_DAYS})")
    ap.add_argument("--before", default=None,
                    help="absolute cutoff YYYY-MM-DD; overrides --days")
    ap.add_argument("--delete", action="store_true",
                    help="actually prune. Without this, nothing is changed.")
    ap.add_argument("--vacuum-full-diagnostics", action="store_true",
                    help="one-time shrink of diagnostics after the first "
                         "purge; refuses during the session")
    args = ap.parse_args()

    from db import get_connection

    today = datetime.now(ET).date()
    try:
        cutoff = R.resolve_cutoff(
            today, days=args.days,
            before=date.fromisoformat(args.before) if args.before else None)
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 2

    with get_connection() as conn:
        if args.vacuum_full_diagnostics:
            return vacuum_full_diagnostics(conn, args.delete)

        print()
        stamp = datetime.now(ET).strftime("%Y-%m-%d %H:%M")
        print(f"=== equity_surface retention — {stamp} ET "
              f"{'(DELETE)' if args.delete else '(dry run)'} ===")
        print(f"kept slices    : {', '.join(RETAINED_SNAPSHOTS)}")
        print(f"cutoff         : full intraday kept from {cutoff} "
              f"(floor: {RETENTION_MIN_DAYS} days)")
        print(f"free on /      : {_root_free()}")

        # --- plan ----------------------------------------------------------
        diag = R.diagnostics_plan(conn, cutoff)
        diag_rows = sum(n for _, n in diag)
        print()
        print(f"[1] {R.DIAG_TABLE}  ({_gb(R.relation_bytes(conn, R.DIAG_TABLE))})")
        if diag:
            print(f"    would delete {diag_rows:,} intraday row(s) across "
                  f"{len(diag)} session(s), {diag[0][0]} .. {diag[-1][0]}")
        else:
            print("    nothing older than the cutoff")

        logged = R.logged_partitions(conn)
        parts = R.list_partitions(conn)
        eligible = [p for p in parts
                    if R.partition_eligible(p[2], cutoff) and p[0] not in logged]
        print()
        print(f"[2] {R.SURFACE_TABLE}  ({len(parts)} partition(s), "
              f"{_gb(sum(p[3] for p in parts))})")
        for name, start, end, size in parts:
            if name in logged:
                state = "slim (logged)"
            elif R.partition_eligible(end, cutoff):
                state = "ELIGIBLE"
            else:
                state = f"kept full until cutoff passes {end}"
            print(f"    {name:<24} {start} .. {end}  {_gb(size):>10}  {state}")
        if not eligible:
            print("    no unlogged partition lies wholly before the cutoff")

        if not args.delete:
            print()
            print("DRY RUN — nothing changed. Re-run with --delete.")
            if eligible:
                print("ELIGIBLE partitions are scanned for intraday rows first;")
                print("one already slim is logged, not rewritten. A rewrite")
                print("needs free space about equal to the twice-daily rows it")
                print("keeps (~350 MB a month).")
            return 0

        # --- apply ---------------------------------------------------------
        R.ensure_log_table(conn)

        print()
        print("[1] diagnostics")
        t0 = time.monotonic()
        deleted = 0
        try:
            for day, _ in diag:
                n = R.delete_diagnostics_day(conn, day)
                deleted += n
                print(f"    {day}  {n:>10,} row(s)")
        except Exception as exc:                              # noqa: BLE001
            conn.rollback()
            print(f"    FAILED after {deleted:,} row(s) — "
                  f"{type(exc).__name__}: {exc}")
            print("    Surface step SKIPPED: diagnostics must lead it, or a")
            print("    metrics rebuild could target a purged snapshot.")
            return 1
        print(f"    {deleted:,} row(s) in {time.monotonic() - t0:.0f}s")

        print()
        print("[2] surface")
        rc = 0
        freed = 0
        for name, start, end, size in eligible:
            t0 = time.monotonic()
            try:
                if not R.has_intraday(conn, name):
                    R.record_already_slim(conn, name, start, end)
                    print(f"    {name}  already slim — logged")
                    continue
                res = R.slim_partition(conn, R.SURFACE_TABLE, name, start, end)
            except Exception as exc:                          # noqa: BLE001
                conn.rollback()
                rc = 1
                print(f"    {name}  FAILED, left as it was — "
                      f"{type(exc).__name__}: {exc}")
                continue
            freed += res["bytes_before"] - res["bytes_after"]
            print(f"    {name}  {res['rows_before']:,} -> "
                  f"{res['rows_after']:,} rows, {_gb(res['bytes_before'])} -> "
                  f"{_gb(res['bytes_after'])}  "
                  f"({time.monotonic() - t0:.0f}s)")
        if not eligible:
            print("    nothing to do")

        if deleted:
            print()
            print("[3] VACUUM (ANALYZE) diagnostics")
            conn.commit()          # hold no locks while VACUUM runs elsewhere
            t0 = time.monotonic()
            R.vacuum(R.DIAG_TABLE)
            print(f"    done in {time.monotonic() - t0:.0f}s")

        print()
        print(f"surface freed : {_gb(freed)}")
        print(f"free on /     : {_root_free()}")
        return rc


if __name__ == "__main__":
    sys.exit(main())
