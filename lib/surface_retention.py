"""
Retention for equity_surface and equity_surface_diagnostics.

Driven by prune_equity_surface.py; the guard half (slimmed_ranges / is_purged)
is also read by build_equity_metrics.py and build_equity_surface.py.

--- Two tables, two mechanisms ------------------------------------------------

equity_surface is monthly-partitioned, and a DELETE there would return no
space at all: autovacuum only makes freed space reusable inside the same
partition's heap, and every new row lands in the CURRENT month's partition.
A purged month would sit at 9-11 GB forever. So a month is slimmed by
rewriting it — copy the RETAINED_SNAPSHOTS rows into a fresh table, then
detach / drop / rename / attach in one transaction — once every day in it is
older than the cutoff. The space returns the moment that commits.

equity_surface_diagnostics is NOT partitioned, so new inserts land in the
same heap the DELETE frees. A plain DELETE plus VACUUM keeps it at a steady
size, and it is pruned daily at exactly the cutoff.

--- Ordering is load-bearing --------------------------------------------------

build_equity_metrics picks its targets from diagnostics. Wherever the surface
has lost its intraday rows, diagnostics must already have lost theirs, or a
rebuild targets a snapshot whose surface is gone and overwrites good metrics
with an ATM-only row. Diagnostics are therefore pruned first, at a cutoff that
is always at or after the surface's, and the surface step does not run if the
diagnostics step failed. equity_retention_log backs that up with an explicit
guard the builders check.

--- Why this is safe beside the live pipeline ---------------------------------

The live pipeline writes only today. A partition is eligible only when its
whole range ends at or before the cutoff, and the cutoff is never later than
today - RETENTION_MIN_DAYS. The swap's one ACCESS EXCLUSIVE lock on the parent
is taken for the detach/attach at the end, after the copy, under a
lock_timeout, at 03:30 ET when nothing else is writing.
"""
from __future__ import annotations

import re
from datetime import date, timedelta

from lib.surface_config import RETAINED_SNAPSHOTS, RETENTION_MIN_DAYS

SURFACE_TABLE = "equity_surface"
DIAG_TABLE = "equity_surface_diagnostics"
LOG_TABLE = "equity_retention_log"

DIAG_BATCH_ROWS = 50_000
# Applied only to the detach/attach at the end of a swap. Waiting longer than
# this for the parent means something unexpected is reading or writing; give
# up, roll back the copy, and let the next night retry.
SWAP_LOCK_TIMEOUT = "60s"

_BOUND_RE = re.compile(
    r"FOR VALUES FROM \('(\d{4}-\d{2}-\d{2})'\) TO \('(\d{4}-\d{2}-\d{2})'\)")


# =============================================================================
# Pure helpers
# =============================================================================
def resolve_cutoff(today: date, days: int | None = None,
                   before: date | None = None) -> date:
    """Rows with trade_date < cutoff are past retention.

    Refuses rather than clamps: a cutoff inside RETENTION_MIN_DAYS is a typo,
    and silently running a different purge than the one asked for is worse
    than not running.
    """
    cutoff = before if before is not None else today - timedelta(days=days)
    latest = today - timedelta(days=RETENTION_MIN_DAYS)
    if cutoff > latest:
        raise ValueError(
            f"cutoff {cutoff} is inside the last {RETENTION_MIN_DAYS} days "
            f"(latest allowed {latest}). Refusing.")
    return cutoff


def parse_bound(expr: str) -> tuple[date, date] | None:
    """(start, end) from pg_get_expr(relpartbound); end is exclusive."""
    m = _BOUND_RE.search(expr or "")
    if not m:
        return None
    return date.fromisoformat(m.group(1)), date.fromisoformat(m.group(2))


def partition_eligible(range_end: date, cutoff: date) -> bool:
    """Every day in [start, range_end) is older than the cutoff."""
    return range_end <= cutoff


def is_purged(trade_date, snapshot: str, ranges: list) -> bool:
    """True when this (trade_date, snapshot) lives in a slimmed partition and
    is not one of the slices that survive slimming."""
    if snapshot in RETAINED_SNAPSHOTS:
        return False
    return any(s <= trade_date < e for s, e in ranges)


def _keep() -> list:
    return list(RETAINED_SNAPSHOTS)


# =============================================================================
# Reads
# =============================================================================
def _table_exists(conn, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", (name,))
        return bool(cur.fetchone()[0])


def slimmed_ranges(conn) -> list:
    """[(start, end_exclusive)] of every logged partition. Empty when the log
    table does not exist yet — nothing has been slimmed by this tool."""
    if not _table_exists(conn, LOG_TABLE):
        return []
    with conn.cursor() as cur:
        cur.execute(f"SELECT range_start, range_end FROM {LOG_TABLE} "
                    f"ORDER BY range_start")
        return [(r[0], r[1]) for r in cur.fetchall()]


def logged_partitions(conn) -> set:
    if not _table_exists(conn, LOG_TABLE):
        return set()
    with conn.cursor() as cur:
        cur.execute(f"SELECT partition_name FROM {LOG_TABLE}")
        return {r[0] for r in cur.fetchall()}


def list_partitions(conn, parent: str = SURFACE_TABLE) -> list:
    """[(name, start, end_exclusive, total_bytes)], oldest first."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.relname, pg_get_expr(c.relpartbound, c.oid), "
            "       pg_total_relation_size(c.oid) "
            "FROM pg_inherits i "
            "JOIN pg_class c ON c.oid = i.inhrelid "
            "WHERE i.inhparent = to_regclass(%s)", (parent,))
        out = []
        for name, expr, size in cur.fetchall():
            b = parse_bound(expr)
            if b is None:          # DEFAULT partition, or a bound we don't own
                continue
            out.append((name, b[0], b[1], int(size)))
    return sorted(out, key=lambda r: r[1])


def has_intraday(conn, partition: str) -> bool:
    """Any row outside RETAINED_SNAPSHOTS. Returns at the first hit on a full
    partition; on a slim one it is a full scan, which is why the answer is
    logged and never asked twice."""
    with conn.cursor() as cur:
        cur.execute(f'SELECT EXISTS (SELECT 1 FROM "{partition}" '
                    f'WHERE snapshot <> ALL(%s))', (_keep(),))
        return bool(cur.fetchone()[0])


def diagnostics_plan(conn, cutoff: date,
                     table: str = DIAG_TABLE) -> list:
    """[(trade_date, rows)] of intraday diagnostics older than the cutoff."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT trade_date, count(*) FROM {table} "
            f"WHERE trade_date < %s AND snapshot <> ALL(%s) "
            f"GROUP BY trade_date ORDER BY trade_date", (cutoff, _keep()))
        return [(r[0], int(r[1])) for r in cur.fetchall()]


def relation_bytes(conn, name: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_total_relation_size(to_regclass(%s))", (name,))
        v = cur.fetchone()[0]
    return int(v or 0)


# =============================================================================
# Writes
# =============================================================================
def ensure_log_table(conn) -> None:
    from pathlib import Path
    sql = (Path(__file__).resolve().parent.parent / "sql"
           / "16_equity_retention.sql").read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def delete_diagnostics_day(conn, day: date, table: str = DIAG_TABLE,
                           batch: int = DIAG_BATCH_ROWS) -> int:
    """Delete one trade_date's intraday diagnostics, `batch` rows per commit.

    Batched not for locks — the live job only upserts today's rows, so the two
    never contend for a row — but to keep each transaction short, so the WAL
    arrives in pieces and the xmin horizon is not held back for the whole
    purge.
    """
    total = 0
    while True:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {table} WHERE ctid = ANY(ARRAY("
                f"  SELECT ctid FROM {table} "
                f"  WHERE trade_date = %s AND snapshot <> ALL(%s) LIMIT %s))",
                (day, _keep(), batch))
            n = cur.rowcount
        conn.commit()
        total += n
        if n < batch:
            return total


def slim_partition(conn, parent: str, partition: str, start: date,
                   end: date) -> dict:
    """Rewrite one monthly partition down to RETAINED_SNAPSHOTS.

    One transaction, so a failure anywhere leaves the original attached and
    untouched:

      1. SHARE-lock the partition: blocks a stray backfill into this month,
         nothing else. The parent is not locked yet.
      2. Copy the kept rows into a table built LIKE the partition, INCLUDING
         ALL — its indexes exist before the swap, so ATTACH adopts them
         rather than building under the parent's lock.
      3. Verify the copy against a count of the source.
      4. A CHECK matching the bound, so ATTACH skips its validation scan.
      5. detach, drop, rename, attach. The only parent lock, held for
         milliseconds, under SWAP_LOCK_TIMEOUT.
      6. Log it, in the same transaction.
    """
    slim = f"{partition}_slim"
    keep = _keep()
    bytes_before = relation_bytes(conn, partition)
    try:
        with conn.cursor() as cur:
            # A leftover copy from a run that died before COMMIT cannot exist
            # — the CREATE rolled back with it — but a hand-made one could.
            # Refuse rather than drop something this code did not create.
            cur.execute("SELECT to_regclass(%s) IS NOT NULL", (slim,))
            if cur.fetchone()[0]:
                raise RuntimeError(f"{slim} already exists; inspect and drop "
                                   f"it by hand before re-running")

            cur.execute(f'LOCK TABLE "{partition}" IN SHARE MODE')
            cur.execute(f'CREATE TABLE "{slim}" (LIKE "{partition}" '
                        f'INCLUDING ALL)')
            cur.execute(f'SELECT count(*), count(*) FILTER '
                        f'(WHERE snapshot = ANY(%s)) FROM "{partition}"',
                        (keep,))
            rows_before, want = (int(x) for x in cur.fetchone())
            cur.execute(f'INSERT INTO "{slim}" SELECT * FROM "{partition}" '
                        f'WHERE snapshot = ANY(%s)', (keep,))
            got = cur.rowcount
            if got != want:
                raise RuntimeError(f"{partition}: copied {got:,} rows, source "
                                   f"holds {want:,} kept rows")

            chk = f"{slim}_bound"
            cur.execute(f'ALTER TABLE "{slim}" ADD CONSTRAINT "{chk}" '
                        f'CHECK (trade_date >= %s AND trade_date < %s)',
                        (start, end))

            cur.execute(f"SET LOCAL lock_timeout = '{SWAP_LOCK_TIMEOUT}'")
            cur.execute(f'ALTER TABLE "{parent}" DETACH PARTITION '
                        f'"{partition}"')
            cur.execute(f'DROP TABLE "{partition}"')
            cur.execute(f'ALTER TABLE "{slim}" RENAME TO "{partition}"')
            cur.execute(f'ALTER TABLE "{parent}" ATTACH PARTITION '
                        f'"{partition}" FOR VALUES FROM (%s) TO (%s)',
                        (start, end))
            # Redundant with the partition bound once attached.
            cur.execute(f'ALTER TABLE "{partition}" DROP CONSTRAINT "{chk}"')

            cur.execute("SELECT pg_total_relation_size(to_regclass(%s))",
                        (partition,))
            bytes_after = int(cur.fetchone()[0] or 0)
            _log(cur, partition, start, end, "swap",
                 rows_before, got, bytes_before, bytes_after)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"rows_before": rows_before, "rows_after": got,
            "bytes_before": bytes_before, "bytes_after": bytes_after}


def record_already_slim(conn, partition: str, start: date, end: date) -> None:
    b = relation_bytes(conn, partition)
    with conn.cursor() as cur:
        _log(cur, partition, start, end, "already_slim", None, None, b, b)
    conn.commit()


def _log(cur, partition, start, end, method, rows_before, rows_after,
         bytes_before, bytes_after) -> None:
    cur.execute(
        f"INSERT INTO {LOG_TABLE} (partition_name, range_start, range_end, "
        f"  method, rows_before, rows_after, bytes_before, bytes_after) "
        f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
        f"ON CONFLICT (partition_name) DO UPDATE SET "
        f"  range_start = EXCLUDED.range_start, "
        f"  range_end = EXCLUDED.range_end, method = EXCLUDED.method, "
        f"  slimmed_at = now(), rows_before = EXCLUDED.rows_before, "
        f"  rows_after = EXCLUDED.rows_after, "
        f"  bytes_before = EXCLUDED.bytes_before, "
        f"  bytes_after = EXCLUDED.bytes_after",
        (partition, start, end, method, rows_before, rows_after,
         bytes_before, bytes_after))


def maintenance_connection():
    """A fresh autocommit connection for VACUUM, never the caller's.

    VACUUM cannot run inside a transaction block, and the caller's connection
    cannot be switched out of one: prune_equity_surface.main holds it in
    `with get_connection() as conn`, and since psycopg2 2.9 that block is a
    transaction even with autocommit on. Flipping autocommit on the caller's
    connection is what failed in the first --delete run, and its finally-clause
    restore then failed too, masking the original error. A separate
    connection, autocommit before its first statement and never entered with
    `with`, cannot inherit a transaction.

    max_parallel_maintenance_workers = 0: Postgres runs in Docker with the
    default 64 MB /dev/shm, and a parallel index vacuum on the 2 GB
    diagnostics table failed with "could not resize shared memory segment".
    Serial is slower but needs no shared memory, so the nightly run does not
    depend on the container's shm_size.
    """
    from db import get_connection
    conn = get_connection()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SET max_parallel_maintenance_workers = 0")
    return conn


def vacuum(table: str, full: bool = False) -> None:
    conn = maintenance_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"VACUUM ({'FULL, ' if full else ''}ANALYZE) {table}")
    finally:
        conn.close()
