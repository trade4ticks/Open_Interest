"""
test_equity_retention.py — retention for equity_surface / diagnostics.

The pure half (cutoff, bound parsing, eligibility, the builders' guard) needs
nothing. The database half builds a scratch partitioned table and a scratch
diagnostics table, so it never touches the real ones, and is skipped without
a reachable Postgres, saying so.

Run:  python test_equity_retention.py     (exit 1 on any failure)
"""
from __future__ import annotations

import sys
from datetime import date

from lib import surface_retention as R
from lib.surface_config import RETAINED_SNAPSHOTS, RETENTION_MIN_DAYS

PASS, FAIL, SKIP = [], [], []


def check(name, got, want):
    ok = got == want
    (PASS if ok else FAIL).append(name)
    print(f"  [{'ok  ' if ok else 'FAIL'}] {name:<58} got={got!r} want={want!r}")


def raises(fn) -> bool:
    try:
        fn()
    except ValueError:
        return True
    return False


# ---------------------------------------------------------------------------
print("\n=== 1. cutoff ===")
today = date(2026, 10, 25)
check("30 days back", R.resolve_cutoff(today, days=30), date(2026, 9, 25))
check("--before overrides --days",
      R.resolve_cutoff(today, days=30, before=date(2026, 9, 1)),
      date(2026, 9, 1))
check(f"exactly {RETENTION_MIN_DAYS} days is allowed",
      R.resolve_cutoff(today, days=RETENTION_MIN_DAYS), date(2026, 10, 18))
check("inside the floor is refused, not clamped",
      raises(lambda: R.resolve_cutoff(today, days=RETENTION_MIN_DAYS - 1)),
      True)
check("--days 0 (today) is refused",
      raises(lambda: R.resolve_cutoff(today, days=0)), True)
check("a --before in the future is refused",
      raises(lambda: R.resolve_cutoff(today, before=date(2026, 11, 1))), True)

# ---------------------------------------------------------------------------
print("\n=== 2. partition bounds ===")
check("monthly bound parses",
      R.parse_bound("FOR VALUES FROM ('2026-08-01') TO ('2026-09-01')"),
      (date(2026, 8, 1), date(2026, 9, 1)))
check("DEFAULT partition is ignored", R.parse_bound("DEFAULT"), None)
check("empty bound is ignored", R.parse_bound(None), None)

# ---------------------------------------------------------------------------
print("\n=== 3. a month is eligible only when wholly before the cutoff ===")
oct_end = date(2026, 11, 1)          # exclusive bound of October
check("Oct, on Nov 30 (cutoff Oct 31): not yet",
      R.partition_eligible(oct_end, R.resolve_cutoff(date(2026, 11, 30), 30)),
      False)
check("Oct, on Dec 1 (cutoff Nov 1): eligible",
      R.partition_eligible(oct_end, R.resolve_cutoff(date(2026, 12, 1), 30)),
      True)
check("current month is never eligible",
      R.partition_eligible(date(2026, 11, 1), R.resolve_cutoff(today, 30)),
      False)

# ---------------------------------------------------------------------------
print("\n=== 4. the builders' guard ===")
ranges = [(date(2026, 8, 1), date(2026, 9, 1))]
check("intraday bucket in a slimmed month is purged",
      R.is_purged(date(2026, 8, 25), "1015", ranges), True)
for s in RETAINED_SNAPSHOTS:
    check(f"retained slice {s} is never purged",
          R.is_purged(date(2026, 8, 25), s, ranges), False)
check("first day of the next month is not in range",
      R.is_purged(date(2026, 9, 1), "1015", ranges), False)
check("nothing logged -> nothing purged",
      R.is_purged(date(2026, 8, 25), "1015", []), False)

# ---------------------------------------------------------------------------
print("\n=== 5. swap + diagnostics delete on scratch tables (needs Postgres) ===")
PARENT, DIAG = "zz_retention_surface", "zz_retention_diag"
P_AUG, P_SEP = f"{PARENT}_202608", f"{PARENT}_202609"
try:
    from db import get_connection
    conn = get_connection()
except Exception as exc:                                      # noqa: BLE001
    conn = None
    SKIP.append("database tests")
    print(f"  [SKIP] database tests — no reachable Postgres "
          f"({type(exc).__name__})")

if conn is not None:
    def cleanup():
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {PARENT} CASCADE")
            cur.execute(f"DROP TABLE IF EXISTS {P_AUG}_slim")
            cur.execute(f"DROP TABLE IF EXISTS {DIAG}")
            if R._table_exists(conn, R.LOG_TABLE):
                cur.execute(f"DELETE FROM {R.LOG_TABLE} "
                            f"WHERE partition_name LIKE 'zz\\_retention%%'")
        conn.commit()

    try:
        cleanup()
        R.ensure_log_table(conn)
        snaps = ["0945", "1015", "1300", "1545"]
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE {PARENT} (ticker TEXT NOT NULL, "
                f"trade_date DATE NOT NULL, snapshot TEXT NOT NULL, "
                f"dte SMALLINT NOT NULL, iv DOUBLE PRECISION, "
                f"UNIQUE (ticker, trade_date, snapshot, dte)) "
                f"PARTITION BY RANGE (trade_date)")
            cur.execute(f"CREATE INDEX ON {PARENT} "
                        f"(ticker, trade_date, snapshot DESC)")
            for name, a, b in ((P_AUG, "2026-08-01", "2026-09-01"),
                               (P_SEP, "2026-09-01", "2026-10-01")):
                cur.execute(f"CREATE TABLE {name} PARTITION OF {PARENT} "
                            f"FOR VALUES FROM ('{a}') TO ('{b}')")
            for d in ("2026-08-24", "2026-08-25", "2026-09-02"):
                for s in snaps:
                    for dte in (7, 30):
                        cur.execute(f"INSERT INTO {PARENT} VALUES "
                                    f"('ZZ', %s, %s, %s, 0.2)", (d, s, dte))
            cur.execute(f"CREATE TABLE {DIAG} (ticker TEXT, trade_date DATE, "
                        f"snapshot TEXT, expiry DATE)")
            for d in ("2026-08-24", "2026-09-30"):
                for s in snaps:
                    for i in range(5):
                        cur.execute(f"INSERT INTO {DIAG} VALUES "
                                    f"('ZZ', %s, %s, %s)", (d, s, d))
        conn.commit()

        parts = R.list_partitions(conn, PARENT)
        check("both scratch partitions listed", [p[0] for p in parts],
              [P_AUG, P_SEP])
        check("Aug has intraday before the swap",
              R.has_intraday(conn, P_AUG), True)

        res = R.slim_partition(conn, PARENT, P_AUG,
                               date(2026, 8, 1), date(2026, 9, 1))
        with conn.cursor() as cur:
            cur.execute(f"SELECT DISTINCT snapshot FROM {P_AUG} ORDER BY 1")
            kept = [r[0] for r in cur.fetchall()]
            cur.execute(f"SELECT count(*) FROM {P_SEP}")
            sep_rows = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM pg_inherits "
                        "WHERE inhrelid = to_regclass(%s)", (P_AUG,))
            attached = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM pg_indexes WHERE tablename = %s",
                        (P_AUG,))
            n_idx = cur.fetchone()[0]
            cur.execute(f"SELECT method, rows_before, rows_after "
                        f"FROM {R.LOG_TABLE} WHERE partition_name = %s",
                        (P_AUG,))
            logrow = cur.fetchone()
        check("only retained slices survive", kept, list(RETAINED_SNAPSHOTS))
        check("rows before / after", (res["rows_before"], res["rows_after"]),
              (16, 8))
        check("slim table is attached back under the parent", attached, 1)
        check("both indexes carried across", n_idx, 2)
        check("the other month is untouched", sep_rows, 8)
        check("swap is logged", logrow, ("swap", 16, 8))
        check("Aug now reads as slim", R.has_intraday(conn, P_AUG), False)
        check("guard sees the logged range",
              (date(2026, 8, 1), date(2026, 9, 1)) in R.slimmed_ranges(conn),
              True)

        # Leftover *_slim table: refuse and leave the partition attached.
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {P_SEP}_slim (x int)")
        conn.commit()
        try:
            R.slim_partition(conn, PARENT, P_SEP,
                             date(2026, 9, 1), date(2026, 10, 1))
            refused = False
        except RuntimeError:
            refused = True
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {P_SEP}")
            sep_after = cur.fetchone()[0]
            cur.execute(f"DROP TABLE {P_SEP}_slim")
        conn.commit()
        check("a pre-existing _slim table is refused", refused, True)
        check("refusal leaves the partition whole", sep_after, 8)

        plan = R.diagnostics_plan(conn, date(2026, 9, 25), table=DIAG)
        check("diagnostics plan: intraday rows before the cutoff only",
              plan, [(date(2026, 8, 24), 10)])
        n = R.delete_diagnostics_day(conn, date(2026, 8, 24), table=DIAG,
                                     batch=3)
        with conn.cursor() as cur:
            cur.execute(f"SELECT trade_date, snapshot, count(*) FROM {DIAG} "
                        f"GROUP BY 1, 2 ORDER BY 1, 2")
            left = [(str(a), b, c) for a, b, c in cur.fetchall()]
        check("batched delete removes every intraday row (batch < total)",
              n, 10)
        check("kept slices and the in-window day are untouched", left, [
            ("2026-08-24", "0945", 5), ("2026-08-24", "1545", 5),
            ("2026-09-30", "0945", 5), ("2026-09-30", "1015", 5),
            ("2026-09-30", "1300", 5), ("2026-09-30", "1545", 5)])
    except Exception as exc:                                  # noqa: BLE001
        FAIL.append(f"database tests raised {type(exc).__name__}")
        print(f"  [FAIL] database tests raised {type(exc).__name__}: {exc}")
    finally:
        try:
            cleanup()
        finally:
            conn.close()

# ---------------------------------------------------------------------------
print(f"\n{len(PASS)} passed, {len(FAIL)} failed, {len(SKIP)} skipped")
for f in FAIL:
    print(f"  FAIL: {f}")
sys.exit(1 if FAIL else 0)
