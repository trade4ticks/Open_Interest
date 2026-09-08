"""The intraday column set must reach the table, and a failing run must stop.

    python -m scalp.tests.test_schema_sync      (exit 1 on any failure)

Both halves reproduce a real incident. A recompute ran five workers at 100%
CPU for 32 minutes and wrote one row: the quiet-window columns were in
config.INTRADAY_COLUMNS and not in the table, because CREATE TABLE IF NOT
EXISTS does nothing to a table that already exists. Every symbol-day computed
correctly and then failed in the INSERT, and because the exception escaped
while all 10,656 units were already submitted, the pool's __exit__ sat in
shutdown(wait=True) draining work it was about to discard.

No database and no pool are needed to test either part: the migration SQL is a
pure function of the config, and the abort path is a plain exception. Both are
checked against the config rather than against a hardcoded list, so adding a
metric cannot pass these tests while failing in production.
"""
import sys

from scalp import compute, config, db, quiet

PASS, FAIL = [], []


def check(name, got, want):
    ok = got == want
    (PASS if ok else FAIL).append(name)
    print(f"  [{'ok  ' if ok else 'FAIL'}] {name:<58} got={got!r} want={want!r}")


print("=== 1. every configured column is reconciled onto the table ===")
sql = db.intraday_migration_sql()
names = [n for n, _ in config.INTRADAY_COLUMNS]

missing_parent = [n for n in names
                  if f"ALTER TABLE intraday_metrics ADD COLUMN IF NOT EXISTS {n} "
                  not in sql]
check("every column is ALTERed onto intraday_metrics", missing_parent, [])

missing_monthly = [n for n in names
                   if f"ALTER TABLE intraday_monthly ADD COLUMN IF NOT EXISTS {n} "
                   not in sql]
check("...and onto intraday_monthly", missing_monthly, [])

check("one statement per column per table",
      sql.count("ADD COLUMN IF NOT EXISTS"), len(names) * 2)
check("every statement is IF NOT EXISTS, so re-running is a no-op",
      sql.count("ADD COLUMN ") == sql.count("ADD COLUMN IF NOT EXISTS"), True)

# The specific columns whose absence caused the incident.
for col in (config.QUIET_PRIMARY_COLUMN, config.QUIET_PRIMARY_ELIGIBLE_COLUMN):
    check(f"{col} is in the config set", col in names, True)
    check(f"...and is reconciled onto the table", f" {col} " in sql, True)

# The pinned noise column moved from _rms to _p75; the NEW one must be added.
check("the current noise pin is reconciled",
      f" {config.INTRADAY_NOISE_COLUMN} " in sql, True)

# Nothing here may drop or retype: a renamed metric leaves its history behind
# and intraday_monthly keeps that indefinitely.
check("the migration never drops a column", "DROP COLUMN" in sql, False)
check("...and never retypes one", "ALTER COLUMN" in sql, False)


print("\n=== 2. the DDL and the migration agree on the column set ===")
# Two generators over one config. If they disagree, a fresh database and a
# migrated one end up with different tables, which is worse than either bug.
ddl = db.intraday_ddl()
in_ddl = [n for n in names if f"    {n}" in ddl or f"\n    {n} " in ddl]
check("the CREATE names every configured column", len(in_ddl), len(names))
check("both are generated from config.INTRADAY_COLUMNS, not a literal list",
      len(names) > 0 and all(f" {n} " in sql for n in names), True)


print("\n=== 3. a systematic write failure aborts the run ===")
check("AbortRun exists and is an exception",
      issubclass(compute.AbortRun, Exception), True)
check("the abort streak is short — a systematic fault fails on row 1",
      compute.FAILURE_ABORT_STREAK <= 5, True)
check("progress is time-based, with a heartbeat interval",
      compute.HEARTBEAT_SEC > 0, True)
check("...and the interval is short enough that silence is diagnostic",
      compute.HEARTBEAT_SEC <= 60, True)

src = open(compute.__file__, encoding="utf-8").read()

# The exact shape of the incident: `with ProcessPoolExecutor(...)` calls
# shutdown(wait=True) on exit, which drains every submitted unit before an
# exception can surface. The explicit shutdown must cancel instead.
check("the pool is NOT used as a context manager",
      "with ProcessPoolExecutor" in src, False)
check("...and is shut down without waiting",
      "shutdown(wait=False, cancel_futures=True)" in src, True)
check("the write path is guarded",
      "except Exception as exc:" in src and "WRITE FAILED" in src, True)
check("worker failures abort too, not just write failures",
      src.count("_abort_if_systematic()") >= 2, True)
check("a success resets the streak, so isolated failures are survivable",
      "recent_failures.clear()" in src, True)
check("a heartbeat thread exists so silence means stopped",
      "def heartbeat" in src and "daemon=True" in src, True)

# The rollup and the vacuum must not run on a partial write: both would be
# computed from incomplete data and would look like real output.
i_abort = src.index("RUN ABORTED")
i_rollup = src.index("upsert_intraday_monthly")
check("the abort is raised BEFORE the monthly rollup", i_abort < i_rollup, True)


print("\n=== 4. drift detection reports missing separately from extra ===")
# `missing` is fatal (the writer names its columns, so every INSERT fails);
# `extra` is what a renamed metric leaves behind and is kept deliberately.
import inspect
sig = inspect.signature(db.intraday_column_drift)
check("intraday_column_drift takes a connection", list(sig.parameters), ["conn"])
doc = (db.intraday_column_drift.__doc__ or "")
check("...and documents that extra columns are kept", "kept" in doc, True)


print(f"\n{'=' * 68}")
print(f"PASSED {len(PASS)} / {len(PASS) + len(FAIL)}")
if FAIL:
    print("FAILED:")
    for f in FAIL:
        print("  -", f)
    sys.exit(1)
print("ALL GREEN")
