"""
build_spx_catalog.py — the metric catalog for the four SPX tables, written to
`spx_metric_catalog` (one row per stored metric column). Definitions in
lib/spx_catalog.py.

    python build_spx_catalog.py --dry-run          # checks + summary, writes nothing
    python build_spx_catalog.py --dry-run --list   # ... and every row
    python build_spx_catalog.py                    # write
    python build_spx_catalog.py --print-ddl

When SPX_FEATURES_DSN is set the build also reads information_schema and fails
unless every live table's columns are exactly the catalogued metrics plus its
keys — so a column added to a table without a catalog row is caught.

IDEMPOTENT: the four tables' catalog rows are deleted and re-inserted in one
transaction.
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter

import psycopg2
import psycopg2.extras

import config
import lib.spx_catalog as C
import lib.spx_features as F


def ddl() -> str:
    return (f"CREATE TABLE IF NOT EXISTS {C.TABLE_C} (\n"
            "    metric TEXT NOT NULL,\n"
            "    table_name TEXT NOT NULL,\n"
            "    description TEXT NOT NULL,\n"
            "    family TEXT NOT NULL,\n"
            "    surfaced BOOLEAN NOT NULL,\n"
            "    built_at TIMESTAMPTZ NOT NULL DEFAULT now(),\n"
            "    PRIMARY KEY (table_name, metric)\n);")


def live_columns(dsn: str) -> dict:
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT table_name, column_name FROM information_schema.columns "
                    "WHERE table_schema = current_schema() AND table_name = ANY(%s)", (list(C.TABLE_COLUMNS),))
        out: dict = {}
        for t, c in cur.fetchall():
            out.setdefault(t, set()).add(c)
    return out


def write(dsn: str, rows: list[dict]) -> int:
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl())
            cur.execute(f"DELETE FROM {C.TABLE_C} WHERE table_name = ANY(%s)", (list(C.TABLE_COLUMNS),))
            deleted = cur.rowcount
            psycopg2.extras.execute_values(
                cur, f"INSERT INTO {C.TABLE_C} ({', '.join(C.CATALOG_COLUMNS)}) VALUES %s",
                [tuple(r[c] for c in C.CATALOG_COLUMNS) for r in rows])
        conn.commit()
    print(f"  replaced {deleted} existing row(s)")
    return len(rows)


def summary(rows: list[dict]) -> None:
    cnt = Counter((r["table_name"], r["family"], r["surfaced"]) for r in rows)
    print(f"  {'table':<18}{'family':<16}{'surfaced':>9}{'stored-only':>13}")
    for t in C.TABLE_COLUMNS:
        for fam in sorted({f for tt, f, _ in cnt if tt == t}):
            print(f"  {t:<18}{fam:<16}{cnt[(t, fam, True)]:>9}{cnt[(t, fam, False)]:>13}")
    print(f"  total: {len(rows)} rows, {sum(r['surfaced'] for r in rows)} surfaced")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--list", action="store_true", help="print every catalog row")
    p.add_argument("--print-ddl", action="store_true")
    args = p.parse_args()
    if args.print_ddl:
        print(ddl())
        return 0
    if not args.dry_run and not config.SPX_FEATURES_DSN:
        p.error("SPX_FEATURES_DSN is not set (.env); use --dry-run")

    rows = C.catalog_rows()
    print(f"=== SPX metric catalog{'  DRY RUN' if args.dry_run else ''} ===")
    summary(rows)
    if args.list:
        for r in rows:
            print(f"\n  {r['table_name']}.{r['metric']}  [{r['family']}{'' if r['surfaced'] else ', NOT surfaced'}]"
                  f"\n    {r['description']}")

    print("\n--- hard checks ---")
    chk = F.Checker()
    db = live_columns(config.SPX_FEATURES_DSN) if config.SPX_FEATURES_DSN else None
    if db is None:
        chk.report("SPX_FEATURES_DSN not set: live tables not compared")
    C.check_catalog(rows, chk, db)
    if chk.failed:
        print(f"\n{len(chk.failed)} hard check(s) FAILED — nothing written.")
        return 1
    if args.dry_run:
        print("\nAll hard checks pass. DRY RUN — nothing written.")
        return 0
    n = write(config.SPX_FEATURES_DSN, rows)
    print(f"\nWROTE {n} rows to {C.TABLE_C}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
