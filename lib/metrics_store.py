"""
Equity surface metrics: schema, catalog, z-scores and writes (stage 4).

The ~600 metric columns are added to the SQL skeletons by sync_metrics_schema()
from lib/metrics_config.py, mirroring lib/trade_path_schema.py. The skeleton
owns the keys and the partitioning; this module owns everything generated.

check_catalog_drift() is the reason that split is safe. It compares
information_schema against the config on every run and raises rather than
letting the table and the registry diverge — a divergence is not an error at
write time, it is a column that is silently NULL forever and a dashboard entry
that returns nothing.

NumPy sanitisation is not optional. psycopg2 has no adapter for numpy.float64,
so it falls back to repr() and emits `np.float64(2.46)`, which Postgres
rejects; NaN and Inf must become NULL or they poison every downstream
aggregate silently. _py() from lib.surface_store does both and is reused here
rather than reimplemented.
"""
from __future__ import annotations

import logging
import re

import psycopg2.extras

from lib.metrics_config import (
    BASE_COLUMNS, BASE_NAMES, KEY_COLUMNS, Z_BASE_COLUMNS, Z_COLUMNS,
    Z_MIN_OBS, Z_NAMES, Z_WINDOWS, catalog_rows,
)
from lib.surface_store import _py

log = logging.getLogger(__name__)

METRICS_TABLE = "equity_metrics"
Z_TABLE = "equity_metrics_z"
CATALOG_TABLE = "equity_metrics_catalog"

# Present in the skeleton, not generated from the registry, and excluded from
# every drift comparison.
_SKELETON_COLS = set(KEY_COLUMNS) | {"built_at"}

_SAFE = re.compile(r"^[a-z_][a-z0-9_]*$")


def _validate(col: str) -> None:
    """Registry names reach SQL by interpolation, so they are validated rather
    than trusted. They are developer-authored, but a name with a quote in it
    would be an injection point and is trivially preventable."""
    if not _SAFE.match(col) or len(col) > 63:
        raise ValueError(f"unsafe or over-long column name from registry: "
                         f"{col!r}")


class SchemaNotInitialised(RuntimeError):
    """The skeletons from sql/09_equity_metrics.sql are missing."""


def _require_tables(conn) -> None:
    missing = []
    with conn.cursor() as cur:
        for t in (METRICS_TABLE, Z_TABLE, CATALOG_TABLE):
            cur.execute("SELECT to_regclass(%s)", (f"public.{t}",))
            if cur.fetchone()[0] is None:
                missing.append(t)
    if missing:
        raise SchemaNotInitialised(
            f"missing table(s): {', '.join(missing)}.\n"
            f"Apply the skeleton first:  python build_equity_metrics.py init-db\n"
            f"(it runs sql/09_equity_metrics.sql, which owns those definitions; "
            f"this module only adds the generated columns to them)")


def _existing_columns(conn, table: str) -> set:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s AND table_schema = 'public'", (table,))
        return {r[0] for r in cur.fetchall()}


def sync_metrics_schema(conn) -> tuple:
    """Add every missing metric column. Idempotent. Never drops.

    ADD COLUMN on a partitioned parent cascades to existing children, so this
    is safe to run against a table that already holds a backfill.
    """
    _require_tables(conn)
    added = {METRICS_TABLE: [], Z_TABLE: []}
    for table, cols in ((METRICS_TABLE, BASE_COLUMNS), (Z_TABLE, Z_COLUMNS)):
        existing = _existing_columns(conn, table)
        with conn.cursor() as cur:
            for c in cols:
                _validate(c.name)
                if c.name in existing:
                    continue
                cur.execute(f"ALTER TABLE {table} "
                            f"ADD COLUMN IF NOT EXISTS {c.name} {c.sql_type}")
                added[table].append(c.name)
    conn.commit()
    for t, names in added.items():
        if names:
            log.info("%s: added %d column(s), e.g. %s",
                     t, len(names), names[:5])
    return len(added[METRICS_TABLE]), len(added[Z_TABLE])


def check_catalog_drift(conn) -> None:
    """Raise unless the tables and the registry agree exactly.

    Both directions matter. A column in the DB but not the registry is an
    orphan the catalog will never describe; a column in the registry but not
    the DB means sync did not run, and every write of it would be dropped.
    """
    problems = []
    for table, names in ((METRICS_TABLE, BASE_NAMES), (Z_TABLE, Z_NAMES)):
        have = _existing_columns(conn, table) - _SKELETON_COLS
        want = set(names)
        orphan, absent = sorted(have - want), sorted(want - have)
        if orphan:
            problems.append(f"{table}: {len(orphan)} column(s) in the table "
                            f"but not in metrics_config: {orphan[:8]}")
        if absent:
            problems.append(f"{table}: {len(absent)} column(s) in "
                            f"metrics_config but not in the table "
                            f"(run sync_metrics_schema): {absent[:8]}")

    cat = _existing_columns(conn, CATALOG_TABLE)
    if cat:
        with conn.cursor() as cur:
            cur.execute(f"SELECT column_name FROM {CATALOG_TABLE}")
            described = {r[0] for r in cur.fetchall()}
        expected = set(BASE_NAMES) | set(Z_NAMES)
        stale = sorted(described - expected)
        if stale:
            problems.append(f"{CATALOG_TABLE}: {len(stale)} row(s) describe "
                            f"columns that no longer exist: {stale[:8]}")
    if problems:
        raise SchemaNotInitialised("metrics schema drift:\n  - "
                                   + "\n  - ".join(problems))


def sync_catalog(conn) -> int:
    """Regenerate equity_metrics_catalog from the registry.

    Deletes rows describing columns that no longer exist, so a renamed metric
    does not leave a dead entry in the dashboard's picker.
    """
    rows = catalog_rows()
    cols = ["column_name", "table_name", "family", "tenor", "wing", "form",
            "base_column", "units", "description", "formula"]
    sql = (f"INSERT INTO {CATALOG_TABLE} ({', '.join(cols)}) VALUES %s "
           f"ON CONFLICT (column_name) DO UPDATE SET "
           + ", ".join(f"{c} = EXCLUDED.{c}" for c in cols[1:]))
    vals = [tuple(_py(r[c]) for c in cols) for r in rows]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, vals, page_size=500)
        cur.execute(
            f"DELETE FROM {CATALOG_TABLE} WHERE column_name <> ALL(%s)",
            ([r["column_name"] for r in rows],))
        removed = cur.rowcount
    conn.commit()
    if removed:
        log.info("catalog: removed %d stale row(s)", removed)
    return len(vals)


def ensure_partition(conn, trade_date) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT ensure_equity_metrics_partition(%s)",
                    (_py(trade_date),))


def _upsert(conn, table: str, names: list, rows: list) -> int:
    if not rows:
        return 0
    cols = KEY_COLUMNS + names
    # built_at is omitted from the INSERT so the column DEFAULT applies, and
    # set explicitly on conflict so a reprocessed row records when it was
    # actually rebuilt rather than when it first appeared.
    set_sql = ", ".join(f"{c} = EXCLUDED.{c}" for c in names)
    sql = (f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
           f"ON CONFLICT ({', '.join(KEY_COLUMNS)}) DO UPDATE SET "
           f"{set_sql}, built_at = now()")
    values = [tuple(_py(r.get(c)) for c in cols) for r in rows]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=200)
    return len(values)


def write_metrics(conn, rows: list) -> int:
    if not rows:
        return 0
    for d in {r["trade_date"] for r in rows}:
        ensure_partition(conn, d)
    n = _upsert(conn, METRICS_TABLE, BASE_NAMES, rows)
    conn.commit()
    return n


def write_zscores(conn, rows: list) -> int:
    if not rows:
        return 0
    for d in {r["trade_date"] for r in rows}:
        ensure_partition(conn, d)
    n = _upsert(conn, Z_TABLE, Z_NAMES, rows)
    conn.commit()
    return n


# =============================================================================
# Z-scores
# =============================================================================
_Z_BASES = [c.name for c in Z_BASE_COLUMNS]


def zscore_row(window_by_col: dict, ticker, trade_date, snapshot) -> dict:
    """Build one z row from {base_column: [oldest .. today]} slices.

    Today's value is the last element and is INCLUDED in its own mean and
    stdev — the question a rolling z answers is "how unusual is today against
    the recent past", and the recent past ending yesterday is a different, less
    stable question at a 63-day window.
    """
    out = {"ticker": ticker, "trade_date": trade_date, "snapshot": snapshot}
    for w in Z_WINDOWS:
        need = Z_MIN_OBS[w]
        for base in _Z_BASES:
            series = window_by_col.get((base, w)) or []
            vals = [v for v in series if v is not None]
            today = series[-1] if series else None
            z = None
            if today is not None and len(vals) >= need:
                m = sum(vals) / len(vals)
                var = sum((v - m) ** 2 for v in vals) / (len(vals) - 1)
                sd = var ** 0.5
                # A zero-variance window makes every z either 0/0 or infinite.
                # Constant series are real (a metric pinned at its floor), and
                # NULL is the honest reading.
                if sd > 1e-12:
                    z = (today - m) / sd
            out[f"{base}_z_{w}"] = z
    return out


def _load_metric_history(conn, ticker: str, snapshot: str, upto) -> list:
    """Every (trade_date, values) for this ticker+snapshot up to `upto`.

    Loaded whole rather than per-date: a backfill over a year would otherwise
    re-issue the same 252-row lookback once per date. One ticker-snapshot
    series is ~252 rows a year, so holding it is far cheaper than re-reading.
    """
    cols = ", ".join(_Z_BASES)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT trade_date, {cols} FROM {METRICS_TABLE} "
            f"WHERE ticker = %s AND snapshot = %s AND trade_date <= %s "
            f"ORDER BY trade_date", (ticker, snapshot, _py(upto)))
        return [(r[0], r[1:]) for r in cur.fetchall()]


def zscore_rows(conn, ticker: str, snapshot: str, dates: list) -> list:
    """Z rows for `dates`, all at one snapshot. Reads history once.

    Must run AFTER write_metrics for those dates — today's own value is part of
    its window, and a missing base row yields no z row at all.
    """
    if not dates:
        return []
    hist = _load_metric_history(conn, ticker, snapshot, max(dates))
    if not hist:
        return []
    pos = {d: i for i, (d, _) in enumerate(hist)}
    out = []
    for d in sorted(dates):
        i = pos.get(d)
        if i is None:
            continue                       # no base row: nothing to score
        windows = {}
        for w in Z_WINDOWS:
            chunk = hist[max(0, i - w + 1): i + 1]
            for j, base in enumerate(_Z_BASES):
                windows[(base, w)] = [row[j] for _, row in chunk]
        out.append(zscore_row(windows, ticker, d, snapshot))
    return out


def init_db(conn, sql_path=None) -> None:
    """Apply sql/09_equity_metrics.sql, then sync columns and the catalog."""
    from pathlib import Path
    path = Path(sql_path) if sql_path else (
        Path(__file__).resolve().parent.parent / "sql"
        / "09_equity_metrics.sql")
    with conn.cursor() as cur:
        cur.execute(path.read_text(encoding="utf-8"))
    conn.commit()
    n_base, n_z = sync_metrics_schema(conn)
    n_cat = sync_catalog(conn)
    check_catalog_drift(conn)
    log.info("applied %s — %d base + %d z column(s) added, %d catalog row(s)",
             path.name, n_base, n_z, n_cat)


def snapshots_for_date(conn, trade_date, ticker: str | None = None) -> list:
    """(ticker, snapshot) pairs with a surface but no metrics row yet.

    Diagnostics is the authority on what the surface stage completed — it holds
    a row even for a snapshot that produced no surface, which is exactly the
    case that must not be retried forever.
    """
    sql = ("SELECT DISTINCT d.ticker, d.snapshot "
           "FROM equity_surface_diagnostics d "
           "LEFT JOIN equity_metrics m "
           "  ON m.ticker = d.ticker AND m.trade_date = d.trade_date "
           " AND m.snapshot = d.snapshot "
           "WHERE d.trade_date = %s AND m.ticker IS NULL")
    params = [_py(trade_date)]
    if ticker:
        sql += " AND d.ticker = %s"
        params.append(ticker)
    with conn.cursor() as cur:
        cur.execute(sql + " ORDER BY 1, 2", params)
        return [(r[0], r[1]) for r in cur.fetchall()]
