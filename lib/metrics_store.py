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
from bisect import bisect_left
import re

import psycopg2.extras

from lib.metrics_config import (
    BASE_COLUMNS, BASE_NAMES, KEY_COLUMNS, Z_BASE_COLUMNS, Z_COLUMNS,
    BASELINE_MIN_N, BASELINE_SNAPSHOT,
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


def zscore_row(window_by_col: dict, value_by_col: dict,
               ticker, trade_date, snapshot) -> dict:
    """One z row: each value measured against a window that EXCLUDES it.

    `value_by_col` is the reading being scored, keyed by base column;
    `window_by_col` is {(base, window): [prior baseline observations]}. They
    are separate arguments because the value is no longer guaranteed to be an
    element of its own window — a 10:15 reading is scored against 15:45
    closes, and is not one of them.

    Self-inclusion is gone. Today's value used to sit inside its own mean and
    stdev, which biases sigma upward and shrinks every score toward zero; the
    effect is largest at the 63-day window where one point is 1/21 of the
    minimum sample. Excluding it makes the question unambiguous: how unusual is
    this reading against the closes that preceded it.
    """
    out = {"ticker": ticker, "trade_date": trade_date, "snapshot": snapshot}
    for w in Z_WINDOWS:
        need = max(Z_MIN_OBS[w], BASELINE_MIN_N)
        for base in _Z_BASES:
            series = window_by_col.get((base, w)) or []
            vals = [v for v in series if v is not None]
            today = value_by_col.get(base)
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


def _load_metric_history(conn, ticker: str, upto) -> list:
    """Every (trade_date, values) for this ticker at the BASELINE bucket.

    Not the scored row's own bucket. A 10:15 reading is measured against the
    ticker's recent daily closes, which is the only baseline a 5-minute bucket
    can have — the intraday grid began 2026-08-24, so its own history is one or
    two observations, against which any value is its own maximum.

    Loaded whole rather than per-date: a backfill over a year would otherwise
    re-issue the same lookback once per date. It is also now loaded once per
    TICKER rather than once per (ticker, snapshot), since one baseline series
    serves every bucket of that ticker.
    """
    cols = ", ".join(_Z_BASES)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT trade_date, {cols} FROM {METRICS_TABLE} "
            f"WHERE ticker = %s AND snapshot = %s AND trade_date <= %s "
            f"ORDER BY trade_date",
            (ticker, BASELINE_SNAPSHOT, _py(upto)))
        return [(r[0], r[1:]) for r in cur.fetchall()]


def _load_scored_values(conn, ticker: str, snapshot: str, dates: list) -> dict:
    """{trade_date: (values...)} for the rows being scored, at THEIR bucket.

    Separate from the baseline history because the two are now different
    series: the value comes from the row's own snapshot, the window from the
    baseline one.
    """
    cols = ", ".join(_Z_BASES)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT trade_date, {cols} FROM {METRICS_TABLE} "
            f"WHERE ticker = %s AND snapshot = %s AND trade_date = ANY(%s)",
            (ticker, snapshot, [_py(d) for d in dates]))
        return {r[0]: r[1:] for r in cur.fetchall()}


def zscore_rows(conn, ticker: str, snapshot: str, dates: list) -> list:
    """Z rows for `dates` at one snapshot, scored against the daily baseline.

    Must run AFTER write_metrics for those dates: the value being scored is
    read back from equity_metrics, and a missing base row yields no z row.

    The window is the ticker's BASELINE-bucket history STRICTLY BEFORE each
    date. Strictly, for two reasons that happen to agree: it is what removing
    self-inclusion means at the baseline bucket, and at an intraday bucket the
    same day's close has not happened yet — including it would score a 10:15
    reading against a 15:45 close from its own future.
    """
    if not dates:
        return []
    hist = _load_metric_history(conn, ticker, max(dates))
    if not hist:
        return []
    values = _load_scored_values(conn, ticker, snapshot, dates)
    if not values:
        return []

    hist_dates = [d for d, _ in hist]
    out = []
    for d in sorted(dates):
        row_vals = values.get(d)
        if row_vals is None:
            continue                       # no base row: nothing to score
        # Index of the first baseline observation NOT before d, so the slice
        # below ends strictly before the scored date.
        end = bisect_left(hist_dates, d)
        if end == 0:
            continue                       # no prior baseline history at all
        windows = {}
        for w in Z_WINDOWS:
            chunk = hist[max(0, end - w): end]
            for j, base in enumerate(_Z_BASES):
                windows[(base, w)] = [r[j] for _, r in chunk]
        vals_by_col = {base: row_vals[j] for j, base in enumerate(_Z_BASES)}
        out.append(zscore_row(windows, vals_by_col, ticker, d, snapshot))
    return out


# Idempotent migrations, split by where they must sit relative to the column
# sync. Both lists are re-applied on every run; each file no-ops when its change
# is already in place.
#
# PRE  — renames. sync_metrics_schema only ever ADDs, so if it ran first it
#        would create rv_30d as a fresh NULL column, the rename would then find
#        the new name already present and skip, and rv_1m would be left as an
#        orphan holding all the history while rv_30d sat empty. The drift check
#        catches the orphan, but only after a rebuild had already been run
#        against a column that was silently NULL.
# POST — views over metric columns. On a fresh database 09 creates only the key
#        skeleton and every metric column arrives from the registry, so a view
#        naming rv_7d cannot be created until the sync has added it.
PRE_SYNC_SQL = ["11_rv_tenor_rename.sql", "13_ret_semivol_rename.sql",
                "14_spotvol_tenor_rename.sql"]
POST_SYNC_SQL = ["12_rv_compat_views.sql"]


def _apply_sql(conn, names: list) -> None:
    from pathlib import Path
    sql_dir = Path(__file__).resolve().parent.parent / "sql"
    for name in names:
        path = sql_dir / name
        if not path.exists():
            continue
        with conn.cursor() as cur:
            cur.execute(path.read_text(encoding="utf-8"))
        conn.commit()
        log.info("migration applied: %s", name)


def sync_all(conn) -> tuple:
    """Migrate, add columns, regenerate the catalog, assert no drift.

    THE one entry point for bringing the metrics schema up to date. Both
    init_db.py and metrics_store.init_db() call this rather than sequencing the
    steps themselves — the ordering above is load-bearing, and two copies of it
    is one copy too many.
    """
    _apply_sql(conn, PRE_SYNC_SQL)
    n_base, n_z = sync_metrics_schema(conn)
    n_cat = sync_catalog(conn)
    _apply_sql(conn, POST_SYNC_SQL)
    check_catalog_drift(conn)
    return n_base, n_z, n_cat


def init_db(conn, sql_path=None) -> None:
    """Apply sql/09_equity_metrics.sql, then sync columns and the catalog."""
    from pathlib import Path
    path = Path(sql_path) if sql_path else (
        Path(__file__).resolve().parent.parent / "sql"
        / "09_equity_metrics.sql")
    with conn.cursor() as cur:
        cur.execute(path.read_text(encoding="utf-8"))
    conn.commit()
    n_base, n_z, n_cat = sync_all(conn)
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
