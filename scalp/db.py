"""Postgres access for the equities-scalp pipeline.

SELF-CONTAINED. Does not import the project-root `db.py`, for the same reason
`scalp/thetadata.py` does not import `lib/thetadata.py`: `rm -rf scalp/` must
leave the options pipeline untouched.

POSTGRES HOLDS DERIVED METRICS ONLY. No tick data ever. Parquet is the record.

ITS OWN DATABASE, NOT ITS OWN TABLES IN A SHARED ONE. `SCALP_PG_DB` defaults
to `equities_scalp` and deliberately does NOT fall back to `POSTGRES_DB` — the
host, port, user and password do fall back, so this reuses the same server and
role but never the same database.

Two reasons. The table names here are generic (universe, daily_metrics,
rankings) and would be poor neighbours in a database something else owns. And
if the strategy does not pan out, `DROP DATABASE equities_scalp` removes the
whole project without touching the factor-analysis or IV work.

WHY THE METRIC TABLES ARE LONG, NOT WIDE. `daily_metrics` is
(trade_date, symbol, metric, value) rather than one column per metric. The
metric set is explicitly unsettled — five noise variants at three horizons,
two flicker variants, and a calibration exercise whose entire purpose is to
delete the ones that do not separate. A wide table would need a migration
every time one is added or dropped, and the ranking retains history, so old
rows would carry columns that no longer mean anything.

The cost is that reading it needs a pivot. `rank.py` and the dashboard both do
one, and `metrics_wide()` below is the shared implementation.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import date, datetime, timedelta

import pandas as pd
import psycopg2
import psycopg2.extensions
import psycopg2.extras

from scalp import config

log = logging.getLogger(__name__)


# Databases belonging to the other projects on this server. Pointing the scalp
# pipeline at one of them would put ~30 tables' worth of derived metrics into a
# schema that something else owns, and the tables it creates have generic names
# (universe, daily_metrics, rankings) that are exactly the kind to collide.
#
# The separation is the point: this project gets its own database so it can be
# dropped whole if the strategy does not pan out, without touching anything
# else.
FOREIGN_DATABASES = frozenset({"spx_interpolated", "open_interest", "postgres"})


@contextmanager
def connect():
    if config.PG_DB in FOREIGN_DATABASES:
        raise RuntimeError(
            f"SCALP_PG_DB is set to {config.PG_DB!r}, which belongs to another "
            f"project.\nThe scalp pipeline creates tables named universe, "
            f"daily_metrics, intraday_metrics, provenance and rankings — "
            f"generic enough to collide.\nPoint SCALP_PG_DB at its own "
            f"database (default: equities_scalp)."
        )
    try:
        conn = psycopg2.connect(
            host=config.PG_HOST, port=config.PG_PORT, dbname=config.PG_DB,
            user=config.PG_USER, password=config.PG_PASSWORD,
        )
    except psycopg2.OperationalError as exc:
        if "does not exist" in str(exc):
            raise RuntimeError(
                f"Database {config.PG_DB!r} does not exist on "
                f"{config.PG_HOST}:{config.PG_PORT}.\n\n"
                f"This project deliberately uses its own database rather than "
                f"adding tables to an existing one, so it can be dropped whole "
                f"if the strategy does not work out. Create it once:\n\n"
                f"    createdb -h {config.PG_HOST} -p {config.PG_PORT} "
                f"-U {config.PG_USER} {config.PG_DB}\n\n"
                f"or, from psql:  CREATE DATABASE {config.PG_DB} "
                f"OWNER {config.PG_USER};\n\n"
                f"Then any scalp script will create its own tables on first "
                f"run."
            ) from exc
        raise
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def read_sql(sql: str, params: tuple | None = None) -> pd.DataFrame:
    """Query to DataFrame, without pandas' DBAPI2 path.

    `pd.read_sql` with a raw psycopg2 connection raises a UserWarning that
    only SQLAlchemy connections are supported, and pandas has been signalling
    it will drop the fallback. Building the frame from the cursor is a few
    lines, removes the warning, and adds no dependency — the alternative was
    pulling in SQLAlchemy for the sake of a DataFrame constructor.

    An empty result still returns the right columns, so callers can index into
    the frame without checking for emptiness first.
    """
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


SCHEMA_SQL = """
-- Nightly candidate list, with the values that qualified each name.
CREATE TABLE IF NOT EXISTS universe (
    trade_date      DATE        NOT NULL,
    symbol          TEXT        NOT NULL,
    close           NUMERIC,
    volume          BIGINT,
    dollar_volume   NUMERIC,
    -- Hysteresis and stickiness state, stored rather than recomputed so the
    -- history explains why a name was in the fetch list on a given night.
    qualified       BOOLEAN     NOT NULL,   -- met the ENTRY thresholds today
    retained        BOOLEAN     NOT NULL,   -- held by hysteresis or stickiness
    first_entered   DATE,
    sticky_until    DATE,
    PRIMARY KEY (trade_date, symbol)
);

-- Long format: see the module docstring for why.
CREATE TABLE IF NOT EXISTS daily_metrics (
    trade_date      DATE        NOT NULL,
    symbol          TEXT        NOT NULL,
    metric          TEXT        NOT NULL,
    value           DOUBLE PRECISION,
    PRIMARY KEY (trade_date, symbol, metric)
);
CREATE INDEX IF NOT EXISTS daily_metrics_metric_idx
    ON daily_metrics (metric, trade_date);

-- intraday_metrics is WIDE and PARTITIONED BY DAY. See config.py for the
-- full reasoning; the short version is that the long format produced 32M rows
-- and 5,995 MB over eleven days, 96% of the database, on a root disk that
-- then hit 100%. Wide plus an 18-metric subset is 15,262 rows/day and ~3 MB.
--
-- Daily partitions rather than monthly: retention becomes DROP TABLE, which
-- returns space to the OS instantly and leaves no dead tuples, and individual
-- days can be shed under disk pressure. Monthly would have retained up to 44
-- days against a 14-day policy and offered no lever mid-month.
--
-- The columns are generated from config.INTRADAY_COLUMNS so the pinned noise
-- definition cannot drift between the schema and the writer.

-- What was dropped, by what rule, getting from raw tape to each metrics row.
-- Condition-code exclusions, auction-edge trimming, crossed/locked quotes and
-- same-instant collapsing all silently change the numbers; this makes the
-- share of raw tape a row was computed from readable without opening
-- METRICS.md. Long format for the same reason as daily_metrics: the
-- per-condition-code breakout changes whenever the exclusion list does.
CREATE TABLE IF NOT EXISTS provenance (
    trade_date      DATE        NOT NULL,
    symbol          TEXT        NOT NULL,
    item            TEXT        NOT NULL,
    value           DOUBLE PRECISION,
    PRIMARY KEY (trade_date, symbol, item)
);

-- Every nightly ranking is RETAINED, never overwritten. In a month this is a
-- feature history to test against actual fills, which is the dataset the
-- strategy currently lacks.
CREATE TABLE IF NOT EXISTS rankings (
    run_ts          TIMESTAMPTZ NOT NULL,
    trade_date      DATE        NOT NULL,
    symbol          TEXT        NOT NULL,
    variant         TEXT        NOT NULL,   -- which ratio drove this ranking
    rank            INTEGER     NOT NULL,
    score           DOUBLE PRECISION,
    passed_floors   BOOLEAN     NOT NULL,
    PRIMARY KEY (run_ts, trade_date, symbol, variant)
);
CREATE INDEX IF NOT EXISTS rankings_date_idx ON rankings (trade_date, variant);
"""


def _intraday_column_ddl() -> str:
    return ",\n".join(f"    {name:<28s} {sql_type}"
                       for name, sql_type in config.INTRADAY_COLUMNS)


def intraday_ddl() -> str:
    """CREATE TABLE for the partitioned parent, built from the config column set."""
    return f"""
CREATE TABLE IF NOT EXISTS intraday_metrics (
    trade_date   DATE      NOT NULL,
    symbol       TEXT      NOT NULL,
    bucket_start TIMESTAMP NOT NULL,
    bucket_time  TIME      NOT NULL,
{_intraday_column_ddl()},
    PRIMARY KEY (trade_date, symbol, bucket_start)
) PARTITION BY RANGE (trade_date);
"""


def intraday_monthly_ddl() -> str:
    """The rollup. Same metric subset, one row per (symbol, clock bucket, month).

    ~587 x 26 x 12 = 183k rows a year, tens of MB, KEPT INDEFINITELY.

    This is the only irreversible decision in the intraday design: a month not
    aggregated while its raw parquet is still inside RAW_RETENTION_DAYS can
    never be reconstructed. Written from day one for that reason.

    `sessions` records how many trading days contributed, so a partial month
    reads as partial rather than looking like a quiet one. A month with 3
    sessions and a month with 21 must not be presented alike.
    """
    return f"""
CREATE TABLE IF NOT EXISTS intraday_monthly (
    month        DATE   NOT NULL,
    symbol       TEXT   NOT NULL,
    bucket_time  TIME   NOT NULL,
    sessions     INTEGER NOT NULL,
    trades_total BIGINT  NOT NULL,
{_intraday_column_ddl()},
    PRIMARY KEY (month, symbol, bucket_time)
);
CREATE INDEX IF NOT EXISTS intraday_monthly_symbol_idx
    ON intraday_monthly (symbol, bucket_time, month);
"""


FETCH_RUNS_DDL = """
-- One row per fetch run per date. fetch.py accumulated thin/empty/failed
-- counts in memory and printed them at the end, so a bad run left no record
-- once the terminal scrolled.
--
-- Keyed on run_ts as well as trade_date so a re-fetch APPENDS rather than
-- overwriting the first attempt — the thin-tape count from the run that went
-- wrong is exactly the thing worth looking back at.
CREATE TABLE IF NOT EXISTS fetch_runs (
    run_ts     TIMESTAMPTZ NOT NULL,
    trade_date DATE        NOT NULL,
    ok         INTEGER     NOT NULL DEFAULT 0,
    thin       INTEGER     NOT NULL DEFAULT 0,
    empty      INTEGER     NOT NULL DEFAULT 0,
    failed     INTEGER     NOT NULL DEFAULT 0,
    PRIMARY KEY (run_ts, trade_date)
);
CREATE INDEX IF NOT EXISTS fetch_runs_date_idx ON fetch_runs (trade_date);
"""


def partition_name(day: date) -> str:
    return f"intraday_metrics_{day.strftime('%Y%m%d')}"


def ensure_intraday_partition(day: date) -> str:
    """Create the partition for one day if it does not exist. Idempotent."""
    name = partition_name(day)
    sql = (f"CREATE TABLE IF NOT EXISTS {name} PARTITION OF intraday_metrics "
           f"FOR VALUES FROM (%s) TO (%s)")
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql, (day, day + timedelta(days=1)))
    return name


def intraday_partitions() -> list[tuple[str, date]]:
    """Existing partitions as (table name, the day it holds), oldest first."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT c.relname
              FROM pg_class c
              JOIN pg_inherits i ON i.inhrelid = c.oid
              JOIN pg_class p ON p.oid = i.inhparent
             WHERE p.relname = 'intraday_metrics'
             ORDER BY c.relname
        """)
        names = [r[0] for r in cur.fetchall()]
    out = []
    for name in names:
        stamp = name.rsplit("_", 1)[-1]
        try:
            out.append((name, datetime.strptime(stamp, "%Y%m%d").date()))
        except ValueError:
            continue
    return sorted(out, key=lambda x: x[1])


def drop_intraday_partitions_before(cutoff: date) -> list[tuple[str, date, int]]:
    """DROP every partition holding a day before `cutoff`.

    Returns (name, day, bytes) for each. DROP TABLE returns the space to the
    OS immediately and leaves no dead tuples — which is the whole reason for
    partitioning, since VACUUM FULL could not run when the disk was full.
    """
    dropped = []
    for name, day in intraday_partitions():
        if day >= cutoff:
            continue
        with connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_total_relation_size(%s)", (name,))
            size = int(cur.fetchone()[0] or 0)
            cur.execute(f"DROP TABLE IF EXISTS {name}")
        dropped.append((name, day, size))
    return dropped


def init_schema() -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        cur.execute(intraday_ddl())
        cur.execute(intraday_monthly_ddl())
        cur.execute(FETCH_RUNS_DDL)
    log.info("schema ensured on %s/%s", config.PG_HOST, config.PG_DB)


# --- writes ------------------------------------------------------------------

def _upsert_long(table: str, key_cols: list[str], rows: list[tuple]) -> int:
    if not rows:
        return 0
    cols = ", ".join(key_cols + ["value"])
    conflict = ", ".join(key_cols)
    sql = (f"INSERT INTO {table} ({cols}) VALUES %s "
           f"ON CONFLICT ({conflict}) DO UPDATE SET value = EXCLUDED.value")
    with connect() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
    return len(rows)


def write_daily_metrics(trade_date: date, symbol: str, metrics: dict) -> int:
    rows = [(trade_date, symbol.upper(), k, _num(v))
            for k, v in metrics.items() if _storable(v)]
    return _upsert_long("daily_metrics", ["trade_date", "symbol", "metric"], rows)


def delete_symbol_day(trade_date: date, symbol: str) -> int:
    """Remove every stored row for one symbol-day, across all metric tables.

    The writes are upserts keyed on (date, symbol, metric), so re-running is
    already idempotent and never doubles a row. What an upsert CANNOT do is
    remove a metric that no longer exists: rename or delete one in the code and
    the old rows sit there indefinitely, still keyed, still readable, and now
    wrong.

    That is not hypothetical here — `bid_persist_ms_median_tradesampled` was
    removed and `odd_lot_share` changed meaning when round lots became
    price-tiered. Use compute.py --replace after either kind of change.
    """
    n = 0
    with connect() as conn, conn.cursor() as cur:
        for table in ("daily_metrics", "intraday_metrics", "provenance"):
            cur.execute(f"DELETE FROM {table} "
                        f"WHERE trade_date = %s AND symbol = %s",
                        (trade_date, symbol.upper()))
            n += cur.rowcount
    return n


def write_provenance(trade_date: date, symbol: str, prov: dict) -> int:
    rows = [(trade_date, symbol.upper(), k, _num(v))
            for k, v in prov.items() if _storable(v)]
    return _upsert_long("provenance", ["trade_date", "symbol", "item"], rows)


def provenance_wide(trade_date: date) -> pd.DataFrame:
    """One row per symbol, for the dashboard's provenance panel."""
    long = read_sql(
        "SELECT symbol, item, value FROM provenance WHERE trade_date = %s",
        (trade_date,))
    if long.empty:
        return long
    return long.pivot(index="symbol", columns="item", values="value")


def write_intraday_metrics(trade_date: date, symbol: str,
                           buckets: list[dict]) -> int:
    """One WIDE row per bucket. Assumes the day's partition exists.

    Only config.INTRADAY_METRIC_KEYS are stored — 18 of the 232 a bucket
    carries. The rest are not read at bucket grain and cost 96% of the
    database; see config.py.
    """
    if not buckets:
        return 0
    keys = config.INTRADAY_METRIC_KEYS
    rows = []
    for b in buckets:
        start = b.get("window_start")
        if start is None:
            continue
        stamp = pd.Timestamp(start).to_pydatetime()
        rows.append((trade_date, symbol.upper(), stamp, stamp.time(),
                     *(_num_or_none(b.get(k)) for k in keys)))
    if not rows:
        return 0

    cols = ", ".join(("trade_date", "symbol", "bucket_start", "bucket_time",
                      *keys))
    updates = ", ".join(f"{k} = EXCLUDED.{k}" for k in keys)
    sql = (f"INSERT INTO intraday_metrics ({cols}) VALUES %s "
           f"ON CONFLICT (trade_date, symbol, bucket_start) DO UPDATE SET "
           f"bucket_time = EXCLUDED.bucket_time, {updates}")
    with connect() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
    return len(rows)


def _num_or_none(v):
    """Numeric for storage, or None. Booleans become 1/0, NaN becomes NULL."""
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if not isinstance(v, (int, float)):
        return None
    f = float(v)
    return None if pd.isna(f) else f


def upsert_intraday_monthly(month: date, trade_date: date) -> int:
    """Recompute one month's rollup from the intraday rows currently stored.

    RATIOS ARE TRADE-WEIGHTED. Averaging at_bid_share or two_sided_balance
    unweighted across ~21 sessions would let a bucket with 4 trades count as
    much as one with 400, and that error lands hardest in exactly the sleepy
    midday buckets the rollup exists to measure. config.INTRADAY_TRADE_WEIGHTED
    names the columns that need it; rates and counts are summed or averaged
    plainly.

    Recomputed rather than incremented, so a --replace of one day produces a
    correct month rather than one that drifts. It only sees days still inside
    INTRADAY_RETENTION_DAYS — which is why this must run from day one. A month
    whose raw parquet has aged past RAW_RETENTION_DAYS can never be rebuilt.
    """
    weighted = config.INTRADAY_TRADE_WEIGHTED
    parts = []
    for key in config.INTRADAY_METRIC_KEYS:
        if key in ("rows_raw", "trades"):
            parts.append(f"SUM({key})::double precision AS {key}")
        elif key in weighted:
            # NULLIF guards a bucket with no trades: it contributes nothing
            # rather than turning the whole average into NULL.
            parts.append(
                f"CASE WHEN SUM(CASE WHEN {key} IS NULL THEN 0 ELSE trades END) > 0 "
                f"THEN SUM({key} * trades) / SUM(CASE WHEN {key} IS NULL "
                f"THEN 0 ELSE trades END) ELSE NULL END AS {key}")
        else:
            parts.append(f"AVG({key}) AS {key}")

    cols = ", ".join(config.INTRADAY_METRIC_KEYS)
    updates = ", ".join(f"{k} = EXCLUDED.{k}"
                        for k in config.INTRADAY_METRIC_KEYS)
    sql = f"""
        INSERT INTO intraday_monthly
            (month, symbol, bucket_time, sessions, trades_total, {cols})
        SELECT
            %s::date                              AS month,
            symbol,
            bucket_time,
            COUNT(DISTINCT trade_date)            AS sessions,
            COALESCE(SUM(trades), 0)::bigint      AS trades_total,
            {', '.join(parts)}
        FROM intraday_metrics
        WHERE trade_date >= %s::date
          AND trade_date <  (%s::date + INTERVAL '1 month')
        GROUP BY symbol, bucket_time
        ON CONFLICT (month, symbol, bucket_time) DO UPDATE SET
            sessions = EXCLUDED.sessions,
            trades_total = EXCLUDED.trades_total,
            {updates}
    """
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql, (month, month, month))
        return cur.rowcount


def month_start(day: date) -> date:
    return day.replace(day=1)


# --- fetch_runs --------------------------------------------------------------

def write_fetch_run(run_ts, trade_date: date, ok: int, thin: int,
                    empty: int, failed: int) -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO fetch_runs (run_ts, trade_date, ok, thin, empty, failed)"
            " VALUES (%s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (run_ts, trade_date) DO UPDATE SET"
            " ok = EXCLUDED.ok, thin = EXCLUDED.thin,"
            " empty = EXCLUDED.empty, failed = EXCLUDED.failed",
            (run_ts, trade_date, ok, thin, empty, failed))


def fetch_run_history(limit: int = 50) -> pd.DataFrame:
    return read_sql(
        "SELECT run_ts, trade_date, ok, thin, empty, failed FROM fetch_runs"
        " ORDER BY run_ts DESC, trade_date DESC LIMIT %s", (limit,))


# --- maintenance -------------------------------------------------------------

@contextmanager
def connect_autocommit():
    """VACUUM cannot run inside a transaction block."""
    with connect() as conn:
        old_level = conn.isolation_level
        conn.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        try:
            yield conn
        finally:
            conn.set_isolation_level(old_level)


def vacuum(table: str) -> None:
    """Plain VACUUM. NEVER FULL.

    FULL rewrites the table and needs free space equal to it, which is exactly
    what was unavailable when the root disk hit 100% — the dead tuples could
    not be reclaimed by the one command that would have reclaimed them. Plain
    VACUUM marks space reusable in place, needs no extra room, and is enough
    after a --replace that touched a single day's partition.
    """
    with connect_autocommit() as conn, conn.cursor() as cur:
        cur.execute(f"VACUUM {table}")


def table_stats(table: str) -> dict:
    """Dead-tuple and autovacuum state, for reporting after a --replace."""
    df = read_sql(
        "SELECT relname, n_live_tup, n_dead_tup, last_vacuum, last_autovacuum,"
        " last_analyze, last_autoanalyze"
        " FROM pg_stat_user_tables WHERE relname = %s", (table,))
    return {} if df.empty else df.iloc[0].to_dict()


def postgres_data_directory() -> str:
    """Where the server actually keeps its files.

    Asked of the server rather than read from config. Postgres lives on the
    ROOT disk while the parquet store is on block 3, so a free-space check
    against config.data_dir() passes while root fills — which is what
    happened. A configurable path could drift again; this cannot.
    """
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SHOW data_directory")
        return cur.fetchone()[0]


def postgres_free_space_gb() -> float | None:
    """Free space on the filesystem holding the Postgres data directory.

    Returns None when the server is not local — the path it reports is then
    meaningless here, and a number derived from the wrong filesystem is worse
    than no number.
    """
    import shutil
    from pathlib import Path

    if config.PG_HOST not in ("localhost", "127.0.0.1", "::1", ""):
        return None
    try:
        path = Path(postgres_data_directory())
    except Exception:
        return None
    while not path.exists() and path.parent != path:
        path = path.parent
    try:
        return shutil.disk_usage(path).free / (1024 ** 3)
    except OSError:
        return None


def database_size_report() -> pd.DataFrame:
    """Every table with its size, largest first."""
    return read_sql("""
        SELECT relname AS table_name,
               pg_size_pretty(pg_total_relation_size(c.oid)) AS size,
               pg_total_relation_size(c.oid) AS bytes,
               n_live_tup AS live_rows, n_dead_tup AS dead_rows
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
         WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
         ORDER BY pg_total_relation_size(c.oid) DESC
    """)


def _storable(v) -> bool:
    """Only numeric metrics go in the value column. Window bounds and other
    bookkeeping live in the key, not as a 'metric'."""
    if isinstance(v, bool):
        return True
    return isinstance(v, (int, float)) and not isinstance(v, complex)


def _num(v) -> float | None:
    f = float(v)
    return None if pd.isna(f) else f


def write_universe(trade_date: date, rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO universe (trade_date, symbol, close, volume, dollar_volume,
                              qualified, retained, first_entered, sticky_until)
        VALUES %s
        ON CONFLICT (trade_date, symbol) DO UPDATE SET
            close = EXCLUDED.close, volume = EXCLUDED.volume,
            dollar_volume = EXCLUDED.dollar_volume,
            qualified = EXCLUDED.qualified, retained = EXCLUDED.retained,
            first_entered = EXCLUDED.first_entered,
            sticky_until = EXCLUDED.sticky_until
    """
    values = [(trade_date, r["symbol"], r.get("close"), r.get("volume"),
               r.get("dollar_volume"), r["qualified"], r["retained"],
               r.get("first_entered"), r.get("sticky_until")) for r in rows]
    with connect() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
    return len(values)


def write_ranking(run_ts, trade_date: date, variant: str,
                  rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO rankings (run_ts, trade_date, symbol, variant, rank,
                              score, passed_floors)
        VALUES %s
        ON CONFLICT (run_ts, trade_date, symbol, variant) DO NOTHING
    """
    values = [(run_ts, trade_date, r["symbol"], variant, r["rank"],
               r.get("score"), r.get("passed_floors", False)) for r in rows]
    with connect() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
    return len(values)


# --- reads -------------------------------------------------------------------

def latest_universe_date() -> date | None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT max(trade_date) FROM universe")
        row = cur.fetchone()
    return row[0] if row else None


def universe_symbols(trade_date: date | None = None) -> list[str]:
    """The fetch list: everything qualified or retained on that date."""
    with connect() as conn, conn.cursor() as cur:
        if trade_date is None:
            cur.execute("SELECT max(trade_date) FROM universe")
            got = cur.fetchone()
            trade_date = got[0] if got else None
        if trade_date is None:
            return []
        cur.execute(
            "SELECT symbol FROM universe "
            "WHERE trade_date = %s AND (qualified OR retained) "
            "ORDER BY symbol", (trade_date,))
        return [r[0] for r in cur.fetchall()]


def universe_on(trade_date: date) -> pd.DataFrame:
    """The whole universe snapshot for one date."""
    return read_sql("SELECT * FROM universe WHERE trade_date = %s",
                    (trade_date,))


def universe_history(symbol: str) -> pd.DataFrame:
    return read_sql(
        "SELECT * FROM universe WHERE symbol = %s ORDER BY trade_date",
        (symbol.upper(),))


def metrics_wide(trade_date: date) -> pd.DataFrame:
    """daily_metrics pivoted to one row per symbol. The shared read path for
    rank.py, calibrate.py and the dashboard."""
    long = read_sql(
        "SELECT symbol, metric, value FROM daily_metrics "
        "WHERE trade_date = %s", (trade_date,))
    if long.empty:
        return long
    return long.pivot(index="symbol", columns="metric", values="value")


def metric_history(metric: str, symbols: list[str] | None = None
                   ) -> pd.DataFrame:
    """One metric across dates — the 10-day stability column, and the
    feature history the calibration will eventually be tested against."""
    sql = "SELECT trade_date, symbol, value FROM daily_metrics WHERE metric = %s"
    params: list = [metric]
    if symbols:
        sql += " AND symbol = ANY(%s)"
        params.append([s.upper() for s in symbols])
    return read_sql(sql + " ORDER BY trade_date, symbol", tuple(params))
