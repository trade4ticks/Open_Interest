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
from datetime import date

import pandas as pd
import psycopg2
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

CREATE TABLE IF NOT EXISTS intraday_metrics (
    trade_date      DATE        NOT NULL,
    symbol          TEXT        NOT NULL,
    bucket_start    TIMESTAMP   NOT NULL,
    metric          TEXT        NOT NULL,
    value           DOUBLE PRECISION,
    PRIMARY KEY (trade_date, symbol, bucket_start, metric)
);

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


def init_schema() -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
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
    rows = []
    for b in buckets:
        start = b.get("window_start")
        if start is None:
            continue
        for k, v in b.items():
            if _storable(v):
                rows.append((trade_date, symbol.upper(),
                             pd.Timestamp(start).to_pydatetime(), k, _num(v)))
    return _upsert_long("intraday_metrics",
                        ["trade_date", "symbol", "bucket_start", "metric"], rows)


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
