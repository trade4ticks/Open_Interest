"""Postgres access for the equities-scalp pipeline.

SELF-CONTAINED. Does not import the project-root `db.py`, for the same reason
`scalp/thetadata.py` does not import `lib/thetadata.py`: `rm -rf scalp/` must
leave the options pipeline untouched.

POSTGRES HOLDS DERIVED METRICS ONLY. No tick data ever. Parquet is the record.

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


@contextmanager
def connect():
    conn = psycopg2.connect(
        host=config.PG_HOST, port=config.PG_PORT, dbname=config.PG_DB,
        user=config.PG_USER, password=config.PG_PASSWORD,
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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


def universe_history(symbol: str) -> pd.DataFrame:
    with connect() as conn:
        return pd.read_sql(
            "SELECT * FROM universe WHERE symbol = %s ORDER BY trade_date",
            conn, params=(symbol.upper(),))


def metrics_wide(trade_date: date) -> pd.DataFrame:
    """daily_metrics pivoted to one row per symbol. The shared read path for
    rank.py, calibrate.py and the dashboard."""
    with connect() as conn:
        long = pd.read_sql(
            "SELECT symbol, metric, value FROM daily_metrics "
            "WHERE trade_date = %s", conn, params=(trade_date,))
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
    with connect() as conn:
        return pd.read_sql(sql + " ORDER BY trade_date, symbol", conn,
                           params=tuple(params))
