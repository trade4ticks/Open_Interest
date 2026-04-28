"""Postgres connection + query helpers."""
from __future__ import annotations

import pandas as pd
import psycopg2

from config import PG_DB, PG_HOST, PG_PASSWORD, PG_PORT, PG_USER


def get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )


def read_sql_df(conn, sql: str, params: dict | None = None) -> pd.DataFrame:
    """Run a query against a psycopg2 connection and return a DataFrame.

    Wraps cursor.execute / fetchall so we avoid pandas's
    "only SQLAlchemy connections are supported" UserWarning that fires when
    pd.read_sql is given a raw psycopg2 connection. Behaviourally equivalent
    for our use cases.
    """
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)
