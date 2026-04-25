"""Postgres connection helper."""
from __future__ import annotations

import psycopg2

from config import PG_DB, PG_HOST, PG_PASSWORD, PG_PORT, PG_USER


def get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )
