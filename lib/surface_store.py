"""
Equity option surface: stage 5 (store).

Postgres writes for the three surface tables. Every write is
ON CONFLICT DO UPDATE so reprocessing a date is safe and idempotent; the key
columns are never in the SET clause.

NumPy sanitisation is not optional here. psycopg2 has no adapter for
numpy.float64, so it falls back to repr() and emits literals like
`np.float64(2.46)`, which Postgres rejects with a syntax error. NaN and Inf
have to become NULL for the same reason — Postgres accepts 'NaN' for
DOUBLE PRECISION but it then poisons every downstream aggregate silently.
"""
from __future__ import annotations

import logging
import math
from datetime import date, datetime

import psycopg2.extras

log = logging.getLogger(__name__)

SURFACE_COLS = [
    "ticker", "trade_date", "snapshot", "dte", "put_delta", "iv", "strike",
    "forward", "log_moneyness", "price", "theta", "vega", "gamma",
    "dte_actual", "extrapolated",
]
ATM_COLS = [
    "ticker", "trade_date", "snapshot", "dte", "atm_put_delta", "atm_strike",
    "atm_iv", "atm_forward", "total_var", "underlying_price", "price",
    "theta", "vega", "gamma", "dte_actual",
]
DIAG_COLS = [
    "ticker", "trade_date", "snapshot", "expiry", "dte_actual",
    "forward_price", "risk_free_rate", "forward_method", "n_strikes_raw",
    "n_strikes_clean", "k_min", "k_max", "spline_rmse", "calendar_arb_flag",
    "butterfly_arb_flag", "skipped", "skip_reason",
]

SURFACE_KEYS = ["ticker", "trade_date", "snapshot", "dte", "put_delta"]
ATM_KEYS = ["ticker", "trade_date", "snapshot", "dte"]
DIAG_KEYS = ["ticker", "trade_date", "snapshot", "expiry"]


def _py(v):
    """NumPy scalar -> native Python; NaN/Inf -> None.

    Without this psycopg2 repr()s a numpy.float64 as `np.float64(2.46)` and
    Postgres rejects the statement.
    """
    if v is None:
        return None
    if isinstance(v, (bool,)):
        return bool(v)
    try:
        import numpy as np
        if isinstance(v, np.bool_):
            return bool(v)
        if isinstance(v, np.integer):
            return int(v)
        if isinstance(v, np.floating):
            v = float(v)
    except ImportError:
        pass
    if isinstance(v, float):
        return None if not math.isfinite(v) else v
    if isinstance(v, (datetime,)):
        return v
    if hasattr(v, "to_pydatetime"):          # pandas Timestamp
        return v.to_pydatetime()
    if hasattr(v, "date") and not isinstance(v, date):
        return v.date()
    return v


def _upsert(conn, table: str, cols: list, keys: list, rows: list) -> int:
    if not rows:
        return 0
    updates = [c for c in cols if c not in keys]
    set_sql = ", ".join(f"{c} = EXCLUDED.{c}" for c in updates)
    sql = (f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
           f"ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {set_sql}")
    values = [tuple(_py(r.get(c)) for c in cols) for r in rows]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=500)
    return len(values)


def ensure_partitions(conn, trade_date) -> None:
    """Create the month's child tables before writing into them."""
    d = _py(trade_date)
    with conn.cursor() as cur:
        cur.execute("SELECT ensure_equity_surface_partition(%s)", (d,))
        cur.execute("SELECT ensure_equity_atm_partition(%s)", (d,))


def write_snapshot(conn, result: dict, trade_date) -> dict:
    """Persist one snapshot's surface, ATM and diagnostics rows.

    Diagnostics are written even when no surface row survived — a snapshot that
    produced nothing is exactly the case the diagnostics table exists to
    explain, and it is also what stops an incremental run retrying a date
    forever.
    """
    ensure_partitions(conn, trade_date)
    n_s = _upsert(conn, "equity_surface", SURFACE_COLS, SURFACE_KEYS,
                  result.get("surface", []))
    n_a = _upsert(conn, "equity_atm", ATM_COLS, ATM_KEYS,
                  result.get("atm", []))
    n_d = _upsert(conn, "equity_surface_diagnostics", DIAG_COLS, DIAG_KEYS,
                  result.get("diagnostics", []))
    conn.commit()
    return {"surface": n_s, "atm": n_a, "diagnostics": n_d}


def max_processed_date(conn, ticker: str | None = None):
    """Latest trade_date in diagnostics — where an incremental run resumes."""
    sql = "SELECT MAX(trade_date) FROM equity_surface_diagnostics"
    params = ()
    if ticker:
        sql += " WHERE ticker = %s"
        params = (ticker,)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    return row[0] if row else None


def completed_snapshots(conn, ticker: str, trade_date) -> dict:
    """{snapshot: n_expiries} already in diagnostics for this ticker-date.

    The intraday job compares this against the expirations on disk to decide
    which snapshots still need work, rather than reprocessing the whole day.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT snapshot, count(*) FROM equity_surface_diagnostics "
            "WHERE ticker = %s AND trade_date = %s GROUP BY snapshot",
            (ticker, _py(trade_date)))
        return {r[0]: r[1] for r in cur.fetchall()}


def init_db(conn, sql_path=None) -> None:
    """Apply sql/08_equity_surface.sql. Idempotent."""
    from pathlib import Path
    path = Path(sql_path) if sql_path else (
        Path(__file__).resolve().parent.parent / "sql" / "08_equity_surface.sql")
    with conn.cursor() as cur:
        cur.execute(path.read_text(encoding="utf-8"))
    conn.commit()
    log.info("applied %s", path.name)
