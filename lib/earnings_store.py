"""
Earnings calendar store: fetch from yfinance, upsert, and look up.

get_earnings_dates(limit=24) returns roughly 2020 to the next scheduled date
in ONE request, so history and forward come from the same call — there is no
separate backfill endpoint to reconcile against.

Two tables, because a fund with no earnings and a failed fetch must not look
alike; see sql/10_earnings_calendar.sql.
"""
from __future__ import annotations

import logging
from datetime import date, datetime

import pandas as pd
import psycopg2.extras

log = logging.getLogger(__name__)

CAL_TABLE = "earnings_calendar"
COV_TABLE = "earnings_coverage"

CAL_COLS = ["ticker", "earnings_date", "earnings_ts", "earnings_session",
            "eps_estimate", "reported_eps", "surprise_pct", "is_estimated",
            "fetched_at"]

# yfinance asks for a count of PERIODS, not years. 24 quarters reaches back
# about six years and forward to the next scheduled date, which covers the
# metrics history with room to spare.
DEFAULT_LIMIT = 24

STATUS_OK = "ok"
STATUS_NONE = "no_earnings"
STATUS_ERROR = "error"


def _f(v):
    """NaN and pandas NA both become None; Postgres has no NaN for a float."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _session_of(ts) -> str:
    """'bmo' / 'amc' / 'unknown' from the hour of an ET timestamp.

    Yahoo stamps before-open reports around 06:00-09:00 and after-close ones at
    16:00. The boundary is the 09:30 open: anything at or before it is BMO,
    anything from 16:00 is AMC, and the gap between is a genuine unknown rather
    than a guess — an intraday stamp is Yahoo not knowing either.
    """
    if ts is None or pd.isna(ts):
        return "unknown"
    h, m = ts.hour, ts.minute
    if (h, m) <= (9, 30):
        return "bmo"
    if h >= 16:
        return "amc"
    return "unknown"


def fetch_ticker(ticker: str, limit: int = DEFAULT_LIMIT) -> tuple:
    """(rows, status, error). Never raises.

    An empty frame is STATUS_NONE, not an error: that is what a fund returns,
    and 28 of the 121 tickers are funds.
    """
    import yfinance as yf

    try:
        df = yf.Ticker(ticker).get_earnings_dates(limit=limit)
    except Exception as exc:                                  # noqa: BLE001
        return [], STATUS_ERROR, f"{type(exc).__name__}: {exc}"

    if df is None or df.empty:
        return [], STATUS_NONE, None

    now = datetime.now()
    rows = []
    for ts, r in df.iterrows():
        ts = pd.Timestamp(ts)
        if pd.isna(ts):
            continue
        reported = _f(r.get("Reported EPS"))
        rows.append({
            "ticker": ticker.upper(),
            "earnings_date": ts.date(),
            "earnings_ts": ts.to_pydatetime(),
            "earnings_session": _session_of(ts),
            "eps_estimate": _f(r.get("EPS Estimate")),
            "reported_eps": reported,
            "surprise_pct": _f(r.get("Surprise(%)")),
            # No reported EPS means it has not happened; the date is a
            # projection Yahoo may revise, which is why the upsert overwrites.
            "is_estimated": reported is None,
            "fetched_at": now,
        })
    return rows, (STATUS_OK if rows else STATUS_NONE), None


def upsert_calendar(conn, rows: list) -> int:
    """Overwrite on conflict — a future date that firms up must not be skipped."""
    if not rows:
        return 0
    updates = [c for c in CAL_COLS if c not in ("ticker", "earnings_date")]
    sql = (f"INSERT INTO {CAL_TABLE} ({', '.join(CAL_COLS)}) VALUES %s "
           f"ON CONFLICT (ticker, earnings_date) DO UPDATE SET "
           + ", ".join(f"{c} = EXCLUDED.{c}" for c in updates))
    values = [tuple(r[c] for c in CAL_COLS) for r in rows]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=500)
    return len(values)


def upsert_coverage(conn, ticker: str, status: str, rows: list,
                    error: str | None) -> None:
    today = date.today()
    future = sorted(r["earnings_date"] for r in rows
                    if r["earnings_date"] >= today)
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {COV_TABLE} (ticker, has_earnings, n_dates, "
            f"next_earnings_date, last_status, last_error, last_fetched_at) "
            f"VALUES (%s, %s, %s, %s, %s, %s, now()) "
            f"ON CONFLICT (ticker) DO UPDATE SET "
            f"has_earnings = EXCLUDED.has_earnings, "
            f"n_dates = EXCLUDED.n_dates, "
            f"next_earnings_date = EXCLUDED.next_earnings_date, "
            f"last_status = EXCLUDED.last_status, "
            f"last_error = EXCLUDED.last_error, "
            f"last_fetched_at = EXCLUDED.last_fetched_at",
            (ticker.upper(), status == STATUS_OK, len(rows),
             future[0] if future else None, status, error))


def load_dates(conn, ticker: str) -> list:
    """Ascending earnings dates for one ticker. [] for a fund.

    Loaded whole and cached by the caller: days_to_earnings is evaluated once
    per (ticker, trade_date, snapshot), and a per-row query over a 158-day
    backfill would issue tens of thousands of them for a list of ~25 dates.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT earnings_date FROM {CAL_TABLE} "
            f"WHERE ticker = %s ORDER BY earnings_date", (ticker.upper(),))
        return [r[0] for r in cur.fetchall()]


def days_to_earnings(dates: list, trade_date) -> int | None:
    """Calendar days to the next earnings date ON OR AFTER trade_date.

    Calendar rather than trading days, deliberately: the metric is a proximity
    flag, and the two differ by at most the weekends in between — which does
    not change whether a date is "close". Trading days would also make the
    value depend on the exchange calendar, so a holiday would silently shift
    every historical row on recompute.

    0 on the earnings date itself. None when no date is known at or after
    trade_date — which covers both a fund (no dates at all) and the gap after
    the last known date, since Yahoo publishes only the next confirmed one.
    Those two are distinguished in earnings_coverage, not here.
    """
    if not dates:
        return None
    from bisect import bisect_left
    i = bisect_left(dates, trade_date)
    if i >= len(dates):
        return None
    return (dates[i] - trade_date).days


def init_db(conn, sql_path=None) -> None:
    from pathlib import Path
    path = Path(sql_path) if sql_path else (
        Path(__file__).resolve().parent.parent / "sql"
        / "10_earnings_calendar.sql")
    with conn.cursor() as cur:
        cur.execute(path.read_text(encoding="utf-8"))
    conn.commit()
    log.info("applied %s", path.name)
