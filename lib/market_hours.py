"""NYSE trading-day helpers (thin wrapper over pandas_market_calendars)."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from functools import lru_cache

import pandas_market_calendars as mcal


@lru_cache(maxsize=1)
def _nyse():
    return mcal.get_calendar("NYSE")


def get_trading_days(start: date, end: date) -> list[date]:
    """Return inclusive list of NYSE trading dates between start and end."""
    sched = _nyse().schedule(start_date=start, end_date=end)
    return [d.date() for d in sched.index]


def last_trading_day(today: date | None = None) -> date:
    """Return the most recent fully-completed NYSE session on or before today."""
    today = today or date.today()
    sched = _nyse().schedule(
        start_date=today - timedelta(days=10),
        end_date=today,
    )
    return sched.index[-1].date() if len(sched) else today
