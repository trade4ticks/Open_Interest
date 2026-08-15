"""NYSE trading-day helpers (thin wrapper over pandas_market_calendars)."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo

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


def next_trading_day(ref: date) -> date:
    """Return the first NYSE trading day strictly after ref."""
    sched = _nyse().schedule(
        start_date=ref + timedelta(days=1),
        end_date=ref + timedelta(days=10),
    )
    return sched.index[0].date()


ET = ZoneInfo("America/New_York")


def session_bounds(start: date, end: date) -> dict:
    """{trade_date: (regular_open_et, regular_close_et)} as naive ET datetimes.

    Read from the exchange calendar rather than hardcoded 09:30/16:00 because
    early closes are real and frequent: the day after Thanksgiving, Christmas
    Eve and July 3rd close at 13:00, giving 210 regular-hours minutes instead
    of 390. Anything that classifies bars or counts expected bars against a
    fixed 390 reports those ~15 sessions per decade as defective.
    """
    sched = _nyse().schedule(start_date=start, end_date=end)
    out: dict = {}
    for ts, row in sched.iterrows():
        o = row["market_open"].tz_convert(ET).tz_localize(None).to_pydatetime()
        c = row["market_close"].tz_convert(ET).tz_localize(None).to_pydatetime()
        out[ts.date()] = (o, c)
    return out


def regular_minutes(start: date, end: date) -> dict:
    """{trade_date: number of regular-hours 1-minute bars expected}.

    390 on a normal session, 210 on a 13:00 early close. The bar stamped at
    the closing minute does not exist (a 09:30 bar covers 09:30:00-09:30:59,
    so the last bar of a 16:00 close is 15:59), hence the exclusive end.
    """
    out: dict = {}
    for d, (o, c) in session_bounds(start, end).items():
        out[d] = max(0, int((c - o).total_seconds() // 60))
    return out
