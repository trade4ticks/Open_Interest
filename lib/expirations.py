"""
Standard-monthly expiration helper.

A "standard monthly" is the 3rd Friday of the calendar month. When the 3rd
Friday is a market holiday (the Good Friday case is the recurring example),
the listed expiration shifts to the Thursday before. We don't hardcode the
holiday calendar — we cross-check the candidate against the set of
expirations actually listed in the OI parquet data, which IS the exchange's
own answer.
"""
from __future__ import annotations

from datetime import date, timedelta


def third_friday(year: int, month: int) -> date:
    """3rd Friday of (year, month). Pure calendar math, no holiday handling."""
    first = date(year, month, 1)
    days_to_first_friday = (4 - first.weekday()) % 7  # weekday: Mon=0 ... Fri=4
    return first + timedelta(days=days_to_first_friday + 14)


def next_monthly(trade_date: date, listed_expirations: set[date]) -> date | None:
    """
    Next standard monthly expiration on or after `trade_date`.

    Algorithm:
      1. Walk forward month-by-month, computing the candidate 3rd Friday.
      2. Skip months whose 3rd Friday is before `trade_date`.
      3. Snap to listed_expirations: prefer the candidate Friday; if absent,
         fall back to the Thursday before (Good Friday case); otherwise fall
         back to the Friday itself even if not listed.
      4. If we walk 6 months without a hit, return None.

    `listed_expirations` should be the set of all distinct expiration dates
    for the ticker (e.g. pulled from the parquet store).
    """
    for offset in range(0, 6):
        m = (trade_date.month - 1 + offset) % 12 + 1
        y = trade_date.year + (trade_date.month - 1 + offset) // 12
        cand = third_friday(y, m)
        if cand < trade_date:
            continue
        if cand in listed_expirations:
            return cand
        thurs = cand - timedelta(days=1)
        if thurs in listed_expirations:
            return thurs
        return cand   # candidate Friday wins as fallback
    return None


def build_next_monthly_lookup(
    trade_dates: list[date],
    listed_expirations: set[date],
) -> dict[date, date | None]:
    """Pre-compute next_monthly for a list of trade_dates (cheap, ~µs/row)."""
    return {td: next_monthly(td, listed_expirations) for td in trade_dates}
