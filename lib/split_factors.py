"""
Split-adjustment factors for converting raw (vendor-supplied) option strikes
into split-adjusted units that align with yfinance-adjusted underlying prices.

Used by:
  - build_features.py (OI strikes from the parquet store)
  - fetch_volume_eod.py (volume strikes from ThetaData's EOD endpoint)
  - fetch_iv_chain.py (greeks chain strikes from ThetaData's greeks endpoint)

Convention:
    adjusted_strike = raw_strike * adj_factor

adj_factor for a session is the product of (1/ratio) over all splits on or
after that session. Pre-split sessions get adj_factor != 1 (forward split
shrinks raw strike; reverse split grows it); post-split sessions get 1.0.
"""
from __future__ import annotations

import bisect
from datetime import date

import pandas as pd

from db import read_sql_df


def load_splits(conn, ticker: str) -> pd.DataFrame:
    """Pull non-zero split events for one ticker from underlying_ohlc,
    sorted ascending by trade_date. Empty DataFrame for tickers with no splits."""
    df = read_sql_df(
        conn,
        "SELECT trade_date, splits FROM underlying_ohlc "
        "WHERE ticker = %(ticker)s AND splits IS NOT NULL AND splits != 0 "
        "ORDER BY trade_date",
        {"ticker": ticker},
    )
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return df


def make_split_factors(splits_df: pd.DataFrame, dates: list,
                       inclusive: bool = True) -> pd.DataFrame:
    """Return DataFrame(trade_date, adj_factor) for each date in `dates`.

    adj_factor = product of (1/ratio) over the splits on the far side of the
    boundary. Multiply raw strikes (or raw prices) by adj_factor to express
    them in current, post-split units.

    THE BOUNDARY IS NOT THE SAME FOR EVERY DATA FAMILY.

    inclusive=True (default, OI/strike semantics):
        trade_date <= split_date -> adjust.
        Correct for open interest because of the 1-day publication lag: OI
        stamped trade_date T reports the position as of T-1's close, so on the
        ex-date it is still pre-split. See build_features.py, where this
        convention is chosen explicitly.

    inclusive=False (price semantics):
        trade_date <  split_date -> adjust; the ex-date itself is NOT adjusted.
        Correct for a traded price series, which has no such lag — on the
        ex-date the stock already trades post-split. Applying the inclusive
        boundary to prices divides the ex-date by the split ratio, which shows
        up as a ~+3 return for entries ON the ex-date (4:1) and, for a hold
        whose exit lands on it, as a return numerically identical to no
        adjustment at all because the two equal factors cancel.

    Handles forward (ratio>1) and reverse (ratio<1) splits uniformly.
    Tickers with no splits get adj_factor = 1.0 for every date (no-op).
    """
    if splits_df.empty:
        return pd.DataFrame({"trade_date": dates,
                             "adj_factor":  [1.0] * len(dates)})

    split_dates  = splits_df["trade_date"].tolist()   # sorted asc by query
    split_ratios = splits_df["splits"].tolist()

    # Suffix cumulative product: suffix_factors[i] = prod(1/ratio for splits[i:])
    n = len(split_dates)
    suffix_factors = [1.0] * (n + 1)
    for i in range(n - 1, -1, -1):
        suffix_factors[i] = suffix_factors[i + 1] / split_ratios[i]

    # bisect_left puts a date EQUAL to a split date on the pre-split side;
    # bisect_right puts it on the post-split side. That single choice is the
    # whole difference between the two conventions above.
    locate = bisect.bisect_left if inclusive else bisect.bisect_right
    factors = [suffix_factors[locate(split_dates, td)] for td in dates]
    return pd.DataFrame({"trade_date": dates, "adj_factor": factors})


def make_split_factor_map(splits_df: pd.DataFrame, dates: list,
                          inclusive: bool = True) -> dict:
    """Convenience: same as make_split_factors but returns {date: factor} dict."""
    df = make_split_factors(splits_df, dates, inclusive=inclusive)
    return dict(zip(df["trade_date"], df["adj_factor"]))
