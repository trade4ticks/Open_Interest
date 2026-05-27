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


def make_split_factors(splits_df: pd.DataFrame, dates: list) -> pd.DataFrame:
    """Return DataFrame(trade_date, adj_factor) for each date in `dates`.

    adj_factor = product of (1/ratio) for all splits with split_date >= date.
    Multiply raw strikes by adj_factor to get split-adjusted strikes that
    align with yfinance-adjusted spot prices.

    Boundary convention: trade_date <= split_date -> adjust (split affects this date);
                         trade_date >  split_date -> no adjustment (already past it).
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

    factors = [suffix_factors[bisect.bisect_left(split_dates, td)] for td in dates]
    return pd.DataFrame({"trade_date": dates, "adj_factor": factors})


def make_split_factor_map(splits_df: pd.DataFrame, dates: list) -> dict:
    """Convenience: same as make_split_factors but returns {date: factor} dict."""
    df = make_split_factors(splits_df, dates)
    return dict(zip(df["trade_date"], df["adj_factor"]))
