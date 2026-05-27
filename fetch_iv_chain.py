"""
fetch_iv_chain.py — Pull EOD greeks for one or more tickers from ThetaData
and upsert ATM IV metrics to option_iv_daily.

Usage:
    python fetch_iv_chain.py
    (prompts for tickers + date range)

Endpoint
--------
/v3/option/history/greeks/eod with expiration=* returns the full chain for
one trading day in a single call. Values are pre-computed from closing prices,
so response times are much faster than the on-the-fly first_order endpoint.

Date alignment
--------------
EOD greeks are computed from T-1 closing prices. We store as
trade_date = next_trading_day(T-1) = T, aligning with OI and OHLC.

ATM IV interpolation (per tenor)
---------------------------------
1. Find the two expirations bracketing target DTE (7, 30, 90 calendar days).
2. For each expiration, take calls and interpolate IV linearly between the
   two adjacent strikes that bracket spot.
3. Interpolate the two expiration IVs by DTE position → atm_iv.

25-delta metrics
----------------
iv_25d_call_30d and iv_25d_put_30d are left NULL. EOD IV at OTM strikes is
noisier than near-ATM; the rr/bf/skew derived metrics are unreliable from
EOD data. They can be populated later from a cleaner source.
"""
from __future__ import annotations

import logging
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
from tqdm import tqdm

from db import get_connection, read_sql_df
from lib.market_hours import get_trading_days, last_trading_day, next_trading_day
from lib.parquet_store import list_tickers as list_oi_tickers
from lib.split_factors import load_splits, make_split_factor_map
from lib.thetadata import (
    TerminalServerError,
    TerminalTimeoutError,
    fetch_greeks_eod,
    test_connection,
)

MAX_WORKERS = 4

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Target tenors in calendar days
_TARGET_DTES = [7, 30, 90]

_UPSERT_SQL = """
INSERT INTO option_iv_daily (
    ticker, trade_date, source_session,
    atm_iv_7d, atm_iv_30d, atm_iv_90d,
    iv_25d_call_30d, iv_25d_put_30d
) VALUES (
    %(ticker)s, %(trade_date)s, %(source_session)s,
    %(atm_iv_7d)s, %(atm_iv_30d)s, %(atm_iv_90d)s,
    %(iv_25d_call_30d)s, %(iv_25d_put_30d)s
)
ON CONFLICT (ticker, trade_date) DO UPDATE SET
    source_session  = EXCLUDED.source_session,
    atm_iv_7d       = EXCLUDED.atm_iv_7d,
    atm_iv_30d      = EXCLUDED.atm_iv_30d,
    atm_iv_90d      = EXCLUDED.atm_iv_90d,
    iv_25d_call_30d = EXCLUDED.iv_25d_call_30d,
    iv_25d_put_30d  = EXCLUDED.iv_25d_put_30d
"""


# --- Prompts ---------------------------------------------------------------

def _prompt_tickers() -> list[str]:
    raw = input(
        "Tickers (comma-separated; blank = all tickers in OI store): "
    ).strip()
    if raw:
        return [t.strip().upper() for t in raw.split(",") if t.strip()]
    out = list_oi_tickers()
    if not out:
        raise SystemExit("No tickers in OI store — please specify.")
    return out


def _prompt_date(label: str) -> date:
    while True:
        raw = input(f"{label} (YYYYMMDD): ").strip()
        try:
            return datetime.strptime(raw, "%Y%m%d").date()
        except ValueError:
            print("  Use YYYYMMDD (e.g. 20240102)")


# --- IV interpolation helpers ----------------------------------------------

def _interp(x0: float, y0: float, x1: float, y1: float, x: float) -> float:
    """Linear interpolation at x between (x0,y0) and (x1,y1)."""
    if x1 == x0:
        return (y0 + y1) / 2.0
    return y0 + (y1 - y0) * (x - x0) / (x1 - x0)


def _atm_iv_for_expiration(exp_df: pd.DataFrame, spot: float) -> Optional[float]:
    """
    Given calls for one expiration, interpolate IV at spot.
    exp_df must have columns: strike, implied_vol.
    """
    calls = exp_df.sort_values("strike").dropna(subset=["strike", "implied_vol"])
    calls = calls[calls["implied_vol"] > 0]
    if calls.empty:
        return None

    below = calls[calls["strike"] <= spot]
    above = calls[calls["strike"] >  spot]

    if below.empty and above.empty:
        return None
    if below.empty:
        return float(above.iloc[0]["implied_vol"])
    if above.empty:
        return float(below.iloc[-1]["implied_vol"])

    s0, iv0 = float(below.iloc[-1]["strike"]), float(below.iloc[-1]["implied_vol"])
    s1, iv1 = float(above.iloc[0]["strike"]),  float(above.iloc[0]["implied_vol"])
    return _interp(s0, iv0, s1, iv1, spot)


def _compute_atm_iv(day_df: pd.DataFrame, spot: float, target_dte: int,
                    fetch_date: date) -> Optional[float]:
    """
    Interpolate ATM IV at target_dte using two bracketing expirations.
    day_df: calls for one fetch_date (columns: expiration, strike, implied_vol).
    """
    exps = sorted(day_df["expiration"].unique())
    target_exp = fetch_date + timedelta(days=target_dte)

    before = [e for e in exps if e <= target_exp]
    after  = [e for e in exps if e >  target_exp]

    if not before and not after:
        return None

    if not before:
        exp = after[0]
        return _atm_iv_for_expiration(day_df[day_df["expiration"] == exp], spot)
    if not after:
        exp = before[-1]
        return _atm_iv_for_expiration(day_df[day_df["expiration"] == exp], spot)

    e0, e1 = before[-1], after[0]
    iv0 = _atm_iv_for_expiration(day_df[day_df["expiration"] == e0], spot)
    iv1 = _atm_iv_for_expiration(day_df[day_df["expiration"] == e1], spot)

    if iv0 is None and iv1 is None:
        return None
    if iv0 is None:
        return iv1
    if iv1 is None:
        return iv0

    dte0 = (e0 - fetch_date).days
    dte1 = (e1 - fetch_date).days
    return _interp(dte0, iv0, dte1, iv1, target_dte)


def _compute_day_metrics(day_df: pd.DataFrame, fetch_date: date,
                         spot: float) -> dict:
    """Compute stored IV metrics from the full EOD chain for one fetch_date."""
    calls = day_df[day_df["option_type"] == "C"]
    return {
        "atm_iv_7d":       _compute_atm_iv(calls, spot, 7,  fetch_date),
        "atm_iv_30d":      _compute_atm_iv(calls, spot, 30, fetch_date),
        "atm_iv_90d":      _compute_atm_iv(calls, spot, 90, fetch_date),
        "iv_25d_call_30d": None,
        "iv_25d_put_30d":  None,
    }


# --- Per-ticker pipeline ---------------------------------------------------

def fetch_ticker(conn, ticker: str, fetch_dates: list[date]) -> int:
    """
    Fetch EOD greeks for each fetch_date (one API call per date), compute
    ATM IV metrics, upsert to option_iv_daily. Returns rows upserted.
    """
    if not fetch_dates:
        return 0

    # Load prior close (spot) for each fetch date.
    spot_map: dict[date, float] = {}
    spot_df = read_sql_df(
        conn,
        "SELECT trade_date, close FROM underlying_ohlc "
        "WHERE ticker = %(t)s AND trade_date = ANY(%(dates)s)",
        {"t": ticker, "dates": fetch_dates},
    )
    if not spot_df.empty:
        spot_df["trade_date"] = pd.to_datetime(spot_df["trade_date"]).dt.date
        spot_map = dict(zip(spot_df["trade_date"], spot_df["close"]))

    # Split factors: raw ThetaData strikes need to be multiplied by adj_factor
    # to match the split-adjusted spot from underlying_ohlc. Without this,
    # _atm_iv_for_expiration on a pre-split date finds spot below every strike,
    # falls back to the lowest-strike (deep-ITM) IV, and produces garbage
    # (e.g. atm_iv_30d ~ 1.4-2.6 for 2019 AAPL).
    splits_df     = load_splits(conn, ticker)
    split_factors = make_split_factor_map(splits_df, fetch_dates)

    rows_upserted = 0
    with conn.cursor() as cur:
        for fetch_date in fetch_dates:
            spot = spot_map.get(fetch_date)
            if not spot or not math.isfinite(spot) or spot <= 0:
                log.warning("  %s %s: no spot price — skipping", ticker, fetch_date)
                continue

            try:
                chain = fetch_greeks_eod(ticker, fetch_date)
            except (TerminalTimeoutError, TerminalServerError) as exc:
                log.warning("  TIMEOUT %s %s: %s", ticker, fetch_date, exc)
                continue
            except Exception as exc:
                log.warning("  ERROR   %s %s: %s", ticker, fetch_date, exc)
                continue

            if chain.empty:
                log.info("  %s %s: no chain data", ticker, fetch_date)
                continue

            chain["expiration"] = pd.to_datetime(chain["expiration"]).dt.date

            # Split-adjust strikes so _atm_iv_for_expiration's strike-vs-spot
            # comparison is in consistent units.
            adj_factor = split_factors.get(fetch_date, 1.0)
            if adj_factor != 1.0:
                chain = chain.copy()
                chain["strike"] = chain["strike"] * adj_factor

            metrics    = _compute_day_metrics(chain, fetch_date, spot)
            trade_date = next_trading_day(fetch_date)

            cur.execute(_UPSERT_SQL, {
                "ticker":         ticker,
                "trade_date":     trade_date,
                "source_session": fetch_date,
                **metrics,
            })
            rows_upserted += 1

    if rows_upserted:
        conn.commit()
        log.info("  %s: upserted %d day(s) into option_iv_daily", ticker, rows_upserted)
    return rows_upserted


def _fetch_ticker_isolated(ticker: str, fetch_dates: list[date]) -> int:
    """Open a per-thread DB connection and run fetch_ticker for one ticker."""
    with get_connection() as conn:
        return fetch_ticker(conn, ticker, fetch_dates)


# --- Main ------------------------------------------------------------------

def main() -> None:
    print("=== OI_Research — EOD IV chain fetch ===\n")
    tickers = _prompt_tickers()
    start   = _prompt_date("Fetch start date (T-1 perspective)")
    end     = _prompt_date("Fetch end   date (T-1 perspective)")
    if end < start:
        raise SystemExit("End date must be >= start date.")

    end = min(end, last_trading_day())
    if end < start:
        raise SystemExit("No completed trading days in the requested range.")

    fetch_dates = get_trading_days(start, end)
    if not fetch_dates:
        raise SystemExit("No NYSE trading days in the requested range.")

    print(f"\nFetching {len(tickers)} tickers × {len(fetch_dates)} trading days "
          f"({start} → {end})")

    print("Checking ThetaData ...", end=" ", flush=True)
    if not test_connection():
        raise SystemExit("FAILED — terminal not reachable.")
    print("OK\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_ticker_isolated, t, fetch_dates): t
                   for t in tickers}
        with tqdm(total=len(futures), unit="tk", ncols=90, desc="iv") as bar:
            for fut in as_completed(futures):
                t = futures[fut]
                try:
                    fut.result()
                except (TerminalTimeoutError, TerminalServerError) as exc:
                    log.warning("  TIMEOUT %s: %s", t, exc)
                except Exception as exc:
                    log.warning("  FAIL    %s: %s", t, exc)
                bar.update(1)

    print("\nDone. Run build_features.py next to refresh daily_features.")


if __name__ == "__main__":
    main()
