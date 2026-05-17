"""
fetch_volume_eod.py — Pull EOD option volume for one or more tickers from
ThetaData and upsert aggregated metrics to option_volume_daily.

Usage:
    python fetch_volume_eod.py
    (prompts for tickers + date range)

Date alignment
--------------
The EOD volume report covers trading day T-1 (available at 17:15 ET on T-1).
We store it as trade_date = next_trading_day(fetch_date) = T, so it lands in
the same daily_features row as the OI and OHLC that cover T.

Spot reference
--------------
vol_above_spot, vol_below_spot, vol_within_5pct, vol_within_10pct all use the
close of the fetch date (T-1) as spot. That equals spot_pc for T (prior close).
"""
from __future__ import annotations

import logging
from datetime import date, datetime

import pandas as pd

from db import get_connection, read_sql_df
from lib.market_hours import get_trading_days, last_trading_day, next_trading_day
from lib.thetadata import (
    TerminalServerError,
    TerminalTimeoutError,
    fetch_volume_eod,
    test_connection,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_UPSERT_SQL = """
INSERT INTO option_volume_daily (
    ticker, trade_date,
    total_call_vol, total_put_vol, total_vol,
    vol_0_30d, vol_31_90d,
    vol_weighted_strike_call, vol_weighted_strike_put, vol_weighted_strike_all,
    vol_above_spot, vol_below_spot, vol_within_5pct, vol_within_10pct,
    weighted_avg_dte_vol
) VALUES (
    %(ticker)s, %(trade_date)s,
    %(total_call_vol)s, %(total_put_vol)s, %(total_vol)s,
    %(vol_0_30d)s, %(vol_31_90d)s,
    %(vol_weighted_strike_call)s, %(vol_weighted_strike_put)s,
    %(vol_weighted_strike_all)s,
    %(vol_above_spot)s, %(vol_below_spot)s,
    %(vol_within_5pct)s, %(vol_within_10pct)s,
    %(weighted_avg_dte_vol)s
)
ON CONFLICT (ticker, trade_date) DO UPDATE SET
    total_call_vol            = EXCLUDED.total_call_vol,
    total_put_vol             = EXCLUDED.total_put_vol,
    total_vol                 = EXCLUDED.total_vol,
    vol_0_30d                 = EXCLUDED.vol_0_30d,
    vol_31_90d                = EXCLUDED.vol_31_90d,
    vol_weighted_strike_call  = EXCLUDED.vol_weighted_strike_call,
    vol_weighted_strike_put   = EXCLUDED.vol_weighted_strike_put,
    vol_weighted_strike_all   = EXCLUDED.vol_weighted_strike_all,
    vol_above_spot            = EXCLUDED.vol_above_spot,
    vol_below_spot            = EXCLUDED.vol_below_spot,
    vol_within_5pct           = EXCLUDED.vol_within_5pct,
    vol_within_10pct          = EXCLUDED.vol_within_10pct,
    weighted_avg_dte_vol      = EXCLUDED.weighted_avg_dte_vol
"""


# --- Prompts ---------------------------------------------------------------

def _prompt_tickers(conn) -> list[str]:
    raw = input(
        "Tickers (comma-separated; blank = all tickers in underlying_ohlc): "
    ).strip()
    if raw:
        return [t.strip().upper() for t in raw.split(",") if t.strip()]
    df = read_sql_df(conn,
                     "SELECT DISTINCT ticker FROM underlying_ohlc ORDER BY ticker")
    out = df["ticker"].tolist()
    if not out:
        raise SystemExit("No tickers in underlying_ohlc — please specify.")
    return out


def _prompt_date(label: str) -> date:
    while True:
        raw = input(f"{label} (YYYY-MM-DD): ").strip()
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            print("  Use YYYY-MM-DD (e.g. 2024-01-02)")


# --- Aggregation -----------------------------------------------------------

def _aggregate(df: pd.DataFrame, prior_close: dict[date, float]) -> list[dict]:
    """
    Aggregate a raw volume DataFrame into one dict per trade_date.

    df columns: trade_date, expiration, strike, option_type, volume
    trade_date here is the ThetaData fetch date (T-1 in pipeline terms).
    prior_close: {fetch_date: close_price}
    """
    if df.empty:
        return []

    records = []
    for fetch_date, grp in df.groupby("trade_date"):
        spot = prior_close.get(fetch_date)
        trade_date = next_trading_day(fetch_date)

        grp = grp.copy()
        grp["dte"] = (grp["expiration"] - fetch_date).dt.days

        calls = grp[grp["option_type"] == "C"]
        puts  = grp[grp["option_type"] == "P"]

        total_call_vol = int(calls["volume"].sum()) if not calls.empty else 0
        total_put_vol  = int(puts["volume"].sum())  if not puts.empty  else 0
        total_vol      = total_call_vol + total_put_vol

        vol_0_30d  = int(grp.loc[grp["dte"] <= 30,          "volume"].sum())
        vol_31_90d = int(grp.loc[(grp["dte"] >= 31) & (grp["dte"] <= 90), "volume"].sum())

        def _vws(subset: pd.DataFrame) -> float | None:
            v = subset["volume"].sum()
            if v == 0:
                return None
            return float((subset["strike"] * subset["volume"]).sum() / v)

        vol_weighted_strike_call = _vws(calls)
        vol_weighted_strike_put  = _vws(puts)
        vol_weighted_strike_all  = _vws(grp)

        if spot and spot > 0:
            vol_above_spot   = int(grp.loc[grp["strike"] > spot,  "volume"].sum())
            vol_below_spot   = int(grp.loc[grp["strike"] < spot,  "volume"].sum())
            vol_within_5pct  = int(grp.loc[(grp["strike"] / spot - 1).abs() <= 0.05, "volume"].sum())
            vol_within_10pct = int(grp.loc[(grp["strike"] / spot - 1).abs() <= 0.10, "volume"].sum())
        else:
            vol_above_spot = vol_below_spot = vol_within_5pct = vol_within_10pct = None

        w_dte = None
        if total_vol > 0:
            w_dte = float((grp["dte"] * grp["volume"]).sum() / total_vol)

        records.append({
            "trade_date":              trade_date,
            "total_call_vol":          total_call_vol,
            "total_put_vol":           total_put_vol,
            "total_vol":               total_vol,
            "vol_0_30d":               vol_0_30d,
            "vol_31_90d":              vol_31_90d,
            "vol_weighted_strike_call": vol_weighted_strike_call,
            "vol_weighted_strike_put":  vol_weighted_strike_put,
            "vol_weighted_strike_all":  vol_weighted_strike_all,
            "vol_above_spot":          vol_above_spot,
            "vol_below_spot":          vol_below_spot,
            "vol_within_5pct":         vol_within_5pct,
            "vol_within_10pct":        vol_within_10pct,
            "weighted_avg_dte_vol":    w_dte,
        })
    return records


def _load_prior_close(conn, ticker: str, fetch_dates: list[date]) -> dict[date, float]:
    """Return {fetch_date: close} from underlying_ohlc for the given dates."""
    if not fetch_dates:
        return {}
    df = read_sql_df(
        conn,
        "SELECT trade_date, close FROM underlying_ohlc "
        "WHERE ticker = %(t)s AND trade_date = ANY(%(dates)s)",
        {"t": ticker, "dates": fetch_dates},
    )
    if df.empty:
        return {}
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return dict(zip(df["trade_date"], df["close"]))


# --- Per-ticker pipeline ---------------------------------------------------

def fetch_ticker(conn, ticker: str, fetch_dates: list[date]) -> int:
    """
    Fetch EOD volume for all fetch_dates in one call, aggregate per-day,
    upsert to option_volume_daily. Returns number of rows upserted.
    """
    if not fetch_dates:
        return 0

    start = min(fetch_dates)
    end   = max(fetch_dates)

    try:
        raw = fetch_volume_eod(ticker, start, end)
    except (TerminalTimeoutError, TerminalServerError) as exc:
        log.warning("  TIMEOUT/ERROR %s: %s", ticker, exc)
        return 0

    if raw.empty:
        log.info("  %s: no volume data for %s → %s", ticker, start, end)
        return 0

    raw["trade_date"]  = pd.to_datetime(raw["trade_date"]).dt.date
    raw["expiration"]  = pd.to_datetime(raw["expiration"]).dt.date

    # Filter to only the requested fetch dates (API may return extra).
    fetch_set = set(fetch_dates)
    raw = raw[raw["trade_date"].isin(fetch_set)]
    if raw.empty:
        return 0

    prior_close = _load_prior_close(conn, ticker, fetch_dates)
    records     = _aggregate(raw, prior_close)
    if not records:
        return 0

    with conn.cursor() as cur:
        for rec in records:
            cur.execute(_UPSERT_SQL, {"ticker": ticker, **rec})
    conn.commit()

    log.info("  %s: upserted %d day(s) into option_volume_daily", ticker, len(records))
    return len(records)


# --- Main ------------------------------------------------------------------

def main() -> None:
    print("=== OI_Research — EOD option volume fetch ===\n")

    with get_connection() as conn:
        tickers = _prompt_tickers(conn)
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

        for t in tickers:
            print(f"--- {t} ---")
            fetch_ticker(conn, t, fetch_dates)

    print("\nDone. Run build_features.py next to refresh daily_features.")


if __name__ == "__main__":
    main()
