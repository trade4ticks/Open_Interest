"""
fetch_iv_chain.py — Pull 15:45 IV chain for one or more tickers from
ThetaData and upsert aggregated metrics to option_iv_daily.

Usage:
    python fetch_iv_chain.py
    (prompts for tickers + date range)

Date alignment
--------------
ThetaData's historical greeks endpoint returns data for a specific trading
day T-1. We store the result as trade_date = next_trading_day(T-1) = T, so
it lands in the same daily_features row as OI and OHLC covering T.

Per-expiration calls
--------------------
The /v3/option/history/greeks/first_order endpoint requires a specific
expiration date (no wildcard). We identify the 4-6 expirations that bracket
the 7d, 30d, and 90d DTE targets for each date in the range, then call
once per expiration. Calls are chunked at ≤28 days to stay within API
limits; retry logic mirrors fetch_oi.py.

ATM IV interpolation (per tenor)
---------------------------------
1. Find two expirations E1 < E2 bracketing target DTE.
2. For each expiration, take calls (right='C') and find two adjacent strikes
   s1 < s2 that bracket the underlying_price; linearly interpolate IV.
3. Weight the two expiration IVs by DTE position between them → atm_iv.

25-delta IV (at ~30d expiration)
---------------------------------
delta is returned by ThetaData. For the ~30d expiration:
- Find call where delta ≈ 0.25 → iv_25d_call_30d
- Find put  where delta ≈ -0.25 → iv_25d_put_30d
Linear interpolation between the two nearest delta rows.
"""
from __future__ import annotations

import logging
import math
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd

from db import get_connection, read_sql_df
from lib.market_hours import get_trading_days, last_trading_day, next_trading_day
from lib.parquet_store import list_tickers as list_oi_tickers, read_range as read_oi_range
from lib.thetadata import (
    NoDataError,
    TerminalServerError,
    TerminalTimeoutError,
    fetch_greeks_1545,
    test_connection,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Target tenors in calendar days
_TARGET_DTES = [7, 30, 90]
# Maximum days per API call (≤1 month)
_CHUNK_DAYS  = 28

_UPSERT_SQL = """
INSERT INTO option_iv_daily (
    ticker, trade_date,
    atm_iv_7d, atm_iv_30d, atm_iv_90d,
    iv_25d_call_30d, iv_25d_put_30d
) VALUES (
    %(ticker)s, %(trade_date)s,
    %(atm_iv_7d)s, %(atm_iv_30d)s, %(atm_iv_90d)s,
    %(iv_25d_call_30d)s, %(iv_25d_put_30d)s
)
ON CONFLICT (ticker, trade_date) DO UPDATE SET
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
        raw = input(f"{label} (YYYY-MM-DD): ").strip()
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            print("  Use YYYY-MM-DD (e.g. 2024-01-02)")


# --- Expiration selection --------------------------------------------------

def _get_expirations(conn, ticker: str, fetch_dates: list[date]) -> list[date]:
    """
    Return the union of expirations needed across all fetch_dates.
    For each date, we need the two expirations bracketing each of 7d, 30d, 90d.

    Uses the OI parquet store (via underlying_ohlc or option_oi_raw table) to
    get the available expirations. Falls back to any expiration seen in the OI
    parquet for that ticker.
    """
    if not fetch_dates:
        return []

    min_date = min(fetch_dates)
    max_date = max(fetch_dates)

    # Pull all distinct expirations from the OI parquet store for this window.
    oi = read_oi_range(ticker, min_date, max_date)
    if oi.empty:
        log.warning("  %s: no expirations found in option_oi_raw for window", ticker)
        return []

    oi["expiration"] = pd.to_datetime(oi["expiration"]).dt.date
    all_exps = sorted(oi["expiration"].unique().tolist())

    needed: set[date] = set()
    for fd in fetch_dates:
        # For each target DTE, pick the two expirations that bracket it.
        for target_dte in _TARGET_DTES:
            target_exp = fd + timedelta(days=target_dte)
            before = [e for e in all_exps if e <= target_exp]
            after  = [e for e in all_exps if e >  target_exp]
            if before:
                needed.add(before[-1])
            if after:
                needed.add(after[0])
            # Extra bracket for robustness.
            if len(before) >= 2:
                needed.add(before[-2])
            if len(after) >= 2:
                needed.add(after[1])

    return sorted(needed)


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

    # If only one side, use the nearest expiration directly.
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


def _delta_iv(exp_df: pd.DataFrame, target_delta: float) -> Optional[float]:
    """
    Interpolate implied_vol at target_delta.
    For calls: target_delta ≈ 0.25 (positive).
    For puts:  target_delta ≈ -0.25 (negative).
    exp_df must have columns: delta, implied_vol; already filtered to C or P.
    """
    sub = exp_df.dropna(subset=["delta", "implied_vol"]).copy()
    sub = sub[sub["implied_vol"] > 0]
    if sub.empty:
        return None

    # Sort by delta ascending.
    sub = sub.sort_values("delta")
    deltas = sub["delta"].tolist()
    ivs    = sub["implied_vol"].tolist()

    # For calls (target_delta > 0): delta decreases as strike rises.
    # For puts (target_delta < 0): delta increases (less negative) as strike rises.
    # We need two adjacent rows bracketing target_delta.
    if target_delta >= 0:
        # Find last row with delta <= target and first with delta > target.
        before_idx = [i for i, d in enumerate(deltas) if d <= target_delta]
        after_idx  = [i for i, d in enumerate(deltas) if d >  target_delta]
    else:
        before_idx = [i for i, d in enumerate(deltas) if d <= target_delta]
        after_idx  = [i for i, d in enumerate(deltas) if d >  target_delta]

    if not before_idx and not after_idx:
        return None
    if not before_idx:
        return ivs[after_idx[0]]
    if not after_idx:
        return ivs[before_idx[-1]]

    i0, i1 = before_idx[-1], after_idx[0]
    return _interp(deltas[i0], ivs[i0], deltas[i1], ivs[i1], target_delta)


def _compute_25d_ivs(day_df: pd.DataFrame, fetch_date: date
                     ) -> tuple[Optional[float], Optional[float]]:
    """
    Return (iv_25d_call_30d, iv_25d_put_30d) using the expiration closest to 30 DTE.
    """
    exps = sorted(day_df["expiration"].unique())
    if not exps:
        return None, None

    target_exp = fetch_date + timedelta(days=30)
    # Pick nearest expiration to 30d (from either side).
    nearest = min(exps, key=lambda e: abs((e - target_exp).days))

    exp_df = day_df[day_df["expiration"] == nearest]

    call_df = exp_df[exp_df["option_type"] == "C"]
    put_df  = exp_df[exp_df["option_type"] == "P"]

    iv_call = _delta_iv(call_df, 0.25)
    iv_put  = _delta_iv(put_df,  -0.25)
    return iv_call, iv_put


# --- Core computation per fetch_date --------------------------------------

def _compute_day_metrics(day_df: pd.DataFrame, fetch_date: date,
                         spot: float) -> dict:
    """
    Given the full chain for one fetch_date, compute all 5 stored metrics.
    Returns a dict with keys: atm_iv_7d, atm_iv_30d, atm_iv_90d,
                               iv_25d_call_30d, iv_25d_put_30d.
    """
    calls = day_df[day_df["option_type"] == "C"]

    atm_7  = _compute_atm_iv(calls, spot, 7,  fetch_date)
    atm_30 = _compute_atm_iv(calls, spot, 30, fetch_date)
    atm_90 = _compute_atm_iv(calls, spot, 90, fetch_date)
    c25, p25 = _compute_25d_ivs(day_df, fetch_date)

    return {
        "atm_iv_7d":       atm_7,
        "atm_iv_30d":      atm_30,
        "atm_iv_90d":      atm_90,
        "iv_25d_call_30d": c25,
        "iv_25d_put_30d":  p25,
    }


# --- Fetch loop ------------------------------------------------------------

def _chunk_dates(fetch_dates: list[date]) -> list[tuple[date, date]]:
    """Split a list of dates into ≤_CHUNK_DAYS-calendar-day chunks."""
    if not fetch_dates:
        return []
    chunks = []
    chunk_start = fetch_dates[0]
    for d in fetch_dates:
        if (d - chunk_start).days >= _CHUNK_DAYS:
            chunks.append((chunk_start, fetch_dates[fetch_dates.index(d) - 1]))
            chunk_start = d
    chunks.append((chunk_start, fetch_dates[-1]))
    return chunks


def fetch_ticker(conn, ticker: str, fetch_dates: list[date]) -> int:
    """
    Fetch 15:45 greeks for all expirations × date chunks, compute IV metrics,
    upsert to option_iv_daily. Returns number of rows upserted.
    """
    if not fetch_dates:
        return 0

    # Identify which expirations we need.
    expirations = _get_expirations(conn, ticker, fetch_dates)
    if not expirations:
        log.warning("  %s: no expirations — cannot compute IV metrics", ticker)
        return 0
    log.info("  %s: %d expirations to fetch", ticker, len(expirations))

    # Load spot (prior close for each fetch date).
    spot_map = {}
    spot_df  = read_sql_df(
        conn,
        "SELECT trade_date, close FROM underlying_ohlc "
        "WHERE ticker = %(t)s AND trade_date = ANY(%(dates)s)",
        {"t": ticker, "dates": fetch_dates},
    )
    if not spot_df.empty:
        spot_df["trade_date"] = pd.to_datetime(spot_df["trade_date"]).dt.date
        spot_map = dict(zip(spot_df["trade_date"], spot_df["close"]))

    # Accumulate chain data: {fetch_date: [df, df, ...]}
    chain_by_date: dict[date, list[pd.DataFrame]] = {d: [] for d in fetch_dates}
    fetch_set = set(fetch_dates)

    chunks = _chunk_dates(fetch_dates)

    for exp in expirations:
        for chunk_start, chunk_end in chunks:
            try:
                raw = fetch_greeks_1545(ticker, exp, chunk_start, chunk_end, timeout=120)
            except (TerminalTimeoutError, TerminalServerError) as exc:
                log.warning("  TIMEOUT %s exp=%s %s→%s: %s",
                            ticker, exp, chunk_start, chunk_end, exc)
                continue
            except Exception as exc:
                log.warning("  ERROR   %s exp=%s %s→%s: %s",
                            ticker, exp, chunk_start, chunk_end, exc)
                continue

            if raw.empty:
                continue

            raw["trade_date"] = pd.to_datetime(raw["trade_date"]).dt.date
            raw = raw[raw["trade_date"].isin(fetch_set)]
            if raw.empty:
                continue

            for fetch_date, grp in raw.groupby("trade_date"):
                chain_by_date[fetch_date].append(grp)

    # Compute metrics per fetch_date, upsert.
    rows_upserted = 0
    with conn.cursor() as cur:
        for fetch_date in sorted(fetch_dates):
            parts = chain_by_date.get(fetch_date, [])
            if not parts:
                continue

            day_df = pd.concat(parts, ignore_index=True)
            spot   = spot_map.get(fetch_date)
            if not spot or not math.isfinite(spot) or spot <= 0:
                log.warning("  %s %s: no valid spot — skipping IV", ticker, fetch_date)
                continue

            metrics    = _compute_day_metrics(day_df, fetch_date, spot)
            trade_date = next_trading_day(fetch_date)

            cur.execute(_UPSERT_SQL, {
                "ticker":     ticker,
                "trade_date": trade_date,
                **metrics,
            })
            rows_upserted += 1

    if rows_upserted:
        conn.commit()
        log.info("  %s: upserted %d day(s) into option_iv_daily", ticker, rows_upserted)
    return rows_upserted


# --- Main ------------------------------------------------------------------

def main() -> None:
    print("=== OI_Research — IV chain fetch (15:45 greeks) ===\n")
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

    with get_connection() as conn:
        for t in tickers:
            print(f"--- {t} ---")
            fetch_ticker(conn, t, fetch_dates)

    print("\nDone. Run build_features.py next to refresh daily_features.")


if __name__ == "__main__":
    main()
