"""
fetch_chain_eod.py — Pure I/O fetcher for ThetaData's EOD greeks chain.

Replaces both fetch_volume_eod.py and fetch_iv_chain.py. Writes the raw
per-contract chain to data/chain_eod/{ticker}/{year}.parquet. All metric
computation (volume aggregation, ATM IV interpolation) is done at read
time in build_features.py SQL against the chain_adj DuckDB view, which
applies split adjustment via the existing split_factors mechanism.

By design:
  * No aggregation. No _aggregate, no _atm_iv_for_expiration, no metric math.
  * No Postgres writes. The only output is parquet.
  * No spot reference. The fetcher never queries underlying_ohlc. This
    structurally eliminates the strike-vs-spot bug class that affected
    the deprecated fetchers.

Parquet schema (10 columns) — see lib/chain_store.py:
  trade_date      actual session the data is from (NOT shifted)
  source_session  = trade_date in this store
  feature_date    = next_trading_day(trade_date); the build_features join key
  expiration, strike (raw), option_type ('C'/'P'),
  volume, implied_vol, delta, iv_error (NULL if endpoint omits the field)

Resumable: dates already present in the store are skipped per (ticker, date).
Re-runs cleanly after partial failures — interrupt and restart safely.

Usage:
    python fetch_chain_eod.py
        (prompts for tickers + date range)
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

import pandas as pd
from tqdm import tqdm

from db import get_connection, read_sql_df
from lib.chain_store import (
    has_data,
    list_tickers as chain_list_tickers,
    loaded_dates,
    write_rows,
)
from lib.market_hours import get_trading_days, last_trading_day, next_trading_day
from lib.parquet_store import list_tickers as list_oi_tickers
from lib.thetadata import (
    TerminalServerError,
    TerminalTimeoutError,
    fetch_greeks_eod_current_day,
    fetch_greeks_eod_raw,
    test_connection,
    today_et,
)

MAX_WORKERS = 4

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Process-wide latch so we only log the iv_error-missing warning once per run
# instead of for every (ticker, date) cell.
_IV_ERROR_WARNED = False


# --- Prompts ---------------------------------------------------------------

def _prompt_tickers() -> list[str]:
    raw = input(
        "Tickers (comma-separated; blank = all tickers in OI store): "
    ).strip()
    if raw:
        return [t.strip().upper() for t in raw.split(",") if t.strip()]
    out = list_oi_tickers()
    if not out:
        raise SystemExit(
            "No tickers entered and OI store is empty — please specify."
        )
    return out


def _prompt_date(label: str) -> date:
    while True:
        raw = input(f"{label} (YYYYMMDD): ").strip()
        try:
            return datetime.strptime(raw, "%Y%m%d").date()
        except ValueError:
            print("  Use YYYYMMDD (e.g. 20240102)")


# --- Projection ------------------------------------------------------------

def _project_chain(raw: pd.DataFrame, fetch_date: date) -> pd.DataFrame:
    """Project the vendor's raw EOD greeks DataFrame into the 10-column
    chain_eod schema. Adds source_session and feature_date."""
    global _IV_ERROR_WARNED

    if raw.empty:
        return raw

    # trade_date: prefer the vendor's value (handles ranges/holidays correctly),
    # fall back to the fetch_date we requested.
    if "trade_date" in raw.columns:
        td = pd.to_datetime(raw["trade_date"], errors="coerce").dt.date
        td = td.fillna(fetch_date)
    else:
        td = pd.Series([fetch_date] * len(raw))

    # option_type: vendor may use 'right' or 'option_type'; values C/P or CALL/PUT
    src_otype = raw.get("option_type")
    if src_otype is None:
        src_otype = raw.get("right")
    if src_otype is None:
        # Can't normalize without the field; drop everything.
        return pd.DataFrame()
    otype = src_otype.astype(str).str.strip().str.upper().map(
        lambda s: "C" if s in ("CALL", "C") else ("P" if s in ("PUT", "P") else None)
    )

    # iv_error: opportunistic. Log once if absent.
    if "iv_error" in raw.columns:
        iv_err = pd.to_numeric(raw["iv_error"], errors="coerce")
    else:
        if not _IV_ERROR_WARNED:
            log.warning(
                "ThetaData EOD greeks response has no 'iv_error' column — "
                "storing as NULL. (Filter-by-iv_error in IV SQL will be a no-op.)"
            )
            _IV_ERROR_WARNED = True
        iv_err = pd.Series([float("nan")] * len(raw))

    # Compute feature_date once per unique session, not per row. A single
    # fetch uses start_date == end_date so there's typically just one
    # unique value; the per-row .apply() was calling pandas_market_calendars
    # ~10k times per dense-chain response, dominating wall-clock and
    # holding the GIL (which also serialized the 4-worker pool).
    fd_map = {d: next_trading_day(d) for d in td.unique() if d is not None}
    out = pd.DataFrame({
        "trade_date":     td,
        "source_session": td,
        "feature_date":   td.map(fd_map),
        "expiration":     pd.to_datetime(raw["expiration"], errors="coerce").dt.date,
        "strike":         pd.to_numeric(raw["strike"], errors="coerce"),
        "option_type":    otype,
        "volume":         pd.to_numeric(raw.get("volume", 0), errors="coerce").fillna(0).astype("int64"),
        "implied_vol":    pd.to_numeric(
                              raw["implied_vol"] if "implied_vol" in raw.columns else raw.get("iv"),
                              errors="coerce"),
        "delta":          pd.to_numeric(raw.get("delta"), errors="coerce"),
        "iv_error":       iv_err,
    })

    # Drop rows missing any essential identifier or with unmapped option_type.
    out = out.dropna(subset=["trade_date", "expiration", "strike", "option_type"])
    # Drop rows with non-positive or null IV — non-traded contracts often have
    # IV=0 or NaN from the solver; keep zero-volume contracts in the chain but
    # require a usable IV for them to contribute to anything downstream.
    out = out[out["implied_vol"].notna() & (out["implied_vol"] > 0)]
    return out


# --- Per-ticker fetch ------------------------------------------------------

def fetch_ticker(ticker: str, fetch_dates: list[date]) -> int:
    """For each fetch_date not already in the store, call the EOD greeks
    endpoint and append the projected rows to the parquet store. Returns
    rows written this run."""
    if not fetch_dates:
        return 0

    already = loaded_dates(ticker)
    todo = [d for d in fetch_dates if d not in already]
    if not todo:
        log.info("  %s: 0/%d new dates (all loaded)", ticker, len(fetch_dates))
        return 0
    if len(todo) < len(fetch_dates):
        log.info("  %s: %d/%d already loaded, %d to fetch",
                 ticker, len(fetch_dates) - len(todo), len(fetch_dates), len(todo))

    frames: list[pd.DataFrame] = []
    # Compute once — ThetaData rejects expiration=* for today's date in ET.
    # Historical dates use the efficient wildcard; today uses the per-expiration path.
    _today = today_et()
    for fd in todo:
        try:
            if fd == _today:
                raw = fetch_greeks_eod_current_day(ticker, fd)
            else:
                raw = fetch_greeks_eod_raw(ticker, fd)
        except (TerminalTimeoutError, TerminalServerError) as exc:
            log.warning("  TIMEOUT/ERROR %s %s: %s", ticker, fd, exc)
            continue
        except Exception as exc:
            log.warning("  FAIL %s %s: %s", ticker, fd, exc)
            continue

        if raw.empty:
            continue

        projected = _project_chain(raw, fd)
        if not projected.empty:
            frames.append(projected)

    if not frames:
        log.info("  %s: no chain rows produced", ticker)
        return 0

    combined = pd.concat(frames, ignore_index=True)
    by_year = write_rows(ticker, combined)
    total = sum(by_year.values())
    for y, n in sorted(by_year.items()):
        log.info("    %s/%d.parquet → %d rows total", ticker, y, n)
    return total


# --- Main ------------------------------------------------------------------

def main() -> None:
    print("=== OI_Research — EOD greeks chain fetch (vol + IV refactor) ===\n")
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
        futures = {pool.submit(fetch_ticker, t, fetch_dates): t for t in tickers}
        with tqdm(total=len(futures), unit="tk", ncols=90, desc="chain") as bar:
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
