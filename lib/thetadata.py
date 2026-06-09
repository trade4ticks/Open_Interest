"""
ThetaData v3 client.

Endpoints used:
  /v3/option/list/expirations
  /v3/option/history/open_interest
  /v3/option/history/greeks/eod
  /v3/option/snapshot/open_interest
  /v3/option/snapshot/greeks/implied_volatility
  /v3/option/history/eod
  /v3/option/history/greeks/first_order
  /v3/option/history/quote
"""
from __future__ import annotations

import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from config import THETADATA_BASE_URL

log = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 60

# ThetaData's reference timezone for "current day" — used to decide whether
# to use the expiration=* wildcard (historical) or the per-expiration path
# (current day, which rejects the wildcard with a 400 error).
_ET = ZoneInfo("America/New_York")


# --- Exceptions ------------------------------------------------------------

class ThetaDataError(Exception):
    pass

class NoDataError(ThetaDataError):       """HTTP 472 — empty response."""
class RateLimitError(ThetaDataError):    """HTTP 429."""
class ServerDisconnectedError(ThetaDataError): """HTTP 474."""
class LargeRequestError(ThetaDataError): """HTTP 570 — split needed."""
class TerminalTimeoutError(ThetaDataError): """Read timeout."""
class TerminalServerError(ThetaDataError):  """HTTP 500."""


_STATUS_EXC = {
    429: RateLimitError,
    472: NoDataError,
    474: ServerDisconnectedError,
    570: LargeRequestError,
}


# --- HTTP layer ------------------------------------------------------------

def _get(endpoint: str, params: dict, timeout: int = _DEFAULT_TIMEOUT) -> dict | list:
    base   = f"{THETADATA_BASE_URL}{endpoint}"
    params = {**params, "format": "json"}
    # Build query string manually so wildcard (*) is not percent-encoded to %2A.
    # All our param values are symbols, dates, numbers, or * — none need encoding.
    qs   = "&".join(f"{k}={v}" for k, v in params.items())
    url  = f"{base}?{qs}"
    try:
        resp = requests.get(url, timeout=timeout)
    except requests.exceptions.ReadTimeout:
        raise TerminalTimeoutError(f"Read timeout after {timeout}s")
    except requests.exceptions.ConnectionError:
        raise ConnectionError(
            f"Cannot reach ThetaData at {THETADATA_BASE_URL}. "
            "Is Tailscale up and the terminal running?"
        )
    if resp.status_code == 500:
        raise TerminalServerError(f"HTTP 500: {resp.text[:200]}")
    exc = _STATUS_EXC.get(resp.status_code)
    if exc:
        raise exc(f"HTTP {resp.status_code}: {resp.text[:200]}")
    resp.raise_for_status()
    return resp.json()


def _parse_rows(data: dict | list) -> list[dict]:
    """Normalise the two ThetaData v3 response shapes to a list of dicts."""
    if not data:
        return []
    if isinstance(data, dict) and "header" in data and "response" in data:
        fields = data["header"].get("format", [])
        return [dict(zip(fields, row)) for row in (data.get("response") or []) if row]
    if isinstance(data, dict):
        keys = list(data.keys())
        first_list = next((data[k] for k in keys if isinstance(data[k], list)), None)
        if first_list is None:
            return []
        n = len(first_list)
        return [
            {k: (data[k][i] if isinstance(data[k], list) and i < len(data[k]) else data[k])
             for k in keys}
            for i in range(n)
        ]
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    return []


def _parse_ymd(raw) -> date | None:
    """Accept 'YYYY-MM-DD', 'YYYYMMDD' or 20240102 → date."""
    if raw is None:
        return None
    s = str(raw)
    if len(s) >= 10 and s[4] == "-":
        try:
            return date(int(s[:4]), int(s[5:7]), int(s[8:10]))
        except ValueError:
            return None
    try:
        s = str(int(s))
    except ValueError:
        return None
    if len(s) == 8:
        try:
            return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        except ValueError:
            return None
    return None


# --- Public API ------------------------------------------------------------

def today_et() -> date:
    """Return today's date in US Eastern Time.

    ThetaData defines 'current day' in ET.  Callers use this to decide
    whether to use the expiration=* wildcard (historical dates) or the
    per-expiration path (today, which rejects the wildcard with a 400 error).

    Prefer this over date.today() so the VPS running in UTC doesn't get an
    off-by-one at the ET midnight boundary.
    """
    return datetime.now(_ET).date()


def list_expirations(symbol: str) -> list[date]:
    """All expirations the terminal knows about for `symbol` (sorted ascending).

    Kept as a utility / for `test_connection`. The OI fetch path no longer
    needs it — `fetch_oi_day` returns expirations inline with the rows.
    """
    data = _get("/v3/option/list/expirations", {"symbol": symbol.upper()})
    out: list[date] = []
    for r in _parse_rows(data):
        d = _parse_ymd(r.get("expiration"))
        if d is not None:
            out.append(d)
    return sorted(set(out))


def fetch_oi_day(symbol: str, trading_day: date,
                 timeout: int = _DEFAULT_TIMEOUT) -> pd.DataFrame:
    """
    Fetch the entire OI chain for one ticker on one trading day in one HTTP call.

    Endpoint: /v3/option/history/open_interest with `expiration=*&date=YYYYMMDD`
    (per ThetaData docs — strike defaults to *, right defaults to both).

    Returns DataFrame[trade_date, expiration, strike, option_type, open_interest].
    Empty DataFrame iff terminal returned no data (NOT on transport errors).
    """
    params = {
        "symbol":     symbol.upper(),
        "expiration": "*",
        "date":       trading_day.strftime("%Y%m%d"),
    }

    try:
        data = _get("/v3/option/history/open_interest", params, timeout=timeout)
    except NoDataError:
        return pd.DataFrame()
    except RateLimitError:
        log.warning("Rate limited — sleeping 60s, retrying once (%s %s)",
                    symbol, trading_day)
        time.sleep(60)
        data = _get("/v3/option/history/open_interest", params, timeout=timeout)
    except ServerDisconnectedError:
        log.warning("Server disconnected — sleeping 10s, retrying once (%s %s)",
                    symbol, trading_day)
        time.sleep(10)
        data = _get("/v3/option/history/open_interest", params, timeout=timeout)
    # 570 (LargeRequestError) is unlikely for one ticker on one day; if it
    # happens, let it propagate so the caller can flag the day for retry.

    rows = _parse_rows(data)
    if not rows:
        return pd.DataFrame()

    records = []
    for row in rows:
        # Trading date — should equal trading_day, but parse defensively.
        d = _parse_ymd(row.get("date"))
        if d is None:
            d = trading_day

        exp = _parse_ymd(row.get("expiration"))
        if exp is None:
            continue

        oi = row.get("open_interest")
        if oi is None or oi == 0:
            continue   # zero OI is uninteresting for research

        # ThetaData's API field is named "right"; we normalise to option_type ('C'/'P').
        raw = str(row.get("right") or "").strip().lower()
        option_type = (
            "C" if raw in ("c", "call") else
            "P" if raw in ("p", "put") else
            raw.upper()
        )
        if option_type not in ("C", "P"):
            continue

        records.append({
            "trade_date":    d,
            "expiration":    exp,
            "strike":        row.get("strike"),
            "option_type":   option_type,
            "open_interest": oi,
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["strike"]        = pd.to_numeric(df["strike"],        errors="coerce")
    df["open_interest"] = pd.to_numeric(df["open_interest"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["strike", "open_interest"])
    return df


def fetch_oi_snapshot(symbol: str, trade_date: date,
                      timeout: int = _DEFAULT_TIMEOUT) -> pd.DataFrame:
    """
    Fetch the CURRENT OI chain for one ticker (entire chain, all expirations).

    Endpoint: /v3/option/snapshot/open_interest with `expiration=*` (strike
    and right default to *). The history endpoint typically lags by ~1 day
    (today's row isn't in /history/open_interest yet at 7am ET); the
    snapshot endpoint is the only way to capture today's chain on the day
    itself.

    `trade_date` is the date the caller wants stamped on every row. Pass
    `last_trading_day(today)` so weekend / holiday runs anchor to the most
    recent session (the OI hasn't changed since then).

    Returns DataFrame[trade_date, expiration, strike, option_type, open_interest].
    Empty DataFrame iff terminal returned no data (NOT on transport errors).
    """
    params = {
        "symbol":     symbol.upper(),
        "expiration": "*",
    }

    try:
        data = _get("/v3/option/snapshot/open_interest", params, timeout=timeout)
    except NoDataError:
        return pd.DataFrame()
    except RateLimitError:
        log.warning("Rate limited — sleeping 60s, retrying once (snapshot %s)", symbol)
        time.sleep(60)
        data = _get("/v3/option/snapshot/open_interest", params, timeout=timeout)
    except ServerDisconnectedError:
        log.warning("Server disconnected — sleeping 10s, retrying once (snapshot %s)", symbol)
        time.sleep(10)
        data = _get("/v3/option/snapshot/open_interest", params, timeout=timeout)

    rows = _parse_rows(data)
    if not rows:
        return pd.DataFrame()

    records = []
    for row in rows:
        exp = _parse_ymd(row.get("expiration"))
        if exp is None:
            continue

        oi = row.get("open_interest")
        if oi is None or oi == 0:
            continue

        raw = str(row.get("right") or "").strip().lower()
        option_type = (
            "C" if raw in ("c", "call") else
            "P" if raw in ("p", "put") else
            raw.upper()
        )
        if option_type not in ("C", "P"):
            continue

        records.append({
            "trade_date":    trade_date,
            "expiration":    exp,
            "strike":        row.get("strike"),
            "option_type":   option_type,
            "open_interest": oi,
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["strike"]        = pd.to_numeric(df["strike"],        errors="coerce")
    df["open_interest"] = pd.to_numeric(df["open_interest"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["strike", "open_interest"])
    return df


def fetch_underlying_snapshot(symbol: str,
                              timeout: int = _DEFAULT_TIMEOUT,
                              ) -> tuple[float | None, datetime | None]:
    """Read the live underlying mid + its timestamp from the IV snapshot.

    Endpoint: /v3/option/snapshot/greeks/implied_volatility
    Every row in the response carries the same `underlying_price` and
    `underlying_timestamp` (a single per-call snapshot, not per-contract);
    we read both fields off the first row.  Used by fetch_ohlc_premarket
    as a live premarket-open proxy: yfinance won't serve current-day
    premarket bars before the open, and our ThetaData subscription doesn't
    include regular-stock endpoints — but this options endpoint exposes the
    live underlying price as a side-effect and the values have been
    confirmed current during premarket.

    Returns
    -------
    (price, ts)   — underlying mid + tz-aware ET datetime, when both are
                    parseable from the first row.
    (None, None)  — empty response, missing fields, non-finite / non-positive
                    price, or unparseable timestamp.  Caller writes nothing
                    (safe-failure mode — no stale or null price written).

    Per ThetaData v3 docs `underlying_timestamp` is an ISO-8601 naive string
    `YYYY-MM-DDTHH:mm:ss.SSS` (no offset).  ThetaData's broader timestamp
    convention is ET (the open-interest snapshot page references "06:30 ET"
    for the OCC publish, and the IV article defines `ms_of_day` as ms-since-
    midnight-ET).  We localise to America/New_York here so the downstream
    open_asof_ts TIMESTAMPTZ column is correct.
    """
    params = {
        "symbol":     symbol.upper(),
        "expiration": "*",
    }
    endpoint = "/v3/option/snapshot/greeks/implied_volatility"

    try:
        data = _get(endpoint, params, timeout=timeout)
    except NoDataError:
        return None, None
    except RateLimitError:
        log.warning("Rate limited — sleeping 60s, retrying once (iv_snapshot %s)", symbol)
        time.sleep(60)
        data = _get(endpoint, params, timeout=timeout)
    except ServerDisconnectedError:
        log.warning("Server disconnected — sleeping 10s, retrying once (iv_snapshot %s)", symbol)
        time.sleep(10)
        data = _get(endpoint, params, timeout=timeout)

    rows = _parse_rows(data)
    if not rows:
        return None, None

    first = rows[0]
    raw_price = first.get("underlying_price")
    raw_ts    = first.get("underlying_timestamp")
    if raw_price is None or raw_ts is None:
        return None, None

    # Price must be finite + positive — catches NaN, Inf, 0, negatives.
    try:
        price = float(raw_price)
    except (TypeError, ValueError):
        return None, None
    if not math.isfinite(price) or price <= 0:
        return None, None

    # Naive ISO string per docs; localise to ET (ThetaData convention).
    # pd.Timestamp parses the YYYY-MM-DDTHH:mm:ss.SSS format directly and
    # returns NaT on garbage (caught below).
    try:
        ts_naive = pd.Timestamp(raw_ts)
    except (TypeError, ValueError):
        return None, None
    if pd.isna(ts_naive):
        return None, None
    if ts_naive.tzinfo is None:
        ts_aware = ts_naive.tz_localize(_ET)
    else:
        ts_aware = ts_naive.tz_convert(_ET)

    return price, ts_aware.to_pydatetime()


def fetch_volume_eod(symbol: str, start_date: date, end_date: date,
                     timeout: int = _DEFAULT_TIMEOUT) -> pd.DataFrame:
    """
    Fetch EOD option volume for a date range in one HTTP call.

    Endpoint: /v3/option/history/eod with expiration=* (all expirations).
    Report is available at 17:15 ET, well before the 7am pipeline run.

    Returns DataFrame[trade_date, expiration, strike, option_type, volume].
    Empty DataFrame if terminal returned no data.
    """
    params = {
        "symbol":     symbol.upper(),
        "expiration": "*",
        "start_date": start_date.strftime("%Y%m%d"),
        "end_date":   end_date.strftime("%Y%m%d"),
    }

    try:
        data = _get("/v3/option/history/eod", params, timeout=timeout)
    except NoDataError:
        return pd.DataFrame()
    except RateLimitError:
        log.warning("Rate limited — sleeping 60s, retrying once (vol_eod %s)", symbol)
        time.sleep(60)
        data = _get("/v3/option/history/eod", params, timeout=timeout)
    except ServerDisconnectedError:
        log.warning("Server disconnected — sleeping 10s, retrying once (vol_eod %s)", symbol)
        time.sleep(10)
        data = _get("/v3/option/history/eod", params, timeout=timeout)

    rows = _parse_rows(data)
    if not rows:
        return pd.DataFrame()

    records = []
    for row in rows:
        # The EOD endpoint has no explicit "date" field — the trade date is
        # embedded in the "last_trade" or "created" ISO timestamp (e.g.
        # "2019-01-02T17:40:02.160").  Extract the YYYY-MM-DD prefix.
        date_val = row.get("date") or row.get("trade_date")
        if date_val is None:
            ts = str(row.get("last_trade") or row.get("created") or "")
            date_val = ts[:10] if len(ts) >= 10 else None
        d = _parse_ymd(date_val)
        if d is None:
            continue

        exp = _parse_ymd(row.get("expiration"))
        if exp is None:
            continue

        vol = row.get("volume")
        if vol is None:
            continue

        raw = str(row.get("right") or "").strip().lower()
        option_type = (
            "C" if raw in ("c", "call") else
            "P" if raw in ("p", "put") else
            raw.upper()
        )
        if option_type not in ("C", "P"):
            continue

        records.append({
            "trade_date":  d,
            "expiration":  exp,
            "strike":      row.get("strike"),
            "option_type": option_type,
            "volume":      vol,
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["strike", "volume"])
    df = df.drop_duplicates(subset=["trade_date", "expiration", "strike", "option_type"])
    return df


def fetch_greeks_1545(symbol: str, expiration: date,
                      start_date: date, end_date: date,
                      timeout: int = _DEFAULT_TIMEOUT) -> pd.DataFrame:
    """
    Fetch 15:45 first-order Greeks for one expiration over a date range.

    Endpoint: /v3/option/history/greeks/first_order
    Params: interval=5m, start_time=15:45, end_time=15:45, strike_range=10
    (21 strikes around ATM — keeps response size small).

    expiration must be a specific date (no wildcard — API requirement).
    Callers should loop over the expirations they need and call once per.

    Returns DataFrame[trade_date, expiration, strike, option_type,
                      implied_vol, delta, underlying_price].
    Empty DataFrame if terminal returned no data.
    """
    params = {
        "symbol":     symbol.upper(),
        "expiration": expiration.strftime("%Y%m%d"),
        "start_date": start_date.strftime("%Y%m%d"),
        "end_date":   end_date.strftime("%Y%m%d"),
        "interval":   "5m",
        "start_time": "15:45",
        "end_time":   "15:45",
        "strike_range": 10,
    }

    try:
        data = _get("/v3/option/history/greeks/first_order", params, timeout=timeout)
    except NoDataError:
        return pd.DataFrame()
    except RateLimitError:
        log.warning("Rate limited — sleeping 60s, retrying once (greeks %s %s)",
                    symbol, expiration)
        time.sleep(60)
        data = _get("/v3/option/history/greeks/first_order", params, timeout=timeout)
    except ServerDisconnectedError:
        log.warning("Server disconnected — sleeping 10s, retrying once (greeks %s %s)",
                    symbol, expiration)
        time.sleep(10)
        data = _get("/v3/option/history/greeks/first_order", params, timeout=timeout)

    rows = _parse_rows(data)
    if not rows:
        return pd.DataFrame()

    records = []
    for row in rows:
        d = _parse_ymd(row.get("date"))
        if d is None:
            continue

        exp = _parse_ymd(row.get("expiration"))
        if exp is None:
            exp = expiration

        iv = row.get("implied_vol") or row.get("iv")
        delta = row.get("delta")
        if iv is None or delta is None:
            continue

        underlying = row.get("underlying_price") or row.get("underlying")

        raw = str(row.get("right") or "").strip().lower()
        option_type = (
            "C" if raw in ("c", "call") else
            "P" if raw in ("p", "put") else
            raw.upper()
        )
        if option_type not in ("C", "P"):
            continue

        records.append({
            "trade_date":       d,
            "expiration":       exp,
            "strike":           row.get("strike"),
            "option_type":      option_type,
            "implied_vol":      iv,
            "delta":            delta,
            "underlying_price": underlying,
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["strike"]           = pd.to_numeric(df["strike"],           errors="coerce")
    df["implied_vol"]      = pd.to_numeric(df["implied_vol"],      errors="coerce")
    df["delta"]            = pd.to_numeric(df["delta"],            errors="coerce")
    df["underlying_price"] = pd.to_numeric(df["underlying_price"], errors="coerce")
    df = df.dropna(subset=["strike", "implied_vol", "delta"])
    return df


def fetch_greeks_eod(symbol: str, trade_date: date,
                     timeout: int = _DEFAULT_TIMEOUT) -> pd.DataFrame:
    """
    Fetch EOD greeks for the entire option chain on one trading day.

    Endpoint: /v3/option/history/greeks/eod with expiration=* (all expirations).
    Values are pre-computed from closing prices — faster than first_order.

    Returns DataFrame[trade_date, expiration, strike, option_type,
                      implied_vol, delta, underlying_price].
    Empty DataFrame if terminal returned no data.
    """
    date_str = trade_date.strftime("%Y%m%d")
    params = {
        "symbol":     symbol.upper(),
        "expiration": "*",
        "start_date": date_str,
        "end_date":   date_str,
    }

    try:
        data = _get("/v3/option/history/greeks/eod", params, timeout=timeout)
    except NoDataError:
        return pd.DataFrame()
    except RateLimitError:
        log.warning("Rate limited — sleeping 60s, retrying once (greeks_eod %s)", symbol)
        time.sleep(60)
        data = _get("/v3/option/history/greeks/eod", params, timeout=timeout)
    except ServerDisconnectedError:
        log.warning("Server disconnected — sleeping 10s, retrying once (greeks_eod %s)", symbol)
        time.sleep(10)
        data = _get("/v3/option/history/greeks/eod", params, timeout=timeout)

    rows = _parse_rows(data)
    if not rows:
        return pd.DataFrame()

    records = []
    for row in rows:
        exp = _parse_ymd(row.get("expiration"))
        if exp is None:
            continue

        iv = row.get("implied_vol")
        if iv is None or iv == 0:
            continue

        raw = str(row.get("right") or "").strip().lower()
        option_type = (
            "C" if raw in ("c", "call") else
            "P" if raw in ("p", "put") else
            raw.upper()
        )
        if option_type not in ("C", "P"):
            continue

        records.append({
            "trade_date":       trade_date,
            "expiration":       exp,
            "strike":           row.get("strike"),
            "option_type":      option_type,
            "implied_vol":      iv,
            "delta":            row.get("delta"),
            "underlying_price": row.get("underlying_price"),
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["strike"]           = pd.to_numeric(df["strike"],           errors="coerce")
    df["implied_vol"]      = pd.to_numeric(df["implied_vol"],      errors="coerce")
    df["delta"]            = pd.to_numeric(df["delta"],            errors="coerce")
    df["underlying_price"] = pd.to_numeric(df["underlying_price"], errors="coerce")
    df = df.dropna(subset=["strike", "implied_vol"])
    df = df.drop_duplicates(subset=["expiration", "strike", "option_type"])
    return df


def fetch_greeks_eod_raw(symbol: str, trade_date: date,
                         timeout: int = _DEFAULT_TIMEOUT) -> pd.DataFrame:
    """
    Same endpoint as fetch_greeks_eod, but returns ALL fields from the
    response with NO field-filtering and NO row-filtering.

    Used by fetch_chain_eod.py to populate the raw chain parquet store.
    The legacy fetch_greeks_eod drops volume, iv_error, and any other
    field its 7-column projection didn't list; the raw variant preserves
    everything for downstream projection in the new fetcher.

    Returns a DataFrame whose columns match the vendor's field names
    exactly (whatever the EOD greeks response includes). Empty DataFrame
    on NoDataError or empty response.
    """
    date_str = trade_date.strftime("%Y%m%d")
    params = {
        "symbol":     symbol.upper(),
        "expiration": "*",
        "start_date": date_str,
        "end_date":   date_str,
    }

    try:
        data = _get("/v3/option/history/greeks/eod", params, timeout=timeout)
    except NoDataError:
        return pd.DataFrame()
    except RateLimitError:
        log.warning("Rate limited — sleeping 60s, retrying once (greeks_eod_raw %s)", symbol)
        time.sleep(60)
        data = _get("/v3/option/history/greeks/eod", params, timeout=timeout)
    except ServerDisconnectedError:
        log.warning("Server disconnected — sleeping 10s, retrying once (greeks_eod_raw %s)", symbol)
        time.sleep(10)
        data = _get("/v3/option/history/greeks/eod", params, timeout=timeout)

    rows = _parse_rows(data)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def fetch_greeks_eod_raw_for_expiration(
    symbol: str,
    expiration: date,
    trade_date: date,
    timeout: int = _DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Fetch raw EOD greeks for ONE specific expiration on one trading day.

    Same endpoint as fetch_greeks_eod_raw but passes expiration=YYYYMMDD
    instead of expiration=*.  Required for current-day fetches: ThetaData
    rejects expiration=* for today's date with a 400 error.

    Carries identical retry-and-backoff logic to fetch_greeks_eod_raw so
    a transient 429 or disconnect retries before propagating.

    Returns a raw DataFrame (all vendor fields, same shape as
    fetch_greeks_eod_raw).  Empty DataFrame on NoDataError or empty response.
    """
    date_str = trade_date.strftime("%Y%m%d")
    exp_str  = expiration.strftime("%Y%m%d")
    params = {
        "symbol":     symbol.upper(),
        "expiration": exp_str,
        "start_date": date_str,
        "end_date":   date_str,
    }

    try:
        data = _get("/v3/option/history/greeks/eod", params, timeout=timeout)
    except NoDataError:
        return pd.DataFrame()
    except RateLimitError:
        log.warning("Rate limited — sleeping 60s, retrying once (greeks_eod_exp %s %s)",
                    symbol, exp_str)
        time.sleep(60)
        data = _get("/v3/option/history/greeks/eod", params, timeout=timeout)
    except ServerDisconnectedError:
        log.warning("Server disconnected — sleeping 10s, retrying once (greeks_eod_exp %s %s)",
                    symbol, exp_str)
        time.sleep(10)
        data = _get("/v3/option/history/greeks/eod", params, timeout=timeout)

    rows = _parse_rows(data)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def fetch_greeks_eod_current_day(
    symbol: str,
    trade_date: date,
    exp_workers: int = 4,
    timeout: int = _DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Fetch raw EOD greeks for EVERY expiration on the current trading day.

    ThetaData rejects expiration=* for the current calendar date in ET with
    a 400 error.  This function works around that by:
      1. Enumerating expirations via /v3/option/list/expirations (fresh each
         call — new expirations get listed over time; never use stale data).
      2. Fetching each expiration individually via
         fetch_greeks_eod_raw_for_expiration, parallelised across exp_workers
         threads.

    Each per-expiration call carries the same retry-and-backoff logic as
    fetch_greeks_eod_raw — a transient 429 or disconnect on one expiration
    retries before failing, so the returned frame is as complete as possible.
    A failed expiration is logged at WARNING and excluded from the concat;
    the rest are returned.

    Returns a concatenated raw DataFrame in the same shape as
    fetch_greeks_eod_raw.  Empty DataFrame if list_expirations returns nothing
    or all per-expiration calls return empty.
    """
    expirations = list_expirations(symbol)
    if not expirations:
        log.debug("  %s: list_expirations returned nothing — "
                  "no current-day chain to fetch", symbol)
        return pd.DataFrame()

    log.debug("  %s: current-day per-expiration fetch — %d expirations, "
              "%d workers", symbol, len(expirations), exp_workers)

    frames: list[pd.DataFrame] = []

    def _fetch_one(exp: date) -> pd.DataFrame:
        return fetch_greeks_eod_raw_for_expiration(
            symbol, exp, trade_date, timeout=timeout
        )

    with ThreadPoolExecutor(max_workers=exp_workers) as pool:
        fut_to_exp = {pool.submit(_fetch_one, exp): exp for exp in expirations}
        for fut in as_completed(fut_to_exp):
            exp = fut_to_exp[fut]
            try:
                df = fut.result()
                if not df.empty:
                    frames.append(df)
            except Exception as exc:
                log.warning("  %s exp %s: current-day greeks fetch failed — %s",
                            symbol, exp, exc)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def fetch_option_quotes_at(symbol: str, expiration: date, trade_date: date,
                           time_str: str, interval: str = "5m",
                           timeout: int = _DEFAULT_TIMEOUT) -> pd.DataFrame:
    """
    Fetch option bid/ask quotes for one expiration at a specific intraday time.

    Endpoint: /v3/option/history/quote
    Use interval=5m + time_str='09:35' for entry snapshot.
    Use interval=30m + time_str='15:30' for exit snapshot.

    Returns DataFrame[strike, option_type, bid, ask].
    Empty DataFrame if no data.
    """
    params = {
        "symbol":     symbol.upper(),
        "expiration": expiration.strftime("%Y%m%d"),
        "date":       trade_date.strftime("%Y%m%d"),
        "interval":   interval,
        "start_time": time_str,
        "end_time":   time_str,
    }

    try:
        data = _get("/v3/option/history/quote", params, timeout=timeout)
    except NoDataError:
        return pd.DataFrame()
    except RateLimitError:
        log.warning("Rate limited — sleeping 60s, retrying once (quote %s %s)",
                    symbol, trade_date)
        time.sleep(60)
        data = _get("/v3/option/history/quote", params, timeout=timeout)
    except ServerDisconnectedError:
        log.warning("Server disconnected — sleeping 10s, retrying once (quote %s %s)",
                    symbol, trade_date)
        time.sleep(10)
        data = _get("/v3/option/history/quote", params, timeout=timeout)

    rows = _parse_rows(data)
    if not rows:
        return pd.DataFrame()

    records = []
    for row in rows:
        bid = row.get("bid")
        ask = row.get("ask")
        if bid is None or ask is None:
            continue

        raw = str(row.get("right") or "").strip().lower()
        option_type = (
            "C" if raw in ("c", "call") else
            "P" if raw in ("p", "put") else
            raw.upper()
        )
        if option_type not in ("C", "P"):
            continue

        records.append({
            "strike":      row.get("strike"),
            "option_type": option_type,
            "bid":         bid,
            "ask":         ask,
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    df["bid"]    = pd.to_numeric(df["bid"],    errors="coerce")
    df["ask"]    = pd.to_numeric(df["ask"],    errors="coerce")
    df = df.dropna(subset=["strike", "bid", "ask"])
    df = df.drop_duplicates(subset=["strike", "option_type"])
    return df


def test_connection() -> bool:
    try:
        return bool(list_expirations("SPY"))
    except Exception as e:
        log.error("Connection test failed: %s", e)
        return False
