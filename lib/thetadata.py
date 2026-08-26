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

import json
import logging
import random
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import requests
from pyarrow import csv as pacsv

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
class BadRequestError(ThetaDataError):
    """HTTP 400 — the terminal rejected the request.

    Most often an EOD request for a session that has not closed: the data does
    not exist yet, so the endpoint refuses rather than returning 472. Untyped
    this escaped as a raw requests.HTTPError, bypassing every caller's
    handling and killing the run.
    """


_STATUS_EXC = {
    400: BadRequestError,
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


def _parse_frame(data: dict | list) -> pd.DataFrame:
    """Response -> DataFrame without building one dict per row.

    `_parse_rows` materialises a dict per row so its callers can index by
    field name. For the header/format shape the response is already a list of
    row lists, which pandas can consume directly with the field names as
    columns — the dicts exist only to be thrown away by the DataFrame
    constructor a moment later.

    On a 57,876-row intraday response that measured 1.6x faster end to end
    (0.343s -> 0.220s) with byte-identical output, and it matters more than
    the ratio suggests because it runs in the pool workers and holds the GIL
    while it does. Other shapes fall back to _parse_rows unchanged.
    """
    if not data:
        return pd.DataFrame()
    if isinstance(data, dict) and "header" in data and "response" in data:
        fields = data["header"].get("format", []) or []
        resp = data.get("response") or []
        if fields:
            return pd.DataFrame(resp, columns=fields)
    rows = _parse_rows(data)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


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
         call — new expirations get listed over time; never use stale data),
         then filtering to exp >= trade_date so already-expired contracts
         don't waste a per-expiration round-trip on a guaranteed NoData.
      2. Fetching each remaining expiration individually via
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

    # Filter to exp >= trade_date.  /list/expirations returns the FULL
    # historical set (e.g. AAPL: 793 total, ~768 expired) — without this
    # filter the per-expiration fan-out spends ~97% of its calls on
    # guaranteed-NoData round-trips for contracts that expired before
    # today, dominating runtime.  Safe: anything currently listed has
    # expiration >= trade_date by definition; a 0DTE contract
    # (exp == trade_date) is retained by >=.
    n_total = len(expirations)
    expirations = [e for e in expirations if e >= trade_date]
    log.debug("  %s: current-day per-expiration fetch — %d live (of %d "
              "total), %d workers", symbol, len(expirations), n_total,
              exp_workers)

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


# --- Intraday chain snapshots (fetch_chain_snapshots.py) --------------------
#
# These functions are used ONLY by fetch_chain_snapshots.py.  They carry their
# own connection cap because the ThetaData subscription allows 4 concurrent
# connections, and the snapshot fetcher must never exceed it regardless of how
# its loops are structured.
#
# The cap is deliberately NOT applied to the shared `_get()` above: doing so
# would silently throttle the existing pipeline fetchers (fetch_chain_eod.py
# can put up to 4 tickers x 4 expirations = 16 requests in flight), which is
# out of scope for this module's callers.

SNAPSHOT_MAX_CONNECTIONS = 4
_SNAPSHOT_SEM = threading.BoundedSemaphore(SNAPSHOT_MAX_CONNECTIONS)


def set_max_connections(n: int) -> None:
    """Change the connection cap at runtime (--connections).

    Rebuilds the semaphore, so callers must invoke this BEFORE any request is
    in flight. Vendor guidance is that client in-flight requests should match
    the Theta Terminal's HTTP_CONCURRENCY setting (default 4) — exceeding it
    is documented to cause timeouts rather than clean rejections.
    """
    global SNAPSHOT_MAX_CONNECTIONS, _SNAPSHOT_SEM
    if n < 1:
        raise ValueError("connections must be >= 1")
    SNAPSHOT_MAX_CONNECTIONS = n
    _SNAPSHOT_SEM = threading.BoundedSemaphore(n)


def max_connections() -> int:
    """Current cap, read dynamically so --connections is reflected everywhere."""
    return SNAPSHOT_MAX_CONNECTIONS


# --- Retry policy -----------------------------------------------------------
# Replaces a flat 60s sleep with one retry. That was both too long (the
# documented API limit is ~10 req/s, and 4 workers at ~1s latency run at ~4/s,
# so a full minute of penance was never proportionate) and too few attempts (a
# second failure discarded the whole unit).
#
# Equal jitter — half the ceiling plus a random half — guarantees a real wait
# while decorrelating the workers, which otherwise retry in lockstep and
# recreate the burst that caused the 429.
RETRY_MAX_ATTEMPTS = 5          # 1 initial attempt + 4 retries
RETRY_BASE_SECONDS = 1.0
RETRY_MAX_SLEEP    = 32.0


def _backoff_delay(attempt: int) -> float:
    ceiling = min(RETRY_MAX_SLEEP, RETRY_BASE_SECONDS * (2 ** (attempt - 1)))
    return ceiling / 2.0 + random.uniform(0.0, ceiling / 2.0)


def describe_retry_policy() -> str:
    return (f"exponential backoff with equal jitter, "
            f"{RETRY_MAX_ATTEMPTS} attempts, base {RETRY_BASE_SECONDS:.0f}s, "
            f"cap {RETRY_MAX_SLEEP:.0f}s (worst case ~"
            f"{sum(min(RETRY_MAX_SLEEP, RETRY_BASE_SECONDS * 2 ** (a - 1)) for a in range(1, RETRY_MAX_ATTEMPTS)):.0f}s "
            f"of sleep per unit)")

# requests' scalar `timeout=` is a CONNECT and INTER-BYTE READ timeout, not a
# cap on total request duration: every chunk received resets the read clock, so
# a server that trickles bytes — or streams a very large body slowly — never
# trips it and can hold a worker indefinitely. With only 4 connections, a
# handful of such requests freezes the run. The snapshot path therefore
# enforces its own hard wall-clock deadline on top of the socket timeouts.
SNAPSHOT_CONNECT_TIMEOUT = 10     # seconds to establish the TCP connection
SNAPSHOT_READ_TIMEOUT    = 45     # seconds of socket silence before giving up
SNAPSHOT_TOTAL_TIMEOUT   = 180    # hard cap on total time for one request

# --- Worker-side timing accumulators ---------------------------------------
# Splits time spent inside a request into HTTP transfer vs local decoding, so
# "the vendor is slow" can be separated from "we are slow decoding what the
# vendor sent". Mutated from worker threads, hence the lock.
_TIMING_LOCK = threading.Lock()
SNAPSHOT_TIMING: dict[str, float] = {
    "http_seconds":     0.0,   # connect + stream the body
    "parse_seconds":    0.0,   # json.loads + row normalisation + DataFrame build
    "http_count":       0.0,
    "http_bytes":       0.0,
    # Time inside a task but OUTSIDE the HTTP call — the buckets that were
    # previously invisible because only successful calls were ever accounted.
    "sem_wait_seconds": 0.0,   # blocked acquiring the connection semaphore
    "backoff_seconds":  0.0,   # sleeping in the 429 / 474 retry ladder
    "retry_count":      0.0,   # retries attempted (any reason)
    "retry_429":        0.0,   # rate-limit retries
    "retry_474":        0.0,   # disconnect retries
    "retry_exhausted":  0.0,   # units that used every attempt and still failed
}


def describe_http_config() -> str:
    """Report the ACTUAL connection pooling in effect, not the intended one."""
    try:
        import requests as _rq
        from requests.adapters import HTTPAdapter
        d = HTTPAdapter()
        return (
            f"requests {_rq.__version__} | module-level requests.get() is used, "
            f"which constructs a NEW Session per call — so connections are NOT "
            f"reused and every request pays a fresh TCP handshake. Adapter "
            f"defaults pool_connections={d._pool_connections}, "
            f"pool_maxsize={d._pool_maxsize} apply per Session and are "
            f"discarded with it. Workers therefore never block on connection "
            f"checkout, but nor do they benefit from keep-alive."
        )
    except Exception as exc:                       # pragma: no cover
        return f"(could not introspect requests config: {exc})"


def reset_snapshot_timing() -> None:
    with _TIMING_LOCK:
        for k in SNAPSHOT_TIMING:
            SNAPSHOT_TIMING[k] = 0.0


def _add_timing(**kw: float) -> None:
    with _TIMING_LOCK:
        for k, v in kw.items():
            SNAPSHOT_TIMING[k] += v


# --- Response format --------------------------------------------------------
# The vendor serves the same content as JSON or CSV. JSON costs a full Python
# object graph per response — json.loads, then a list per row, then pandas
# transposing it — all of which holds the GIL. On a 43,992-row response that
# measured 0.090s + 0.068s; across a 121-ticker run it was 1,076.7s of
# GIL-held worker time, which starved the main thread's transform (measured
# 22.6s at 8 connections against 4.0s at 1 connection — 5.6x pure contention).
#
# CSV parses in pyarrow's C++ reader, which RELEASES the GIL and produces an
# Arrow table directly, skipping the object graph entirely.
#
# JSON is kept switchable, not removed: if the vendor's CSV has an edge case
# we have not hit — empty results, a session with no data, an unquoted field
# containing a comma — switching back must not need a rebuild. There is no
# automatic fallback on parse failure, deliberately: silently reverting would
# hide exactly the edge cases this flag exists to expose.
_RESPONSE_FORMAT = "csv"


def set_response_format(fmt: str) -> None:
    if fmt not in ("csv", "json"):
        raise ValueError(f"response format must be csv or json, got {fmt!r}")
    global _RESPONSE_FORMAT
    _RESPONSE_FORMAT = fmt


def response_format() -> str:
    return _RESPONSE_FORMAT


# Explicit types, never Arrow's inference. Inference would decide per response
# what a column is, so a session where every iv_error happened to be integral
# would come back int64 while its neighbours came back double — and the
# schema cast at write time would be the first thing to notice. `timestamp`
# and `expiration` stay strings so the downstream projection parses them
# exactly as it does on the JSON path; making the CSV reader parse them would
# be a second, differently-behaved date parser to keep in agreement.
_CSV_STRING_COLS = ("symbol", "root", "ticker", "expiration", "right",
                    "option_type", "date", "timestamp", "underlying_timestamp")
_CSV_FLOAT_COLS = ("strike", "bid", "ask", "delta", "theta", "vega", "rho",
                   "epsilon", "lambda", "implied_vol", "iv", "iv_error",
                   "underlying_price")
_CSV_COLUMN_TYPES = {c: pa.string() for c in _CSV_STRING_COLS}
_CSV_COLUMN_TYPES.update({c: pa.float64() for c in _CSV_FLOAT_COLS})

_CSV_SCHEMA_WARNED = False


def _parse_csv_frame(body: bytes) -> pd.DataFrame:
    """CSV bytes -> DataFrame via pyarrow's C++ reader.

    Columns not in _CSV_COLUMN_TYPES are left to inference; unknown fields are
    reported once per run rather than silently typed.
    """
    global _CSV_SCHEMA_WARNED
    if not body or not body.strip():
        return pd.DataFrame()

    table = pacsv.read_csv(
        pa.BufferReader(body),
        convert_options=pacsv.ConvertOptions(
            column_types=_CSV_COLUMN_TYPES,
            null_values=["", "NaN", "nan", "null", "NULL"],
            strings_can_be_null=True,
        ),
    )
    if not _CSV_SCHEMA_WARNED:
        unknown = [n for n in table.schema.names if n not in _CSV_COLUMN_TYPES]
        if unknown:
            _CSV_SCHEMA_WARNED = True
            log.warning("CSV response has column(s) with no explicit type, "
                        "left to inference: %s", ", ".join(unknown))
    return table.to_pandas()


# /v3/option/history/eod is used only for expiration enumeration. CSV is worth
# a great deal there (see enumerate_expirations_eod), but unlike
# /v3/option/history/greeks/first_order it has no production mileage on CSV in
# this codebase. So it is probed rather than assumed: the first call that comes
# back without a usable `expiration` column demotes the whole run to JSON and
# redoes that call, loudly and exactly once.
#
# This is a deliberate exception to the "no automatic fallback on parse
# failure" rule stated for _RESPONSE_FORMAT above, and the difference is the
# blast radius: a bad format on a data response loses one cell, while a bad
# format on the ENUMERATION response means zero expirations, which silently
# turns the entire run into a no-op that reports "NOTHING WRITTEN" for every
# ticker. Failing over once and saying so is the safer of the two.
_EOD_ENUM_FORMAT: str | None = None
_EOD_ENUM_LOCK = threading.Lock()


def _eod_enum_format() -> str:
    with _EOD_ENUM_LOCK:
        return _EOD_ENUM_FORMAT or response_format()


def _demote_eod_enum_format(reason: str) -> None:
    global _EOD_ENUM_FORMAT
    with _EOD_ENUM_LOCK:
        if _EOD_ENUM_FORMAT == "json":
            return
        _EOD_ENUM_FORMAT = "json"
    log.warning("expiration enumeration: /v3/option/history/eod did not serve "
                "usable CSV (%s) — falling back to JSON for the rest of this "
                "run. Enumeration will be slower and allocate more, but "
                "coverage is unaffected.", reason)


def _csv_unique_column(body: bytes, column: str) -> list:
    """Distinct values of ONE column of a CSV response, without pandas.

    For callers that want a handful of distinct values out of a response with
    a million rows — expiration enumeration being the case that matters. The
    row-dict path builds a dict per row and then parses the same ~50 dates tens
    of thousands of times over; this reads the CSV in pyarrow, takes the unique
    of a single column, and hands back a short list.
    """
    if not body or not body.strip():
        return []
    table = pacsv.read_csv(
        pa.BufferReader(body),
        convert_options=pacsv.ConvertOptions(
            column_types=_CSV_COLUMN_TYPES,
            null_values=["", "NaN", "nan", "null", "NULL"],
            strings_can_be_null=True,
        ),
    )
    if column not in table.schema.names:
        return []
    return table.column(column).unique().to_pylist()


def _get_snapshot(endpoint: str, params: dict,
                  total_timeout: int = SNAPSHOT_TOTAL_TIMEOUT,
                  fmt: str = "json"):
    """GET with a hard total-duration deadline, for the snapshot endpoints.

    Streams the body and checks elapsed wall time between chunks, so a
    slow-trickle response is aborted at `total_timeout` instead of running
    forever.  Raises TerminalTimeoutError on either the socket timeout or the
    total deadline; callers record the unit as failed and move on.
    """
    base   = f"{THETADATA_BASE_URL}{endpoint}"
    params = {**params, "format": fmt}
    # Query string built manually so * is not percent-encoded, as in _get.
    qs  = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{base}?{qs}"

    t0 = time.monotonic()
    try:
        resp = requests.get(
            url,
            timeout=(SNAPSHOT_CONNECT_TIMEOUT, SNAPSHOT_READ_TIMEOUT),
            stream=True,
        )
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout):
        raise TerminalTimeoutError(
            f"socket timeout (connect={SNAPSHOT_CONNECT_TIMEOUT}s, "
            f"read={SNAPSHOT_READ_TIMEOUT}s)"
        )
    except requests.exceptions.ConnectionError:
        raise ConnectionError(
            f"Cannot reach ThetaData at {THETADATA_BASE_URL}. "
            "Is Tailscale up and the terminal running?"
        )

    with resp:
        if resp.status_code == 500:
            raise TerminalServerError(f"HTTP 500: {resp.text[:200]}")
        exc = _STATUS_EXC.get(resp.status_code)
        if exc:
            raise exc(f"HTTP {resp.status_code}: {resp.text[:200]}")
        resp.raise_for_status()

        chunks: list[bytes] = []
        nbytes = 0
        try:
            for chunk in resp.iter_content(chunk_size=1 << 16):
                if chunk:
                    chunks.append(chunk)
                    nbytes += len(chunk)
                elapsed = time.monotonic() - t0
                if elapsed > total_timeout:
                    raise TerminalTimeoutError(
                        f"exceeded total timeout {total_timeout}s "
                        f"({nbytes:,} bytes read in {elapsed:.0f}s) — response "
                        "was still streaming; aborted so the worker is freed"
                    )
        except requests.exceptions.ReadTimeout:
            raise TerminalTimeoutError(
                f"read timeout after {SNAPSHOT_READ_TIMEOUT}s of socket "
                f"silence ({nbytes:,} bytes read)"
            )

    http_secs = time.monotonic() - t0
    body = b"".join(chunks)

    if fmt != "json":
        # CSV is handed back undecoded — pyarrow's reader consumes the bytes
        # directly, so there is deliberately no Python object graph built here.
        _add_timing(http_seconds=http_secs, http_count=1,
                    http_bytes=float(nbytes))
        return body

    t1 = time.monotonic()
    out = json.loads(body) if body else {}
    _add_timing(http_seconds=http_secs, parse_seconds=time.monotonic() - t1,
                http_count=1, http_bytes=float(nbytes))
    return out


def _get_capped(endpoint: str, params: dict, timeout: int = _DEFAULT_TIMEOUT,
                fmt: str = "json"):
    """`_get_snapshot` behind the 4-connection snapshot semaphore.

    The semaphore is released as soon as the request returns OR raises —
    including on timeout — so a hung request frees its slot for the other
    workers rather than starving them, and the retry sleeps in
    `_get_with_retry` never hold a connection slot.
    """
    t_sem = time.monotonic()
    _SNAPSHOT_SEM.acquire()
    _add_timing(sem_wait_seconds=time.monotonic() - t_sem)
    try:
        return _get_snapshot(endpoint, params, fmt=fmt)
    finally:
        _SNAPSHOT_SEM.release()


def _get_with_retry(endpoint: str, params: dict, timeout: int, label: str,
                    fmt: str = "json"):
    """Shared retry ladder for the snapshot endpoints.

    429 (rate limit)        -> sleep 60s, retry once
    474 (server disconnect) -> sleep 10s, retry once
    472 (no data)           -> propagates as NoDataError; caller treats as empty
    570 (large request)     -> propagates as LargeRequestError; caller halves
                               the date window and retries both halves
    read timeout / 500      -> propagate; caller logs and excludes this unit
    """
    last_exc: Exception | None = None
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            return _get_capped(endpoint, params, timeout=timeout, fmt=fmt)
        except RateLimitError as exc:
            last_exc, reason = exc, "429"
        except ServerDisconnectedError as exc:
            last_exc, reason = exc, "474"

        if attempt == RETRY_MAX_ATTEMPTS:
            _add_timing(retry_exhausted=1)
            log.warning("%s on %s — exhausted %d attempts, giving up",
                        reason, label, RETRY_MAX_ATTEMPTS)
            break

        delay = _backoff_delay(attempt)
        log.warning("%s on %s — attempt %d/%d, backing off %.1fs",
                    reason, label, attempt, RETRY_MAX_ATTEMPTS, delay)
        t0 = time.monotonic()
        time.sleep(delay)
        _add_timing(**{"backoff_seconds": time.monotonic() - t0,
                       "retry_count": 1, f"retry_{reason}": 1})

    raise last_exc


def enumerate_expirations_eod(symbol: str, start_date: date, end_date: date,
                              timeout: int = _DEFAULT_TIMEOUT) -> set[date]:
    """Distinct expirations that EXISTED at any point in [start_date, end_date].

    Endpoint: /v3/option/history/eod with expiration=* over a date range.

    This is the expiration source for the intraday snapshot fetcher, chosen
    over /v3/option/list/expirations and over the OI parquet store because:

      * It is keyed per-date — fetching a 2019 window returns 2019's
        expirations, not the full historical set, so there is no
        historical over-fetch waste.
      * It reports what the exchange actually listed, including a brand-new
        weekly on its listing day.  An enumeration source keyed off traded
        activity (the OI store drops zero-OI rows) would systematically miss
        a new weekly on day one — a guaranteed weekly hole on every ticker
        that lists weeklies.
      * It is one consistent source for backfill and live alike, so there is
        no source-switching between code paths.

    The returned set is the UNION across every date in the window, with no
    filtering whatsoever — no DTE cap, no `exp >= trade_date` prune, no
    intersection against any other source.  Callers must fetch all of it.
    Combinations that did not exist on a given date simply return no data and
    are excluded downstream, which is what makes the union safe.

    HTTP 570 (response too large) halves the date window and unions both
    halves, so a wide chain (SPY, SPX) self-adjusts without losing coverage.

    Returns an empty set iff the terminal returned no data.
    """
    params = {
        "symbol":     symbol.upper(),
        "expiration": "*",
        "start_date": start_date.strftime("%Y%m%d"),
        "end_date":   end_date.strftime("%Y%m%d"),
    }
    label = f"enum {symbol} {start_date}..{end_date}"
    # Enumeration is by far the heaviest response in a snapshot run: one
    # session with expiration=* is the entire EOD chain, and only its distinct
    # `expiration` values are wanted. On JSON that meant json.loads building an
    # object graph for every row, then `_parse_rows` building a dict per row on
    # top of it, then parsing the same ~50 dates once per row — all to fill a
    # set of ~50 entries. The CSV path reads it in pyarrow and takes the unique
    # of one column.
    #
    # /v3/option/history/greeks/first_order is known to serve CSV (the intraday
    # fetcher has run on it in production). /v3/option/history/eod is NOT
    # independently confirmed, so this one probes rather than assumes: see
    # _eod_enum_format().
    fmt = _eod_enum_format()

    try:
        data = _get_with_retry("/v3/option/history/eod", params, timeout, label,
                               fmt=fmt)
    except NoDataError:
        return set()
    except BadRequestError as exc:
        # EOD data does not exist until the session closes (~17:15 ET), and the
        # terminal answers 400 rather than 472 for a session that has not
        # happened yet. lib.market_hours.last_trading_day returns TODAY when
        # today is a trading day, so both fetchers routinely ask for it — a
        # run started before the close would otherwise die on a raw HTTPError.
        #
        # Treated as "nothing listed yet" ONLY when the window could plausibly
        # be unclosed. A 400 on a historical date is a real malformed request
        # and still propagates.
        if end_date >= today_et():
            log.warning("  %s: EOD not published yet for %s (session not "
                        "closed) — enumerating nothing; a later run will "
                        "pick it up", symbol, end_date)
            return set()
        raise
    except LargeRequestError:
        if start_date >= end_date:
            raise   # cannot split a single day — let the caller see it
        mid = start_date + (end_date - start_date) // 2
        log.info("  570 on %s — halving window", label)
        return (
            enumerate_expirations_eod(symbol, start_date, mid, timeout)
            | enumerate_expirations_eod(symbol, mid + timedelta(days=1), end_date, timeout)
        )

    t_parse = time.monotonic()
    out: set[date] = set()
    if fmt == "csv":
        try:
            values = _csv_unique_column(data, "expiration")
        except Exception as exc:
            _demote_eod_enum_format(f"CSV parse raised {type(exc).__name__}: {exc}")
            values = []
        if not values and data:
            # Bytes came back but no expiration column survived. On this
            # endpoint that is indistinguishable from "CSV is not served here",
            # and guessing wrong empties the store, so demote to JSON for the
            # rest of the run and redo this one call.
            _demote_eod_enum_format("response carried no usable `expiration` "
                                    "column")
            data = _get_with_retry("/v3/option/history/eod", params, timeout,
                                   label, fmt="json")
            values = [r.get("expiration") for r in _parse_rows(data)]
    else:
        values = [r.get("expiration") for r in _parse_rows(data)]
    for v in values:
        d = _parse_ymd(v)
        if d is not None:
            out.add(d)
    _add_timing(parse_seconds=time.monotonic() - t_parse)
    return out


def fetch_first_order_snapshot(symbol: str,
                               timeout: int = _DEFAULT_TIMEOUT) -> pd.DataFrame:
    """The WHOLE current chain for one ticker, in a single request.

        /v3/option/snapshot/greeks/first_order  with  expiration=*

    THE WILDCARD WORKS HERE. It is rejected by the HISTORY variant, which is
    why fetch_chain_intraday.py enumerates expirations and issues one call per
    (session, expiration). Probed and verified on the snapshot variant:

        SPY   13,410 rows   32 expirations   0.95s
        AAPL   3,348 rows   23 expirations   0.20s

    16-37x faster than the per-expiration path, and it returns all 17 fields
    including delta, vega, implied_vol and underlying_price — the four that
    clean_chain and build_snapshot read.

    THIS IS "NOW", NOT A MOMENT. A snapshot cannot retrieve a past instant, so
    it can serve the live path and nothing else; historical work still goes
    through the history endpoint. Coverage is complete for live use — a probe
    run after the close showed one expiration missing, and it was the one that
    had expired at that session's close.

    Returns the vendor frame unprojected. Empty DataFrame on NoDataError.
    """
    params = {"symbol": symbol.upper(), "expiration": "*"}
    label = f"snap_first_order {symbol} exp=*"
    fmt = response_format()
    try:
        data = _get_with_retry(
            "/v3/option/snapshot/greeks/first_order", params, timeout, label,
            fmt=fmt,
        )
    except NoDataError:
        return pd.DataFrame()

    t_parse = time.monotonic()
    out = _parse_csv_frame(data) if fmt == "csv" else _parse_frame(data)
    _add_timing(parse_seconds=time.monotonic() - t_parse)
    return out


def fetch_first_order_window(symbol: str, expiration: date, trade_date: date,
                             start_time: str, end_time: str,
                             interval: str = "5m",
                             timeout: int = _DEFAULT_TIMEOUT) -> pd.DataFrame:
    """Raw first-order greeks/quotes for ONE expiration over ONE session,
    across a time window at `interval` granularity.

    Generalises fetch_first_order_raw, which is the start_time == end_time
    (single instant) case.  Used by fetch_chain_intraday.py to pull a full
    day of 5-minute bars in one call:

        start_date == end_date == trade_date     (one session, never a range)
        start_time=09:35:00, end_time=16:00:00, interval=5m

    The single-session constraint is deliberate and load-bearing — see
    fetch_first_order_raw for why multi-day windows caused 570s and timeouts.

    All strikes and both rights are returned — no `strike_range` is sent.

    Returns ALL vendor fields with no field- or row-filtering.  Empty
    DataFrame on NoDataError or an empty response.  A 570 propagates: the
    window is already one session, so there is no date range left to split.
    """
    date_str = trade_date.strftime("%Y%m%d")
    params = {
        "symbol":     symbol.upper(),
        "expiration": expiration.strftime("%Y%m%d"),
        "start_date": date_str,
        "end_date":   date_str,
        "interval":   interval,
        "start_time": start_time,
        "end_time":   end_time,
    }
    label = (f"first_order {symbol} exp={expiration} {trade_date} "
             f"{start_time}..{end_time} @{interval}")
    fmt = response_format()

    try:
        data = _get_with_retry(
            "/v3/option/history/greeks/first_order", params, timeout, label,
            fmt=fmt,
        )
    except NoDataError:
        return pd.DataFrame()
    except LargeRequestError:
        log.warning("  570 on %s — already a single session, cannot split "
                    "the date range further", label)
        raise

    t_parse = time.monotonic()
    # CSV bytes go straight into pyarrow's C++ reader; JSON still builds the
    # Python object graph. Same downstream frame either way.
    out = _parse_csv_frame(data) if fmt == "csv" else _parse_frame(data)
    _add_timing(parse_seconds=time.monotonic() - t_parse)
    return out


def fetch_first_order_raw(symbol: str, expiration: date, trade_date: date,
                          snapshot_time: str,
                          interval: str = "5m",
                          timeout: int = _DEFAULT_TIMEOUT) -> pd.DataFrame:
    """Raw first-order greeks/quotes: ONE expiration, ONE session, ONE instant.

    Endpoint: /v3/option/history/greeks/first_order

    This is a point query in BOTH dimensions, matching the shape of a
    hand-issued browser call exactly:

        start_time == end_time == snapshot_time   (one instant, not a bar)
        start_date == end_date == trade_date      (one session, not a range)

    The time dimension was always a point query.  The DATE dimension was not:
    an earlier version passed a window of up to 30 calendar days in a single
    call, which multiplied the response by ~21 sessions and made the terminal
    compute greeks for the entire month of a full-width chain in one request.
    That was the source of the 570s, the 60s timeouts, and the halving-plus-
    retry cascade they triggered.  Callers now iterate sessions themselves.

    All strikes and both rights are returned — no `strike_range` is sent.

    Returns ALL vendor fields with no field- or row-filtering; projection to
    the stored schema happens in fetch_chain_snapshots.py.  Empty DataFrame on
    NoDataError or an empty response.

    A 570 on a single-session point query cannot be relieved by splitting the
    date range any further, so it propagates to the caller, which records the
    unit as failed rather than retrying a request that would fail identically.

    Response format: this function honours `response_format()` (CSV by
    default), like `fetch_first_order_window` above. It previously did not —
    it took the module default of JSON and then went through `_parse_rows`,
    which materialises one Python dict per row on top of the object graph
    json.loads has already built. That is the exact path the section comment at
    the top of this module records as abandoned for holding the GIL, and it is
    also the worst of the two for resident memory: millions of short-lived
    dicts and strs per run fragment pymalloc's arenas, so the freed space is
    not returned to the OS and RSS ratchets upward for the life of the process.
    The CSV reader parses in pyarrow's C++ and never builds the object graph.

    `fetch_first_order_window` is the same endpoint with start_time != end_time
    and was already switched; leaving this one behind was an oversight, not a
    distinction. The returned frame is the same either way.
    """
    date_str = trade_date.strftime("%Y%m%d")
    params = {
        "symbol":     symbol.upper(),
        "expiration": expiration.strftime("%Y%m%d"),
        "start_date": date_str,
        "end_date":   date_str,
        "interval":   interval,
        "start_time": snapshot_time,
        "end_time":   snapshot_time,
    }
    label = (f"first_order {symbol} exp={expiration} "
             f"{trade_date} @{snapshot_time}")
    fmt = response_format()

    try:
        data = _get_with_retry(
            "/v3/option/history/greeks/first_order", params, timeout, label,
            fmt=fmt,
        )
    except NoDataError:
        return pd.DataFrame()
    except LargeRequestError:
        log.warning("  570 on %s — already a single-session point query, "
                    "cannot split further", label)
        raise

    t_parse = time.monotonic()
    out = _parse_csv_frame(data) if fmt == "csv" else _parse_frame(data)
    _add_timing(parse_seconds=time.monotonic() - t_parse)
    return out


def test_connection() -> bool:
    try:
        return bool(list_expirations("SPY"))
    except Exception as e:
        log.error("Connection test failed: %s", e)
        return False
