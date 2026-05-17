"""
ThetaData v3 client — daily Open Interest only.

Endpoints used:
  /v3/option/list/expirations
  /v3/option/history/open_interest
"""
from __future__ import annotations

import logging
import time
from datetime import date

import pandas as pd
import requests

from config import THETADATA_BASE_URL

log = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 60


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
    params = {
        "symbol":     symbol.upper(),
        "expiration": "*",
        "date":       trade_date.strftime("%Y%m%d"),
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


def test_connection() -> bool:
    try:
        return bool(list_expirations("SPY"))
    except Exception as e:
        log.error("Connection test failed: %s", e)
        return False
