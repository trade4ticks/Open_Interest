"""
ThetaData v3 client — daily Open Interest only.

Endpoints used:
  /v3/option/list/expirations
  /v3/option/history/open_interest
"""
from __future__ import annotations

import logging
import time
from datetime import date, timedelta

import pandas as pd
import requests

from config import THETADATA_BASE_URL

log = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 120
_BULK_TIMEOUT    = 300


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
    url    = f"{THETADATA_BASE_URL}{endpoint}"
    params = {**params, "format": "json"}
    try:
        resp = requests.get(url, params=params, timeout=timeout)
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


def _row_date(row: dict) -> date | None:
    """Pull a date out of a row's 'date' (YYYYMMDD int) or ISO 'timestamp' field."""
    raw = row.get("date")
    if raw is not None:
        s = str(int(raw))
        if len(s) == 8:
            return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    ts = str(row.get("timestamp") or "")
    if len(ts) >= 10 and ts[4] == "-":
        return date(int(ts[:4]), int(ts[5:7]), int(ts[8:10]))
    return None


# --- Public API ------------------------------------------------------------

def list_expirations(symbol: str) -> list[date]:
    """All expirations the terminal knows about for `symbol` (sorted ascending)."""
    data = _get("/v3/option/list/expirations", {"symbol": symbol.upper()})
    rows = _parse_rows(data)
    out: list[date] = []
    for r in rows:
        s = str(r.get("expiration") or "")[:10]
        if not s or s[4] != "-":
            continue
        try:
            out.append(date(int(s[:4]), int(s[5:7]), int(s[8:10])))
        except ValueError:
            continue
    return sorted(set(out))


def fetch_oi(
    symbol: str,
    expiration: date,
    start_date: date,
    end_date: date,
    timeout: int = _BULK_TIMEOUT,
) -> pd.DataFrame:
    """
    Fetch daily OI for all strikes of one expiration over a date range.

    Returns DataFrame[trade_date, strike, right, open_interest].
    Empty DataFrame iff terminal returned no data (NOT on transport errors).
    """
    params = {
        "symbol":     symbol.upper(),
        "expiration": expiration.strftime("%Y%m%d"),
        "strike":     "*",
        "right":      "both",
        "start_date": start_date.strftime("%Y%m%d"),
        "end_date":   end_date.strftime("%Y%m%d"),
    }

    try:
        data = _get("/v3/option/history/open_interest", params, timeout=timeout)
    except NoDataError:
        return pd.DataFrame()
    except RateLimitError:
        log.warning("Rate limited — sleeping 60s, retrying once")
        time.sleep(60)
        data = _get("/v3/option/history/open_interest", params, timeout=timeout)
    except ServerDisconnectedError:
        log.warning("Server disconnected — sleeping 10s, retrying once")
        time.sleep(10)
        data = _get("/v3/option/history/open_interest", params, timeout=timeout)
    except LargeRequestError:
        # Split the date range in half and recurse.
        if start_date == end_date:
            raise
        mid = start_date + (end_date - start_date) // 2
        log.warning("570 — splitting %s %s %s→%s at %s",
                    symbol, expiration, start_date, end_date, mid)
        left  = fetch_oi(symbol, expiration, start_date, mid, timeout)
        right = fetch_oi(symbol, expiration, mid + timedelta(days=1), end_date, timeout)
        if left.empty and right.empty:
            return pd.DataFrame()
        return pd.concat([left, right], ignore_index=True)

    rows = _parse_rows(data)
    if not rows:
        return pd.DataFrame()

    records = []
    for row in rows:
        d = _row_date(row)
        if d is None:
            continue
        oi = row.get("open_interest")
        if oi is None or oi == 0:
            # zero OI is genuinely uninteresting for a research project
            continue
        right_raw = str(row.get("right") or "").strip().lower()
        right_val = (
            "C" if right_raw in ("c", "call") else
            "P" if right_raw in ("p", "put") else
            right_raw.upper()
        )
        if right_val not in ("C", "P"):
            continue
        records.append({
            "trade_date":    d,
            "strike":        row.get("strike"),
            "right":         right_val,
            "open_interest": oi,
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["strike"]        = pd.to_numeric(df["strike"],        errors="coerce")
    df["open_interest"] = pd.to_numeric(df["open_interest"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["strike", "open_interest"])
    return df


def test_connection() -> bool:
    try:
        return bool(list_expirations("SPY"))
    except Exception as e:
        log.error("Connection test failed: %s", e)
        return False
