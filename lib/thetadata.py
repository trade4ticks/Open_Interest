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


def test_connection() -> bool:
    try:
        return bool(list_expirations("SPY"))
    except Exception as e:
        log.error("Connection test failed: %s", e)
        return False
