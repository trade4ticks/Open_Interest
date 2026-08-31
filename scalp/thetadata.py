"""ThetaData v3 STOCK client for the equities-scalp pipeline.

SELF-CONTAINED BY DESIGN — DO NOT REFACTOR INTO A SHARED MODULE.

This is a deliberate copy of the patterns proven in ../lib/thetadata.py: typed
per-status exceptions, the jittered exponential retry ladder, the connection
semaphore matching the terminal's HTTP_CONCURRENCY, the streaming GET with a
hard total-duration deadline, and the CSV-via-pyarrow parse path. It imports
none of it.

The reason is not stylistic. The stock subscription may be dropped, and the
options pipeline has to keep working untouched if it is. A shared module would
make one project's lifecycle a dependency of the other's. `rm -rf scalp/` must
leave the options code completely unaffected, and a few hundred duplicated
lines is a fair price for that guarantee.

Two deliberate differences from the options client:

  * CSV is the only wire format here. The options module keeps JSON switchable
    because it has edge cases it may need to fall back for; this one is new and
    its heaviest responses (trade_quote — every trade paired with the prevailing
    NBBO) are the largest in either project. pyarrow's reader releases the GIL
    and produces an Arrow table directly, skipping the Python object graph
    entirely. `format=json` is not wired up at all rather than being wired up
    and unused.

  * Arrow types are INFERRED, not declared. The options module declares them
    because its schema is known and stable, and inference would let one
    session's all-integral column come back int64 while its neighbour came back
    double. The stock schema is NOT yet known — establishing it is exactly what
    step 0 does — and a declared guess would silently coerce the thing being
    measured. Types get declared here once step 0 reports what the columns
    actually are.

VENUE: not hardcoded anywhere. Every request resolves its venue through
config.VENUE_BY_ENDPOINT, which currently records the snapshot endpoint as
measured and both history endpoints as unresolved. See the comment there.
"""
from __future__ import annotations

import logging
import random
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import requests
from pyarrow import csv as pacsv

from scalp import config

log = logging.getLogger(__name__)

# ThetaData's reference timezone for "current day".
_ET = ZoneInfo("America/New_York")


# --- Exceptions --------------------------------------------------------------

class ThetaDataError(Exception):
    """Base for every error this module raises deliberately."""
    def __init__(self, message: str, status: int | None = None, body: str = ""):
        super().__init__(message)
        self.status = status
        self.body = body


class NoDataError(ThetaDataError):             """HTTP 472 — empty response."""
class RateLimitError(ThetaDataError):          """HTTP 429."""
class ServerDisconnectedError(ThetaDataError):  """HTTP 474."""
class LargeRequestError(ThetaDataError):       """HTTP 570 — split the request."""
class TerminalTimeoutError(ThetaDataError):    """Socket timeout or total deadline."""
class TerminalServerError(ThetaDataError):     """HTTP 500."""
class BadRequestError(ThetaDataError):
    """HTTP 400 — the terminal rejected the request as malformed.

    For this project the most interesting case is a rejected `venue`
    parameter: an endpoint that does not accept venue at all should answer
    400 rather than silently ignoring it. Distinguishing "rejected" from
    "accepted and ignored" is what s1_venue_check.py exists to do, and it
    needs the status and body, hence the fields on ThetaDataError.
    """


class NotEntitledError(ThetaDataError):
    """HTTP 401 / 403 — authenticated but not entitled to this endpoint.

    This is the failure mode that matters for step 0: `trade_quote` being
    Pro-only rather than Standard. It is called out as its own type so the
    availability probe can report "not on your subscription" separately from
    "the terminal is unreachable" or "the request was malformed".
    """


_STATUS_EXC: dict[int, type[ThetaDataError]] = {
    400: BadRequestError,
    401: NotEntitledError,
    403: NotEntitledError,
    429: RateLimitError,
    472: NoDataError,
    474: ServerDisconnectedError,
    500: TerminalServerError,
    570: LargeRequestError,
}


# --- Connection cap ----------------------------------------------------------
# Vendor guidance: in-flight client requests should match the terminal's
# HTTP_CONCURRENCY (default 4). Exceeding it is documented to cause timeouts
# rather than clean rejections.

_MAX_CONNECTIONS = config.MAX_CONNECTIONS
_SEM = threading.BoundedSemaphore(_MAX_CONNECTIONS)


def set_max_connections(n: int) -> None:
    """Change the cap at runtime (--connections). Call BEFORE any request."""
    global _MAX_CONNECTIONS, _SEM
    if n < 1:
        raise ValueError("connections must be >= 1")
    _MAX_CONNECTIONS = n
    _SEM = threading.BoundedSemaphore(n)


def max_connections() -> int:
    return _MAX_CONNECTIONS


# --- Retry policy ------------------------------------------------------------
# Equal jitter — half the ceiling plus a random half — guarantees a real wait
# while decorrelating workers, which otherwise retry in lockstep and recreate
# the burst that caused the 429 in the first place.

RETRY_MAX_ATTEMPTS = 5          # 1 initial attempt + 4 retries
RETRY_BASE_SECONDS = 1.0
RETRY_MAX_SLEEP    = 32.0


def _backoff_delay(attempt: int) -> float:
    ceiling = min(RETRY_MAX_SLEEP, RETRY_BASE_SECONDS * (2 ** (attempt - 1)))
    return ceiling / 2.0 + random.uniform(0.0, ceiling / 2.0)


def describe_retry_policy() -> str:
    worst = sum(min(RETRY_MAX_SLEEP, RETRY_BASE_SECONDS * 2 ** (a - 1))
                for a in range(1, RETRY_MAX_ATTEMPTS))
    return (f"exponential backoff with equal jitter, {RETRY_MAX_ATTEMPTS} attempts, "
            f"base {RETRY_BASE_SECONDS:.0f}s, cap {RETRY_MAX_SLEEP:.0f}s "
            f"(worst case ~{worst:.0f}s of sleep per request)")


# --- Venue resolution --------------------------------------------------------

class _Unset:
    """Sentinel: 'use the configured policy'.

    Distinct from None, which means 'send no venue parameter at all'. The
    difference is load-bearing for s1_venue_check.py, which must be able to
    issue an explicitly venue-less request against an endpoint whose policy
    might later say otherwise.
    """
    def __repr__(self) -> str:
        return "<use policy>"


UNSET = _Unset()


def venue_for(endpoint: str) -> str | None:
    """The venue this endpoint is configured to send, or None to send none.

    Unknown endpoints return None rather than a default. Guessing a venue for
    an endpoint nobody has tested is precisely the failure this table exists
    to prevent.
    """
    return config.VENUE_BY_ENDPOINT.get(endpoint)


# --- Response container ------------------------------------------------------

@dataclass
class RawResponse:
    """One HTTP response plus the measurements step 0 needs.

    Timing and byte counts are returned per-request rather than accumulated in
    module-level counters: the discovery scripts compare individual calls
    against each other, and a shared accumulator would have to be reset between
    them.
    """
    endpoint: str
    url: str
    status: int
    body: bytes
    seconds: float
    venue_sent: str | None
    params: dict[str, Any] = field(default_factory=dict)

    @property
    def nbytes(self) -> int:
        return len(self.body)

    def frame(self) -> pd.DataFrame:
        return parse_csv(self.body)


def parse_csv(body: bytes) -> pd.DataFrame:
    """CSV bytes -> DataFrame via pyarrow's C++ reader.

    Types are inferred. See the module docstring: the stock schema is what step
    0 is measuring, so declaring types here would coerce the measurement.
    """
    if not body or not body.strip():
        return pd.DataFrame()
    table = pacsv.read_csv(
        pa.BufferReader(body),
        convert_options=pacsv.ConvertOptions(
            null_values=["", "NaN", "nan", "null", "NULL"],
            strings_can_be_null=True,
        ),
    )
    return table.to_pandas()


# --- HTTP layer --------------------------------------------------------------

def _build_url(endpoint: str, params: dict[str, Any]) -> str:
    # Query string built manually so a wildcard `*` is not percent-encoded to
    # %2A. Every value here is a symbol, date, time, number or * — none needs
    # encoding.
    qs = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
    return f"{config.THETADATA_BASE_URL}{endpoint}?{qs}"


def _request_once(endpoint: str, params: dict[str, Any],
                  venue: str | None | _Unset = UNSET,
                  total_timeout: int | None = None) -> RawResponse:
    """One GET with a hard total-duration deadline. No retries.

    Streams the body and checks elapsed wall time between chunks, so a
    slow-trickle response is aborted at `total_timeout` rather than holding a
    connection slot forever.
    """
    resolved_venue = venue_for(endpoint) if isinstance(venue, _Unset) else venue
    total_timeout = config.TOTAL_TIMEOUT if total_timeout is None else total_timeout

    sent = {**params, "format": "csv"}
    if resolved_venue is not None:
        sent["venue"] = resolved_venue
    url = _build_url(endpoint, sent)

    t0 = time.monotonic()
    try:
        resp = requests.get(
            url,
            timeout=(config.CONNECT_TIMEOUT, config.READ_TIMEOUT),
            stream=True,
        )
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout):
        raise TerminalTimeoutError(
            f"socket timeout (connect={config.CONNECT_TIMEOUT}s, "
            f"read={config.READ_TIMEOUT}s) on {endpoint}"
        )
    except requests.exceptions.ConnectionError as exc:
        raise ConnectionError(
            f"Cannot reach ThetaData at {config.THETADATA_BASE_URL}. "
            f"Is Tailscale up and the terminal running?  ({exc})"
        )

    with resp:
        exc_cls = _STATUS_EXC.get(resp.status_code)
        if exc_cls:
            body_preview = resp.text[:300]
            raise exc_cls(f"HTTP {resp.status_code} on {endpoint}: {body_preview}",
                          status=resp.status_code, body=body_preview)
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
                        f"exceeded total timeout {total_timeout}s on {endpoint} "
                        f"({nbytes:,} bytes in {elapsed:.0f}s) — response was "
                        "still streaming; aborted so the connection is freed"
                    )
        except requests.exceptions.ReadTimeout:
            raise TerminalTimeoutError(
                f"read timeout after {config.READ_TIMEOUT}s of socket silence "
                f"on {endpoint} ({nbytes:,} bytes read)"
            )

    return RawResponse(
        endpoint=endpoint,
        url=url,
        status=resp.status_code,
        body=b"".join(chunks),
        seconds=time.monotonic() - t0,
        venue_sent=resolved_venue,
        params=sent,
    )


def request(endpoint: str, params: dict[str, Any],
            venue: str | None | _Unset = UNSET,
            total_timeout: int | None = None,
            label: str | None = None,
            retry: bool = True) -> RawResponse:
    """GET behind the connection semaphore, with the retry ladder.

    429 and 474 retry with jittered backoff. Everything else propagates:
      * 472 (no data)        -> NoDataError; callers usually treat as empty
      * 570 (too large)      -> LargeRequestError; caller splits the window
      * 401/403              -> NotEntitledError; subscription problem
      * 400                  -> BadRequestError; malformed, or venue rejected
      * timeout / 500        -> caller logs and excludes this unit

    The semaphore is released as soon as the request returns OR raises —
    including on timeout — so a hung request frees its slot rather than
    starving the other workers, and the backoff sleeps never hold a slot.

    `retry=False` is for probes that want the first answer verbatim: an
    availability check should report a 429 as a 429, not spend 30 seconds
    hiding it.
    """
    label = label or endpoint
    attempts = RETRY_MAX_ATTEMPTS if retry else 1
    last_exc: ThetaDataError | None = None

    for attempt in range(1, attempts + 1):
        _SEM.acquire()
        try:
            return _request_once(endpoint, params, venue=venue,
                                 total_timeout=total_timeout)
        except RateLimitError as exc:
            last_exc, reason = exc, "429"
        except ServerDisconnectedError as exc:
            last_exc, reason = exc, "474"
        finally:
            _SEM.release()

        if attempt == attempts:
            log.warning("%s on %s — exhausted %d attempt(s), giving up",
                        reason, label, attempts)
            break

        delay = _backoff_delay(attempt)
        log.warning("%s on %s — attempt %d/%d, backing off %.1fs",
                    reason, label, attempt, attempts, delay)
        time.sleep(delay)

    assert last_exc is not None
    raise last_exc


# --- Date / time helpers -----------------------------------------------------

def ymd(d: date | str) -> str:
    """Accept a date or 'YYYY-MM-DD' and return ThetaData's YYYYMMDD."""
    if isinstance(d, str):
        s = d.replace("-", "")
        if len(s) != 8 or not s.isdigit():
            raise ValueError(f"expected YYYY-MM-DD or YYYYMMDD, got {d!r}")
        return s
    return d.strftime("%Y%m%d")


def parse_date(s: str) -> date:
    return datetime.strptime(s.replace("-", ""), "%Y%m%d").date()


def today_et() -> date:
    """Today's date in US Eastern Time.

    The vendor defines "current day" in ET, and the VPS runs in UTC — so
    date.today() is a day ahead there for the five hours after 19:00 ET. Every
    is-this-date-in-the-past check has to use this, because getting it wrong
    means either refusing a legitimate same-day run or accepting a historical
    one as live.
    """
    return datetime.now(_ET).date()


# --- Endpoints ---------------------------------------------------------------
#
# Thin wrappers. Each returns the RawResponse so callers keep access to timing,
# byte count and the venue actually sent; `.frame()` gives the DataFrame.
# No field projection and no row filtering happens here — step 0 has to see
# exactly what the vendor sends.

EP_SYMBOLS      = "/v3/stock/list/symbols"
EP_SNAPSHOT_OHLC = "/v3/stock/snapshot/ohlc"
EP_TRADE_QUOTE  = "/v3/stock/history/trade_quote"
EP_QUOTE        = "/v3/stock/history/quote"
EP_HISTORY_EOD  = "/v3/stock/history/eod"


def history_eod(symbol: str, start_date: date | str,
                end_date: date | str, **kw) -> RawResponse:
    """Settled OHLC + volume for one symbol over a date range.

    THE HISTORICAL COUNTERPART TO snapshot/ohlc. The snapshot endpoint is
    "now" — it cannot retrieve a past session, and asking it for one silently
    returns the current partial day. This one is keyed by date, so it is the
    only way to reconstruct a universe for a session that has already closed.

    Per-symbol; there is no wildcard here, unlike the snapshot. That makes
    rebuilding a past universe a loop over the roster rather than one call.
    """
    params: dict[str, Any] = {
        "symbol":     symbol.upper(),
        "start_date": ymd(start_date),
        "end_date":   ymd(end_date),
    }
    return request(EP_HISTORY_EOD, params,
                   label=f"history/eod {symbol} {params['start_date']}",
                   **kw)


def list_symbols(**kw) -> RawResponse:
    """Full symbol roster."""
    return request(EP_SYMBOLS, {}, label="list/symbols", **kw)


def snapshot_ohlc(symbol: str = "*", **kw) -> RawResponse:
    """Whole-market OHLC snapshot in one call. Only meaningful after RTH.

    Returns timestamp, symbol, OHLC, volume and count (trade count).
    """
    return request(EP_SNAPSHOT_OHLC, {"symbol": symbol},
                   label=f"snapshot/ohlc {symbol}", **kw)


def trade_quote(symbol: str, start_date: date | str, end_date: date | str,
                start_time: str | None = None, end_time: str | None = None,
                **kw) -> RawResponse:
    """Every trade paired with the prevailing NBBO at that trade.

    Per-symbol only — no wildcard. Multi-day supported, capped at one month.
    `start_time`/`end_time` are omitted entirely when None, because whether
    this endpoint accepts them is one of the things step 0 establishes.
    """
    params: dict[str, Any] = {
        "symbol":     symbol.upper(),
        "start_date": ymd(start_date),
        "end_date":   ymd(end_date),
    }
    if start_time is not None:
        params["start_time"] = start_time
    if end_time is not None:
        params["end_time"] = end_time
    return request(EP_TRADE_QUOTE, params,
                   label=f"trade_quote {symbol} {params['start_date']}..{params['end_date']}",
                   **kw)


def quote(symbol: str, start_date: date | str, end_date: date | str,
          interval: str = "tick",
          start_time: str | None = None, end_time: str | None = None,
          **kw) -> RawResponse:
    """NBBO quote records.

    Intervals `tick` through `1h`, but sub-1m is single-day only — so a
    tick-interval call must have start_date == end_date.
    """
    params: dict[str, Any] = {
        "symbol":     symbol.upper(),
        "start_date": ymd(start_date),
        "end_date":   ymd(end_date),
        "interval":   interval,
    }
    if start_time is not None:
        params["start_time"] = start_time
    if end_time is not None:
        params["end_time"] = end_time
    return request(EP_QUOTE, params,
                   label=f"quote {symbol} {params['start_date']} @{interval}",
                   **kw)


def test_connection() -> bool:
    """Cheapest possible reachability check. Not an entitlement check."""
    try:
        list_symbols(retry=False)
        return True
    except Exception as exc:
        log.error("Connection test failed: %s", exc)
        return False
