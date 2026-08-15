"""
Polygon.io / Massive REST client — 1-minute equity aggregates.

Deliberately a sibling of lib/thetadata.py's snapshot path rather than a
generic client: it carries the same hard-won pieces (bounded connections,
exponential backoff with equal jitter, a hard wall-clock deadline per request,
and a timing dict shaped for print_timing_summary) so runs of the two fetchers
are directly comparable.

Endpoint
--------
    /v2/aggs/ticker/{ticker}/range/1/minute/{from}/{to}

    adjusted=false   — NOT the default. Polygon adjusts to TODAY's split basis,
                       so a future split silently reprices stored history onto a
                       different basis than newly-fetched data. The project
                       already applies one universal split adjustment at read
                       time (see docs/daily_features_data_dictionary.md,
                       "Split adjustment — two universal scalings"); storing
                       as-traded keeps that the single source of truth.
    limit=50000      — set explicitly. The documented DEFAULT IS 5000, which
                       would silently truncate every chunk to ~5 sessions.
    sort=asc         — deterministic order, so a truncated response loses the
                       TAIL of the range rather than an arbitrary middle.

Rate limits (verified 2026-08, vendor knowledge base)
-----------------------------------------------------
Paid plans: "unlimited API requests", with the explicit caveat that usage is
monitored and the vendor recommends "staying under 100 requests per second" to
avoid throttling. There is no documented concurrent-connection cap and no
documented 429 threshold — so the ceiling here is self-imposed and the 429
path is still implemented, because an undocumented limit is still a limit.

The 50,000 cap and why chunks are MONTHLY
------------------------------------------
`limit` caps "base aggregates queried", max 50,000. With extended hours a
session yields up to 960 one-minute bars (04:00-20:00 ET), so:

    quarter (max 64 sessions) x 960 = 61,440 bars  -> OVER the cap
    month   (max 23 sessions) x 960 = 22,080 bars  -> 2.3x headroom

The vendor's own guidance agrees: "limit minute/hourly requests to one-month
timeframes per query to avoid gaps." Quarterly chunking would truncate the
densest tickers silently. `fetch_aggs_minute` additionally reports truncation
(results == limit with no next_url) so the caller can split and refetch rather
than trust a short response.
"""
from __future__ import annotations

import logging
import random
import threading
import time
from datetime import date

import requests

from config import POLYGON_API_KEY, POLYGON_BASE_URL

log = logging.getLogger(__name__)

AGGS_LIMIT = 50_000          # vendor max; must be sent explicitly (default 5000)
MAX_PAGES  = 20              # next_url follow cap — a runaway-loop backstop


# --- Exceptions -------------------------------------------------------------

class PolygonError(Exception):
    pass


class RateLimitError(PolygonError):
    """HTTP 429."""


class ServerError(PolygonError):
    """HTTP 5xx."""


class AuthError(PolygonError):
    """HTTP 401/403 — bad or unentitled API key. Never retried."""


class RequestTimeoutError(PolygonError):
    """Socket timeout or the hard total-duration deadline."""


class TruncatedResponseError(PolygonError):
    """Response hit the 50k cap with no next_url — the range must be split."""


# --- Connection cap ---------------------------------------------------------
# Vendor documents no concurrent-connection limit, so this is a self-imposed
# politeness bound rather than a hard requirement. It is enforced with a
# semaphore for the same reason the ThetaData path is: no loop structure
# anywhere can then exceed it by accident.

MAX_CONNECTIONS = 8
_SEM = threading.BoundedSemaphore(MAX_CONNECTIONS)


def set_max_connections(n: int) -> None:
    """Change the cap at runtime (--connections). Rebuilds the semaphore, so
    callers must invoke this BEFORE any request is in flight."""
    global MAX_CONNECTIONS, _SEM
    if n < 1:
        raise ValueError("connections must be >= 1")
    MAX_CONNECTIONS = n
    _SEM = threading.BoundedSemaphore(n)


def max_connections() -> int:
    return MAX_CONNECTIONS


# --- Retry policy -----------------------------------------------------------
# Equal jitter: half the ceiling plus a random half. Guarantees a real wait
# while decorrelating workers, which otherwise retry in lockstep and recreate
# the burst that caused the 429. Identical shape to lib/thetadata.py so the two
# fetchers' retry lines in the summary mean the same thing.

RETRY_MAX_ATTEMPTS = 5          # 1 initial attempt + 4 retries
RETRY_BASE_SECONDS = 1.0
RETRY_MAX_SLEEP    = 32.0

# requests' scalar timeout= is a CONNECT and INTER-BYTE READ timeout, not a cap
# on total duration: every chunk received resets the read clock, so a server
# trickling a large body never trips it and can hold a worker indefinitely.
CONNECT_TIMEOUT = 10
READ_TIMEOUT    = 45
TOTAL_TIMEOUT   = 180


def _backoff_delay(attempt: int) -> float:
    ceiling = min(RETRY_MAX_SLEEP, RETRY_BASE_SECONDS * (2 ** (attempt - 1)))
    return ceiling / 2.0 + random.uniform(0.0, ceiling / 2.0)


def describe_retry_policy() -> str:
    worst = sum(min(RETRY_MAX_SLEEP, RETRY_BASE_SECONDS * 2 ** (a - 1))
                for a in range(1, RETRY_MAX_ATTEMPTS))
    return (f"exponential backoff with equal jitter, "
            f"{RETRY_MAX_ATTEMPTS} attempts, base {RETRY_BASE_SECONDS:.0f}s, "
            f"cap {RETRY_MAX_SLEEP:.0f}s (worst case ~{worst:.0f}s of sleep "
            f"per unit)")


def describe_http_config() -> str:
    return (f"connect {CONNECT_TIMEOUT}s, read {READ_TIMEOUT}s, "
            f"hard total {TOTAL_TIMEOUT}s, {MAX_CONNECTIONS} connections")


# --- Timing -----------------------------------------------------------------
# Same key names as lib.thetadata.SNAPSHOT_TIMING so
# lib.chain_fetch_common.print_timing_summary renders it unchanged.

POLYGON_TIMING: dict[str, float] = {
    "http_seconds":     0.0,
    "parse_seconds":    0.0,
    "http_bytes":       0.0,
    "sem_wait_seconds": 0.0,
    "backoff_seconds":  0.0,
    "retry_count":      0.0,
    "retry_429":        0.0,
    "retry_474":        0.0,   # unused here; kept so the summary format matches
    "retry_5xx":        0.0,
    "retry_exhausted":  0.0,
    "pages_followed":   0.0,
    "truncated":        0.0,
}
_TIMING_LOCK = threading.Lock()


def reset_timing() -> None:
    with _TIMING_LOCK:
        for k in POLYGON_TIMING:
            POLYGON_TIMING[k] = 0.0


def _add_timing(**kw: float) -> None:
    with _TIMING_LOCK:
        for k, v in kw.items():
            POLYGON_TIMING[k] = POLYGON_TIMING.get(k, 0.0) + v


# --- Session ----------------------------------------------------------------
# One pooled Session, sized to the connection cap. Without pool_maxsize the
# adapter defaults to 10 and silently serialises anything above that.

_SESSION: requests.Session | None = None
_SESSION_LOCK = threading.Lock()


def _session() -> requests.Session:
    global _SESSION
    with _SESSION_LOCK:
        if _SESSION is None:
            s = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=max(MAX_CONNECTIONS, 10),
                pool_maxsize=max(MAX_CONNECTIONS, 10),
                max_retries=0,          # retries are ours, with jitter
            )
            s.mount("https://", adapter)
            s.mount("http://", adapter)
            s.headers.update({"Accept-Encoding": "gzip"})
            _SESSION = s
        return _SESSION


# --- Low-level GET ----------------------------------------------------------

def _get_once(url: str, params: dict | None, label: str) -> dict:
    """One HTTP attempt with a hard wall-clock deadline. Raises on failure."""
    if not POLYGON_API_KEY:
        raise AuthError(
            "POLYGON_API_KEY is not set. Add it to .env "
            "(see .env.example) before fetching."
        )

    t_sem = time.monotonic()
    with _SEM:
        _add_timing(sem_wait_seconds=time.monotonic() - t_sem)

        p = dict(params or {})
        p["apiKey"] = POLYGON_API_KEY

        t0 = time.monotonic()
        try:
            resp = _session().get(url, params=p, stream=True,
                                  timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        except requests.exceptions.Timeout as exc:
            raise RequestTimeoutError(f"connect/read timeout on {label}") from exc
        except requests.exceptions.RequestException as exc:
            raise PolygonError(f"{type(exc).__name__} on {label}: {exc}") from exc

        try:
            status = resp.status_code
            if status == 429:
                retry_after = resp.headers.get("Retry-After")
                raise RateLimitError(retry_after or "429")
            if status in (401, 403):
                raise AuthError(
                    f"HTTP {status} on {label} — API key rejected or the plan "
                    f"does not entitle this endpoint/date range. "
                    f"Body: {resp.text[:200]}"
                )
            if status >= 500:
                raise ServerError(f"HTTP {status} on {label}: {resp.text[:200]}")
            if status != 200:
                raise PolygonError(f"HTTP {status} on {label}: {resp.text[:200]}")

            # Stream so a slow-trickle body is aborted at the total deadline
            # instead of holding a worker forever.
            chunks: list[bytes] = []
            nbytes = 0
            for chunk in resp.iter_content(chunk_size=256 * 1024):
                if chunk:
                    chunks.append(chunk)
                    nbytes += len(chunk)
                if time.monotonic() - t0 > TOTAL_TIMEOUT:
                    raise RequestTimeoutError(
                        f"exceeded hard total timeout {TOTAL_TIMEOUT}s on {label} "
                        f"after {nbytes / 1e6:.1f} MB"
                    )
            body = b"".join(chunks)
        finally:
            resp.close()

        _add_timing(http_seconds=time.monotonic() - t0, http_bytes=float(nbytes))

    t_parse = time.monotonic()
    try:
        import json
        data = json.loads(body)
    except Exception as exc:
        raise PolygonError(f"unparseable JSON on {label}: {exc}") from exc
    _add_timing(parse_seconds=time.monotonic() - t_parse)

    if not isinstance(data, dict):
        raise PolygonError(f"unexpected JSON shape on {label}: {type(data)}")
    return data


def _get(url: str, params: dict | None, label: str) -> dict:
    """GET with retries. 429 / 5xx / timeout back off with equal jitter;
    auth failures propagate immediately (retrying a bad key is pointless)."""
    last_exc: Exception | None = None
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            return _get_once(url, params, label)
        except AuthError:
            raise
        except RateLimitError as exc:
            last_exc, reason = exc, "429"
            _add_timing(retry_429=1)
        except ServerError as exc:
            last_exc, reason = exc, "5xx"
            _add_timing(retry_5xx=1)
        except RequestTimeoutError as exc:
            last_exc, reason = exc, "timeout"

        if attempt == RETRY_MAX_ATTEMPTS:
            _add_timing(retry_exhausted=1)
            log.warning("%s on %s — exhausted %d attempts, giving up",
                        reason, label, RETRY_MAX_ATTEMPTS)
            break

        # Honour Retry-After when the vendor sends one; otherwise jittered
        # exponential. Capped either way so one hostile header cannot stall
        # the run.
        delay = _backoff_delay(attempt)
        if reason == "429" and last_exc is not None:
            try:
                delay = max(delay, min(float(str(last_exc)), RETRY_MAX_SLEEP))
            except (TypeError, ValueError):
                pass
        _add_timing(retry_count=1, backoff_seconds=delay)
        log.warning("%s on %s — attempt %d/%d, sleeping %.1fs",
                    reason, label, attempt, RETRY_MAX_ATTEMPTS, delay)
        time.sleep(delay)

    assert last_exc is not None
    raise last_exc


# --- Aggregates -------------------------------------------------------------

def fetch_aggs_minute(ticker: str, start: date, end: date,
                      adjusted: bool = False,
                      limit: int = AGGS_LIMIT) -> list[dict]:
    """1-minute bars for [start, end] inclusive. Returns the raw `results`.

    Includes extended hours: the aggregates endpoint returns every minute that
    traded, 04:00-20:00 ET, with no session filter to opt into.

    Raises TruncatedResponseError when the response fills `limit` and offers no
    next_url — the caller must split the range rather than accept a short read.
    That is the failure mode monthly chunking exists to avoid, so it should
    never fire; it is here because "should never" is not "cannot".
    """
    url = (f"{POLYGON_BASE_URL}/v2/aggs/ticker/{ticker.upper()}"
           f"/range/1/minute/{start.isoformat()}/{end.isoformat()}")
    params = {
        "adjusted": "true" if adjusted else "false",
        "sort":     "asc",
        "limit":    int(limit),
    }
    label = f"{ticker} {start}..{end}"

    out: list[dict] = []
    data = _get(url, params, label)
    results = data.get("results") or []
    out.extend(results)

    # Follow next_url when present. The v2 aggregates endpoint does not always
    # paginate, so this is defensive: if the vendor does hand back a cursor we
    # consume it, and if it never does the loop is a no-op.
    pages = 0
    next_url = data.get("next_url")
    while next_url and pages < MAX_PAGES:
        pages += 1
        _add_timing(pages_followed=1)
        data = _get(next_url, None, f"{label} p{pages + 1}")
        page_results = data.get("results") or []
        out.extend(page_results)
        next_url = data.get("next_url")
    if next_url and pages >= MAX_PAGES:
        log.error("  %s: next_url still present after %d pages — data may be "
                  "incomplete for this chunk", label, MAX_PAGES)

    if len(results) >= limit and not data.get("next_url") and pages == 0:
        _add_timing(truncated=1)
        raise TruncatedResponseError(
            f"{label}: response returned {len(results)} bars, hitting the "
            f"limit of {limit}, with no next_url — the range must be split."
        )

    return out


def fetch_ticker_events(ticker: str) -> list:
    """Symbol-change history for the entity CURRENTLY trading as `ticker`.

        GET /vX/reference/tickers/{id}/events?types=ticker_change

    Returns the raw `results.events` list, newest first as the vendor sends it:

        [{"date": "2022-06-09", "type": "ticker_change",
          "ticker_change": {"ticker": "META"}},
         {"date": "2012-05-18", "type": "ticker_change",
          "ticker_change": {"ticker": "FB"}}]

    Each entry is the date that symbol BECAME active, so the symbol in force on
    date D is the entry with the greatest date <= D.

    Direction matters: the vendor documents that querying a ticker returns
    events for "the entity currently represented by that ticker" — which is the
    direction needed here (current symbol -> its former names), not the reverse.

    The endpoint is flagged experimental by the vendor. It is therefore treated
    as best-effort: an unavailable or unentitled endpoint returns [] and the
    caller falls back to identity mapping rather than aborting a backfill.
    """
    url = f"{POLYGON_BASE_URL}/vX/reference/tickers/{ticker.upper()}/events"
    try:
        data = _get(url, {"types": "ticker_change"}, f"events {ticker}")
    except AuthError as exc:
        log.warning("ticker events unavailable for %s (not entitled?) — %s",
                    ticker, exc)
        return []
    except PolygonError as exc:
        log.warning("ticker events failed for %s — %s", ticker, exc)
        return []
    results = data.get("results") or {}
    return results.get("events") or []


def fetch_ticker_details(ticker: str) -> dict:
    """Reference data for one ticker: list_date, delisted_utc, name, figi, cik.

        GET /v3/reference/tickers/{ticker}

    Used to validate the leading-empty report — `list_date` is the vendor's own
    statement of when the symbol first traded, so a leading gap that ends
    exactly at list_date is explained, and one that does not is suspicious.

    Best-effort for the same reason as fetch_ticker_events: a reference lookup
    failing must not fail a data audit.
    """
    url = f"{POLYGON_BASE_URL}/v3/reference/tickers/{ticker.upper()}"
    try:
        data = _get(url, None, f"details {ticker}")
    except PolygonError as exc:
        log.warning("ticker details failed for %s — %s", ticker, exc)
        return {}
    return data.get("results") or {}


def test_connection() -> bool:
    """Cheap key/entitlement probe: one recent minute-bar request for SPY."""
    try:
        url = (f"{POLYGON_BASE_URL}/v2/aggs/ticker/SPY"
               f"/range/1/minute/2024-01-03/2024-01-03")
        data = _get_once(url, {"adjusted": "false", "sort": "asc", "limit": 5},
                         "connection test")
        return bool(data.get("results"))
    except AuthError as exc:
        log.error("Polygon auth failed: %s", exc)
        return False
    except Exception as exc:
        log.error("Cannot reach Polygon at %s — %s", POLYGON_BASE_URL, exc)
        return False
