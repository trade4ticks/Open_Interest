"""Configuration for the equities-scalp pipeline.

SELF-CONTAINED. This module deliberately does not import the project-root
`config.py`, and nothing here is read by the options pipeline. Every knob the
strategy owner tunes lives in this one file.

Environment variables are read from a `.env` at the project root if one is
present, but every SCALP_* name falls back to a working default so the scripts
run with no .env at all.
"""
from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:                                    # pragma: no cover
    pass                                               # defaults below suffice

SCALP_ROOT = Path(__file__).resolve().parent


# --- ThetaData terminal ------------------------------------------------------
# The terminal runs on the VPS (100.76.94.99) and is reached over Tailscale.
# Default is localhost so the same code works if a terminal is ever run beside
# the scripts; production sets SCALP_THETADATA_BASE_URL explicitly.
THETADATA_BASE_URL = os.environ.get(
    "SCALP_THETADATA_BASE_URL",
    os.environ.get("THETADATA_BASE_URL", "http://localhost:25503"),
)

# Vendor guidance is that client in-flight requests should match the terminal's
# HTTP_CONCURRENCY (default 4). Exceeding it is documented to cause timeouts
# rather than clean rejections, which is much harder to diagnose.
MAX_CONNECTIONS = int(os.environ.get("SCALP_MAX_CONNECTIONS", "4"))

# requests' scalar `timeout=` is a connect and INTER-BYTE read timeout, not a
# cap on total duration: every chunk resets the read clock, so a slow-trickle
# response never trips it and can hold a worker indefinitely. The client
# enforces its own wall-clock deadline on top. trade_quote responses are the
# largest in this project, so TOTAL is generous.
CONNECT_TIMEOUT = int(os.environ.get("SCALP_CONNECT_TIMEOUT", "10"))
READ_TIMEOUT    = int(os.environ.get("SCALP_READ_TIMEOUT",    "60"))
TOTAL_TIMEOUT   = int(os.environ.get("SCALP_TOTAL_TIMEOUT",   "900"))


# --- venue ------------------------------------------------------------------
#
# STATE OF KNOWLEDGE, precisely.
#
# What was actually established: `/v3/stock/snapshot/ohlc` defaults to `nqb`
# (Nasdaq Basic — Nasdaq exchange plus Nasdaq TRF only), which returned 44% of
# true volume and omitted ~10,000 symbols. Passing venue=utp_cta fixed it, and
# FDX then matched its EOD figure of 956,900 shares exactly.
#
# What was NOT established: anything about the HISTORY endpoints. Theta's docs
# describe a 15-minute delayed feed from all three SIP networks alongside a
# real-time Nasdaq Basic feed, which suggests the historical endpoints may
# already be consolidated — in which case the parameter is redundant there, or
# possibly not even accepted.
#
# So the policy is per-endpoint and the history entries are UNRESOLVED. A value
# of None means "send no venue parameter". Nothing here is a guess dressed up
# as a default: the snapshot entry is measured, the rest are open until
# scalp/step0/s1_venue_check.py reports.
#
# A silently Nasdaq-only spread measurement would look completely plausible and
# be wrong, with no EOD figure available to catch it. That is the whole reason
# this is a table and not a constant.
VENUE_UTP_CTA = "utp_cta"

VENUE_BY_ENDPOINT: dict[str, str | None] = {
    # MEASURED: default is nqb, utp_cta required.
    "/v3/stock/snapshot/ohlc":       VENUE_UTP_CTA,

    # UNRESOLVED — do not change until s1_venue_check.py has run.
    "/v3/stock/history/trade_quote": None,
    "/v3/stock/history/quote":       None,

    # Roster endpoint; venue is not expected to be meaningful.
    "/v3/stock/list/symbols":        None,
}

# Set True once s1 has run and VENUE_BY_ENDPOINT reflects its findings. The
# fetch scripts refuse to run a bulk pull while this is False, so a multi-hour
# fetch cannot be launched against an unverified venue assumption.
VENUE_POLICY_VERIFIED = False


# --- known-good reference figure (used by s1) --------------------------------
# FDX consolidated volume on 2026-08-28, matched exactly by snapshot/ohlc with
# venue=utp_cta. The Nasdaq-only figure is ~44% of this.
VENUE_CHECK_SYMBOL   = "FDX"
VENUE_CHECK_DATE     = "2026-08-28"
VENUE_CHECK_EXPECTED = 956_900
VENUE_CHECK_TOLERANCE = 0.01      # 1% — SIP tapes should match near-exactly


# --- storage -----------------------------------------------------------------
# Parquet is the record. Sits alongside the existing /data/chain_eod,
# /data/equity_1min, /data/oi_raw, /data/spx_options on the VPS.
DATA_DIR   = Path(os.environ.get("SCALP_DATA_DIR", "/data/equities_scalp")).resolve()
RAW_DIR    = DATA_DIR / "raw"          # partitioned symbol=X/date=YYYY-MM-DD
STEP0_DIR  = DATA_DIR / "_step0"       # discovery scratch; safe to delete

PARQUET_COMPRESSION = os.environ.get("SCALP_PARQUET_COMPRESSION", "zstd")


# --- Postgres (derived metrics only — no tick data ever) ---------------------
PG_HOST     = os.environ.get("SCALP_PG_HOST", os.environ.get("POSTGRES_HOST", "localhost"))
PG_PORT     = int(os.environ.get("SCALP_PG_PORT", os.environ.get("POSTGRES_PORT", "5432")))
PG_DB       = os.environ.get("SCALP_PG_DB", "equities_scalp")
PG_USER     = os.environ.get("SCALP_PG_USER", os.environ.get("POSTGRES_USER", "portfolio"))
PG_PASSWORD = os.environ.get("SCALP_PG_PASSWORD", os.environ.get("POSTGRES_PASSWORD", "portfolio"))


# --- universe filters --------------------------------------------------------
# Entry thresholds. Yielded ~544 symbols on 2026-08-28 data.
UNIVERSE_MIN_PRICE      = float(os.environ.get("SCALP_MIN_PRICE", "100"))
UNIVERSE_MAX_PRICE      = float(os.environ.get("SCALP_MAX_PRICE", "2000"))
UNIVERSE_MIN_DOLLAR_VOL = float(os.environ.get("SCALP_MIN_DOLLAR_VOL", "100e6"))

# Hysteresis: a name already in the universe is only dropped below these, so
# boundary names don't flicker in and out leaving ragged history.
UNIVERSE_EXIT_PRICE      = float(os.environ.get("SCALP_EXIT_PRICE", "85"))
UNIVERSE_EXIT_DOLLAR_VOL = float(os.environ.get("SCALP_EXIT_DOLLAR_VOL", "70e6"))

# Stickiness: once a symbol enters, keep fetching it this many calendar days
# even after it drops out. Costs little, preserves continuity.
UNIVERSE_STICKY_DAYS = int(os.environ.get("SCALP_STICKY_DAYS", "30"))


# --- metric windows ----------------------------------------------------------
# Regular trading hours, ET. The metric functions take arbitrary start/end
# bounds; these are only the defaults the nightly batch passes.
RTH_START = "09:30:00"
RTH_END   = "16:00:00"
MARKET_TZ = "America/New_York"

# Noise horizons, seconds. Fixed clock, not trade-to-trade — otherwise busy
# stocks look artificially calm.
NOISE_HORIZONS_SEC = (5, 10, 30)

# Intraday granularity stored alongside the daily aggregate, for the
# morning-vs-afternoon question.
INTRADAY_BUCKET_MINUTES = 15


# --- ranking floors ----------------------------------------------------------
# Each metric alone has an obvious failure case, so all three are floors and
# the ranking is a ratio of two of them.
MIN_SPREAD_CENTS   = float(os.environ.get("SCALP_MIN_SPREAD_CENTS", "5"))
MIN_TRADES_PER_MIN = float(os.environ.get("SCALP_MIN_TRADES_PER_MIN", "10"))
MAX_NOISE_BPS      = float(os.environ.get("SCALP_MAX_NOISE_BPS", "10"))


# --- calibration -------------------------------------------------------------
# Realised $/minute-of-attention across two live sessions (616 round trips,
# $1,966 net). This is the ground truth the noise variants are judged against.
# Dollar volume is deliberately absent from the ranking: across these names it
# has no relationship to outcome (MRNA highest at $3.6B and lost money; FDX
# near the bottom at $317M and was third best).
REALISED_DOLLARS_PER_MIN: dict[str, float] = {
    "EXPE":  4.87,
    "LLY":   4.02,
    "FDX":   3.13,
    "PANW":  2.80,
    "A":     1.64,
    "DG":    1.53,
    "TER":   1.52,
    "STX":   0.89,
    "LITE":  0.51,
    "LII":   0.33,
    "DELL":  0.03,
    "Q":    -0.23,
    "MRNA": -1.32,
    "DLTR": -1.32,
    "INTU": -1.91,
}

# Crude noise proxies derived from the owner's own fill prices. Right ballpark
# only — not a benchmark to fit, just a sanity check that a computed noise
# variant lands in the same order of magnitude.
APPROX_NOISE_BPS_FROM_FILLS: dict[str, float] = {
    "FDX":  1.8,
    "LLY":  0.85,
    "LITE": 5.2,
    "DLTR": 2.7,
}
