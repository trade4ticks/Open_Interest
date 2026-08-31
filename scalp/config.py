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
# SETTLED by scalp/step0/s1_venue_check.py. Do not change these without
# rerunning it.
#
# snapshot/ohlc — MEASURED, needs the parameter. The default `nqb` (Nasdaq
# Basic: Nasdaq exchange plus Nasdaq TRF only) returned 44% of true volume and
# omitted ~10,000 symbols; venue=utp_cta matched FDX's EOD figure of 956,900
# shares exactly.
#
# history/trade_quote — MEASURED, does NOT need it, and the parameter has no
# effect. s1 found the with- and without-venue responses BYTE-IDENTICAL, and
# 20 distinct exchange codes in the tape. The endpoint accepts the parameter
# and ignores it: it already reads the consolidated tape. This matches Theta's
# documented feed arrangement — a 15-minute delayed feed from all three SIP
# networks alongside the real-time Nasdaq Basic feed.
#
# The earlier belief that utp_cta was required everywhere was a generalisation
# from the snapshot result, made without evidence. It was wrong for the
# history endpoints.
#
# DO NOT add the parameter to the history endpoints "to be safe". An
# unnecessary parameter on an endpoint that ignores it is harmless; an
# unnecessary parameter on an endpoint that interprets it differently than
# assumed is exactly how this went wrong the first time. Sending nothing is
# the measured-correct behaviour, not an omission.
VENUE_UTP_CTA = "utp_cta"

VENUE_BY_ENDPOINT: dict[str, str | None] = {
    # Required. Default nqb is Nasdaq-only.
    "/v3/stock/snapshot/ohlc":       VENUE_UTP_CTA,

    # Accepted and ignored — already consolidated. Send nothing.
    "/v3/stock/history/trade_quote": None,
    "/v3/stock/history/quote":       None,

    # Roster endpoint; venue is not meaningful.
    "/v3/stock/list/symbols":        None,
}

# s1 has run and the table above reflects its findings. The fetch scripts
# refuse a bulk pull while this is False, so a multi-hour backfill cannot be
# launched against an unverified venue assumption.
VENUE_POLICY_VERIFIED = True


# --- time bounds -------------------------------------------------------------
# CONFIRMED by s0/s1: `start_time` and `end_time` are accepted on
# history/trade_quote (format HH:MM:SS).
#
# This is what makes the Phase 2 intraday re-rank cheap. A mid-session re-rank
# pulls the last 30 minutes per symbol rather than a full day, so the same
# metric functions run over a window that costs a fraction of the nightly
# batch. It is also why the metric computation takes arbitrary start/end bounds
# rather than assuming a full session — the intraday path is not being built
# now, but nothing should wall it off.
#
# UNRESOLVED: what the endpoint's DEFAULT bounds are when the parameters are
# omitted. s6_session_bounds.py settles it. Until then, do not assume an
# omitted window means the full session — see the volume-gap note below.
SUPPORTS_TIME_BOUNDS = True

# --- the 19% volume gap: RESOLVED --------------------------------------------
# Settled by s6_session_bounds.py on FDX 2026-08-28. The arithmetic, in full,
# because a bare flag is useless to anyone reading this later:
#
#     RTH prints (default window, 09:30-16:00)      776,192
#     closing cross, counted ONCE                 + 186,344
#                                                 ---------
#                                                   962,536
#     EOD consolidated reference                    956,900   (+0.59%)
#
# There was never any missing volume. Two things were happening at once:
#
#   1. The default query window IS regular hours — s6 found the no-parameter
#      pull byte-identical to an explicit 09:30-16:00 pull. The closing cross
#      prints at 16:00:03, three seconds outside it, so it was excluded by
#      construction. That accounts for the whole 19%.
#
#   2. Widening the window to 04:00-20:00 does NOT simply add it back. It
#      overshoots to 180% of EOD, because the official close is RE-REPORTED on
#      a schedule through the evening. The same 186,344 shares at $330.88 on
#      NYSE appear five times: once at 16:00:03.158 as condition 98 (CLOSING),
#      then four more times as condition 51 (MC_OFFICIAL_CLOSE) at
#      16:00:03.212, 16:10:00, 18:30:00 and 19:00:00. The opening auction does
#      the same thing — 20,056 shares once as 62 (OPEN_REPORT) and once as 66
#      (MC_OFFICIAL_OPEN).
#
# So a naive sum over a wide window double-counts auctions, and a naive sum
# over the default window omits the closing cross entirely. Neither is a tape
# problem and neither affects this pipeline, which pulls RTH only: trade counts
# and spreads were never contaminated. The numbers are now understood rather
# than merely close.
VOLUME_GAP_RESOLVED = True


# --- known-good reference figure (used by s1) --------------------------------
# FDX consolidated volume on 2026-08-28, matched exactly by snapshot/ohlc with
# venue=utp_cta. The Nasdaq-only figure is ~44% of this.
VENUE_CHECK_SYMBOL   = "FDX"
VENUE_CHECK_DATE     = "2026-08-28"
VENUE_CHECK_EXPECTED = 956_900
VENUE_CHECK_TOLERANCE = 0.01      # 1% — SIP tapes should match near-exactly


# --- exchange codes ----------------------------------------------------------
# Verbatim from ThetaData's published enum:
#   https://docs.thetadata.us/Articles/Errors-Exchanges-Conditions/Exchanges.html
#
# The `exchange` column on trade_quote is an integer. Without this table it is
# unreadable, and off-exchange share — which was on the metric list — would
# need a separate data source. It doesn't: the column already carries it.
#
# On FDX 2026-08-28, code 57 alone carried 15,515 of 30,982 trades. That is
# FINRA/NASDAQ TRF, i.e. half the tape printed off-exchange, which is ordinary
# for a large cap and is exactly the quantity worth measuring.
EXCHANGE_NAMES: dict[int, str] = {
    1: "Nasdaq Exchange",                        2: "Nasdaq Alternative Display Facility",
    3: "New York Stock Exchange",                4: "American Stock Exchange",
    5: "Chicago Board Options Exchange",         6: "International Securities Exchange",
    7: "NYSE ARCA (Pacific)",                    8: "National Stock Exchange (Cincinnati)",
    9: "Philadelphia Stock Exchange",           10: "Options Pricing Reporting Authority",
    11: "Boston Stock/Options Exchange",        12: "Nasdaq Global+Select Market (NMS)",
    13: "Nasdaq Capital Market (SmallCap)",     14: "Nasdaq Bulletin Board",
    15: "Nasdaq OTC",                           16: "Nasdaq Indexes (GIDS)",
    17: "Chicago Stock Exchange",               18: "Toronto Stock Exchange",
    19: "Canadian Venture Exchange",            20: "Chicago Mercantile Exchange",
    21: "New York Board of Trade",              22: "ISE Mercury",
    23: "COMEX (division of NYMEX)",            24: "Chicago Board of Trade",
    25: "New York Mercantile Exchange",         26: "Kansas City Board of Trade",
    27: "Minneapolis Grain Exchange",           28: "NYSE/ARCA Bonds",
    29: "Nasdaq Basic",                         30: "Dow Jones Indices",
    31: "ISE Gemini",                           32: "Singapore International Monetary Exchange",
    33: "London Stock Exchange",                34: "Eurex",
    35: "Implied Price",                        36: "Data Transmission Network",
    37: "London Metals Exchange Matched Trades", 38: "London Metals Exchange",
    39: "Intercontinental Exchange (IPE)",      40: "Nasdaq Mutual Funds (MFDS)",
    41: "COMEX Clearport",                      42: "CBOE C2 Option Exchange",
    43: "Miami Exchange",                       44: "NYMEX Clearport",
    45: "Barclays",                             46: "Miami Emerald Options Exchange",
    47: "NASDAQ Boston",                        48: "HotSpot Eurex US",
    49: "Eurex US",                             50: "Eurex EU",
    51: "Euronext Commodities",                 52: "Euronext Index Derivatives",
    53: "Euronext Interest Rates",              54: "CBOE Futures Exchange",
    55: "Philadelphia Board of Trade",          56: "CME Floor",
    57: "FINRA/NASDAQ Trade Reporting Facility", 58: "BSE Trade Reporting Facility",
    59: "NYSE Trade Reporting Facility",        60: "BATS Trading",
    61: "CBOT Floor",                           62: "Pink Sheets",
    63: "BATS Y Exchange",                      64: "Direct Edge A",
    65: "Direct Edge X",                        66: "Russell Indexes",
    67: "CME Indexes",                          68: "Investors Exchange",
    69: "Miami Pearl Options Exchange",         70: "London Stock Exchange",
    71: "NYSE Global Index Feed",               72: "TSX Indexes",
    73: "Members Exchange",                     74: "EMPTY",
    75: "Long-Term Stock Exchange",             76: "EMPTY",
    77: "24X National Exchange",
    # 78 is NOT in the vendor's published enum. It appears in all four s4
    # census symbols at trivial volume, which is the signature of a real venue
    # the docs have not caught up with rather than corrupt data. Labelled
    # explicitly so it reads as a known unknown instead of silently falling
    # through exchange_name() as an unrecognised code.
    78: "unidentified venue (code 78, absent from vendor enum)",
}

# Codes present in real data but not in the vendor's enum. Tracked separately
# so off-exchange share can report them rather than absorbing them into the
# on-exchange bucket by default — they are neither confirmed lit nor confirmed
# off-exchange, and pretending otherwise picks an answer we have not measured.
UNIDENTIFIED_EXCHANGE_CODES: frozenset[int] = frozenset({78})

# The reporting facilities — trades executed away from an exchange and printed
# to a TRF or the ADF. This is the definition of off-exchange share, and it is
# the set of codes that are NOT a lit venue, not a guess at which venues are
# "dark".
#
#   2  Nasdaq Alternative Display Facility (ADF)
#   57 FINRA/NASDAQ Trade Reporting Facility
#   58 BSE Trade Reporting Facility
#   59 NYSE Trade Reporting Facility
OFF_EXCHANGE_CODES: frozenset[int] = frozenset({2, 57, 58, 59})


def exchange_name(code) -> str:
    """Readable name for an exchange code, or a marked placeholder.

    Unknown codes are returned as `unknown(<code>)` rather than dropped or
    silently bucketed as on-exchange. A code missing from the table is a
    finding — the vendor added a venue — not a row to discard.
    """
    try:
        return EXCHANGE_NAMES[int(code)]
    except (TypeError, ValueError):
        return f"unknown({code!r})"
    except KeyError:
        return f"unknown({int(code)})"


def is_off_exchange(code) -> bool:
    """True for TRF/ADF prints. Unknown codes are NOT counted as off-exchange.

    Counting an unrecognised code as off-exchange would inflate the metric
    every time the vendor adds a venue. Unknown codes surface through
    exchange_name() instead.
    """
    try:
        return int(code) in OFF_EXCHANGE_CODES
    except (TypeError, ValueError):
        return False


# --- trade condition codes ---------------------------------------------------
# PARTIAL, and deliberately so. Only the codes read verbatim from the vendor's
# published table are here:
#   https://http-docs.thetadata.us/Articles/Data-And-Requests/Values/Trade-Conditions.html
#
# The full enum runs past 148 and has not been transcribed. Nothing may assume
# this table is complete: s4_conditions.py reports every code it observes,
# labelling the ones it can and printing the rest as unknown. An invented
# label on a code that drives an exclusion decision is worse than no label.
#
# The codes that matter to the volume-gap question are 62 / 66 (opening) and
# 98 / 51 (closing) — a closing cross that is present in a pull shows up as
# code 98 prints, which is a far more direct test than inferring it from a
# volume delta.
TRADE_CONDITIONS: dict[int, str] = {
    1:   "FORM_T",                  # before and after regular hours
    7:   "OPEN_REPORT_IN_SEQ",      # opening report, first price
    26:  "OPEN_DETAIL",             # opening detail, multi-part open reports
    45:  "MATCH_CROSS",             # crossing-session trade
    51:  "MC_OFFICIAL_CLOSE",       # market centre official closing value
    62:  "OPEN_REPORT",             # opening trade report
    66:  "MC_OFFICIAL_OPEN",        # market centre official opening value
    96:  "DERIVATIVE",              # derivatively priced
    98:  "CLOSING",                 # market centre closing prints (closing auction)
    115: "ODD_LOT",                 # any trade with size 1-99
    148: "EXTENDED_HOURS_TRADE",    # executed outside regular market hours
}

# Filler values in the condition columns — not codes. `ext_condition1..4` are
# padded with 255 when the print carries fewer than four extended conditions,
# and 0 appears the same way. Treating either as a real code fills every
# census with a meaningless top entry.
NO_CONDITION_SENTINELS: frozenset[int] = frozenset({0, 255})


# --- auction prints vs restatements: the distinction that matters ------------
# These two sets look similar and behave completely differently. Confusing them
# is what made a wide-window pull report 180% of true daily volume.
#
# GENUINE AUCTION EXECUTIONS. Real prints, real shares, counted ONCE. If we
# ever want auction volume as a feature, this is the set to sum.
AUCTION_PRINT_CONDITION_CODES: frozenset[int] = frozenset({
    7,    # OPEN_REPORT_IN_SEQ  — opening report, first price
    26,   # OPEN_DETAIL         — opening detail, multi-part open reports
    45,   # MATCH_CROSS         — crossing-session trade
    62,   # OPEN_REPORT         — opening trade report
    98,   # CLOSING             — market centre closing print (closing auction)
})

# RESTATEMENTS. NOT executions. These re-report an auction that already
# printed under 62 or 98, on a schedule running for hours afterwards. On FDX
# the official close was restated four times between 16:00:03 and 19:00:00,
# each carrying the full 186,344 shares.
#
# Summing these adds volume that never traded. They are excluded from every
# count — shares, trades, and arrival rates alike.
RESTATEMENT_CONDITION_CODES: frozenset[int] = frozenset({
    51,   # MC_OFFICIAL_CLOSE — restates the 98 print
    66,   # MC_OFFICIAL_OPEN  — restates the 62 print
})

# Codes that mark a print outside regular hours.
EXTENDED_HOURS_CONDITION_CODES: frozenset[int] = frozenset({1, 148})

# OFF-QUOTE PRINTS, set from the s4 census across FDX, LLY, LITE and DLTR.
#
# The criterion is measured behaviour, never the code's name: a code is
# excluded when its prints sit systematically away from the prevailing quote
# in every symbol, because such a print is not an arrival at the quote and
# counting it as one corrupts trades/min and the at-bid/at-ask classification.
#
#   96  DERIVATIVE       45-70% outside NBBO, 4.3-9.4 bps off mid
#   4   (unlabelled)     46-62% outside NBBO
#   124 (unlabelled)     40-100% outside NBBO
#
# 4 and 124 carry no vendor label and are excluded purely on measurement.
# That is the right basis — an unlabelled code printing half its volume
# outside the NBBO is disqualified by what it does, not by what it is called.
OFF_QUOTE_CONDITION_CODES: frozenset[int] = frozenset({4, 96, 124})

# KEPT, deliberately, and worth recording why:
#
#   115 ODD_LOT   75-86% of ALL trades, only 1.2-2.8 bps off mid, 6-11%
#                 outside NBBO. These are clean executions near the mid.
#                 Excluding odd lots is a common reflex and it would delete
#                 most of the tape for exactly the names this strategy trades.
#
#   95  (unlabelled)  Borderline: 17-25% outside NBBO, but carries 15-18% of
#                 volume and sits close to the mid. Kept for now. REVISIT
#                 AFTER CALIBRATION — if the ranking is sensitive to it, that
#                 sensitivity is itself the finding.
REVISIT_AFTER_CALIBRATION: frozenset[int] = frozenset({95})

# Everything excluded from volume, trade-count and arrival-rate metrics.
EXCLUDED_CONDITION_CODES: frozenset[int] = (
    RESTATEMENT_CONDITION_CODES | OFF_QUOTE_CONDITION_CODES
)


def condition_name(code) -> str:
    """Readable name for a condition code, or a marked placeholder.

    The table is known-incomplete, so an unknown code is normal and is
    labelled as such rather than treated as an error.
    """
    try:
        code_i = int(code)
    except (TypeError, ValueError):
        return f"unknown({code!r})"
    if code_i in NO_CONDITION_SENTINELS:
        return "(none)"
    return TRADE_CONDITIONS.get(code_i, f"unlabelled({code_i})")


def is_restatement(code) -> bool:
    """True for a code that re-reports an auction that already printed."""
    try:
        return int(code) in RESTATEMENT_CONDITION_CODES
    except (TypeError, ValueError):
        return False


# --- restatement guard -------------------------------------------------------
# A backstop for the condition-code exclusion above, for symbols or venues
# where a restatement arrives under a code we have not seen. The signature is
# an identical (size, price, exchange) triple reappearing at timestamps more
# than a second apart.
#
# That signature alone is NOT sufficient on its own, and the threshold below is
# why: a 100-share print at the same price on the same venue recurs constantly
# in any liquid name, entirely legitimately. Requiring a large print keeps the
# guard on the class of event it was built for — an auction being re-reported —
# rather than firing on ordinary round-lot repetition.
#
# The guard FLAGS and REPORTS. It never drops rows on its own: the condition
# codes are the primary defence and this exists to catch what they miss.
RESTATEMENT_MIN_SHARES = int(os.environ.get("SCALP_RESTATEMENT_MIN_SHARES", "5000"))
RESTATEMENT_MIN_SECONDS = float(os.environ.get("SCALP_RESTATEMENT_MIN_SECONDS", "1.0"))


# --- storage -----------------------------------------------------------------
#
# CORRECTION TO THE BRIEF. The brief said `/data/equities_scalp/`. That is the
# main VPS disk and it is the wrong place. Parquet goes to volume storage
# block 3, the mount that already holds equity_1min and the other equity
# parquet stores.
#
# The measured backfill is 3.1 GB and grows every session. Writing that to the
# VPS root would fill it silently, and the failure would surface as something
# unrelated breaking days later.
#
# SO THERE IS NO DEFAULT. `SCALP_DATA_DIR` must be set explicitly and is
# validated before anything is written. A missing or wrong value fails loudly
# at the first call rather than quietly landing 3 GB on the root filesystem.
# This follows the convention the existing stores use — CHAIN_EOD_DIR,
# EQUITY_1MIN_DIR and the rest are all set in .env, never defaulted into the
# project tree.

class ConfigError(RuntimeError):
    """A required setting is missing or points somewhere unusable."""


_DATA_DIR_ENV = "SCALP_DATA_DIR"
_data_dir_raw = os.environ.get(_DATA_DIR_ENV)

# Step 0 scratch is exempt: it is small, disposable, and already lives at
# /data/equities_scalp/_step0. Production pulls are what must not go there.
STEP0_DIR = Path(os.environ.get("SCALP_STEP0_DIR",
                                "/data/equities_scalp/_step0")).resolve()

PARQUET_COMPRESSION = os.environ.get("SCALP_PARQUET_COMPRESSION", "zstd")


def _shares_root_device(path: Path) -> bool:
    """True if `path` sits on the same filesystem as `/`.

    This is the check that actually protects the root disk. A path string can
    look like a mount and not be one — an unmounted /mnt/block3 is just an
    empty directory on the root filesystem, and writing to it fills the VPS
    exactly as writing to /data would. Comparing device ids catches that;
    comparing path prefixes does not.
    """
    if os.name != "posix":
        return False                     # Windows dev box; nothing to protect
    try:
        return os.stat(path).st_dev == os.stat("/").st_dev
    except OSError:
        return False


def data_dir(*, require_separate_volume: bool = True) -> Path:
    """The validated parquet root. Raises rather than guessing.

    Pass require_separate_volume=False only for a deliberate local test on a
    single-disk machine.
    """
    if not _data_dir_raw:
        raise ConfigError(
            f"{_DATA_DIR_ENV} is not set, and there is deliberately no default.\n"
            "Parquet must be written to volume storage block 3, alongside the\n"
            "existing equity_1min store, NOT to the VPS root disk.\n\n"
            f"  export {_DATA_DIR_ENV}=/<block-3-mount>/equities_scalp\n\n"
            "Set it in .env next to the other *_DIR entries."
        )
    path = Path(_data_dir_raw).resolve()
    if not path.parent.exists():
        raise ConfigError(
            f"{_DATA_DIR_ENV}={path} — its parent does not exist. That usually\n"
            "means the volume is not mounted. Refusing to create a directory\n"
            "tree on whatever filesystem is underneath it."
        )
    if require_separate_volume and _shares_root_device(path.parent):
        raise ConfigError(
            f"{_DATA_DIR_ENV}={path} is on the SAME filesystem as /.\n"
            "That is the root disk, which is what this setting exists to avoid.\n"
            "Check the block 3 volume is mounted (`df -h`, `lsblk`) and that the\n"
            "path points at the mount, not at an empty directory beside it."
        )
    return path


def raw_dir() -> Path:
    """Raw pulls, partitioned symbol=X/date=YYYY-MM-DD."""
    return data_dir() / "raw"


# --- retention ---------------------------------------------------------------
#
# THE SPACE BUDGET IS THE BINDING CONSTRAINT, so retention is designed in now
# rather than added after the disk fills.
#
#   block 3 (/mnt/trading_volume_3)  100 GB, 78% used, ~22 GB available
#   backfill (544 symbols x 10 days)  3.1 GB
#   ongoing                          ~310 MB per trading day
#
# That is roughly 60 sessions before it gets tight, and there is no comfortable
# overflow: volume 1 has 20 GB free and volume 2 has 11 GB.
#
# What pruning actually costs: Postgres holds the computed metrics
# permanently, so deleting old raw parquet only removes the ability to
# RECOMPUTE a changed formula over old days. That is a re-pull, not a loss —
# and a re-pull of a month is minutes, per the s3 timing.
#
# DELETION IS NEVER AUTOMATIC. prune.py is run by hand. fetch.py will refuse
# to start when the projected write does not fit, but it will not free space
# to make room — an automated deleter racing an automated writer is how a
# store loses days nobody noticed were gone.
RAW_RETENTION_DAYS = int(os.environ.get("SCALP_RAW_RETENTION_DAYS", "45"))

# Measured from s2/s3: 3.1 GB for 544 symbols x 10 sessions.
ESTIMATED_MB_PER_SYMBOL_DAY = float(
    os.environ.get("SCALP_MB_PER_SYMBOL_DAY", "0.6"))

# fetch.py refuses to start unless the projected write plus this margin fits.
FREE_SPACE_MARGIN_GB = float(os.environ.get("SCALP_FREE_SPACE_MARGIN_GB", "3.0"))


def free_space_gb(path: Path | None = None) -> float:
    """Free space on the filesystem holding `path` (defaults to the data dir)."""
    import shutil
    target = path or data_dir()
    return shutil.disk_usage(target).free / (1024 ** 3)


def projected_write_gb(n_symbols: int, n_days: int) -> float:
    return n_symbols * n_days * ESTIMATED_MB_PER_SYMBOL_DAY / 1024


# --- auction exclusion -------------------------------------------------------
# Auction prints and the quotes around them are not tradeable conditions. The
# FDX opening quote was bid 330.45 / ask 336.15 — a $5.70 spread that is an
# auction artifact, and one such observation distorts a daily average.
#
# Excluded from SPREAD and NOISE only. Trade counts and volume keep the full
# RTH window, because an arrival is an arrival.
#
# Calibration reports every metric BOTH with and without this exclusion, so
# the sensitivity is visible rather than assumed.
EXCLUDE_OPEN_MINUTES  = float(os.environ.get("SCALP_EXCLUDE_OPEN_MINUTES", "1"))
EXCLUDE_CLOSE_MINUTES = float(os.environ.get("SCALP_EXCLUDE_CLOSE_MINUTES", "1"))


# --- flicker variants --------------------------------------------------------
# Both computed, neither assumed. s5 found 78.4% of records identical on
# price, size AND venue — venue turnover explained only 1.7 points of the
# 80.1%, so the repeats remain largely unexplained.
#
# The likely cause is that the NBBO is recomputed on every participant's quote
# update rather than only when the best changes: a venue behind the inside
# adjusting its quote fires a record while the NBBO itself is unchanged. The
# endpoint returns the NBBO, so that cause is not visible in the columns we
# get and cannot be confirmed from this data.
#
# If it is right, the raw record count measures TOTAL QUOTE TRAFFIC across all
# venues, not inside-market instability. Still book activity, still possibly
# predictive — but a different quantity from the flicker metric as intended.
# So both are computed and calibration decides, exactly as with the noise
# variants.
FLICKER_VARIANTS = (
    "quote_records_per_min",   # raw record count — total quote traffic
    "nbbo_changes_per_min",    # records where bid or ask price actually moved
    "bid_changes_per_min",     # one-sided, given the 1,266:1 asymmetry
    "ask_changes_per_min",
)


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

# --- noise variants ----------------------------------------------------------
# All computed, all stored, compared empirically against realised $/min. No
# single one is assumed correct.
#
# BID-SIDE AND ASK-SIDE ARE FIRST-CLASS OUTPUTS, NOT DIAGNOSTICS. s5 settled
# this on FDX: over 10,278 consecutive quote records there were 602 bid-only
# moves, 664 ask-only moves, and exactly ONE where both sides moved together.
# One in ten thousand. Two-sided repricing essentially never happens — the
# midpoint moves because one side twitched, and a mid-based noise number is
# therefore an average of a quantity that is one-sided almost every time it
# changes. The asymmetry is the phenomenon, and the mid destroys it by
# construction.
NOISE_VARIANTS = (
    "tw_mid",       # time-weighted midpoint, weighted by quote duration
    "last_mid",     # instantaneous last-quote midpoint, for comparison
    "trade_price",  # trade prices, for comparison (contains the spread itself)
    "bid_side",     # bid alone — first-class
    "ask_side",     # ask alone — first-class
)

# --- off_mid_bps: a candidate ranking metric ---------------------------------
# Median absolute distance of a trade from the prevailing midpoint, in bps,
# over ordinary (non-excluded) prints. s4 computes it per condition code
# already; this promotes it to a ranking candidate.
#
# It may simply be the answer. On ordinary odd-lot trades:
#
#     FDX  1.21 bps   realised  $3.13/min
#     LLY  1.45 bps   realised  $4.02/min
#     DLTR 1.17 bps   realised -$1.32/min
#     LITE 2.78 bps   realised  $0.51/min
#
# LITE is more than double every other name on this measure and was the worst
# tradeable name of the four. It is a spread-and-noise composite that comes
# free from every trade — no bucketing, no time-weighting, no horizon choice.
#
# Note what it does NOT separate: FDX and DLTR sit 0.04 bps apart and are at
# opposite ends of the realised results. So this is a candidate, not a
# conclusion — it goes into calibration beside the four noise variants and the
# comparison decides.
COMPUTE_OFF_MID_BPS = True

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
