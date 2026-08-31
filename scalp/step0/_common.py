"""Shared helpers for the step 0 discovery scripts.

Deliberately small. These scripts are meant to be read as easily as run, so
anything that would hide what a script is actually doing stays in the script.

The one thing worth centralising is COLUMN RESOLUTION. Nothing in this
codebase knows the v3 stock schema yet — that is what step 0 measures — so no
script may assume a column called `size` or `condition` exists. Each resolves
the column it needs from a candidate list and says out loud which one it
picked, or fails loudly naming every column it did see. A script that silently
picked the wrong column would produce a plausible number, which is the exact
failure mode this whole exercise is guarding against.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from scalp import config, thetadata as td


# --- Output ------------------------------------------------------------------

def setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )


def banner(title: str) -> None:
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def section(title: str) -> None:
    print()
    print(f"--- {title} " + "-" * max(0, 73 - len(title)))


def fmt_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} PB"


def env_summary() -> None:
    """Print exactly what this run is pointed at, before it does anything.

    Every step 0 result gets pasted back and interpreted later; without the
    terminal URL and the venue policy printed alongside it, a result is not
    reproducible.
    """
    section("environment")
    print(f"terminal      : {config.THETADATA_BASE_URL}")
    print(f"connections   : {td.max_connections()}")
    print(f"timeouts      : connect={config.CONNECT_TIMEOUT}s "
          f"read={config.READ_TIMEOUT}s total={config.TOTAL_TIMEOUT}s")
    print(f"retry         : {td.describe_retry_policy()}")
    print(f"step0 output  : {config.STEP0_DIR}")
    print("venue policy  :")
    for endpoint, venue in config.VENUE_BY_ENDPOINT.items():
        shown = venue if venue is not None else "(none sent)"
        print(f"                {endpoint:<34s} {shown}")
    print(f"policy verified: {config.VENUE_POLICY_VERIFIED}")


def describe_response(raw: td.RawResponse) -> None:
    section("response")
    print(f"url           : {raw.url}")
    print(f"status        : {raw.status}")
    print(f"venue sent    : {raw.venue_sent if raw.venue_sent else '(none)'}")
    print(f"wall clock    : {raw.seconds:.2f} s")
    print(f"body size     : {fmt_bytes(raw.nbytes)} ({raw.nbytes:,} bytes)")
    if raw.seconds > 0:
        print(f"throughput    : {fmt_bytes(raw.nbytes / raw.seconds)}/s")


def describe_frame(df: pd.DataFrame, sample: int = 5) -> None:
    """Columns, inferred dtypes, null counts and a few real rows.

    The columns ARE the finding here — every later design decision depends on
    what the vendor actually sends, and nothing downstream should be written
    until this output has been read.
    """
    section("schema as returned")
    print(f"rows          : {len(df):,}")
    print(f"columns       : {len(df.columns)}")
    if df.empty:
        print("EMPTY RESPONSE — no schema to report.")
        return
    print()
    print(f"{'column':<26s} {'inferred dtype':<16s} {'nulls':>10s}  {'example'}")
    print("-" * 78)
    for col in df.columns:
        nulls = int(df[col].isna().sum())
        first = df[col].dropna()
        example = repr(first.iloc[0])[:24] if len(first) else "(all null)"
        print(f"{col:<26s} {str(df[col].dtype):<16s} {nulls:>10,}  {example}")

    if sample:
        section(f"first {sample} rows")
        with pd.option_context("display.max_columns", None,
                               "display.width", 200):
            print(df.head(sample).to_string())


# --- Column resolution -------------------------------------------------------
#
# Candidate lists live here so all six scripts resolve a given field the same
# way. They are ordered most-specific-first, and matching is exact (case
# insensitive), never substring — `size` must not pick up `bid_size`.
#
# The `trade_*` prefixed forms come from the s1 output, which showed
# trade_quote returning trade-side fields under a `trade_` prefix. The bare
# forms are kept because the vendor's own field reference documents this
# endpoint's trade fields as `timestamp`, `sequence`, `condition`,
# `ext_condition1..4`, `size`, `exchange`, `price` — the two naming schemes
# appear in different places, and neither is assumed.

CAND_TRADE_TIME  = ["trade_timestamp", "timestamp", "ms_of_day", "time", "datetime"]
CAND_QUOTE_TIME  = ["quote_timestamp", "bid_timestamp", "timestamp", "ms_of_day"]
CAND_TRADE_PRICE = ["trade_price", "price", "last"]
CAND_TRADE_SIZE  = ["trade_size", "size", "quantity", "shares"]
CAND_EXCHANGE    = ["trade_exchange", "exchange", "exch"]
CAND_CONDITION   = ["trade_condition", "condition", "conditions", "cond"]
CAND_SEQUENCE    = ["trade_sequence", "sequence", "seq"]
CAND_BID         = ["bid", "bid_price", "nbbo_bid"]
CAND_ASK         = ["ask", "ask_price", "nbbo_ask"]
CAND_BID_SIZE    = ["bid_size", "bidsize"]
CAND_ASK_SIZE    = ["ask_size", "asksize"]
CAND_BID_EXCH    = ["bid_exchange", "bid_exch", "bid_venue"]
CAND_ASK_EXCH    = ["ask_exchange", "ask_exch", "ask_venue"]
CAND_BID_COND    = ["bid_condition"]
CAND_ASK_COND    = ["ask_condition"]


def condition_columns(df: pd.DataFrame) -> list[str]:
    """Every column that looks like it carries a condition code.

    Scanned rather than matched against a list: the vendor documents both a
    `condition` field and `ext_condition1..4`, and the extended ones can carry
    the auction and odd-lot markers that the primary field does not.
    """
    return [col for col in df.columns if "cond" in col.lower()]


def find_column(df: pd.DataFrame, candidates: list[str], purpose: str,
                required: bool = True) -> str | None:
    """Resolve one logical column to whatever the vendor actually called it.

    Matches case-insensitively and reports the choice. On failure prints every
    column that IS present, because at that point the candidate list is the
    thing that is wrong and the real names are what the next edit needs.
    """
    lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower:
            hit = lower[cand.lower()]
            print(f"  {purpose:<22s} -> column {hit!r}")
            return hit

    msg = (f"  {purpose:<22s} -> NOT FOUND. Looked for "
           f"{candidates}. Columns present: {list(df.columns)}")
    if required:
        print(msg)
        raise SystemExit(
            f"\nFAILED: could not resolve the {purpose} column. This is a "
            "finding, not a crash — paste the column list above back and the "
            "candidate list gets corrected."
        )
    print(msg + "  (optional — continuing)")
    return None


# --- Parquet -----------------------------------------------------------------

def step0_dir() -> Path:
    config.STEP0_DIR.mkdir(parents=True, exist_ok=True)
    return config.STEP0_DIR


def measure_parquet(df: pd.DataFrame, stem: str,
                    compressions: tuple[str, ...] = ("zstd", "snappy")) -> dict[str, int]:
    """Write the frame once per compression and return on-disk sizes.

    The storage estimate in the brief (~1-1.5 MB per symbol-day, so 544 x 10
    days ~ 5-8 GB) is explicitly unverified. This is what verifies it, and the
    codec comparison decides what the store uses.
    """
    out = step0_dir()
    sizes: dict[str, int] = {}
    table = pa.Table.from_pandas(df, preserve_index=False)
    for codec in compressions:
        path = out / f"{stem}.{codec}.parquet"
        pq.write_table(table, path, compression=codec)
        sizes[codec] = path.stat().st_size
    return sizes


def report_parquet_sizes(sizes: dict[str, int], n_rows: int,
                         symbol_days: int = 1) -> None:
    section("parquet on disk")
    for codec, nbytes in sizes.items():
        per_day = nbytes / max(symbol_days, 1)
        print(f"{codec:<8s} {fmt_bytes(nbytes):>12s}   "
              f"{nbytes / max(n_rows, 1):>6.1f} bytes/row   "
              f"{fmt_bytes(per_day)}/symbol-day")
    best = min(sizes, key=lambda k: sizes[k])
    per_day = sizes[best] / max(symbol_days, 1)
    print()
    print(f"Extrapolated at {best}, 544 symbols x 10 days: "
          f"{fmt_bytes(per_day * 544 * 10)}")
    print("(Brief's unverified estimate was 5-8 GB. A result far above that "
          "changes the storage plan.)")


# --- Error reporting ---------------------------------------------------------

def report_error(exc: BaseException, what: str) -> None:
    """Print a failure the way it needs to be pasted back — status and body."""
    print(f"  {what}: FAILED")
    print(f"    type   : {type(exc).__name__}")
    print(f"    message: {exc}")
    status = getattr(exc, "status", None)
    if status is not None:
        print(f"    status : {status}")
    body = getattr(exc, "body", "")
    if body:
        print(f"    body   : {body[:300]}")


def die(message: str) -> None:
    print()
    print("!" * 78)
    print(message)
    print("!" * 78)
    sys.exit(1)
