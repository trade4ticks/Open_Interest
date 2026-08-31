"""Column names as the vendor actually returns them, and how to resolve them.

Confirmed from step 0 on `/v3/stock/history/trade_quote`:

    trade_timestamp  size  price  exchange
    condition  ext_condition1  ext_condition2  ext_condition3  ext_condition4
    bid  ask  bid_size  ask_size  bid_condition  ask_condition

Resolution stays candidate-based rather than hardcoded. The vendor's own field
reference documents these fields unprefixed (`timestamp`, `sequence`,
`condition`, `size`, `exchange`, `price`) while the CSV this endpoint serves
prefixes the trade timestamp. Two naming schemes are in play, so the pipeline
resolves rather than assumes — and says which name it picked.
"""
from __future__ import annotations

import pandas as pd

CAND_TRADE_TIME = ["trade_timestamp", "timestamp", "ms_of_day", "time", "datetime"]
CAND_PRICE      = ["trade_price", "price", "last"]
CAND_SIZE       = ["trade_size", "size", "quantity", "shares"]
CAND_EXCHANGE   = ["trade_exchange", "exchange", "exch"]
CAND_BID        = ["bid", "bid_price", "nbbo_bid"]
CAND_ASK        = ["ask", "ask_price", "nbbo_ask"]
CAND_BID_SIZE   = ["bid_size", "bidsize"]
CAND_ASK_SIZE   = ["ask_size", "asksize"]


class SchemaError(RuntimeError):
    """A required column could not be resolved. Names every column present."""


def find(df: pd.DataFrame, candidates: list[str], purpose: str,
         required: bool = True) -> str | None:
    """Exact, case-insensitive match. Never substring — `size` must not pick
    up `bid_size`, which is present in the same frame."""
    lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    if required:
        raise SchemaError(
            f"could not resolve the {purpose} column. Looked for {candidates}. "
            f"Columns present: {list(df.columns)}"
        )
    return None


def condition_columns(df: pd.DataFrame) -> list[str]:
    """Every column carrying a condition code.

    Includes `bid_condition` and `ask_condition`, which are quote-side and do
    NOT mark a trade — they are excluded by the caller when building the trade
    exclusion mask. Scanned rather than listed because `ext_condition1..4`
    carry the auction and odd-lot markers the primary column does not.
    """
    return [c for c in df.columns if "cond" in c.lower()]


def trade_condition_columns(df: pd.DataFrame) -> list[str]:
    """Condition columns that describe the TRADE, not the quote either side.

    `bid_condition` / `ask_condition` describe the quote. Feeding them into
    the trade exclusion mask would drop trades on the strength of a quote
    attribute, which is a different thing entirely.
    """
    return [c for c in condition_columns(df)
            if not c.lower().startswith(("bid", "ask"))]


def parse_times(series: pd.Series, session_date=None) -> pd.Series:
    """Timestamps to datetimes, accepting either vendor convention.

    ISO strings parse directly. A numeric column is ms-since-ET-midnight and
    needs the session date to become an absolute time.
    """
    if pd.api.types.is_numeric_dtype(series):
        lo, hi = series.min(), series.max()
        if not (0 <= lo and hi <= 86_400_000):
            raise SchemaError(
                f"numeric timestamp column ranges {lo}..{hi}, which is not "
                "milliseconds since midnight — refusing to guess its units"
            )
        if session_date is None:
            raise SchemaError("numeric timestamps need the session date")
        base = pd.Timestamp(session_date)
        return base + pd.to_timedelta(series, unit="ms")
    out = pd.to_datetime(series, errors="coerce")
    if out.isna().all():
        raise SchemaError("timestamp column did not parse as datetimes")
    return out
