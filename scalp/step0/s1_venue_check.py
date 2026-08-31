"""Step 0.1 — does /v3/stock/history/trade_quote need venue=utp_cta?

THE ASSUMPTION BEING TESTED, STATED HONESTLY.

What was established: `/v3/stock/snapshot/ohlc` defaults to `nqb` (Nasdaq
Basic — Nasdaq exchange plus Nasdaq TRF only), that this returned 44% of true
volume and omitted ~10,000 symbols, and that venue=utp_cta fixed it, with FDX
then matching its EOD figure of 956,900 shares exactly.

What was NOT established: anything at all about the HISTORY endpoints. That
generalisation was made without evidence. Theta's docs describe a 15-minute
delayed feed from all three SIP networks alongside a real-time Nasdaq Basic
feed, which suggests the history endpoints may already be consolidated — in
which case the parameter is redundant there, or possibly not even accepted.

So: pull the same symbol-day twice, once with the parameter and once without,
sum the trade sizes, and compare both against the known consolidated figure.

    python -m scalp.step0.s1_venue_check
    python -m scalp.step0.s1_venue_check --symbol FDX --date 2026-08-28

HOW TO READ THE RESULT

  * Both totals = 956,900          -> the history default is ALREADY
                                      consolidated. The parameter is
                                      unnecessary here. Leave the history
                                      entries in VENUE_BY_ENDPOINT as None.
  * Either total near 421,000      -> that call is Nasdaq-only (44%). The
                                      parameter IS required on this endpoint;
                                      set it in VENUE_BY_ENDPOINT.
  * utp_cta call returns HTTP 400  -> the parameter is REJECTED outright here.
                                      Whatever the no-venue total is, is what
                                      this endpoint can give.
  * Responses byte-identical       -> the parameter is ACCEPTED AND IGNORED.
                                      Decisive: the endpoint takes it and does
                                      nothing with it.
  * Neither total near either
    figure                         -> do NOT reconcile it by picking the
                                      closer one. Something else differs
                                      (session bounds, condition inclusion)
                                      and it has to be understood before any
                                      spread number is trusted.

WHY THIS GATES EVERYTHING DOWNSTREAM: a silently Nasdaq-only spread
measurement looks completely plausible and there is no EOD figure to catch it.
This is the only point in the whole pipeline where that check is possible.
"""
from __future__ import annotations

import argparse

import pandas as pd

from scalp import config, thetadata as td
from scalp.step0 import _common as c


# Candidate names for the trade-size column, most specific first. Exact
# case-insensitive matching only — `bid_size` and `ask_size` will also be in
# this response and must not be picked up.
SIZE_CANDIDATES = ["size", "trade_size", "quantity", "shares", "volume"]
TIME_CANDIDATES = ["ms_of_day", "timestamp", "time", "datetime", "sip_timestamp"]
EXCHANGE_CANDIDATES = ["exchange", "venue", "exch", "trade_exchange"]

RTH_START_MS = 9 * 3_600_000 + 30 * 60_000     # 09:30:00 ET -> 34,200,000
RTH_END_MS   = 16 * 3_600_000                  # 16:00:00 ET -> 57,600,000


def _sum_sizes(df: pd.DataFrame, size_col: str) -> int:
    return int(pd.to_numeric(df[size_col], errors="coerce").fillna(0).sum())


def _rth_subset(df: pd.DataFrame, time_col: str | None) -> pd.DataFrame | None:
    """Rows inside 09:30-16:00 ET, or None if the time column can't be read.

    Reported alongside the full-response total because the 956,900 reference
    is an EOD consolidated figure and it is not established whether that
    includes extended hours. If the full-day total overshoots and the RTH
    total matches, the difference is session bounds, not tape coverage — a
    completely different conclusion.
    """
    if time_col is None:
        return None
    col = df[time_col]

    if pd.api.types.is_numeric_dtype(col):
        # Assume milliseconds since ET midnight (ThetaData's ms_of_day
        # convention). Sanity-check the range before trusting it.
        lo, hi = col.min(), col.max()
        if not (0 <= lo and hi <= 86_400_000):
            print(f"  time column {time_col!r} is numeric but ranges "
                  f"{lo}..{hi} — not ms-since-midnight. Skipping RTH split.")
            return None
        return df[(col >= RTH_START_MS) & (col < RTH_END_MS)]

    parsed = pd.to_datetime(col, errors="coerce")
    if parsed.isna().all():
        print(f"  time column {time_col!r} did not parse as a datetime. "
              "Skipping RTH split.")
        return None
    tod = parsed.dt.time
    start = pd.Timestamp("09:30:00").time()
    end = pd.Timestamp("16:00:00").time()
    return df[(tod >= start) & (tod < end)]


def _pull(symbol: str, day: str, venue: str | None, label: str):
    """One trade_quote pull. Returns (RawResponse, DataFrame) or (None, None)."""
    print()
    print(f"[{label}]")
    try:
        raw = td.trade_quote(symbol, day, day, venue=venue, total_timeout=900)
    except td.BadRequestError as exc:
        print("  HTTP 400 — REJECTED OUTRIGHT")
        print(f"  body: {exc.body[:300]}")
        return None, None
    except Exception as exc:
        c.report_error(exc, label)
        return None, None

    df = raw.frame()
    print(f"  status {raw.status}  |  {raw.seconds:.2f}s  |  "
          f"{c.fmt_bytes(raw.nbytes)}  |  {len(df):,} rows")
    return raw, df


def _compare(total: int, expected: int, tol: float) -> str:
    """Classify a volume total against the known consolidated figure."""
    if expected <= 0:
        return "no reference figure"
    ratio = total / expected
    if abs(ratio - 1.0) <= tol:
        return f"MATCHES consolidated ({ratio:6.1%} of {expected:,})"
    # 44% was the measured Nasdaq-only share on the snapshot endpoint.
    if 0.38 <= ratio <= 0.50:
        return f"NASDAQ-ONLY territory ({ratio:6.1%} of {expected:,})"
    return f"UNEXPLAINED ({ratio:6.1%} of {expected:,})"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--symbol", default=config.VENUE_CHECK_SYMBOL)
    ap.add_argument("--date", default=config.VENUE_CHECK_DATE)
    ap.add_argument("--expected", type=int, default=config.VENUE_CHECK_EXPECTED,
                    help="known consolidated share volume for symbol/date")
    ap.add_argument("--tolerance", type=float, default=config.VENUE_CHECK_TOLERANCE)
    ap.add_argument("--skip-control", action="store_true",
                    help="skip the snapshot/ohlc control pair")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    c.setup_logging(args.verbose)
    c.banner("STEP 0.1 — VENUE PARAMETER ON history/trade_quote")
    c.env_summary()
    print()
    print(f"symbol        : {args.symbol}")
    print(f"date          : {args.date}")
    print(f"reference     : {args.expected:,} shares (consolidated, "
          f"+/-{args.tolerance:.0%})")
    print(f"nasdaq-only would be roughly {int(args.expected * 0.44):,} (44%)")

    # --- the two pulls ------------------------------------------------------
    raw_none, df_none = _pull(args.symbol, args.date, None,
                              "A. trade_quote WITHOUT venue")
    raw_utp, df_utp = _pull(args.symbol, args.date, config.VENUE_UTP_CTA,
                            f"B. trade_quote WITH venue={config.VENUE_UTP_CTA}")

    param_rejected = raw_utp is None and raw_none is not None

    if raw_none is None and raw_utp is None:
        c.die("Both pulls failed — nothing to compare. This is a connectivity "
              "or entitlement problem, not a venue finding. Check s0 first.")

    # --- byte-identity: the decisive 'accepted and ignored' signal ----------
    c.section("response identity")
    if raw_none is not None and raw_utp is not None:
        identical = raw_none.body == raw_utp.body
        print(f"without venue : {raw_none.nbytes:,} bytes")
        print(f"with utp_cta  : {raw_utp.nbytes:,} bytes")
        print(f"byte-identical: {identical}")
        if identical:
            print()
            print("DECISIVE: the endpoint ACCEPTS the parameter and IGNORES it.")
            print("Whatever tape the default reads is the only tape available")
            print("here, and the volume totals below say which one that is.")
    elif param_rejected:
        print("with utp_cta  : REJECTED (HTTP 400)")
        print("The parameter is not accepted on this endpoint at all.")

    # --- schema + totals ----------------------------------------------------
    reference_df = df_none if df_none is not None and not df_none.empty else df_utp
    if reference_df is None or reference_df.empty:
        c.die("A pull succeeded but returned no rows — cannot sum trade sizes. "
              "Rerun with --date set to a different regular session.")

    c.describe_frame(reference_df, sample=3)

    c.section("column resolution")
    size_col = c.find_column(reference_df, SIZE_CANDIDATES, "trade size")
    time_col = c.find_column(reference_df, TIME_CANDIDATES, "timestamp",
                             required=False)
    exch_col = c.find_column(reference_df, EXCHANGE_CANDIDATES, "exchange",
                             required=False)

    c.section("share volume")
    print(f"{'pull':<28s} {'rows':>12s} {'shares (all)':>16s}  verdict")
    print("-" * 78)
    totals: dict[str, int] = {}
    for label, df in (("without venue", df_none),
                      (f"venue={config.VENUE_UTP_CTA}", df_utp)):
        if df is None:
            print(f"{label:<28s} {'—':>12s} {'REJECTED / FAILED':>16s}")
            continue
        total = _sum_sizes(df, size_col)
        totals[label] = total
        print(f"{label:<28s} {len(df):>12,} {total:>16,}  "
              f"{_compare(total, args.expected, args.tolerance)}")

    # RTH-only totals, when the timestamp column allows it.
    if time_col is not None:
        c.section("share volume, regular hours only (09:30-16:00 ET)")
        print("The reference figure is an EOD consolidated number and it is NOT")
        print("established whether it includes extended hours. If the full-day")
        print("total overshoots but this one matches, the difference is session")
        print("bounds — not tape coverage.")
        print()
        print(f"{'pull':<28s} {'rows':>12s} {'shares (RTH)':>16s}  verdict")
        print("-" * 78)
        for label, df in (("without venue", df_none),
                          (f"venue={config.VENUE_UTP_CTA}", df_utp)):
            if df is None:
                continue
            sub = _rth_subset(df, time_col)
            if sub is None:
                break
            total = _sum_sizes(sub, size_col)
            print(f"{label:<28s} {len(sub):>12,} {total:>16,}  "
                  f"{_compare(total, args.expected, args.tolerance)}")

    # Exchange breakdown — if the tape is Nasdaq-only it shows up here as a
    # handful of exchange codes rather than the full SIP set.
    if exch_col is not None:
        c.section(f"exchange codes present (column {exch_col!r})")
        for label, df in (("without venue", df_none),
                          (f"venue={config.VENUE_UTP_CTA}", df_utp)):
            if df is None:
                continue
            counts = df[exch_col].value_counts()
            print(f"\n{label}: {len(counts)} distinct code(s)")
            print(counts.head(30).to_string())
        print()
        print("A consolidated tape shows many exchange codes. Two or three")
        print("(Nasdaq plus its TRF) means Nasdaq-only regardless of the totals.")

    # --- control ------------------------------------------------------------
    if not args.skip_control:
        c.section("control: snapshot/ohlc with and without venue")
        print("This validates the MEASUREMENT, not the figure. snapshot/ohlc is")
        print("'now' — it returns the current/most recent session, not --date —")
        print("so only the RATIO between the two pulls is meaningful here. If")
        print("the no-venue pull is ~44% of the utp_cta pull, the method works")
        print("and the trade_quote result above can be trusted.")
        try:
            snap_none = td.snapshot_ohlc(args.symbol, venue=None, total_timeout=120)
            snap_utp = td.snapshot_ohlc(args.symbol, venue=config.VENUE_UTP_CTA,
                                        total_timeout=120)
            dn, du = snap_none.frame(), snap_utp.frame()
            print()
            vol_col = c.find_column(du, ["volume", "size"], "snapshot volume",
                                    required=False)
            if vol_col and not dn.empty and not du.empty:
                vn = _sum_sizes(dn, vol_col)
                vu = _sum_sizes(du, vol_col)
                print(f"  without venue : {vn:,}")
                print(f"  utp_cta       : {vu:,}")
                if vu:
                    print(f"  ratio         : {vn / vu:.1%} "
                          f"(~44% reproduces the known result)")
        except Exception as exc:
            c.report_error(exc, "snapshot control")

    # --- what to do next ----------------------------------------------------
    c.banner("NEXT")
    print("Set scalp/config.py VENUE_BY_ENDPOINT from the totals above, then")
    print("set VENUE_POLICY_VERIFIED = True. The fetch scripts refuse to run a")
    print("bulk pull while it is False, so a multi-hour backfill cannot be")
    print("launched against an unverified venue assumption.")
    print()
    print("Do not set it to utp_cta 'to be safe' if the totals matched without")
    print("it. An unnecessary parameter on an endpoint that ignores it is")
    print("harmless; an unnecessary parameter on an endpoint that interprets it")
    print("differently than assumed is exactly how this went wrong once already.")


if __name__ == "__main__":
    main()
