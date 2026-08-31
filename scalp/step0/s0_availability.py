"""Step 0.0 — GATE: is /v3/stock/history/trade_quote on the Standard plan?

RUN THIS ALONE. STOP. REPORT. Nothing else in step 0 matters if trade_quote is
Pro-only — the entire metric set is built from trade-paired NBBO, and there is
no substitute endpoint on this subscription.

There is a specific reason to doubt the answer: lib/thetadata.py's
`fetch_underlying_snapshot` docstring states that the subscription "doesn't
include regular-stock endpoints", and works around it by reading
`underlying_price` off an options endpoint. That comment may simply predate a
subscription change — but until this probe says otherwise it is the only
first-hand evidence in the codebase about stock entitlement, and it disagrees
with the plan.

Four probes, cheapest first:

  1. list/symbols      — are stock endpoints reachable and entitled at all?
  2. snapshot/ohlc     — the endpoint the universe filter depends on.
  3. history/trade_quote — THE GATE. One minute of one day, not a full session.
  4. history/quote     — the flicker/quote-stability source.

Probe 3 is deliberately bounded to 09:30-09:31 so this gate stays fast. If the
endpoint rejects start_time/end_time it retries without them, which is itself a
finding worth having before s2 runs a full day.

    python -m scalp.step0.s0_availability
    python -m scalp.step0.s0_availability --symbol FDX --date 2026-08-28

WHAT A BAD RESULT LOOKS LIKE
  * 401 / 403 on probe 3          -> trade_quote is not on Standard. Full stop;
                                     the pipeline as designed cannot be built.
  * 401 / 403 on probes 1 and 2   -> no stock entitlement at all. The
                                     fetch_underlying_snapshot docstring was
                                     current, not stale.
  * ConnectionError on everything -> Tailscale or the terminal, not the plan.
                                     Not a finding; fix and rerun.
  * 472 on probe 3 for a date the
    other probes returned data for -> ambiguous. Rerun with a different --date
                                     before concluding anything.
"""
from __future__ import annotations

import argparse

from scalp import config, thetadata as td
from scalp.step0 import _common as c


# A short window keeps the gate fast. Whether the endpoint honours these at all
# is itself unknown, hence the fallback below.
PROBE_START_TIME = "09:30:00"
PROBE_END_TIME   = "09:31:00"


def _probe(name: str, fn, *, gating: bool) -> tuple[bool, str]:
    """Run one probe. Returns (ok, verdict-line)."""
    print()
    print(f"[{name}]")
    try:
        raw = fn()
    except td.NotEntitledError as exc:
        c.report_error(exc, name)
        return False, (f"{name}: NOT ENTITLED (HTTP {exc.status}) — "
                       "this endpoint is not on the subscription.")
    except td.NoDataError as exc:
        c.report_error(exc, name)
        return False, (f"{name}: HTTP 472 no data. Endpoint exists and is "
                       "entitled, but returned nothing for these arguments.")
    except ConnectionError as exc:
        c.report_error(exc, name)
        return False, f"{name}: UNREACHABLE — {exc}"
    except Exception as exc:
        c.report_error(exc, name)
        return False, f"{name}: FAILED — {type(exc).__name__}: {exc}"

    df = raw.frame()
    print(f"  status {raw.status}  |  {raw.seconds:.2f}s  |  "
          f"{c.fmt_bytes(raw.nbytes)}  |  {len(df):,} rows  |  "
          f"venue={raw.venue_sent or '(none)'}")
    if not df.empty:
        print(f"  columns: {list(df.columns)}")
    if gating and df.empty:
        return False, (f"{name}: entitled but EMPTY. Treat as inconclusive — "
                       "rerun on a different date before concluding.")
    return True, f"{name}: OK ({len(df):,} rows)"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--symbol", default=config.VENUE_CHECK_SYMBOL)
    ap.add_argument("--date", default=config.VENUE_CHECK_DATE,
                    help="a known-good regular trading day (YYYY-MM-DD)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    c.setup_logging(args.verbose)
    c.banner("STEP 0.0 — SUBSCRIPTION AVAILABILITY GATE")
    c.env_summary()
    print()
    print(f"symbol        : {args.symbol}")
    print(f"date          : {args.date}")
    print()
    print("No retries on these probes: a 429 should be reported as a 429, not")
    print("hidden behind 30 seconds of backoff.")

    verdicts: list[str] = []

    # --- 1. roster ----------------------------------------------------------
    ok_symbols, v = _probe(
        "1. list/symbols",
        lambda: td.list_symbols(retry=False, total_timeout=120),
        gating=False,
    )
    verdicts.append(v)

    # --- 2. snapshot --------------------------------------------------------
    ok_snapshot, v = _probe(
        "2. snapshot/ohlc (single symbol)",
        lambda: td.snapshot_ohlc(args.symbol, retry=False, total_timeout=120),
        gating=False,
    )
    verdicts.append(v)

    # --- 3. trade_quote — THE GATE -----------------------------------------
    print()
    print("[3. history/trade_quote — THE GATE]")
    print(f"  bounded to {PROBE_START_TIME}-{PROBE_END_TIME} to keep the gate fast")
    ok_tq = False
    time_bounds_accepted: bool | None = None
    try:
        raw = td.trade_quote(args.symbol, args.date, args.date,
                             start_time=PROBE_START_TIME,
                             end_time=PROBE_END_TIME,
                             retry=False, total_timeout=180)
        time_bounds_accepted = True
    except td.BadRequestError as exc:
        # A 400 here is ambiguous: malformed request, or an endpoint that does
        # not take time bounds. Retry without them rather than reporting "not
        # available" on what may be a parameter problem.
        print(f"  HTTP 400 with time bounds: {exc.body[:200]}")
        print("  -> retrying WITHOUT start_time/end_time (whole session)")
        time_bounds_accepted = False
        try:
            raw = td.trade_quote(args.symbol, args.date, args.date,
                                 retry=False, total_timeout=600)
        except Exception as exc2:
            c.report_error(exc2, "3. trade_quote (no time bounds)")
            raw = None
    except Exception as exc:
        c.report_error(exc, "3. trade_quote")
        raw = None

    if raw is not None:
        df = raw.frame()
        print(f"  status {raw.status}  |  {raw.seconds:.2f}s  |  "
              f"{c.fmt_bytes(raw.nbytes)}  |  {len(df):,} rows  |  "
              f"venue={raw.venue_sent or '(none)'}")
        if not df.empty:
            print(f"  columns: {list(df.columns)}")
            ok_tq = True
            verdicts.append(f"3. trade_quote: AVAILABLE ({len(df):,} rows)")
        else:
            verdicts.append("3. trade_quote: entitled but EMPTY — inconclusive, "
                            "rerun on another date.")
    else:
        verdicts.append("3. trade_quote: FAILED — see above.")

    if time_bounds_accepted is not None:
        verdicts.append(
            f"   start_time/end_time on trade_quote: "
            f"{'ACCEPTED' if time_bounds_accepted else 'REJECTED (400)'}"
        )

    # --- 4. quote -----------------------------------------------------------
    ok_quote, v = _probe(
        "4. history/quote (1m, bounded)",
        lambda: td.quote(args.symbol, args.date, args.date, interval="1m",
                         start_time=PROBE_START_TIME, end_time=PROBE_END_TIME,
                         retry=False, total_timeout=180),
        gating=False,
    )
    verdicts.append(v)

    # --- verdict ------------------------------------------------------------
    c.banner("VERDICT")
    for v in verdicts:
        print(f"  {v}")
    print()
    if ok_tq:
        print("GO. trade_quote is available on this subscription.")
        print("Next: run s1_venue_check.py. Do not run anything else until the")
        print("venue question is settled — every metric depends on which tape")
        print("the history endpoints are actually reading from.")
    else:
        print("NO-GO. trade_quote did not return usable data.")
        print()
        print("If the failure was 401/403, the pipeline as designed cannot be")
        print("built on this subscription and the whole plan needs revisiting")
        print("before any more code is written.")
        print("If it was a connection error, this says nothing about the")
        print("subscription — fix the terminal or Tailscale and rerun.")
        print("If it was 472 on a date the other probes returned data for,")
        print("rerun with --date set to a different regular session.")


if __name__ == "__main__":
    main()
