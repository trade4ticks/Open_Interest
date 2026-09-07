"""Why does audit_chain_snapshots see a session that fetch_chain_snapshots won't fetch?

    python probe_session_gap.py --ticker BABA --date 2024-06-13
    python probe_session_gap.py --ticker LCID --date 2022-06-27
    python probe_session_gap.py --ticker LLY  --date 2022-06-27

READ-ONLY. Issues vendor GET requests and prints what came back. Writes
nothing, touches no parquet, touches no database.

WHAT IS ACTUALLY IN DISPUTE

The two tools do NOT disagree about the calendar. Both import get_trading_days
from lib.market_hours, which wraps pandas_market_calendars' NYSE calendar.
2024-06-13 was a Thursday and not a holiday, so both agree it is a session.

They disagree about what counts as EVIDENCE of a session:

  audit_chain_snapshots  a session is a session if the exchange calendar says
                         so. 06-12 and 06-14 are stored, 06-13 is inside the
                         same coverage block, so 06-13 is missing.

  fetch_chain_snapshots  a session is worth fetching if
                         enumerate_expirations_eod returns expirations for it.

The fetcher's log makes that look like a calendar claim when it is not:

    BABA 2024-06-13..2024-06-13: 0 sessions, 0 point queries, 1 requests

`%d sessions` prints len(exp_by_session) — sessions that RETURNED
EXPIRATIONS — not the number of trading days in range. "1 requests" is the
enumeration call it did make. So the fetcher asked, got an empty set, and
said "0 sessions".

fetch_chain_snapshots.py, in the enumeration result handler:

    exps = sorted(e for e in raw_exps if e >= sess)
    if not exps:
        continue          # <- silent: no log, no counter, no enum_failure

An enumeration that RAISES is recorded in enum_failures and warned about. An
enumeration that returns an EMPTY SET is dropped without trace. And --force
cannot help, because --force (repair) chooses which CELLS to refetch — it
runs downstream of enumeration, and there is nothing downstream of an empty
enumeration.

THE HYPOTHESIS THIS PROBE EXISTS TO TEST

The fetcher enumerates ONE SESSION AT A TIME:

    enumerate_expirations_eod(ticker, sess, sess)      # start == end

If /v3/option/history/eod is sparse for single-day requests but complete over
a range, the result is exactly the reported pattern: scattered missing dates,
and THE SAME dates across unrelated tickers — because the gap would be a
property of the endpoint and the date, not of the ticker. LCID and LLY missing
an identical set of 15 dates is very hard to explain any other way; genuine
per-ticker vendor gaps would not align.

Test D below is therefore the one to read first.

HOW TO READ THE RESULT

  D returns 06-13 but A does not
        The single-day enumeration is the bug. The data is there and the
        fetcher cannot see it. Fix: enumerate over the window, not per
        session. All 52 cells are then fetchable.

  A and D both empty, but E returns rows
        Enumeration is looking in the wrong place entirely — the greeks
        endpoint has data the EOD endpoint does not list. Fix: enumerate from
        a source that agrees with where the data lives.

  A, D and E all empty, and the controls (06-12 / 06-14) all return data
        A genuine vendor gap for this ticker-date. The audit is then a true
        positive that is nonetheless unfixable, and the right move is to
        record the date as vendor-absent so it stops being reported forever.

  The controls come back empty too
        Something is wrong with this probe or the terminal, not with the data.
        Stop and read the errors rather than concluding anything.
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta

from lib.market_hours import get_trading_days
from lib.thetadata import (
    NoDataError,
    enumerate_expirations_eod,
    fetch_first_order_raw,
    list_expirations,
)

logging.basicConfig(level=logging.WARNING,
                    format="%(levelname)-7s %(message)s")


def parse_day(s: str) -> date:
    return datetime.strptime(s.replace("-", ""), "%Y%m%d").date()


def rule(title: str) -> None:
    print()
    print("-" * 72)
    print(title)
    print("-" * 72)


def show_enum(label: str, ticker: str, lo: date, hi: date):
    """Run one enumeration and report what came back."""
    try:
        got = enumerate_expirations_eod(ticker, lo, hi)
    except Exception as exc:
        print(f"  {label}: RAISED {type(exc).__name__}: {exc}")
        return None
    print(f"  {label}: {len(got)} expiration(s)")
    if got:
        ordered = sorted(got)
        head = ", ".join(str(d) for d in ordered[:6])
        tail = f" ... {ordered[-1]}" if len(ordered) > 6 else ""
        print(f"      {head}{tail}")
    return got


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ticker", default="BABA")
    ap.add_argument("--date", default="2024-06-13",
                    help="the disputed session")
    ap.add_argument("--window", type=int, default=5,
                    help="trading days either side for the range test")
    args = ap.parse_args()

    ticker = args.ticker.upper()
    day = parse_day(args.date)

    print(f"probing {ticker} {day} ({day.strftime('%A')})")

    # --- is it a session at all? --------------------------------------------
    rule("0. the exchange calendar (what the AUDIT uses)")
    span = get_trading_days(day - timedelta(days=10), day + timedelta(days=10))
    print(f"  NYSE sessions within +/-10 days : {len(span)}")
    print(f"  {day} is a trading day          : {day in span}")
    if day not in span:
        print()
        print("  The calendar does NOT consider this a session. The audit is")
        print("  then generating a false positive and the fetcher is right.")
        print("  Stop here — the rest of this probe is about a different bug.")
        return
    neighbours = [d for d in span if d != day]
    before = max((d for d in neighbours if d < day), default=None)
    after = min((d for d in neighbours if d > day), default=None)
    print(f"  previous session                : {before}")
    print(f"  next session                    : {after}")

    # --- A: exactly what the fetcher does -----------------------------------
    rule("A. single-day enumeration — EXACTLY what the fetcher calls")
    print(f"  enumerate_expirations_eod({ticker!r}, {day}, {day})")
    single = show_enum("result", ticker, day, day)

    # --- B, C: the neighbours, as controls ----------------------------------
    rule("B. the neighbouring sessions, single-day (controls)")
    for control in (before, after):
        if control is None:
            continue
        show_enum(f"{control}", ticker, control, control)

    # --- D: the same date inside a RANGE ------------------------------------
    rule("D. the SAME date inside a multi-day range  <- read this first")
    lo_idx = max(0, span.index(day) - args.window)
    hi_idx = min(len(span) - 1, span.index(day) + args.window)
    lo, hi = span[lo_idx], span[hi_idx]
    print(f"  enumerate_expirations_eod({ticker!r}, {lo}, {hi})")
    ranged = show_enum("result", ticker, lo, hi)

    if single is not None and ranged is not None:
        only_in_range = ranged - single
        print()
        print(f"  single-day returned : {len(single)}")
        print(f"  range returned      : {len(ranged)}")
        print(f"  in range, not single: {len(only_in_range)}")
        if not single and ranged:
            print()
            print("  *** The range sees expirations the single-day call does")
            print("      not. The fetcher enumerates one session at a time, so")
            print("      this is the bug — and it explains why unrelated")
            print("      tickers share a missing-date set.")

    # --- E: does the DATA exist, regardless of enumeration? -----------------
    rule("E. does the underlying data exist for this session?")
    print("  The decisive question: enumeration aside, will the greeks")
    print("  endpoint return rows for this ticker-date?")
    candidates = sorted(ranged or single or set())
    if not candidates:
        try:
            listed = [e for e in list_expirations(ticker) if e >= day]
            candidates = sorted(listed)[:3]
            print(f"  (no enumeration result — trying "
                  f"/v3/option/list/expirations, {len(listed)} listed >= {day})")
        except Exception as exc:
            print(f"  list_expirations RAISED {type(exc).__name__}: {exc}")
            candidates = []

    tried = 0
    found = 0
    for exp in candidates:
        if tried >= 3:
            break
        tried += 1
        try:
            raw = fetch_first_order_raw(ticker, exp, day, "15:45")
        except NoDataError:
            print(f"  exp {exp}: NoData")
            continue
        except Exception as exc:
            print(f"  exp {exp}: RAISED {type(exc).__name__}: {exc}")
            continue
        print(f"  exp {exp}: {len(raw)} row(s)")
        if not raw.empty:
            found += 1

    # --- verdict ------------------------------------------------------------
    rule("VERDICT")
    if not single and ranged:
        print(f"The AUDIT is right and the FETCHER is wrong for {ticker} {day}.")
        print()
        print("The single-day enumeration returns nothing while the same date")
        print("inside a range returns expirations. The data is fetchable; the")
        print("fetcher just cannot see it, because it enumerates per session.")
        print()
        print("Fix: enumerate over the whole window and slice per session,")
        print("rather than one call per session. That also cuts enumeration")
        print("calls by the window length.")
    elif not single and not ranged and found:
        print(f"The AUDIT is right and ENUMERATION is looking in the wrong place.")
        print()
        print("Neither EOD enumeration path lists this session, but the greeks")
        print("endpoint returns rows for it. The EOD endpoint is not a")
        print("trustworthy index of where the data is.")
    elif not single and not ranged and not found:
        print(f"Looks like a genuine vendor gap for {ticker} {day}.")
        print()
        print("The calendar says it is a session, and nothing the vendor")
        print("serves has data for it. The audit is a TRUE positive that is")
        print("nevertheless unfixable — the right response is to record the")
        print("date as vendor-absent so it stops being reported every run,")
        print("not to keep re-fetching it.")
        print()
        print("Check the controls in section B before accepting this: if the")
        print("neighbouring sessions also came back empty, the problem is the")
        print("probe or the terminal, not the data.")
    else:
        print("The single-day enumeration DID return expirations here.")
        print()
        print("That contradicts the reported behaviour, so something differs")
        print("between this probe and the failing run — a different date, a")
        print("since-fixed vendor gap, or the CSV/JSON enumeration format")
        print("demotion in lib/thetadata.enumerate_expirations_eod. Re-run")
        print("the exact failing command and compare.")

    print()
    print("REGARDLESS of the verdict above, one fetcher bug is already")
    print("confirmed by reading the code: an empty enumeration is dropped")
    print("with a bare `continue` — no log, no counter, no enum_failures")
    print("entry — and the run then reports '0 sessions', which reads as a")
    print("calendar claim it never made.")


if __name__ == "__main__":
    main()
