"""Is the stored tape consolidated, or Nasdaq-only? Reads parquet, nothing else.

    python -m scalp.audit_venue --start 2026-08-17 --end 2026-08-28
    python -m scalp.audit_venue --start ... --end ... --sample 40 --full

WHY THIS EXISTS. The venue policy was wrong for a week. s1 concluded that
`history/trade_quote` ignored `venue=utp_cta`, having tested it against a date
whose tape had already settled — both paths returned the same thing because
there was nothing left to differ. A re-run against a fresh session showed:

    without venue :  623,352 rows,   5 exchange codes
    with utp_cta  :  826,801 rows,  21 exchange codes

Five codes is Nasdaq exchange plus Nasdaq TRF. So the default is
feed-dependent — real-time Nasdaq on a recent date, consolidated once settled
— and any symbol-day fetched without the parameter while the session was
still recent got 75% of the tape.

That test passed on ONE date, so the backfill it authorised cannot be assumed
good on the strength of it. This checks what is actually on disk.

WHAT IT MEASURES. Distinct exchange codes per stored symbol-day. A
consolidated tape carries prints from many venues — Aug 28 showed 20 — and a
Nasdaq-only one carries about five. Unlike a share-volume comparison this
needs no per-symbol reference figure, which is what makes it usable across a
whole store.

HOW TO READ IT
  * >= 15 codes, no Nasdaq-only signature  -> consolidated. Keep.
  * ~5 codes, all in {1, 9, 11, 57, 58}    -> Nasdaq-only. Re-fetch that day.
  * A whole DATE thin while others are fine -> that session was fetched before
    it settled, or before the parameter was set. Re-fetch the date.
  * A single SYMBOL thin on every date      -> more likely a genuinely
    single-venue name than a fetch problem. Check it before re-fetching.

Reads only. It never fetches, never writes, and never deletes — deciding what
to re-pull is left to the person reading the output.
"""
from __future__ import annotations

import argparse
import random
from collections import defaultdict
from datetime import date

import pandas as pd

from scalp import config, schema, store

# The five codes the Nasdaq-only tape carried on 2026-08-31. A file whose
# codes are a subset of these is the signature, regardless of how many it has.
NASDAQ_ONLY_CODES = frozenset({1, 9, 11, 57, 58})


def codes_in_file(symbol: str, day: date) -> tuple[int, set[int], int]:
    """(distinct code count, the codes, row count) for one stored symbol-day."""
    df = store.read_day(symbol, day)
    if df.empty:
        return 0, set(), 0
    col = schema.find(df, schema.CAND_EXCHANGE, "exchange", required=False)
    if col is None:
        return -1, set(), len(df)
    values = pd.to_numeric(df[col], errors="coerce").dropna()
    codes = {int(v) for v in values.unique()}
    return len(codes), codes, len(df)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--sample", type=int, default=20,
                    help="symbol-days to check (default 20). Spread across "
                         "every date in range, so a bad DATE is visible.")
    ap.add_argument("--full", action="store_true",
                    help="check every stored symbol-day in range instead of "
                         "sampling. Slow but exhaustive.")
    ap.add_argument("--symbols", default=None,
                    help="comma-separated subset; default is the store")
    ap.add_argument("--threshold", type=int, default=config.MIN_EXCHANGE_CODES)
    ap.add_argument("--seed", type=int, default=0,
                    help="sampling seed, so a re-run checks the same files")
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    days = [d for d in store.trading_days(start, end)]
    symbols = ([s.strip().upper() for s in args.symbols.split(",") if s.strip()]
               if args.symbols else store.stored_symbols())
    if not symbols:
        raise SystemExit("nothing in the store")

    # Every (symbol, day) actually on disk.
    available = [(s, d) for d in days for s in symbols if store.has_day(s, d)]
    if not available:
        raise SystemExit(f"no stored symbol-days between {start} and {end}")

    if args.full:
        chosen = available
    else:
        # Spread the sample across dates rather than taking it uniformly, so a
        # single bad session cannot hide behind a majority of good ones.
        by_day: dict[date, list] = defaultdict(list)
        for sym, day in available:
            by_day[day].append((sym, day))
        rng = random.Random(args.seed)
        per_day = max(1, args.sample // max(len(by_day), 1))
        chosen = []
        for day in sorted(by_day):
            pool = by_day[day]
            rng.shuffle(pool)
            chosen.extend(pool[:per_day])

    print()
    print(f"store      : {config.raw_dir()}")
    print(f"range      : {start} .. {end}  ({len(days)} sessions)")
    print(f"on disk    : {len(available):,} symbol-days")
    print(f"checking   : {len(chosen):,}  ({'all' if args.full else 'sampled'})")
    print(f"threshold  : fewer than {args.threshold} codes is suspect")
    print()
    print(f"{'symbol':<8s} {'date':<12s} {'rows':>10s} {'codes':>6s}  verdict")
    print("-" * 78)

    per_date: dict[date, list[int]] = defaultdict(list)
    per_symbol: dict[str, list[int]] = defaultdict(list)
    suspect: list[tuple[str, date, int, set[int]]] = []

    for symbol, day in sorted(chosen, key=lambda x: (x[1], x[0])):
        count, codes, rows = codes_in_file(symbol, day)
        per_date[day].append(count)
        per_symbol[symbol].append(count)

        if count < 0:
            verdict = "NO EXCHANGE COLUMN"
        elif count >= args.threshold:
            verdict = "consolidated"
        elif codes and codes <= NASDAQ_ONLY_CODES:
            verdict = f"NASDAQ-ONLY  {sorted(codes)}"
            suspect.append((symbol, day, count, codes))
        else:
            verdict = f"THIN  {sorted(codes)}"
            suspect.append((symbol, day, count, codes))
        print(f"{symbol:<8s} {day.isoformat():<12s} {rows:>10,} {count:>6d}  "
              f"{verdict}")

    # --- by date -------------------------------------------------------------
    print()
    print("by date — a whole session thin means that DATE needs re-fetching")
    print(f"{'date':<12s} {'files':>6s} {'min':>5s} {'median':>7s} {'max':>5s}  verdict")
    print("-" * 78)
    bad_dates = []
    for day in sorted(per_date):
        counts = sorted(c for c in per_date[day] if c >= 0)
        if not counts:
            continue
        median = counts[len(counts) // 2]
        flag = "OK" if median >= args.threshold else "RE-FETCH THIS DATE"
        if median < args.threshold:
            bad_dates.append(day)
        print(f"{day.isoformat():<12s} {len(counts):>6d} {min(counts):>5d} "
              f"{median:>7d} {max(counts):>5d}  {flag}")

    # --- verdict -------------------------------------------------------------
    print()
    print("=" * 78)
    if not suspect:
        print("ALL SAMPLED FILES LOOK CONSOLIDATED.")
        print()
        print("That is evidence, not proof — this is a sample unless --full")
        print("was passed, and the check reads exchange-code variety rather")
        print("than comparing against a known share count. A file could still")
        print("be short of prints while carrying codes from many venues.")
        return

    print(f"{len(suspect)} SUSPECT SYMBOL-DAY(S).")
    print()
    if bad_dates:
        print("Whole dates below the threshold — re-fetch these:")
        for day in bad_dates:
            print(f"    python -m scalp.fetch --start {day} --end {day}")
        print()
        print("Delete the existing files first, or fetch.py will skip them:")
        print("    python -m scalp.prune --before <the day after> --delete")
        print("  (or remove those symbol/date parquet files directly)")
    else:
        print("No whole date is bad, so this looks symbol-specific rather than")
        print("a fetch-configuration problem. A name that trades on one venue")
        print("legitimately carries few codes — check one by hand before")
        print("re-fetching anything.")


if __name__ == "__main__":
    main()
