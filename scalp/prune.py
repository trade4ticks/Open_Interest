"""Delete old raw parquet. RUN BY HAND. Dry-run by default.

    python -m scalp.prune                          # report only, deletes nothing
    python -m scalp.prune --older-than 45 --delete # actually delete

NOTHING ELSE IN THIS PIPELINE DELETES ANYTHING. fetch.py refuses to start when
a write will not fit, but it will never free space to make room. An automatic
deleter racing an automatic writer is how a store loses days nobody noticed
were gone, and the whole point of retaining a feature history is that it is
still there in three months.

WHAT PRUNING COSTS. Postgres holds the computed metrics permanently, so
deleting raw parquet only removes the ability to RECOMPUTE a changed formula
over old days. That is a re-pull, not a loss — s3 measured 5.2 minutes for a
544-symbol 10-day backfill, so a month is minutes.

WHAT IT DOES NOT COST. Nothing already computed. The daily and 15-minute
metric rows, the universe history and every retained ranking survive
untouched.

WHY IT EXISTS. Block 3 is 100 GB at 78% used — about 22 GB available against a
3.1 GB backfill plus ~310 MB per session, so roughly 60 sessions of headroom.
Volume 1 has 20 GB free and volume 2 has 11 GB, so there is no comfortable
overflow to spill into.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, timedelta

from scalp import config, store


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--older-than", type=int, default=config.RAW_RETENTION_DAYS,
                    help=f"days to keep (default {config.RAW_RETENTION_DAYS})")
    ap.add_argument("--before", default=None,
                    help="absolute cutoff YYYY-MM-DD; overrides --older-than")
    ap.add_argument("--symbols", default=None, help="comma-separated subset")
    ap.add_argument("--delete", action="store_true",
                    help="actually delete. Without this, nothing is removed.")
    args = ap.parse_args()

    root = config.data_dir()
    cutoff = (date.fromisoformat(args.before) if args.before
              else date.today() - timedelta(days=args.older_than))

    symbols = ([s.strip().upper() for s in args.symbols.split(",") if s.strip()]
               if args.symbols else store.stored_symbols())

    victims: list[tuple[str, date, int]] = []
    kept_by_symbol: dict[str, int] = defaultdict(int)
    for symbol in symbols:
        for day in store.stored_days(symbol):
            path = store.day_path(symbol, day)
            if day < cutoff:
                victims.append((symbol, day, path.stat().st_size))
            else:
                kept_by_symbol[symbol] += 1

    total_bytes = sum(v[2] for v in victims)
    dates = sorted({v[1] for v in victims})

    print()
    print(f"store        : {root}")
    print(f"cutoff       : keep {cutoff} and later")
    print(f"symbols      : {len(symbols):,}")
    print(f"store size   : {store.store_bytes() / 1024**3:.2f} GB")
    print(f"free on vol  : {config.free_space_gb():.2f} GB")
    print()
    print(f"would delete : {len(victims):,} symbol-days "
          f"({total_bytes / 1024**3:.2f} GB)")
    if dates:
        print(f"date range   : {dates[0]} .. {dates[-1]} ({len(dates)} sessions)")
    print(f"would keep   : {sum(kept_by_symbol.values()):,} symbol-days")

    if not victims:
        print()
        print("Nothing older than the cutoff. No action.")
        return

    if not args.delete:
        print()
        print("DRY RUN — nothing deleted. Re-run with --delete to remove.")
        print("Computed metrics in Postgres are unaffected either way; this")
        print("only costs the ability to recompute a changed formula over")
        print("these days, which is a re-pull.")
        return

    freed = 0
    for symbol, day, size in victims:
        store.day_path(symbol, day).unlink(missing_ok=True)
        freed += size
    print()
    print(f"deleted {len(victims):,} files, freed {freed / 1024**3:.2f} GB")
    print(f"free on vol now: {config.free_space_gb():.2f} GB")


if __name__ == "__main__":
    main()
