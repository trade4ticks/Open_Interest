"""
prune_chain_live.py — retention for the live 5-minute chain archive.
DRY-RUN BY DEFAULT; nothing is deleted without --delete.

    python prune_chain_live.py                    # report only
    python prune_chain_live.py --delete           # prune
    python prune_chain_live.py --days 45          # a different window
    python prune_chain_live.py --before 2026-09-25

Layout: {CHAIN_LIVE_DIR}/{TICKER}/{YYYYMMDD}/{HHMM}.parquet — see
lib/chain_live_store.py.

WHAT IS KEPT. Every cycle for the last --days (default 30) calendar days.
Older than that, only 0945.parquet and 1545.parquet in each day folder, and
the day folder itself. Those two are the only raw copies of the twice-daily
anchors from 2026-08-24 onward — chain_snapshots has nothing after September
— so they are protected by a hard-coded set checked again at the moment of
each unlink. No flag removes that protection, and the set is deliberately not
imported from lib/surface_config.RETAINED_SNAPSHOTS: a config change must not
be able to widen this script's reach.

WHAT IT DELETES. In a day folder older than the cutoff, regular files named
exactly HHMM.parquet other than the two anchors. Nothing else: not
*.parquet.tmp (reported, left alone), not stray names, not symlinks, not
directories. Folder names that are not a valid YYYYMMDD are reported and
skipped.

REFUSES — deletes nothing at all — if any day folder past the cutoff holds
neither anchor. Pruning it would leave the day with no raw record, and the
cause (both captures failed, a renamed file, a half-copied folder) needs a
look first. Every such folder is listed. Folders inside the window are never
inspected for this: the current session's folder has no 0945 before 09:45.

Safe beside the live pipeline: fetch_live_surface.py writes only today's
folder, and the cutoff is never later than today - RETENTION_MIN_DAYS.

AFTER A PRUNE, verify_surface_fixes.py --source live on a pruned
(date, snapshot) gets an empty frame — read_cycle treats a missing file as
no capture. Re-fitting a pruned cycle needs the historical endpoint.

Cron (03:40 ET Tue-Sat, after prune_equity_surface.py; its own lock, since
the two touch different disks):

    40 3 * * 2-6 flock -n /tmp/chain_live_retention.lock \\
        /Open_Interest/.venv/bin/python /Open_Interest/prune_chain_live.py \\
        --delete >> /Open_Interest/logs/chain_live_retention.log 2>&1
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

from config import CHAIN_LIVE_DIR
from lib.market_hours import ET
from lib.surface_retention import resolve_cutoff
from lib.surface_config import RETENTION_MIN_DAYS

DEFAULT_DAYS = 30

# Never deleted, under any flag. See module docstring.
PROTECTED = frozenset({"0945.parquet", "1545.parquet"})

_CYCLE_RE = re.compile(r"^\d{4}\.parquet$")
_DAY_RE = re.compile(r"^\d{8}$")


@dataclass
class DayPlan:
    ticker: str
    day: date
    path: Path
    delete: list = field(default_factory=list)     # [(Path, bytes)]
    anchors: list = field(default_factory=list)    # protected names present
    tmp: int = 0                                   # *.parquet.tmp left alone

    @property
    def bytes(self) -> int:
        return sum(n for _, n in self.delete)


def _gb(b: int | None) -> str:
    return "?" if b is None else f"{b / 1024**3:.2f} GB"


def _volume(root: Path) -> str:
    try:
        u = shutil.disk_usage(root)
    except OSError:
        return "?"
    return (f"{_gb(u.free)} free of {_gb(u.total)} "
            f"({100 * u.used / u.total:.0f}% used)")


def _parse_day(name: str) -> date | None:
    if not _DAY_RE.match(name):
        return None
    try:
        return date(int(name[:4]), int(name[4:6]), int(name[6:8]))
    except ValueError:
        return None


def _deletable(p: Path) -> bool:
    """The one test every unlink passes. Re-checked at delete time."""
    return (p.name not in PROTECTED
            and _CYCLE_RE.match(p.name) is not None
            and p.is_file() and not p.is_symlink())


def scan(root: Path, cutoff: date) -> tuple[list, list]:
    """([DayPlan] for every day folder older than the cutoff, [bad names])."""
    plans, bad = [], []
    for tdir in sorted(root.iterdir()):
        if not tdir.is_dir() or tdir.is_symlink():
            continue
        for ddir in sorted(tdir.iterdir()):
            if not ddir.is_dir() or ddir.is_symlink():
                continue
            day = _parse_day(ddir.name)
            if day is None:
                bad.append(ddir)
                continue
            if day >= cutoff:
                continue
            plan = DayPlan(tdir.name, day, ddir)
            for f in ddir.iterdir():
                if f.name in PROTECTED and f.is_file():
                    plan.anchors.append(f.name)
                elif f.name.endswith(".parquet.tmp"):
                    plan.tmp += 1
                elif _deletable(f):
                    plan.delete.append((f, f.stat().st_size))
            plans.append(plan)
    return plans, bad


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--days", type=int, default=DEFAULT_DAYS,
                    help=f"days of every cycle to keep (default {DEFAULT_DAYS})")
    ap.add_argument("--before", default=None,
                    help="absolute cutoff YYYY-MM-DD; overrides --days")
    ap.add_argument("--delete", action="store_true",
                    help="actually delete. Without this, nothing is changed.")
    args = ap.parse_args()

    today = datetime.now(ET).date()
    try:
        cutoff = resolve_cutoff(
            today, days=args.days,
            before=date.fromisoformat(args.before) if args.before else None)
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 2

    root = CHAIN_LIVE_DIR
    if not root.is_dir():
        print(f"CHAIN_LIVE_DIR={root} is not a directory. Is the volume "
              f"mounted?", file=sys.stderr)
        return 2

    print()
    stamp = datetime.now(ET).strftime("%Y-%m-%d %H:%M")
    print(f"=== chain_live retention — {stamp} ET "
          f"{'(DELETE)' if args.delete else '(dry run)'} ===")
    print(f"root           : {root}")
    print(f"protected      : {', '.join(sorted(PROTECTED))}")
    print(f"cutoff         : every cycle kept from {cutoff} "
          f"(floor: {RETENTION_MIN_DAYS} days)")
    print(f"volume         : {_volume(root)}")

    t0 = time.monotonic()
    plans, bad = scan(root, cutoff)
    print(f"scanned        : {len(plans):,} day folder(s) past the cutoff "
          f"in {time.monotonic() - t0:.0f}s")

    # --- plan, one line per session date across all tickers --------------
    by_day = defaultdict(lambda: [0, 0, 0])            # tickers, files, bytes
    for p in plans:
        row = by_day[p.day]
        row[0] += 1
        row[1] += len(p.delete)
        row[2] += p.bytes
    print()
    if by_day:
        print(f"    {'date':<10}  {'tickers':>7}  {'files':>7}  {'bytes':>10}")
        for d in sorted(by_day):
            n_t, n_f, n_b = by_day[d]
            print(f"    {d}  {n_t:>7}  {n_f:>7,}  {_gb(n_b):>10}")
    else:
        print("    no day folder older than the cutoff")
    total_files = sum(len(p.delete) for p in plans)
    total_bytes = sum(p.bytes for p in plans)
    print()
    print(f"would delete   : {total_files:,} file(s), {_gb(total_bytes)}")

    # --- anomalies ---------------------------------------------------------
    one = [p for p in plans if len(p.anchors) == 1]
    if one:
        print()
        print(f"only one anchor ({len(one)} folder(s)) — pruned, the one kept:")
        for p in one:
            print(f"    {p.ticker}/{p.day:%Y%m%d}  has {p.anchors[0]}")
    tmp = sum(p.tmp for p in plans)
    if tmp:
        print()
        print(f"left alone     : {tmp} *.parquet.tmp file(s) past the cutoff")
    if bad:
        print()
        print(f"skipped        : {len(bad)} folder(s) not named YYYYMMDD:")
        for b in bad:
            print(f"    {b.relative_to(root)}")

    none_ = [p for p in plans if not p.anchors]
    if none_:
        print()
        print(f"REFUSING: {len(none_)} day folder(s) past the cutoff hold "
              f"neither 0945 nor 1545. Nothing deleted.")
        for p in none_:
            print(f"    {p.ticker}/{p.day:%Y%m%d}  "
                  f"({len(p.delete)} other cycle(s))")
        return 2

    if not args.delete:
        print()
        print("DRY RUN — nothing changed. Re-run with --delete.")
        return 0

    # --- apply -------------------------------------------------------------
    print()
    t0 = time.monotonic()
    deleted = freed = failed = 0
    for p in plans:
        for f, n in p.delete:
            if not _deletable(f):          # changed since the scan; leave it
                continue
            try:
                os.unlink(f)
            except OSError as exc:
                failed += 1
                print(f"    FAILED {f.relative_to(root)} — {exc}")
                continue
            deleted += 1
            freed += n
    print(f"deleted        : {deleted:,} file(s), {_gb(freed)} "
          f"in {time.monotonic() - t0:.0f}s")
    if failed:
        print(f"failed         : {failed:,} file(s)")
    print(f"volume         : {_volume(root)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
