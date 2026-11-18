"""
run_spx_intraday.py — today's rows of spx_oi_5min and spx_price_5min, rebuilt
every cycle during the session, so the live Strategy Signal page can use the
80 five-minute metrics as the day trades.

    python run_spx_intraday.py                          # cron: 3-58/5 9-16 * * 1-5
    python run_spx_intraday.py --dry-run                # what this cycle would do; runs nothing
    python run_spx_intraday.py --dry-run --now 202611171015

FETCHES NOTHING. The OI book is frozen as of yesterday's close and the 07:30
run (run_spx_pipeline.py) has already written today's OI; index_ohlc is fed
every 5 minutes by the /Thetadata_Raw_SPX pipeline. Pure computation on data
already on disk.

THE SAME CODE AS THE BACKTEST. Each stage is the existing build script for
today only — build_spx_price_5min.py / build_spx_table_b.py --start T --end T
— with --brief: one-line reports instead of full ones, and for the price table
no lookahead probe (a full recompute from the 2017 seed; the morning rebuild of
the day runs it). Same compute, same hard checks, same write. Live and backtest
numbers come from one code path by construction.

EACH CYCLE (a no-op outside today's NYSE session, [open, close) in ET, and on
non-trading days; cron need not know either):
    1. price_5min   all of today's bars. Needs no open interest.
    2. oi_5min      all of today's bars — only when today's OI is in the SPX
                    store for both roots. If the 07:30 run did not write it,
                    DEGRADED: oi_5min is skipped with a warning and price_5min
                    still runs (48 metrics lost instead of 80).
Every cycle recomputes the whole day, so a bar computed from a value that later
shifted corrects itself on the next cycle. A bar whose spx_open is NULL, NaN,
zero or negative has NULL metrics (lib.spx_table_b.classify_spot, used by both
builds). Bars not yet printed are rows with NULL metrics until they are.

The price is each bar's own spx_open, final the moment the bar exists, so
there is no waiting period and no gate on any other index_ohlc column. By the
user's account of the /Thetadata_Raw_SPX gate, the 09:30 bar first lands at
09:36, so the 09:33 cycle finds no printed bar (logged as expected) and 09:38
is the first with data.

LOG: one line per normal cycle (stages, rows written, bars priced, timing); a
failed stage's full output is written under it. A no-op cycle is one line.

SELF-HEALING: the 07:30 run rebuilds the last 10 sessions of both tables
ending yesterday, so every intraday row is overwritten from the canonical
path the next morning.

LOCK: /tmp/spx_daily.lock, shared with the 07:30 run (both write these
tables). EXIT: 1 if a build stage failed, else 0 — including a degraded cycle
(the 07:30 run has already exited 1 for the missing snapshot).
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import subprocess
import sys
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo

from lib.market_hours import last_trading_day, session_bounds
from run_spx_pipeline import ROOT, _ymd

log = logging.getLogger("spx_intraday")
ET = ZoneInfo("America/New_York")
STAGE_TIMEOUT_MIN = 1.75         # per stage: two stages <= 3.5 min, 1.5 min headroom in the 5-minute cycle


def _now_et() -> datetime:
    return datetime.now(ET).replace(tzinfo=None)


def in_session(now: datetime) -> tuple[bool, str]:
    d = now.date()
    if last_trading_day(d) != d:
        return False, f"{d} is not an NYSE session"
    o, c = session_bounds(d, d)[d]
    if not (o <= now < c):
        return False, f"{now:%H:%M} ET is outside today's session {o:%H:%M}-{c:%H:%M}"
    return True, f"session {o:%H:%M}-{c:%H:%M}"


def todays_oi_present(today: date) -> tuple[bool, str]:
    import config
    import lib.spx_features as F
    last = {r: F.store_last_date(config.SPX_OI_RAW_DIR, r) for r in config.SPX_ROOTS}
    missing = [r for r, d in last.items() if d != today]
    if missing:
        return False, ", ".join(f"{r} last OI {last[r]}" for r in missing)
    return True, ""


def stages(today: date, with_oi: bool) -> list[tuple[str, list[str]]]:
    day = _ymd(today)
    out = [("price_5min", [str(ROOT / "build_spx_price_5min.py"), "--start", day, "--end", day, "--brief"])]
    if with_oi:
        out.append(("oi_5min", [str(ROOT / "build_spx_table_b.py"), "--start", day, "--end", day, "--brief"]))
    return out


def _run(argv: list[str]) -> tuple[int, float, str]:
    """One build as a subprocess, output captured: (rc, seconds, stdout + stderr)."""
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    t0 = time.time()
    try:
        p = subprocess.run([sys.executable, *argv], cwd=ROOT, env=env, capture_output=True, text=True,
                           timeout=STAGE_TIMEOUT_MIN * 60)
        rc, out = p.returncode, (p.stdout or "") + (p.stderr or "")
    except subprocess.TimeoutExpired as e:
        part = [x.decode() if isinstance(x, bytes) else (x or "") for x in (e.stdout, e.stderr)]
        rc, out = 124, "".join(part) + f"\n[killed after {STAGE_TIMEOUT_MIN} min]"
    return rc, time.time() - t0, out


_SPOT = re.compile(r"spot \(--brief\):\s*(.*?); priced through (\S+); unknown before that: (\d+)")
_WROTE = re.compile(r"WROTE ([\d,]+) rows")


def parse(out: str) -> tuple[dict | None, int | None]:
    """From a --brief build's output: ({ok, last, gaps}, rows written).
    last = the last priced bar (None before any), gaps = unknown bars before it."""
    spot = None
    m = _SPOT.search(out)
    if m:
        counts = {}
        for part in m.group(1).split(","):
            k, _, v = part.strip().rpartition(" ")
            if k and v.isdigit():
                counts[k] = int(v)
        spot = {"ok": counts.get("ok", 0), "last": None if m.group(2) == "none" else m.group(2),
                "gaps": int(m.group(3))}
    w = _WROTE.search(out)
    return spot, (int(w.group(1).replace(",", "")) if w else None)


def run_cycle(now: datetime, dry_run: bool = False) -> int:
    ok, why = in_session(now)
    if not ok:
        log.info("intraday %s no-op: %s", f"{now:%Y-%m-%d %H:%M}", why)
        return 0
    today = now.date()
    t0 = time.time()
    with_oi, why_oi = todays_oi_present(today)
    if not with_oi:
        log.warning("intraday %s DEGRADED: today's OI is not in the SPX store (%s); the 07:30 run did not "
                    "write it. spx_oi_5min skipped, spx_price_5min built.", f"{now:%H:%M}", why_oi)
    plan = stages(today, with_oi)
    if dry_run:
        log.info("intraday %s DRY RUN (%s): %s", f"{now:%Y-%m-%d %H:%M}", why,
                 "; ".join(f"{n} {' '.join(a[1:])}" for n, a in plan))
        return 0

    parts, failed, spot = [], [], None
    for name, argv in plan:
        rc, dt, out = _run(argv)
        sp, wrote = parse(out)
        if rc != 0:
            failed.append(name)
            parts.append(f"{name} FAILED rc={rc} {dt:.1f}s")
            log.error("intraday %s: %s FAILED (rc=%d, %.1fs); its output:\n%s", f"{now:%H:%M}", name, rc, dt,
                      out.rstrip())
            continue
        spot = spot or sp
        parts.append(f"{name} ok {dt:.1f}s ({wrote if wrote is not None else '?'} rows)")

    if spot is None:
        bars = "bars priced: unknown"
    elif spot["last"] is None:
        bars = ("no printed bars yet (expected before the 09:30 bar lands in index_ohlc, ~09:36); "
                "today's rows written with NULL metrics")
    else:
        bars = f"{spot['ok']} bars priced through {spot['last']}" + (
            f", {spot['gaps']} before it with no usable open (NaN / zero / missing)" if spot["gaps"] else "")
    (log.error if failed else log.info)(
        "intraday %s: %s%s | %s | total %.1fs", f"{now:%H:%M}", ", ".join(parts),
        "" if with_oi else ", oi_5min SKIPPED (degraded)", bars, time.time() - t0)
    return 1 if failed else 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Today's spx_oi_5min / spx_price_5min rows, every 5 minutes.",
                                 formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="print what this cycle would do; run nothing")
    ap.add_argument("--now", type=lambda s: datetime.strptime(s, "%Y%m%d%H%M"),
                    help="pretend it is this ET time (YYYYMMDDHHMM); --dry-run only")
    args = ap.parse_args()
    if args.now and not args.dry_run:
        ap.error("--now is only for --dry-run")
    return run_cycle(args.now or _now_et(), args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
