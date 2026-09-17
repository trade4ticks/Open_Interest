"""
run_nightly_paths.py — nightly 1-minute bar fetch, trade_paths build, and split
repair stage 4.

Nothing ever scheduled fetch_equity_1min.py or build_trade_paths.py. The bars
stopped at 2026-07-31, paths can only resolve as far as the bars reach, and the
exits page went quiet after early July. This is that schedule.

    # crontab, weekdays 20:45 ET
    45 20 * * 1-5 flock -n /tmp/oi_paths.lock /Open_Interest/.venv/bin/python /Open_Interest/run_nightly_paths.py >> /Open_Interest/logs/nightly_paths.log 2>&1

Its own lock, not /tmp/oi_research.lock. The pipelines take that lock with
`flock -n`, which SKIPS a run rather than waiting, so a long catch-up night that
held it past 07:00 would silently cost the premarket run. This job only waits
for that lock to be free (step 0) and never holds it.

Steps
-----
0. Wait for /tmp/oi_research.lock to be free — the EVENING run on a heavy night
   (a split repair, a full bin rebuild) — so two memory-heavy jobs never share
   the 7.8 GB box. Acquired and released immediately. Gives up after
   WAIT_FOR_PIPELINE_MINUTES and exits nonzero rather than colliding.

1. fetch_equity_1min.py --tickers <OI universe> --start today-10d --end today
   The rolling start is load-bearing. The fetcher's manifest keys on
   (chunk_start, chunk_end) clipped to the requested range, so a start that
   moves every day gives the recent chunks a new key every night and they are
   refetched; keep-last dedupe makes that free. A fixed start would record a
   month's final chunk once and never revisit it.
   Every argument is passed explicitly, and stdin is /dev/null: the fetcher
   prompts for anything missing, and under cron a prompt must fail, not hang.

2. build_trade_paths.py --force --start today-90d
   --force because its manifest skips by (ticker, anchor) and records 'ok' even
   when rows came out truncated, so without it the nightly run builds NOTHING —
   not even new dates. --start filters entries only; ATR, MAs and swing lows are
   still computed from full bar history, so each row is identical to a full
   rebuild's. 90 days covers the 40-session truncation tail (~57 calendar days)
   plus about three weeks of missed nights.
   Runs whatever step 1 returned. The fetcher exits 1 both when one ticker failed
   and when nothing landed at all, so its exit code cannot gate this; and on
   unchanged bars the build is idempotent, so running it costs time and nothing
   else. Refusing to build because one ticker failed is how data goes stale.

3. Split repair stage 4 (lib/split_repairs.py):
   build_trade_paths.py --force --tickers <pending> — FULL history, because a
   new split rescales every price column of every row and step 2 reaches only
   the last 90 days. It lives here rather than in run_pipeline.py so that
   trade_paths has exactly one writer, that writer never overlaps a pipeline
   run, and a split first seen at 09:35 or 18:00 is repaired the same night.
   Stamped only on success; an unstamped repair is retried the next night.

Exit status is nonzero if any step failed.
"""
from __future__ import annotations

import logging
import subprocess
import sys
import time
from contextlib import closing
from datetime import date, timedelta
from pathlib import Path

from lib.market_hours import get_trading_days

HERE = Path(__file__).resolve().parent
LOGS_DIR = HERE / "logs"
LOGS_DIR.mkdir(exist_ok=True)

PIPELINE_LOCK = "/tmp/oi_research.lock"
WAIT_FOR_PIPELINE_MINUTES = 90

FETCH_LOOKBACK_DAYS = 10
PATHS_LOOKBACK_DAYS = 90

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("nightly_paths")


def _wait_for_lock_free(path: str, timeout_s: float) -> bool:
    """Block until nobody holds `path`, then release it at once.

    fcntl.flock takes the same lock as flock(1), so this sees the pipeline's
    cron lock. Imported here because the module is Linux-only and this file is
    also opened on the Windows dev box.
    """
    import fcntl

    deadline = time.monotonic() + timeout_s
    with open(path, "a") as fh:
        while True:
            try:
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    return False
                time.sleep(30)
                continue
            fcntl.flock(fh, fcntl.LOCK_UN)
            return True


def _run(label: str, cmd: list) -> int:
    log.info("--- %s: %s ---", label, " ".join(cmd))
    t0 = time.monotonic()
    rc = subprocess.run(cmd, stdin=subprocess.DEVNULL, cwd=HERE).returncode
    log.info("--- %s: exit %d after %.0fs ---", label, rc, time.monotonic() - t0)
    return rc


def main() -> int:
    today = date.today()
    if not get_trading_days(today, today):
        log.info("Today (%s) is not a trading day — exiting.", today)
        return 0

    log.info("=" * 60)
    log.info("Nightly paths starting (today = %s)", today)

    # 0. Let a long EVENING run finish first.
    if not _wait_for_lock_free(PIPELINE_LOCK, WAIT_FOR_PIPELINE_MINUTES * 60):
        log.error("%s still held after %d minutes — not starting, to avoid "
                  "running alongside the pipeline.", PIPELINE_LOCK,
                  WAIT_FOR_PIPELINE_MINUTES)
        return 1

    # 1. Bars. The universe is the one the fetcher itself defaults to.
    from lib.parquet_store import list_tickers
    tickers = list_tickers()
    if not tickers:
        log.error("OI store universe is empty — nothing to fetch or build.")
        return 1
    rc_fetch = _run("fetch_equity_1min", [
        sys.executable, str(HERE / "fetch_equity_1min.py"),
        "--tickers", ",".join(tickers),
        "--start", (today - timedelta(days=FETCH_LOOKBACK_DAYS)).strftime("%Y%m%d"),
        "--end", today.strftime("%Y%m%d"),
    ])
    if rc_fetch != 0:
        log.error("fetch_equity_1min exited %d — building anyway (see module "
                  "docstring); check its log for which tickers failed.", rc_fetch)

    # 2. Rolling paths rebuild.
    rc_paths = _run("build_trade_paths (rolling)", [
        sys.executable, str(HERE / "build_trade_paths.py"),
        "--force",
        "--start", (today - timedelta(days=PATHS_LOOKBACK_DAYS)).strftime("%Y%m%d"),
    ])

    # 3. Split repair stage 4.
    from db import get_connection
    from lib import split_repairs
    with closing(get_connection()) as conn:
        pending = split_repairs.pending(conn, "paths_rebuilt_at")
    rc_split = 0
    if pending:
        rc_split = _run(f"split repair 4/4 ({','.join(pending)})", [
            sys.executable, str(HERE / "build_trade_paths.py"),
            "--force", "--tickers", ",".join(pending),
        ])
        if rc_split == 0:
            with closing(get_connection()) as conn:
                split_repairs.mark_done(conn, pending, "paths_rebuilt_at")
        else:
            log.error("split repair 4/4 NOT marked — retried tomorrow night")

    log.info("Nightly paths complete (fetch = %d, paths = %d, split_paths = %d)",
             rc_fetch, rc_paths, rc_split)
    return rc_fetch or rc_paths or rc_split


if __name__ == "__main__":
    sys.exit(main())
