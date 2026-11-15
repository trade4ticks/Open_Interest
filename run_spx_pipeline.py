"""
run_spx_pipeline.py — daily SPX pipeline, two tiers, one lock, one log.

    python run_spx_pipeline.py --tier EARLY              # history fetch + all four builds
    python run_spx_pipeline.py --tier PREOPEN            # today's OI snapshot + today's spx_oi_daily row
    python run_spx_pipeline.py --tier EARLY --dry-run    # print stages and dates, run nothing
    python run_spx_pipeline.py --tier PREOPEN --dry-run  # dependency check + ONE freshness assessment; writes nothing
    python run_spx_pipeline.py --measure-freshness 60    # calibrate the freshness test on stored day pairs; read-only
    python run_spx_pipeline.py --tier EARLY --today 20261117 --dry-run

EARLY tier — stages, in order:
    1. fetch        fetch_spx_options.py fetch   trailing FETCH_LOOKBACK_DAYS calendar days, ending yesterday
    2. oi_daily     build_spx_features.py        spx_oi_daily
    3. oi_5min      build_spx_table_b.py         spx_oi_5min
    4. price_daily  build_spx_price_daily.py     spx_price_daily
    5. price_5min   build_spx_price_5min.py      spx_price_5min
The catalog (build_spx_catalog.py) is NOT a stage: it describes schema, not
data, and is run by hand after a deployment that changes columns.

PREOPEN tier — today's spx_oi_daily row before the open:
    0. requires the EARLY run of the same morning to have completed: the stored
       T-1 and T-2 chains of both roots and session T-1's volume must be on
       disk, or it fails with a message saying so (it never compares against
       whatever happens to be there)
    1. poll the snapshot endpoint (/v3/option/snapshot/open_interest), both
       roots, until lib.spx_oi_freshness says today's publication has landed:
       first attempt at start, then every POLL_INTERVAL_MIN minutes,
       POLL_ATTEMPTS attempts in all. No clock time anywhere — cron sets the
       start, the content test is the only gate. Never fresh -> nothing
       written, exit 1.
    2. write both roots' snapshot to the SPX OI store, labelled today
    3. build_spx_features.py over the BUILD_WINDOW_SESSIONS ending today
Nothing for the 5-minute tables.

DEPENDENCIES
------------
    fetch ──> oi_daily     reads the raw SPX/SPXW OI + volume parquet stores
    fetch ──> oi_5min      reads the same stores itself (it computes its own
                           day-level book via compute_table_a; it does NOT read
                           spx_oi_daily rows), plus index_ohlc for the bar price
    price_daily, price_5min  read only public.index_ohlc (filled by the
                           /Thetadata_Raw_SPX pipeline); independent of the
                           fetch, of tables A / B and of each other
    EARLY ──> PREOPEN      T-1's chain and volume, and the T-2 chain the
                           freshness test calibrates against

EARLY: A FAILED STAGE DOES NOT STOP THE PIPELINE. Every stage runs; each exit
status is recorded; a summary is printed; the exit code is 1 if any stage
failed. That is safe for the OI builds too: they rebuild from whatever the
stores hold, cap their end at the last OI date present in both roots, and fail
their own hard checks (writing nothing) on a half-written day. A stage that
hangs past STAGE_TIMEOUT_MIN is killed and recorded as failed.

WINDOWS (EARLY)
---------------
Each build rebuilds the last BUILD_WINDOW_SESSIONS NYSE sessions ending at its
own last buildable row. With L = the last completed session before today:
    oi_daily, oi_5min   end L      (today's OI is the PREOPEN tier's job)
    price_daily         end next session after L (that row uses only L and earlier)
    price_5min          end L      (a bar of today has no price yet)
"""
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from lib.market_hours import get_trading_days, last_trading_day, next_trading_day

ROOT = Path(__file__).resolve().parent
(ROOT / "logs").mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("spx_pipeline")

FETCH_LOOKBACK_DAYS = 7          # calendar days, ending yesterday
BUILD_WINDOW_SESSIONS = 10       # NYSE sessions each build rewrites
STAGE_TIMEOUT_MIN = 30           # per stage; a hung stage is killed and counted as failed
POLL_INTERVAL_MIN = 10           # PREOPEN: minutes between snapshot attempts
POLL_ATTEMPTS = 7                # PREOPEN: attempts in all (first at start; 7 spans 60 minutes)


def _ymd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _window_start(end: date, n: int = BUILD_WINDOW_SESSIONS) -> date:
    return get_trading_days(end - timedelta(days=3 * n + 15), end)[-n:][0]


def _build(script: str, end: date) -> list[str]:
    return [str(ROOT / script), "--start", _ymd(_window_start(end)), "--end", _ymd(end)]


def plan(today: date) -> list[tuple[str, list[str], str]]:
    """EARLY tier: [(stage name, argv after the python executable, description)]."""
    last = last_trading_day(today - timedelta(days=1))      # last completed session before today
    nxt = next_trading_day(last)
    fetch_start, fetch_end = today - timedelta(days=FETCH_LOOKBACK_DAYS), today - timedelta(days=1)
    return [
        ("fetch", [str(ROOT / "fetch_spx_options.py"), "fetch", "--start", _ymd(fetch_start),
                   "--end", _ymd(fetch_end)], "SPX/SPXW raw OI + EOD volume"),
        ("oi_daily", _build("build_spx_features.py", last), "spx_oi_daily"),
        ("oi_5min", _build("build_spx_table_b.py", last), "spx_oi_5min"),
        ("price_daily", _build("build_spx_price_daily.py", nxt), "spx_price_daily"),
        ("price_5min", _build("build_spx_price_5min.py", last), "spx_price_5min"),
    ]


def run_stage(i: int, n: int, name: str, argv: list[str], what: str) -> tuple[int, float]:
    cmd = [sys.executable, *argv]
    log.info("=" * 72)
    log.info("STAGE %d/%d  %s  (%s)", i, n, name, what)
    log.info("  %s", " ".join(cmd))
    log.info("=" * 72)
    for h in logging.getLogger().handlers:
        h.flush()
    t0 = time.time()
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    try:
        rc = subprocess.run(cmd, cwd=ROOT, env=env, timeout=STAGE_TIMEOUT_MIN * 60).returncode
    except subprocess.TimeoutExpired:
        log.error("STAGE %s killed after %d min", name, STAGE_TIMEOUT_MIN)
        rc = 124
    dt = time.time() - t0
    (log.info if rc == 0 else log.error)("STAGE %d/%d  %s  finished rc=%d in %.1fs", i, n, name, rc, dt)
    return rc, dt


def _summary(tier: str, today: date, results: list, t0: float) -> int:
    failed = [r for r in results if r[2] != 0]
    log.info("#" * 72)
    log.info("SPX pipeline summary (tier = %s, today = %s)", tier, today)
    for name, what, rc, dt in results:
        log.info("  %-12s %-28s %-8s rc=%-4d %7.1fs", name, what, "ok" if rc == 0 else "FAILED", rc, dt)
    log.info("  total %.1fs; %d of %d stage(s) failed%s", time.time() - t0, len(failed), len(results),
             (": " + ", ".join(r[0] for r in failed)) if failed else "")
    log.info("#" * 72)
    return 1 if failed else 0


# --- EARLY ------------------------------------------------------------------------------

def run_early(today: date, dry_run: bool) -> int:
    stages = plan(today)
    if dry_run:
        for i, (name, argv, what) in enumerate(stages, 1):
            log.info("  %d. %-12s %-18s %s", i, name, what, " ".join(argv[1:]))
        return 0
    t0 = time.time()
    results = []
    for i, (name, argv, what) in enumerate(stages, 1):
        rc, dt = run_stage(i, len(stages), name, argv, what)
        results.append((name, what, rc, dt))
    return _summary("EARLY", today, results, t0)


# --- PREOPEN -------------------------------------------------------------------------------

def _spx():
    """The SPX store machinery, imported only when needed. fetch_spx_options
    owns the redirect of lib.parquet_store onto SPX_OI_RAW_DIR and the guards
    that keep SPX out of the 120-ticker tree."""
    import config
    import fetch_spx_options as FS
    import lib.parquet_store as oi_store
    import lib.spx_oi_freshness as FR
    import lib.spx_volume_store as vol_store
    from lib.thetadata import fetch_oi_snapshot, test_connection
    return config, FS, oi_store, FR, vol_store, fetch_oi_snapshot, test_connection


def _volume_present(vol_store, root: str, day: date) -> bool:
    df = vol_store.read_year(root, day.year)
    return not df.empty and (df["trade_date"] == day).any()


def _snapshot(root: str, today: date, fetch_oi_snapshot, FR):
    """Today's snapshot for one root, shaped exactly as fetch_oi_snapshot.fetch_ticker
    would write it (duplicate keys summed) — but NOT written."""
    df = fetch_oi_snapshot(root, today)
    if df.empty:
        return df
    df = df.groupby(["trade_date", "expiration", "strike", "option_type"], as_index=False)["open_interest"].sum()
    return FR.normalise(df)


def run_preopen(today: date, dry_run: bool) -> int:
    t0 = time.time()
    if last_trading_day(today) != today:
        log.info("PREOPEN: %s is not an NYSE session; nothing to do", today)
        return 0
    config, FS, oi_store, FR, vol_store, fetch_oi_snapshot, test_connection = _spx()
    prev = last_trading_day(today - timedelta(days=1))
    prev2 = last_trading_day(prev - timedelta(days=1))
    store = config.SPX_OI_RAW_DIR
    FS._redirect_oi_store()
    FS._assert_prod_tree_clean()

    # 0. The EARLY run of this morning must have landed T-1 (and T-2) and T-1's volume.
    log.info("=" * 72)
    log.info("PREOPEN %s: dependency check (T-1 = %s, T-2 = %s)", today, prev, prev2)
    missing = []
    prev_chain, prev2_chain = {}, {}
    for root in config.SPX_ROOTS:
        prev_chain[root] = FR.stored_chain(store, root, prev)
        prev2_chain[root] = FR.stored_chain(store, root, prev2)
        if prev_chain[root].empty:
            missing.append(f"OI chain {root} {prev}")
        if prev2_chain[root].empty:
            missing.append(f"OI chain {root} {prev2}")
        if not _volume_present(vol_store, root, prev):
            missing.append(f"volume {root} session {prev}")
        log.info("  %s: T-1 chain %s rows, T-2 chain %s rows, T-1 volume %s", root,
                 f"{len(prev_chain[root]):,}", f"{len(prev2_chain[root]):,}",
                 "present" if _volume_present(vol_store, root, prev) else "MISSING")
    if missing:
        log.error("PREOPEN cannot run: %s not in the SPX stores. The EARLY run "
                  "(run_spx_pipeline.py --tier EARLY) must complete first; it fetches through "
                  "yesterday. Nothing fetched, nothing written.", "; ".join(missing))
        return 1

    # 1. Poll until fresh.
    if not test_connection():
        log.error("ThetaData terminal not reachable at start; will keep trying on schedule")
    snaps = None
    attempts = 1 if dry_run else POLL_ATTEMPTS
    for attempt in range(1, attempts + 1):
        log.info("-" * 72)
        log.info("PREOPEN snapshot attempt %d/%d", attempt, attempts)
        try:
            got = {r: _snapshot(r, today, fetch_oi_snapshot, FR) for r in config.SPX_ROOTS}
            verdicts = [FR.assess(r, got[r], prev_chain[r], prev2_chain[r], today, prev) for r in config.SPX_ROOTS]
        except Exception as exc:                      # transport / terminal errors: retry
            log.warning("  snapshot request failed: %s", exc)
            got, verdicts = None, []
        for v in verdicts:
            (log.info if v.fresh else log.warning)(
                "  %-5s %s  [rows %s, shared %s vs yesterday's %s, identical %.1f%%]", v.root, v.reason,
                f"{v.rows:,}", f"{v.shared:,}", f"{v.ref_shared:,}", 100 * v.identical_share)
        if verdicts and all(v.fresh for v in verdicts):
            snaps = got
            break
        if attempt < attempts:
            log.info("  not fresh yet; next attempt in %d min", POLL_INTERVAL_MIN)
            time.sleep(POLL_INTERVAL_MIN * 60)

    if dry_run:
        log.info("PREOPEN DRY RUN: %s; nothing written, nothing built",
                 "fresh" if snaps else "not fresh on this attempt")
        return 0
    if snaps is None:
        log.error("PREOPEN: no fresh snapshot after %d attempts over %d min. Nothing written; "
                  "spx_oi_daily has no row for %s.", POLL_ATTEMPTS, (POLL_ATTEMPTS - 1) * POLL_INTERVAL_MIN, today)
        return _summary("PREOPEN", today, [("snapshot", "today's OI, both roots", 1, time.time() - t0)], t0)

    # 2. Write, then read back.
    results = []
    ok = True
    for root, df in snaps.items():
        FS._assert_oi_target(root)
        oi_store.write_rows(root, df)
        back = FR.stored_chain(store, root, today)
        m = df.merge(back, on=FR.KEYS, how="left", suffixes=("", "_stored"))
        good = len(back) >= len(df) and bool((m["open_interest"] == m["open_interest_stored"]).all())
        expired = int((df["expiration"] < today).sum())
        (log.info if good else log.error)("  wrote %s %s: %s rows (%s expiring before today), read back %s",
                                          root, today, f"{len(df):,}", f"{expired:,}",
                                          "ok" if good else "MISMATCH")
        ok &= good
    FS._assert_prod_tree_clean()
    results.append(("snapshot", "today's OI, both roots", 0 if ok else 1, time.time() - t0))
    if not ok:
        return _summary("PREOPEN", today, results, t0)

    # 3. Today's spx_oi_daily row.
    rc, dt = run_stage(1, 1, "oi_daily", _build("build_spx_features.py", today), "spx_oi_daily incl. today")
    results.append(("oi_daily", "spx_oi_daily incl. today", rc, dt))
    return _summary("PREOPEN", today, results, t0)


def measure_freshness(n_days: int, today: date) -> int:
    """Read-only: identical share / shared count over the last n stored day pairs, per root."""
    config, FS, oi_store, FR, vol_store, *_ = _spx()
    days = get_trading_days(today - timedelta(days=2 * n_days + 10), today - timedelta(days=1))[-(n_days + 1):]
    for root in config.SPX_ROOTS:
        m = FR.measure(config.SPX_OI_RAW_DIR, root, days)
        if m.empty:
            log.info("%s: no stored day pairs", root)
            continue
        s, c = m["identical_share"], m["shared"]
        log.info("%s: %d day pairs %s → %s", root, len(m), m["day"].min(), m["day"].max())
        log.info("  identical share  min %.2f%%  median %.2f%%  max %.2f%%   (stale threshold %.0f%%)",
                 100 * s.min(), 100 * s.median(), 100 * s.max(), 100 * FR.STALE_IDENTICAL_SHARE)
        log.info("  shared contracts min %s  median %s  max %s", f"{c.min():,}", f"{int(c.median()):,}", f"{c.max():,}")
        ratio = (c / c.shift(1)).dropna()
        if len(ratio):
            log.info("  day-over-day shared ratio  min %.3f  max %.3f   (band %.2f-%.2f)",
                     ratio.min(), ratio.max(), *FR.SHARED_BAND)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Daily SPX pipeline (EARLY / PREOPEN tiers).",
                                 formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    ap.add_argument("--tier", choices=["EARLY", "PREOPEN"], default="EARLY",
                    help="EARLY (default): fetch + four builds. PREOPEN: today's OI snapshot + spx_oi_daily.")
    ap.add_argument("--today", type=lambda s: datetime.strptime(s, "%Y%m%d").date(),
                    help="pretend today is this date (YYYYMMDD); EARLY --dry-run only")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--measure-freshness", type=int, metavar="DAYS",
                    help="calibrate the freshness test on the last DAYS stored day pairs; read-only")
    args = ap.parse_args()

    today = args.today or date.today()
    if args.today and not (args.tier == "EARLY" and args.dry_run) and not args.measure_freshness:
        ap.error("--today is only for EARLY --dry-run (PREOPEN fetches the live snapshot, which is always today's)")
    if args.measure_freshness:
        return measure_freshness(args.measure_freshness, today)
    log.info("#" * 72)
    log.info("SPX pipeline starting (tier = %s, today = %s)%s", args.tier, today, "  DRY RUN" if args.dry_run else "")
    return run_early(today, args.dry_run) if args.tier == "EARLY" else run_preopen(today, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
