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


def _prev_close(day: date) -> tuple[float | None, str]:
    """The close of session `day` from index_ohlc — the last grid bar's spx_close,
    exactly as the price tables derive it. (close, '') or (None, why)."""
    import config
    import lib.spx_price as P
    if not config.SPX_INDEX_OHLC_DSN:
        return None, "SPX_INDEX_OHLC_DSN is not set"
    try:
        bars = P.load_ohlc(config.SPX_INDEX_OHLC_DSN, day, day)
    except Exception as exc:
        return None, f"index_ohlc could not be read: {exc}"
    d = P.daily_ohlc(bars, [day]).iloc[0]
    c = d["close"]
    if c is None or c != c:                                  # None or NaN
        return None, (f"index_ohlc has no usable close for {day}: the session's last grid bar has no "
                      f"positive spx_close ({len(bars)} index_ohlc rows that day, {int(d['n_bars'])} grid bars)")
    return float(c), ""


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

    # 0b. The freshness test needs T-1's close. No close, no test, no write — and no
    #     fallback to a spot-free comparison: a missing close must be found out about.
    spot, why = _prev_close(prev)
    if spot is None:
        log.error("PREOPEN cannot run: no spot for the freshness test. %s. index_ohlc is filled by the "
                  "/Thetadata_Raw_SPX pipeline; the close of %s must be there before today's OI can be "
                  "checked. Nothing fetched, nothing written; spx_oi_daily has no row for %s.", why, prev, today)
        return 1
    log.info("  spot for the freshness test: %s close %.2f (index_ohlc)", prev, spot)

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
            verdicts = [FR.assess(r, got[r], prev_chain[r], prev2_chain[r], today, prev, spot)
                        for r in config.SPX_ROOTS]
        except Exception as exc:                      # transport / terminal errors: retry
            log.warning("  snapshot request failed: %s", exc)
            got, verdicts = None, []
        for v in verdicts:
            (log.info if v.fresh else log.warning)(
                "  %-5s %s  [rows %s; front %s, %d contracts, identical %.1f%%; all-alive shared %s vs "
                "yesterday's %s]", v.root, v.reason, f"{v.rows:,}", v.front, v.front_contracts,
                100 * v.identical_share, f"{v.shared:,}", f"{v.ref_shared:,}")
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


def measure_freshness(n_days: int, today: date, min_contracts: int) -> int:
    """Read-only. Identical share between consecutive stored chains, per root and
    per contract subset (lib.spx_oi_freshness.VARIANTS), over the full history
    (n_days = 0) or the last n_days pairs. Moneyness uses the close of the session
    before each day from index_ohlc; nothing reads the volume store."""
    import config
    import lib.spx_features as F
    import lib.spx_oi_freshness as FR
    import lib.spx_price as P
    import pandas as pd
    if not config.SPX_INDEX_OHLC_DSN:
        log.error("SPX_INDEX_OHLC_DSN is not set: the moneyness variants need index_ohlc closes")
        return 2
    store = config.SPX_OI_RAW_DIR
    firsts = [d for d in (F.store_first_date(store, r) for r in config.SPX_ROOTS) if d]
    lasts = [d for d in (F.store_last_date(store, r) for r in config.SPX_ROOTS) if d]
    if not firsts:
        log.error("no stored SPX OI under %s", store)
        return 2
    first, last = min(firsts), max(lasts)
    sessions = get_trading_days(first - timedelta(days=10), last)
    bars = P.load_ohlc(config.SPX_INDEX_OHLC_DSN, sessions[0], last)
    daily = P.daily_ohlc(bars, sessions)
    pd_frame = daily[["trade_date", "close"]].copy()
    pd_frame["trade_date"] = list(sessions[1:]) + [next_trading_day(sessions[-1])]   # close of T-1 -> row T
    spot = pd_frame.rename(columns={"close": "spot"}).dropna()
    log.info("#" * 72)
    log.info("Freshness measurement: stored SPX OI %s → %s; spot = close of the session before T "
             "(index_ohlc); %s; a pair is scored only with >= %d contracts in the subset",
             first, last, "full history" if not n_days else f"last {n_days} pairs", min_contracts)
    log.info("A stale snapshot IS the T-1 chain: it scores 100% on every variant. The margin is 100% minus "
             "the highest score a fresh day reached. front = the nearest expiration >= T in the stored T-1 "
             "chain, applied to both sides.")
    for root in config.SPX_ROOTS:
        m = FR.measure_variants(store, root, spot, sessions)
        if m.empty:
            log.info("%s: no stored chains", root)
            continue
        stored_pairs = m.attrs["stored_pairs"]
        days = sorted(m["trade_date"].unique())
        if n_days:
            days = days[-n_days:]
            m = m[m["trade_date"].isin(set(days))]
        log.info("=" * 72)
        log.info("%s: %d day pairs in the store; %d scorable (previous stored day is the previous NYSE "
                 "session, and a T-1 close exists); %s", root, stored_pairs, len(days),
                 f"{days[0]} → {days[-1]}" if days else "none")
        log.info("  %-15s %7s %7s  %7s %7s %7s %7s %7s  %7s %7s %7s %7s", "variant", "scored", "skipped",
                 "min", "p01", "median", "p99", "max", "n_min", "n_p05", "n_med", "n_max")
        for k in FR.VARIANTS:
            v = m[m["variant"] == k]
            ok_ = v[v["contracts"] >= min_contracts]
            skipped = len(v) - len(ok_)
            if ok_.empty:
                log.info("  %-15s %7d %7d  no day has %d contracts in this subset (median %d)", k, 0, skipped,
                         min_contracts, int(v["contracts"].median()) if len(v) else 0)
                continue
            q = ok_["identical_share"].quantile([0.0, 0.01, 0.5, 0.99, 1.0]).to_numpy() * 100
            n = ok_["contracts"]
            log.info("  %-15s %7d %7d  %6.1f%% %6.1f%% %6.1f%% %6.1f%% %6.1f%%  %7s %7s %7s %7s", k, len(ok_),
                     skipped, *q, f"{n.min():,}", f"{int(n.quantile(0.05)):,}", f"{int(n.median()):,}",
                     f"{n.max():,}")
        log.info("  highest-scoring fresh days per variant (the ones a threshold must clear):")
        for k in FR.VARIANTS:
            v = m[(m["variant"] == k) & (m["contracts"] >= min_contracts)].nlargest(3, "identical_share")
            if len(v):
                log.info("    %-15s %s", k, ", ".join(f"{d} {s:.1%} (n={c:,})" for d, s, c in
                                                     zip(v["trade_date"], v["identical_share"], v["contracts"])))
        # The live test's two sanity checks, against the full history.
        prev_map = {sessions[i]: sessions[i - 1] for i in range(1, len(sessions))}
        a = m[m["variant"] == "a_all"].set_index("trade_date")["contracts"]
        pairs = [(d, a[d] / a[prev_map[d]]) for d in a.index if prev_map.get(d) in a.index and a[prev_map[d]] > 0]
        if pairs:
            r = pd.Series([x for _, x in pairs], index=[d for d, _ in pairs])
            q = r.quantile([0.0, 0.01, 0.05, 0.5, 0.95, 0.99, 1.0]).to_numpy()
            lo, hi = FR.SHARED_BAND
            log.info("  truncation check — all-alive shared count, today's pair / yesterday's pair (%d ratios):", len(r))
            log.info("    min %.3f  p01 %.3f  p05 %.3f  median %.3f  p95 %.3f  p99 %.3f  max %.3f", *q)
            log.info("    lowest : %s", ", ".join(f"{d} {x:.3f}" for d, x in r.nsmallest(5).items()))
            log.info("    highest: %s", ", ".join(f"{d} {x:.3f}" for d, x in r.nlargest(5).items()))
            out = int(((r < lo) | (r > hi)).sum())
            log.info("    the provisional band %.2f-%.2f would have refused %d fresh day(s)%s", lo, hi, out,
                     (": " + ", ".join(str(d) for d in r[(r < lo) | (r > hi)].index[:10])) if out else "")
        f5 = m[m["variant"] == "front_5pct"].set_index("trade_date")["contracts"]
        low = f5[f5 < FR.FRONT_MIN_CONTRACTS]
        log.info("  front-contract floor — front_5pct contracts: min %d, p01 %d, p05 %d; the provisional floor %d "
                 "would have refused %d fresh day(s)%s", f5.min(), int(f5.quantile(0.01)), int(f5.quantile(0.05)),
                 FR.FRONT_MIN_CONTRACTS, len(low), (": " + ", ".join(f"{d} ({n})" for d, n in low.items())[:400])
                 if len(low) else "")
        log.info("    fewest: %s", ", ".join(f"{d} {n}" for d, n in f5.nsmallest(5).items()))
        sk = m[(m["contracts"] < min_contracts)]
        if len(sk):
            log.info("  skipped pairs by variant and year (< %d contracts):", min_contracts)
            tab = sk.assign(year=pd.to_datetime(sk["trade_date"]).dt.year).groupby(["variant", "year"]).size()
            for (k, y), c in tab.items():
                log.info("    %-15s %d  %d", k, y, c)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Daily SPX pipeline (EARLY / PREOPEN tiers).",
                                 formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    ap.add_argument("--tier", choices=["EARLY", "PREOPEN"], default="EARLY",
                    help="EARLY (default): fetch + four builds. PREOPEN: today's OI snapshot + spx_oi_daily.")
    ap.add_argument("--today", type=lambda s: datetime.strptime(s, "%Y%m%d").date(),
                    help="pretend today is this date (YYYYMMDD); EARLY --dry-run only")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--measure-freshness", type=int, nargs="?", const=0, metavar="DAYS",
                    help="read-only: identical share per root and contract subset over the full history "
                         "(or the last DAYS pairs)")
    ap.add_argument("--min-contracts", type=int, default=1,
                    help="--measure-freshness: fewest contracts a subset needs for a pair to be scored")
    args = ap.parse_args()

    today = args.today or date.today()
    if args.today and not (args.tier == "EARLY" and args.dry_run) and args.measure_freshness is None:
        ap.error("--today is only for EARLY --dry-run (PREOPEN fetches the live snapshot, which is always today's)")
    if args.measure_freshness is not None:
        return measure_freshness(args.measure_freshness, today, args.min_contracts)
    log.info("#" * 72)
    log.info("SPX pipeline starting (tier = %s, today = %s)%s", args.tier, today, "  DRY RUN" if args.dry_run else "")
    return run_early(today, args.dry_run) if args.tier == "EARLY" else run_preopen(today, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
