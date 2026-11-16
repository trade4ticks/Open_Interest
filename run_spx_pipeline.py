"""
run_spx_pipeline.py — the daily SPX pipeline: ONE run, one cron line, one lock, one log.

    python run_spx_pipeline.py                         # what cron runs, 07:30 ET Mon-Fri
    python run_spx_pipeline.py --dry-run               # print the stages and their dates; run nothing
    python run_spx_pipeline.py --dry-run --today 20261117
    python run_spx_pipeline.py --check-freshness       # dependency check + ONE freshness assessment of
                                                       # today's snapshot; no fetch, no write, no build
    python run_spx_pipeline.py --measure-freshness     # calibrate the freshness test on stored pairs; read-only

STAGES, in order
----------------
    1. fetch        fetch_spx_options.py fetch: history OI + EOD volume, trailing
                    FETCH_LOOKBACK_DAYS calendar days ending yesterday. Writes
                    the T-1 chain. Runs ONCE, before the first freshness attempt.
    2. snapshot     today's OI from /v3/option/snapshot/open_interest, both roots,
                    written only when lib.spx_oi_freshness says it is today's
                    publication (front_5pct, 70%; band 0.70-1.25; floor 20).
                    First it verifies that what it needs is on disk: the T-1 and
                    T-2 chains and session T-1's volume for both roots, and T-1's
                    close in index_ohlc (no close -> fail loudly, no spot-free
                    fallback). Then it polls: an attempt now, then every
                    POLL_INTERVAL_MIN minutes, POLL_ATTEMPTS in all; only the
                    snapshot request repeats, never the fetch. Both roots must pass
                    in the same attempt. No clock time anywhere: the content test
                    is the only gate. Skipped on a weekday that is not an NYSE
                    session.
    3. oi_daily     build_spx_features.py        spx_oi_daily, through TODAY if stage 2 wrote
                                                 today's OI, else through yesterday
    4. oi_5min      build_spx_table_b.py         spx_oi_5min
    5. price_daily  build_spx_price_daily.py     spx_price_daily
    6. price_5min   build_spx_price_5min.py      spx_price_5min
The catalog (build_spx_catalog.py) is NOT a stage: it describes schema, not
data, and is run by hand after a deployment that changes columns.

A FAILED STAGE DOES NOT STOP THE RUN. Every stage runs; each exit status is
recorded; a summary is printed; the exit code is 1 if any stage failed. A
snapshot that never goes fresh therefore costs today's spx_oi_daily row only:
the fetch has already committed, and the four builds still run (oi_daily
through yesterday). The OI builds are safe after a failed fetch too: they
rebuild from what the stores hold, cap their end at the last OI date in both
roots, and fail their own hard checks (writing nothing) on a half-written day.
A fetch or build that hangs past STAGE_TIMEOUT_MIN is killed and counted failed.

DEPENDENCIES
------------
    fetch ──> snapshot     the T-1 chain is the freshness test's reference
    fetch ──> oi_daily     raw SPX/SPXW OI + volume parquet stores
    snapshot ──> oi_daily  today's row (only)
    fetch ──> oi_5min      reads the same stores itself (computes its own
                           day-level book via compute_table_a; never reads
                           spx_oi_daily rows), plus index_ohlc for bar prices
    price_daily, price_5min  read only public.index_ohlc (filled by the
                           /Thetadata_Raw_SPX pipeline); independent of the
                           fetch, the snapshot, tables A / B and each other

WINDOWS
-------
The fetch covers the last FETCH_LOOKBACK_DAYS (7) calendar days ending
yesterday. The run is Mon-Fri only, so the longest stretch between a session
and the next run is Friday -> Tuesday when Monday is a holiday (4 days); 7 also
lets a missed run heal on the next one. Each build rebuilds the last
BUILD_WINDOW_SESSIONS (10) sessions ending at its own last buildable row. With
L = the last completed session before today:
    oi_daily     end today when stage 2 wrote today's OI, else L
    oi_5min      end L     (a bar of today has no price yet at 07:30)
    price_daily  end next session after L (that row uses only L and earlier)
    price_5min   end L
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
STAGE_TIMEOUT_MIN = 30           # per subprocess stage; a hung stage is killed and counted as failed
POLL_INTERVAL_MIN = 10           # minutes between snapshot attempts
POLL_ATTEMPTS = 7                # attempts in all (one at start + 6 retries = 60 minutes)


def _ymd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _window_start(end: date, n: int = BUILD_WINDOW_SESSIONS) -> date:
    return get_trading_days(end - timedelta(days=3 * n + 15), end)[-n:][0]


def _build(script: str, end: date) -> list[str]:
    return [str(ROOT / script), "--start", _ymd(_window_start(end)), "--end", _ymd(end)]


def fetch_argv(today: date) -> list[str]:
    start, end = today - timedelta(days=FETCH_LOOKBACK_DAYS), today - timedelta(days=1)
    return [str(ROOT / "fetch_spx_options.py"), "fetch", "--start", _ymd(start), "--end", _ymd(end)]


def builds(today: date, snapshot_written: bool) -> list[tuple[str, list[str], str]]:
    """Stages 3-6: [(stage name, argv after the python executable, description)]."""
    last = last_trading_day(today - timedelta(days=1))      # last completed session before today
    nxt = next_trading_day(last)
    a_end = today if snapshot_written else last
    return [
        ("oi_daily", _build("build_spx_features.py", a_end),
         "spx_oi_daily" + (" incl. today" if snapshot_written else "")),
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


def _summary(today: date, results: list, t0: float) -> int:
    failed = [r for r in results if r[2] not in (0, None)]
    log.info("#" * 72)
    log.info("SPX pipeline summary (today = %s)", today)
    for name, what, rc, dt in results:
        status = "ok" if rc == 0 else ("skipped" if rc is None else "FAILED")
        log.info("  %-12s %-28s %-8s rc=%-4s %7.1fs", name, what, status, "-" if rc is None else rc, dt)
    log.info("  total %.1fs; %d of %d stage(s) failed%s", time.time() - t0, len(failed), len(results),
             (": " + ", ".join(r[0] for r in failed)) if failed else "")
    log.info("#" * 72)
    return 1 if failed else 0


# --- Stage 2: today's OI snapshot ------------------------------------------------------------

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


def run_snapshot(today: date, check_only: bool = False) -> tuple[int, bool]:
    """Stage 2. Returns (rc, written). check_only: one assessment, nothing written."""
    config, FS, oi_store, FR, vol_store, fetch_oi_snapshot, test_connection = _spx()
    prev = last_trading_day(today - timedelta(days=1))
    prev2 = last_trading_day(prev - timedelta(days=1))
    store = config.SPX_OI_RAW_DIR
    FS._redirect_oi_store()
    FS._assert_prod_tree_clean()

    # What the freshness test needs must be on disk now — stage 1 writes the T-1 chain.
    log.info("snapshot %s: checking inputs (T-1 = %s, T-2 = %s)", today, prev, prev2)
    missing = []
    prev_chain, prev2_chain = {}, {}
    for root in config.SPX_ROOTS:
        prev_chain[root] = FR.stored_chain(store, root, prev)
        prev2_chain[root] = FR.stored_chain(store, root, prev2)
        vol_ok = _volume_present(vol_store, root, prev)
        if prev_chain[root].empty:
            missing.append(f"OI chain {root} {prev}")
        if prev2_chain[root].empty:
            missing.append(f"OI chain {root} {prev2}")
        if not vol_ok:
            missing.append(f"volume {root} session {prev}")
        log.info("  %s: T-1 chain %s rows, T-2 chain %s rows, T-1 volume %s", root,
                 f"{len(prev_chain[root]):,}", f"{len(prev2_chain[root]):,}", "present" if vol_ok else "MISSING")
    if missing:
        log.error("snapshot cannot run: %s not in the SPX stores after the history fetch (stage 1). "
                  "Check stage 1's output above. No snapshot requested, nothing written; spx_oi_daily "
                  "has no row for %s.", "; ".join(missing), today)
        return 1, False

    # T-1's close. No close, no test, no write, and no fallback to a spot-free comparison.
    spot, why = _prev_close(prev)
    if spot is None:
        log.error("snapshot cannot run: no spot for the freshness test. %s. index_ohlc is filled by the "
                  "/Thetadata_Raw_SPX pipeline; the close of %s must be there before today's OI can be "
                  "checked. No snapshot requested, nothing written; spx_oi_daily has no row for %s.",
                  why, prev, today)
        return 1, False
    log.info("  spot for the freshness test: %s close %.2f (index_ohlc)", prev, spot)

    if not test_connection():
        log.error("ThetaData terminal not reachable at start; will keep trying on schedule")
    snaps = None
    attempts = 1 if check_only else POLL_ATTEMPTS
    for attempt in range(1, attempts + 1):
        log.info("-" * 72)
        log.info("snapshot attempt %d/%d", attempt, attempts)
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

    if check_only:
        log.info("CHECK ONLY: %s; nothing written", "fresh" if snaps else "not fresh on this attempt")
        return 0, False
    if snaps is None:
        log.error("no fresh snapshot after %d attempts over %d min. Nothing written; spx_oi_daily has no "
                  "row for %s (the builds still run, through yesterday).",
                  POLL_ATTEMPTS, (POLL_ATTEMPTS - 1) * POLL_INTERVAL_MIN, today)
        return 1, False

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
    return (0, True) if ok else (1, False)


# --- The run ---------------------------------------------------------------------------------

def run_daily(today: date) -> int:
    t0 = time.time()
    results = []
    session = last_trading_day(today) == today
    n = 2 + 4

    # 1. History fetch — once, before any freshness attempt.
    rc, dt = run_stage(1, n, "fetch", fetch_argv(today), "SPX/SPXW history OI + EOD volume")
    results.append(("fetch", "history OI + EOD volume", rc, dt))

    # 2. Today's snapshot, gated on freshness.
    log.info("=" * 72)
    log.info("STAGE 2/%d  snapshot  (today's OI, both roots)", n)
    log.info("=" * 72)
    t1 = time.time()
    if session:
        try:
            rc, written = run_snapshot(today)
        except Exception as exc:          # never let stage 2 take the builds down with it
            log.exception("snapshot stage crashed: %s", exc)
            rc, written = 1, False
    else:
        log.info("  %s is not an NYSE session: no snapshot today", today)
        rc, written = None, False
    (log.info if rc in (0, None) else log.error)("STAGE 2/%d  snapshot  finished rc=%s in %.1fs (%s)", n,
                                                 "-" if rc is None else rc, time.time() - t1,
                                                 "today's OI written" if written else "nothing written")
    results.append(("snapshot", "today's OI, both roots", rc, time.time() - t1))

    # 3-6. All four builds, whatever stages 1-2 did.
    for i, (name, argv, what) in enumerate(builds(today, written), 3):
        rc, dt = run_stage(i, n, name, argv, what)
        results.append((name, what, rc, dt))
    return _summary(today, results, t0)


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
            log.info("    the band %.2f-%.2f would have refused %d fresh day(s)%s", lo, hi, out,
                     (": " + ", ".join(str(d) for d in r[(r < lo) | (r > hi)].index[:10])) if out else "")
        f5 = m[m["variant"] == "front_5pct"].set_index("trade_date")["contracts"]
        low = f5[f5 < FR.FRONT_MIN_CONTRACTS]
        log.info("  front-contract floor — front_5pct contracts: min %d, p01 %d, p05 %d; the floor %d "
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
    ap = argparse.ArgumentParser(description="Daily SPX pipeline: fetch, today's OI snapshot, four builds.",
                                 formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    ap.add_argument("--today", type=lambda s: datetime.strptime(s, "%Y%m%d").date(),
                    help="pretend today is this date (YYYYMMDD); --dry-run only")
    ap.add_argument("--dry-run", action="store_true", help="print the stages and their dates; run nothing")
    ap.add_argument("--check-freshness", action="store_true",
                    help="input check + ONE freshness assessment of today's snapshot; no fetch, write or build")
    ap.add_argument("--measure-freshness", type=int, nargs="?", const=0, metavar="DAYS",
                    help="read-only: identical share per root and contract subset over the full history "
                         "(or the last DAYS pairs)")
    ap.add_argument("--min-contracts", type=int, default=1,
                    help="--measure-freshness: fewest contracts a subset needs for a pair to be scored")
    args = ap.parse_args()

    today = args.today or date.today()
    if args.today and not args.dry_run:
        ap.error("--today is only for --dry-run (the snapshot is always today's)")
    if args.measure_freshness is not None:
        return measure_freshness(args.measure_freshness, today, args.min_contracts)
    log.info("#" * 72)
    if args.check_freshness:
        log.info("SPX pipeline: freshness check only (today = %s)", today)
        if last_trading_day(today) != today:
            log.info("  %s is not an NYSE session", today)
            return 0
        return run_snapshot(today, check_only=True)[0]
    log.info("SPX pipeline starting (today = %s)%s", today, "  DRY RUN" if args.dry_run else "")
    if args.dry_run:
        session = last_trading_day(today) == today
        log.info("  1. %-12s %s", "fetch", " ".join(fetch_argv(today)[1:]))
        log.info("  2. %-12s %s", "snapshot", f"today's OI ({today}), freshness-gated, up to {POLL_ATTEMPTS} "
                 f"attempts {POLL_INTERVAL_MIN} min apart" if session else f"skipped: {today} is not a session")
        for i, written in ((3, True), (3, False)):
            if not session and written:
                continue
            log.info("  if the snapshot %s:", "is written" if written else "is not written")
            for j, (name, argv, what) in enumerate(builds(today, written), i):
                log.info("  %d. %-12s %-24s %s", j, name, what, " ".join(argv[1:]))
        return 0
    return run_daily(today)


if __name__ == "__main__":
    sys.exit(main())
