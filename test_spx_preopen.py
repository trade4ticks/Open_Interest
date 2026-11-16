"""
test_spx_preopen.py — run_spx_pipeline.py offline (the single daily run):
temporary SPX stores, the snapshot endpoint mocked, the subprocess stages
(fetch and the four builds) stubbed, no waiting.

Covers: the history fetch runs once, before the first snapshot request, and
the four builds run after the snapshot stage whatever it did; the T-1 chain
stage 1 writes is what stage 2 compares against; inputs missing after the
fetch, a missing T-1 close, a crash inside stage 2, a snapshot that never goes
fresh: nothing written, exit 1, builds still run (spx_oi_daily through
yesterday); stale-then-fresh writes today and builds spx_oi_daily through
today; a holiday weekday skips only the snapshot; front_5pct (front from the
stored T-1 chain), the 0.70-1.25 band and the 20-contract floor; one root
stale and one fresh writes neither; --check-freshness writes nothing; the
freshness measurements on stored pairs.

Run:  python test_spx_preopen.py     (exit 1 on any failure)
"""
from __future__ import annotations

import shutil
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

import config

TMP = Path(tempfile.mkdtemp(prefix="spx_preopen_test_"))
config.SPX_OI_RAW_DIR = TMP / "spx_oi_raw"
config.SPX_VOLUME_DIR = TMP / "spx_volume"

import fetch_spx_options as FS            # noqa: E402  (after the config override)
import lib.parquet_store as oi_store      # noqa: E402
import lib.spx_oi_freshness as FR         # noqa: E402
import lib.spx_volume_store as vol_store  # noqa: E402
import run_spx_pipeline as R              # noqa: E402
from lib.market_hours import get_trading_days  # noqa: E402

vol_store.SPX_VOLUME_DIR = config.SPX_VOLUME_DIR
FAILS: list[str] = []


def ok(cond, label: str) -> None:
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
    if not cond:
        FAILS.append(label)


TODAY = date(2026, 11, 17)                       # a Tuesday
DAYS = get_trading_days(TODAY - timedelta(days=12), TODAY)
PREV, PREV2 = DAYS[-2], DAYS[-3]
RNG = np.random.default_rng(3)


def evolve(chain: pd.DataFrame, day: date, keep_expired: bool = False) -> pd.DataFrame:
    """The next day's chain: near-the-money contracts of the front expiration
    (within 5% of 6000) trade every day and ~90% of them change OI, as the
    front_5pct test assumes; ~45% of all others change. A new expiry rolls in;
    contracts expiring before `day` drop out unless keep_expired (purge lag)."""
    c = chain.copy()
    c["trade_date"] = day
    if not keep_expired:
        c = c[c["expiration"] >= day]
    alive = c.loc[c["expiration"] >= day, "expiration"]
    front = alive.min() if len(alive) else None
    near = (c["expiration"] == front) & ((c["strike"] / 6000.0 - 1).abs() <= 0.05)
    chg = RNG.random(len(c)) < np.where(near, 0.90, 0.45)
    c.loc[chg, "open_interest"] = c.loc[chg, "open_interest"] + RNG.integers(1, 500, chg.sum())
    new_exp = day + timedelta(days=30)
    add = pd.DataFrame({"trade_date": day, "expiration": new_exp,
                        "strike": np.arange(5000, 7000, 25.0).repeat(2),
                        "option_type": ["C", "P"] * 80, "open_interest": RNG.integers(1, 1000, 160)})
    return FR.normalise(pd.concat([c, add], ignore_index=True))


def base_chain(day: date, n_exp: int) -> pd.DataFrame:
    rows = []
    for k in range(n_exp):
        exp = day + timedelta(days=k)
        for s in np.arange(5000, 7000, 25.0):
            for t in ("C", "P"):
                rows.append((day, exp, s, t, int(RNG.integers(1, 5000))))
    return FR.normalise(pd.DataFrame(rows, columns=FR.COLS))


def reset_store(with_prev=True, with_prev_vol=True):
    shutil.rmtree(TMP, ignore_errors=True)
    FS._redirect_oi_store()
    chains = {}
    for root, n_exp in (("SPX", 4), ("SPXW", 12)):
        c2 = base_chain(PREV2, n_exp)
        c1 = evolve(c2, PREV)
        chains[root] = (c2, c1)
        oi_store.write_rows(root, c2)
        if with_prev:
            oi_store.write_rows(root, c1)
        vdays = [PREV2] + ([PREV] if with_prev_vol else [])
        for d in vdays:
            vol_store.write_rows(root, pd.DataFrame({"trade_date": d, "expiration": d, "strike": 6000.0,
                                                     "option_type": "C", "volume": 10, "trade_count": 1}, index=[0]))
    return chains


class Mock:
    """Snapshot endpoint: per attempt, per root, a frame."""
    def __init__(self, script, events=None):
        self.script, self.calls, self.events = script, 0, events if events is not None else []

    def __call__(self, root, today):
        attempt = self.calls // 2
        self.calls += 1
        self.events.append("snapshot_request")
        df = self.script[min(attempt, len(self.script) - 1)][root]
        return df.assign(trade_date=today)


SPOT = 6000.0


def run(script, close=(SPOT, ""), today=TODAY, fetch_writes=None, check_only=False, crash=False):
    """Drive run_spx_pipeline.run_daily (or run_snapshot(check_only)) with the
    subprocess stages stubbed and the snapshot endpoint mocked. Returns a dict:
    rc, stages [(name, argv)], attempts, events (order of fetch / snapshot calls)."""
    events, stages = [], []
    mock = Mock(script, events)
    real = R._spx

    def patched():
        c, fs, store, fr, vs, _, _ = real()
        return c, fs, store, fr, vs, mock, (lambda: True)

    def stage(i, n, name, argv, what):
        stages.append((name, argv))
        events.append(name)
        if name == "fetch" and fetch_writes:
            fetch_writes()
        return 0, 0.0

    def close_fn(day):
        if crash:
            raise RuntimeError("boom")
        return close
    R._spx = patched
    R._prev_close = close_fn
    R.run_stage = stage
    R.time.sleep = lambda sec: None
    try:
        rc = R.run_snapshot(today, check_only=True)[0] if check_only else R.run_daily(today)
    finally:
        R._spx = real
    return {"rc": rc, "stages": stages, "attempts": mock.calls // 2, "events": events}


def build_ends(res) -> dict:
    return {name: argv[-1] for name, argv in res["stages"] if name != "fetch"}


def today_rows(root):
    return FR.stored_chain(config.SPX_OI_RAW_DIR, root, TODAY)


def third_fridays(after: date, n: int) -> list[date]:
    out, y, mth = [], after.year, after.month
    while len(out) < n:
        first = date(y, mth, 1)
        fri = first + timedelta(days=(4 - first.weekday()) % 7 + 14)
        if fri >= after:
            out.append(fri)
        y, mth = (y + 1, 1) if mth == 12 else (y, mth + 1)
    return out


def measurement_tests() -> None:
    """lib.spx_oi_freshness.measure_variants and the --measure-freshness report on a synthetic store."""
    print("\nmeasure_variants: full history, by contract subset")
    shutil.rmtree(TMP, ignore_errors=True)
    FS._redirect_oi_store()
    sess = get_trading_days(date(2026, 9, 1), date(2026, 11, 13))
    stale_day, gap_day = sess[20], sess[35]
    strikes = np.arange(5000, 7025, 25.0)
    chains = {"SPX": None, "SPXW": None}
    for d in sess:
        for root in chains:
            exps = ([e for e in sess if e >= d][:10] + [d + timedelta(days=k) for k in (45, 90, 180)]
                    if root == "SPXW" else third_fridays(d, 3))
            prev = chains[root]
            fresh = pd.DataFrame([(d, e, k, t, int(RNG.integers(1, 5000))) for e in exps for k in strikes
                                  for t in ("C", "P")], columns=FR.COLS)
            if prev is not None:
                carried = prev.assign(trade_date=d)
                carried = carried[carried["expiration"] >= d]
                keyed = fresh.merge(carried, on=FR.KEYS, how="left", suffixes=("", "_old"))
                keep = keyed["open_interest_old"].notna() & (RNG.random(len(keyed)) >= 0.45)
                keyed.loc[keep, "open_interest"] = keyed.loc[keep, "open_interest_old"].astype(int)
                fresh = keyed[FR.COLS]
            if d == stale_day:
                fresh = prev.assign(trade_date=d)                  # yesterday's chain, relabelled
            chains[root] = FR.normalise(fresh)
            if d != gap_day:
                oi_store.write_rows(root, chains[root])
    spot = pd.DataFrame({"trade_date": sess, "spot": 6000.0})
    for root in ("SPXW", "SPX"):
        m = FR.measure_variants(config.SPX_OI_RAW_DIR, root, spot, sess)
        a = m[m["variant"] == "a_all"].set_index("trade_date")
        ok(a.loc[stale_day, "identical_share"] == 1.0, f"{root}: the planted stale day scores 100% (a_all)")
        st = m[(m["trade_date"] == stale_day) & (m["contracts"] > 0)]
        ok((st["identical_share"] == 1.0).all(), f"{root}: ... and 100% on every variant with contracts")
        normal = a.drop(index=stale_day)["identical_share"]
        ok(normal.between(0.45, 0.65).all(), f"{root}: fresh days {normal.min():.1%} - {normal.max():.1%}")
        ok(gap_day not in a.index and sess[36] not in a.index,
           f"{root}: the missing session and the day after it (2-session gap) are not scored")
    # Front = nearest expiration >= T in the STORED T-1 chain. Strike grid 5000..7000 step 25,
    # spot 6000: +/-1% -> 5 strikes, +/-2% -> 9, +/-5% -> 25, both types.
    for root in ("SPXW", "SPX"):
        m = FR.measure_variants(config.SPX_OI_RAW_DIR, root, spot, sess)
        exp_n = {f"front_{x}pct": 2 * int(sum(abs(k / 6000.0 - 1.0) <= x / 100 for k in strikes)) for x in (1, 2, 5)}
        exp_n["front_all"] = 2 * len(strikes)
        for k, n in exp_n.items():
            v = m[(m["variant"] == k) & (m["trade_date"] != stale_day)]
            ok((v["contracts"] == n).all(), f"{root} {k}: {n} contracts every scored day")
        f = m[m["variant"] == "front_all"].drop_duplicates("trade_date")
        if root == "SPXW":
            ok((f["front"] == f["trade_date"]).all(), "SPXW: the front is T itself (an expiry every session)")
        else:
            ok(all(f0 == third_fridays(d, 1)[0] for d, f0 in zip(f["trade_date"], f["front"])),
               "SPX: the front is the next third Friday on or after T")
        st = m[(m["trade_date"] == stale_day) & m["variant"].str.startswith("front")]
        ok((st["identical_share"] == 1.0).all() and (st["contracts"] > 0).all(),
           f"{root}: the stale day joins on the front and scores 100%")

    print("\n--measure-freshness report runs end to end (index_ohlc mocked)")
    import lib.spx_price as P
    orig = (P.load_ohlc, P.daily_ohlc, config.SPX_INDEX_OHLC_DSN)
    P.load_ohlc = lambda dsn, lo, hi: pd.DataFrame()
    P.daily_ohlc = lambda bars, ss: pd.DataFrame({"trade_date": ss, "close": 6000.0})
    config.SPX_INDEX_OHLC_DSN = "mocked"
    try:
        ok(R.measure_freshness(0, date(2026, 11, 14), 100) == 0, "report completes")
    finally:
        P.load_ohlc, P.daily_ohlc, config.SPX_INDEX_OHLC_DSN = orig

    # The front comes from T-1's chain, not the arriving one: an arriving chain that lacks
    # T's expiry entirely (as a stale chain would, where T-1's expiry stands in) scores 0
    # contracts on the front rather than switching to its own nearest expiration.
    shutil.rmtree(TMP, ignore_errors=True)
    FS._redirect_oi_store()
    d1, d2 = sess[40], sess[41]
    old = FR.normalise(pd.DataFrame([(d1, e, k, t, 100) for e in (d1, d2, d2 + timedelta(days=7))
                                     for k in strikes for t in ("C", "P")], columns=FR.COLS))
    new = old[old["expiration"] != d2].assign(trade_date=d2)       # no row at T's own expiry
    oi_store.write_rows("SPXW", old)
    oi_store.write_rows("SPXW", new)
    m = FR.measure_variants(config.SPX_OI_RAW_DIR, "SPXW", spot, sess)
    fa = m[(m["variant"] == "front_all") & (m["trade_date"] == d2)]
    ok(fa.empty or ((fa["front"] == d2).all() and (fa["contracts"] == 0).all()),
       "front fixed by the T-1 chain: no fallback to the arriving chain's nearest expiry")


def main() -> int:
    YD = "20261116"                                   # yesterday, L
    TD = "20261117"                                   # today

    print("one run: fetch once, then the snapshot, then all four builds")
    ch = reset_store()
    stale = {r: ch[r][1] for r in ch}
    fresh = {r: evolve(ch[r][1], TODAY, keep_expired=True) for r in ch}
    ok((fresh["SPXW"]["expiration"] < TODAY).any(), "the fresh SPXW snapshot carries contracts that expired yesterday")
    res = run([stale, stale, fresh])
    ok(res["rc"] == 0 and res["attempts"] == 3, f"written on attempt 3 (rc {res['rc']}, attempts {res['attempts']})")
    ok(res["events"].count("fetch") == 1 and res["events"][0] == "fetch"
       and res["events"].index("fetch") < res["events"].index("snapshot_request"),
       "the history fetch runs once, before the first snapshot request, not on each retry")
    ok([n for n, _ in res["stages"]] == ["fetch", "oi_daily", "oi_5min", "price_daily", "price_5min"]
       and res["events"].index("oi_daily") > max(i for i, e in enumerate(res["events"]) if e == "snapshot_request"),
       "builds after the snapshot stage, in order")
    for r in ch:
        ok(len(today_rows(r)) == len(fresh[r]), f"{r}: {len(today_rows(r)):,} rows labelled {TODAY} in the store")
    e = build_ends(res)
    ok(e == {"oi_daily": TD, "oi_5min": YD, "price_daily": TD, "price_5min": YD},
       f"oi_daily through today; oi_5min / price_5min through yesterday; price_daily through today: {e}")
    v = FR.assess("SPXW", fresh["SPXW"], ch["SPXW"][1], ch["SPXW"][0], TODAY, PREV, SPOT)
    ok(v.fresh and v.identical_share < FR.STALE_IDENTICAL_SHARE,
       f"the fresh chain scores {v.identical_share:.1%} on its front (< {FR.STALE_IDENTICAL_SHARE:.0%})")

    print("\nstep 1 is the dependency: the T-1 chain the fetch writes is what step 2 compares against")
    ch = reset_store(with_prev=False)
    res = run([{r: evolve(ch[r][1], TODAY) for r in ch}],
              fetch_writes=lambda: [oi_store.write_rows(r, ch[r][1]) for r in ch])
    ok(res["rc"] == 0 and len(today_rows("SPXW")) > 0, "T-1 chain absent before the run, written by stage 1, used by stage 2")

    print("\nstep 1 did not write what step 2 needs")
    ch = reset_store(with_prev=False)
    res = run([{}])
    ok(res["rc"] == 1 and res["attempts"] == 0 and today_rows("SPXW").empty,
       "T-1 chain missing after the fetch -> no snapshot request, nothing written, exit 1")
    ok(build_ends(res) == {"oi_daily": YD, "oi_5min": YD, "price_daily": TD, "price_5min": YD},
       "... and all four builds still ran (oi_daily through yesterday)")
    reset_store(with_prev_vol=False)
    res = run([{}])
    ok(res["rc"] == 1 and res["attempts"] == 0 and len(res["stages"]) == 5, "T-1 volume missing -> same")

    print("\nnever fresh: the late publication costs today's row only")
    ch = reset_store()
    res = run([{r: ch[r][1] for r in ch}])
    ok(res["rc"] == 1 and res["attempts"] == R.POLL_ATTEMPTS and today_rows("SPXW").empty,
       f"refused on all {R.POLL_ATTEMPTS} attempts, nothing written, exit 1")
    ok(res["events"].count("fetch") == 1, "the fetch still ran exactly once")
    ok(build_ends(res) == {"oi_daily": YD, "oi_5min": YD, "price_daily": TD, "price_5min": YD},
       "all four builds ran and committed, oi_daily through yesterday")

    print("\nthe spot dependency is loud")
    ch = reset_store()
    res = run([{r: evolve(ch[r][1], TODAY) for r in ch}], close=(None, "index_ohlc has no usable close"))
    ok(res["rc"] == 1 and res["attempts"] == 0 and today_rows("SPX").empty and today_rows("SPXW").empty,
       "no T-1 close -> no snapshot request, nothing written, exit 1; no spot-free fallback")
    ok(len(res["stages"]) == 5, "... the builds still ran")
    try:
        FR.assess("SPXW", ch["SPXW"][1], ch["SPXW"][1], ch["SPXW"][0], TODAY, PREV, None)
        raised = False
    except ValueError:
        raised = True
    ok(raised, "assess() refuses to score without a spot")

    print("\na crash inside the snapshot stage does not take the builds down")
    reset_store()
    res = run([{}], crash=True)
    ok(res["rc"] == 1 and len(res["stages"]) == 5, "stage 2 raised; logged as failed; builds ran")

    print("\nholiday weekday: fetch and builds, no snapshot")
    reset_store()
    res = run([{}], today=date(2026, 11, 26))
    ok(res["rc"] == 0 and res["attempts"] == 0 and [n for n, _ in res["stages"]][0] == "fetch"
       and len(res["stages"]) == 5, "Thanksgiving: snapshot skipped (not a failure), everything else ran")

    print("\ntruncated snapshot")
    ch = reset_store()
    res = run([{r: evolve(ch[r][1], TODAY).sample(frac=0.3, random_state=1) for r in ch}])
    ok(res["rc"] == 1 and today_rows("SPXW").empty, "30% of the chain -> shared count out of band, nothing written")

    print("\nfront_5pct: front from the stored T-1 chain, strikes within 5% of the T-1 close")
    ch = reset_store()
    fresh = {r: evolve(ch[r][1], TODAY) for r in ch}
    v = FR.assess("SPXW", fresh["SPXW"], ch["SPXW"][1], ch["SPXW"][0], TODAY, PREV, SPOT)
    exp_n = 2 * int(sum(abs(k / SPOT - 1) <= 0.05 for k in np.arange(5000, 7000, 25.0)))
    ok(v.front == TODAY and v.front_contracts == exp_n,
       f"SPXW front = {v.front} (T itself), scored on {v.front_contracts} contracts (expected {exp_n})")
    vs = FR.assess("SPX", ch["SPX"][1], ch["SPX"][1], ch["SPX"][0], TODAY, PREV, SPOT)
    ok(not vs.fresh and vs.identical_share == 1.0 and "STALE" in vs.reason,
       f"a stale SPX chain scores 100% on its front ({vs.front}, {vs.front_contracts} contracts): refused")
    ok(FR.STALE_IDENTICAL_SHARE == 0.70 and FR.SHARED_BAND == (0.70, 1.25) and FR.FRONT_MIN_CONTRACTS == 20,
       "70% both roots; band 0.70-1.25; floor 20")

    print("\nfront-contract floor")
    w = fresh["SPXW"]
    no_front = w[w["expiration"] != TODAY]                 # the arriving SPXW chain lacks T's expiry
    v = FR.assess("SPXW", no_front, ch["SPXW"][1], ch["SPXW"][0], TODAY, PREV, SPOT)
    ok(not v.fresh and v.front == TODAY and v.front_contracts == 0 and "too few" in v.reason,
       "front taken from T-1's chain, not the arriving one; 0 contracts -> refused")
    res = run([{"SPX": fresh["SPX"], "SPXW": no_front}])
    ok(res["rc"] == 1 and today_rows("SPX").empty and today_rows("SPXW").empty, "neither root written")

    print("\none root stale, the other fresh")
    ch = reset_store()
    res = run([{"SPX": evolve(ch["SPX"][1], TODAY), "SPXW": ch["SPXW"][1]}])
    ok(res["rc"] == 1 and today_rows("SPX").empty and today_rows("SPXW").empty, "neither root written")

    print("\n--check-freshness")
    ch = reset_store()
    res = run([{r: evolve(ch[r][1], TODAY) for r in ch}], check_only=True)
    ok(res["rc"] == 0 and res["attempts"] == 1 and not res["stages"] and today_rows("SPX").empty,
       "one assessment; no fetch, no write, no build")

    print("\nmeasure on stored pairs")
    m = FR.measure(config.SPX_OI_RAW_DIR, "SPXW", [PREV2, PREV])
    ok(len(m) == 1 and 0.4 < m["identical_share"].iloc[0] < 0.7, f"SPXW pair share {m['identical_share'].iloc[0]:.1%}")

    measurement_tests()

    shutil.rmtree(TMP, ignore_errors=True)
    print(f"\n{'ALL PASS' if not FAILS else f'{len(FAILS)} FAILURE(S)'}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
