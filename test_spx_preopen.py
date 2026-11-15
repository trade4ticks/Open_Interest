"""
test_spx_preopen.py — run_spx_pipeline.py --tier PREOPEN offline: temporary
SPX stores, the snapshot endpoint mocked, the build stage stubbed, no waiting.

Covers: the EARLY-run dependency (missing T-1 chain / T-1 volume fails with
nothing written); a stale snapshot (yesterday's chain) is refused on every
attempt and nothing is written; stale-then-fresh writes on the fresh attempt
and builds spx_oi_daily through today; a truncated snapshot is refused; a fresh
snapshot still carrying yesterday's expiries (vendor purge lag) passes; one
root stale and one fresh writes neither; dry run writes nothing; the
freshness measure on stored pairs.

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
    """The next day's chain: ~45% of contracts change OI, today's expiry rolls in,
    contracts expiring before `day` drop out unless keep_expired (purge lag)."""
    c = chain.copy()
    c["trade_date"] = day
    if not keep_expired:
        c = c[c["expiration"] >= day]
    chg = RNG.random(len(c)) < 0.45
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
    """Snapshot endpoint: per attempt, per root, a frame (or a callable)."""
    def __init__(self, script):
        self.script, self.calls = script, 0

    def __call__(self, root, today):
        attempt = self.calls // 2
        self.calls += 1
        df = self.script[min(attempt, len(self.script) - 1)][root]
        return df.assign(trade_date=today)


def run(script, dry=False):
    builds = []
    mock = Mock(script)
    real = R._spx

    def patched():
        c, fs, store, fr, vs, _, _ = real()
        return c, fs, store, fr, vs, mock, (lambda: True)
    R._spx = patched
    R.run_stage = lambda i, n, name, argv, what: (builds.append(argv) or (0, 0.0))
    R.time.sleep = lambda s: None
    try:
        rc = R.run_preopen(TODAY, dry)
    finally:
        R._spx = real
    return rc, builds, mock.calls // 2


def today_rows(root):
    return FR.stored_chain(config.SPX_OI_RAW_DIR, root, TODAY)


def main() -> int:
    print("dependency on the EARLY run")
    reset_store(with_prev=False)
    rc, builds, n = run([{}])
    ok(rc == 1 and n == 0 and not builds and today_rows("SPXW").empty,
       "T-1 chain missing -> exit 1, no snapshot requested, nothing written")
    reset_store(with_prev_vol=False)
    rc, builds, n = run([{}])
    ok(rc == 1 and n == 0 and today_rows("SPX").empty, "T-1 volume missing -> exit 1, nothing requested")

    print("\nstale every attempt")
    ch = reset_store()
    stale = {r: ch[r][1] for r in ch}                       # yesterday's chain, as the endpoint would serve it
    rc, builds, n = run([stale])
    ok(rc == 1 and n == R.POLL_ATTEMPTS and not builds and today_rows("SPXW").empty,
       f"refused on all {R.POLL_ATTEMPTS} attempts, nothing written, no build")

    print("\nstale, stale, then fresh (fresh SPXW keeps yesterday's expiries: purge lag)")
    ch = reset_store()
    stale = {r: ch[r][1] for r in ch}
    fresh = {r: evolve(ch[r][1], TODAY, keep_expired=True) for r in ch}
    ok((fresh["SPXW"]["expiration"] < TODAY).any(), "the fresh SPXW snapshot carries contracts that expired yesterday")
    rc, builds, n = run([stale, stale, fresh])
    ok(rc == 0 and n == 3, f"written on attempt 3 (rc {rc}, attempts {n})")
    for r in ch:
        back = today_rows(r)
        ok(len(back) == len(fresh[r]), f"{r}: {len(back):,} rows labelled {TODAY} in the store")
    ok(len(builds) == 1 and builds[0][0].endswith("build_spx_features.py") and builds[0][-1] == "20261117",
       f"spx_oi_daily built through today: {builds[0][1:] if builds else None}")
    v = FR.assess("SPXW", fresh["SPXW"], ch["SPXW"][1], ch["SPXW"][0], TODAY, PREV)
    ok(0.4 < v.identical_share < 0.7, f"fresh identical share {v.identical_share:.1%} (calibrated 40-60%)")

    print("\ntruncated snapshot")
    ch = reset_store()
    cut = {r: evolve(ch[r][1], TODAY).sample(frac=0.3, random_state=1) for r in ch}
    rc, builds, n = run([cut])
    ok(rc == 1 and today_rows("SPXW").empty, "30% of the chain -> shared count out of band, nothing written")

    print("\none root stale, the other fresh")
    ch = reset_store()
    mixed = {"SPX": evolve(ch["SPX"][1], TODAY), "SPXW": ch["SPXW"][1]}
    rc, builds, n = run([mixed])
    ok(rc == 1 and today_rows("SPX").empty and today_rows("SPXW").empty, "neither root written")

    print("\ndry run")
    ch = reset_store()
    rc, builds, n = run([{r: evolve(ch[r][1], TODAY) for r in ch}], dry=True)
    ok(rc == 0 and n == 1 and not builds and today_rows("SPX").empty, "one assessment, nothing written or built")

    print("\nmeasure on stored pairs")
    m = FR.measure(config.SPX_OI_RAW_DIR, "SPXW", [PREV2, PREV])
    ok(len(m) == 1 and 0.4 < m["identical_share"].iloc[0] < 0.7, f"SPXW pair share {m['identical_share'].iloc[0]:.1%}")

    shutil.rmtree(TMP, ignore_errors=True)
    print(f"\n{'ALL PASS' if not FAILS else f'{len(FAILS)} FAILURE(S)'}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
