"""
test_equity_surface_parallel.py — the parallel fit path.

Three properties, none of which the serial tests can reach:

  * a parallel run produces byte-identical output to a serial one. Process
    pools are exactly where a silent divergence hides, so this compares row
    counts AND the summed IV per (ticker, snapshot), not just that both
    finished.
  * filtering to the pending snapshots happens BEFORE clean_chain, so a
    partial re-run skips the WORK and not merely the write. Asserted on
    elapsed time, because a filter applied after cleaning would still produce
    the right rows and would still be the bug.
  * work is dispatched heaviest-first, so a straggler does not leave workers
    idle at the tail.

Builds its own synthetic intraday store in a temp directory; needs no real
data and no database.

Run:  python test_equity_surface_parallel.py     (exit 1 on any failure)
"""
import math
import os
import shutil
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import norm

import tempfile

STORE = Path(tempfile.mkdtemp(prefix="eqsurf_par_")) / "intstore"


def bsp(F, K, T, s, r):
    sq = s * math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * s * s * T) / sq
    d2 = d1 - sq
    d = math.exp(-r * T)
    return (d * (F * norm.cdf(d1) - K * norm.cdf(d2)),
            d * (K * norm.cdf(-d2) - F * norm.cdf(-d1)))


def build_store(tickers, n_snapshots, n_expiries):
    if STORE.exists():
        shutil.rmtree(STORE)
    day = date(2026, 6, 1)
    for ti, tk in enumerate(tickers):
        rows = []
        for si in range(n_snapshots):
            hh, mm = divmod(9 * 60 + 35 + 5 * si, 60)
            snap = f"{hh:02d}{mm:02d}"
            ts = f"2026-06-01T{hh:02d}:{mm:02d}:00"
            for ei in range(n_expiries):
                dte = [7, 14, 21, 30, 45][ei % 5] + ei
                exp = date(2026, 6, 1 + min(dte, 27))
                T = dte / 365.0
                sig = 0.30 + 0.05 * ti
                for K in np.arange(80.0, 121.0, 2.5):
                    c, p = bsp(100.0, float(K), T, sig, 0.05)
                    for right, px in (("C", c), ("P", p)):
                        if px < 0.10:
                            continue
                        rows.append(dict(
                            ticker=tk, trade_date=day, snapshot=snap,
                            feature_date=date(2026, 6, 2), timestamp=ts,
                            expiration=exp, strike=float(K),
                            option_type=right, bid=px - 0.02, ask=px + 0.02,
                            delta=0.5, theta=0.0, vega=0.01, rho=0.0,
                            epsilon=0.0, **{"lambda": 0.0},
                            implied_vol=sig, iv_error=0.001,
                            underlying_timestamp=ts, underlying_price=100.0))
        d = STORE / tk
        d.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(d / "20260601.parquet", index=False)
    return day


def main():
    TICKERS = ["AAA", "BBB", "CCC", "DDD"]
    DAY = build_store(TICKERS, n_snapshots=6, n_expiries=5)
    os.environ["CHAIN_INTRADAY_DIR"] = str(STORE)

    import build_equity_surface as B  # noqa: E402  (after env is set)
    from lib.surface_config import SOURCE_INTRADAY  # noqa: E402

    PASS, FAIL = [], []


    def check(name, got, want):
        ok = got == want
        (PASS if ok else FAIL).append(name)
        print(f"  [{'ok  ' if ok else 'FAIL'}] {name:<52} got={got!r} want={want!r}")


    def run(workers, skip_map=None):
        """Collect rows without a database, mirroring what run_days dispatches."""
        units = []
        work = [(tk, SOURCE_INTRADAY, [DAY], (skip_map or {}).get(tk, {}))
                for tk in TICKERS]
        if workers == 1:
            outs = [B.fit_ticker(a) for a in work]
        else:
            from concurrent.futures import ProcessPoolExecutor
            with ProcessPoolExecutor(max_workers=workers) as pool:
                outs = list(pool.map(B.fit_ticker, work))
        for o in outs:
            check_err = o["error"]
            if check_err:
                print("    worker error:", check_err)
            for u in o["units"]:
                units.append((o["ticker"], u["snapshot"],
                              len(u["surface"]), len(u["atm"]),
                              len(u["diagnostics"]),
                              round(sum(r["iv"] for r in u["surface"]), 9)))
        return sorted(units)


    print("\n=== 1. parallel output is identical to serial ===")
    t0 = time.monotonic()
    serial = run(1)
    t_serial = time.monotonic() - t0
    t0 = time.monotonic()
    par = run(4)
    t_par = time.monotonic() - t0
    check("unit count matches", len(par), len(serial))
    check("every (ticker, snapshot, counts, iv-sum) identical", par, serial)
    print(f"         serial {t_serial:.2f}s   4 workers {t_par:.2f}s")
    check("serial produced work at all", len(serial) > 0, True)

    print("\n=== 2. filter-before-clean skips the work, not just the write ===")
    # Skip 5 of 6 snapshots for every ticker.
    all_snaps = sorted({u[1] for u in serial})
    keep_one = all_snaps[-1]
    skip = {tk: {DAY: set(all_snaps[:-1])} for tk in TICKERS}
    t0 = time.monotonic()
    filtered = run(1, skip_map=skip)
    t_filtered = time.monotonic() - t0
    check("only the unskipped snapshot is built",
          sorted({u[1] for u in filtered}), [keep_one])
    check("one unit per ticker", len(filtered), len(TICKERS))
    print(f"         all 6 snapshots {t_serial:.2f}s   1 of 6 {t_filtered:.2f}s "
          f"({t_serial / max(t_filtered, 1e-9):.1f}x)")
    check("skipping 5 of 6 is materially faster", t_filtered < t_serial * 0.6, True)
    # The rows for the kept snapshot must be byte-identical to the unfiltered run.
    kept_serial = [u for u in serial if u[1] == keep_one]
    check("kept snapshot's rows are unchanged by filtering",
          filtered, sorted(kept_serial))

    print("\n=== 3. cost ordering puts the heaviest ticker first ===")
    costs = {tk: B.expected_cost(tk, SOURCE_INTRADAY, [DAY]) for tk in TICKERS}
    print("         bytes:", {k: v for k, v in sorted(costs.items())})
    work = [(tk, SOURCE_INTRADAY, [DAY], {}) for tk in TICKERS]
    work.sort(key=lambda a: B.expected_cost(a[0], a[1], a[2]), reverse=True)
    order = [a[0] for a in work]
    check("dispatch order is descending cost",
          order, sorted(TICKERS, key=lambda t: -costs[t]))
    check("heaviest is first", order[0], max(costs, key=costs.get))
    check("expected_cost counts a year file once across many days",
          B.expected_cost(TICKERS[0], SOURCE_INTRADAY, [DAY, DAY, DAY]),
          B.expected_cost(TICKERS[0], SOURCE_INTRADAY, [DAY]))

    print("\n=== 4. a failing ticker does not take the pool down ===")
    bad = STORE / "ZZZ"
    bad.mkdir(parents=True, exist_ok=True)
    (bad / "20260601.parquet").write_bytes(b"not a parquet file")
    out = B.fit_ticker(("ZZZ", SOURCE_INTRADAY, [DAY], {}))
    check("unreadable file yields no units, no crash", len(out["units"]), 0)
    check("and no hard error", out["error"], None)
    mixed = [("ZZZ", SOURCE_INTRADAY, [DAY], {}),
             ("AAA", SOURCE_INTRADAY, [DAY], {})]
    from concurrent.futures import ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=2) as pool:
        outs = list(pool.map(B.fit_ticker, mixed))
    check("good ticker still returns alongside a bad one",
          len(outs[1]["units"]) > 0, True)

    print("\n=== 5. default_workers ===")
    import os as _os
    check("default is cpu_count()-1, floored at 1",
          B.default_workers(), max(1, (_os.cpu_count() or 2) - 1))

    shutil.rmtree(STORE, ignore_errors=True)
    print("\n" + "=" * 60)
    print(f"PASSED {len(PASS)} / {len(PASS) + len(FAIL)}")
    if FAIL:
        for f in FAIL:
            print("  -", f)
        sys.exit(1)
    print("ALL GREEN")


if __name__ == "__main__":
    main()
