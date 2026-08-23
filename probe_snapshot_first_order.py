"""
probe_snapshot_first_order.py — does /v3/option/snapshot/greeks/first_order
accept expiration=*?

    wildcard:        one request, whole chain
    per-expiration:  one request per listed expiration (what we do today)

Reports success, rows, distinct expirations, the field set, and wall time for
each, then the ratio.

WHY THIS MATTERS
----------------
fetch_chain_snapshots.py is built entirely around the claim, in its own
docstring, that "the first_order endpoint requires a specific expiration — it
rejects expiration=*". That is why it enumerates expirations per session and
issues one point query per (session, expiration): ~35 requests where one might
do. The claim is about the HISTORY variant. The SNAPSHOT variant is a
different endpoint and is referenced nowhere in this codebase.

There is precedent for the wildcard on this family:
/v3/option/snapshot/open_interest takes expiration=* and returns the whole
chain (lib/thetadata.py:286, used by fetch_oi_snapshot). So the rejection may
be specific to history/first_order rather than to first_order generally.

If the wildcard works here, the live intraday path collapses from ~35 requests
per ticker-snapshot to 1.

WHAT WOULD MAKE A "SUCCESS" MISLEADING
--------------------------------------
Three ways a 200 response is still not usable, all checked:

  * FEWER EXPIRATIONS than the chain lists. A wildcard that silently returns
    only the front month is not the whole chain. Compared against
    enumerate_expirations_eod.
  * MISSING FIELDS. build_smile_points weights the spline by `vega` and
    clean_chain derives gamma from `delta`; the smile needs `implied_vol` and
    moneyness needs `underlying_price`. An endpoint missing any of those is
    not a drop-in for first_order.
  * SLOWER THAN THE SUM of the per-expiration calls it replaces. One request
    that takes longer than 35 is not a win.

Usage:
    python probe_snapshot_first_order.py --tickers SPY,AAPL
    python probe_snapshot_first_order.py --tickers SPY --sample 10
"""
from __future__ import annotations

import argparse
import logging
import statistics
import sys
import time
from datetime import datetime

import pandas as pd

from lib.chain_fetch_common import log_path, setup_file_logging
from lib.market_hours import last_trading_day
from lib.thetadata import (
    NoDataError, _get_with_retry, _parse_csv_frame, _parse_frame,
    enumerate_expirations_eod, response_format, set_max_connections,
    test_connection,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("probe_snapshot_fo")

SNAP_FIRST_ORDER = "/v3/option/snapshot/greeks/first_order"

# What production reads off a first_order response.
REQUIRED = {
    "delta":            "clean_chain._add_gamma derives gamma from it",
    "vega":             "surface_fit.build_smile_points weights the spline",
    "implied_vol":      "the smile itself",
    "underlying_price": "moneyness and intrinsic in clean_chain",
}


def call(expiration, symbol: str, fmt: str, timeout: int = 120) -> dict:
    """One snapshot request. `expiration` may be a date or the string '*'."""
    exp = expiration if isinstance(expiration, str) \
        else expiration.strftime("%Y%m%d")
    params = {"symbol": symbol.upper(), "expiration": exp}
    label = f"snap_first_order {symbol} exp={exp}"
    t0 = time.monotonic()
    try:
        data = _get_with_retry(SNAP_FIRST_ORDER, params, timeout, label,
                               fmt=fmt)
        df = _parse_csv_frame(data) if fmt == "csv" else _parse_frame(data)
        err = None
    except NoDataError:
        df, err = pd.DataFrame(), "NoDataError (endpoint reachable, no rows)"
    except Exception as exc:                                  # noqa: BLE001
        df, err = pd.DataFrame(), f"{type(exc).__name__}: {exc}"
    return {"secs": time.monotonic() - t0, "df": df, "error": err}


def n_expirations(df: pd.DataFrame) -> int:
    for c in ("expiration", "exp", "expiry"):
        if c in df.columns:
            return int(pd.Series(df[c]).nunique())
    return -1


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Probe expiration=* on snapshot/greeks/first_order.")
    ap.add_argument("--tickers", default="SPY,AAPL")
    ap.add_argument("--sample", type=int, default=0,
                    help="per-expiration calls to time (0 = every listed "
                         "expiration, which is the honest comparison; a "
                         "smaller sample is extrapolated and labelled as such)")
    ap.add_argument("--format", dest="fmt", choices=["csv", "json"],
                    default=None, help="default: response_format() (csv)")
    args = ap.parse_args()

    log_file = setup_file_logging("probe_snapshot_first_order")
    print("=== expiration=* on /v3/option/snapshot/greeks/first_order ===")
    print(f"Log: {log_file}\n")
    log.info("argv: %s", " ".join(sys.argv[1:]))

    set_max_connections(1)          # latency, not queueing
    fmt = args.fmt or response_format()
    day = last_trading_day()
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    print("Checking ThetaData ...", end=" ", flush=True)
    if not test_connection():
        raise SystemExit("FAILED — terminal not reachable.")
    print(f"OK   (format={fmt}, enumeration session={day})\n")

    verdicts = []
    for tk in tickers:
        print("=" * 68)
        print(f"{tk}")
        print("=" * 68)

        try:
            listed = sorted(e for e in enumerate_expirations_eod(tk, day, day)
                            if e >= day)
        except Exception as exc:                              # noqa: BLE001
            log.error("  enumeration failed — %s", exc)
            listed = []
        print(f"  chain lists {len(listed)} expiration(s) on {day}")

        # --- the wildcard --------------------------------------------------
        w = call("*", tk, fmt)
        if w["error"]:
            print(f"\n  WILDCARD REJECTED — {w['error']}")
            print(f"  ({w['secs']:.2f}s to fail)")
            verdicts.append((tk, False, None, None))
            continue

        df = w["df"]
        n_exp = n_expirations(df)
        print(f"\n  WILDCARD OK      {w['secs']:>7.2f}s   {len(df):>8,} rows   "
              f"{n_exp if n_exp >= 0 else '?':>4} expirations")
        print(f"  fields ({len(df.columns)}): {', '.join(sorted(df.columns))}")

        missing = [c for c in REQUIRED if c not in df.columns]
        # 'iv' is the vendor's other spelling for implied_vol.
        if "implied_vol" in missing and "iv" in df.columns:
            missing.remove("implied_vol")
        for c in REQUIRED:
            present = (c in df.columns
                       or (c == "implied_vol" and "iv" in df.columns))
            print(f"    {c:<20}{'present' if present else 'MISSING':<9}"
                  f"{REQUIRED[c]}")

        covers = (n_exp >= len(listed) > 0) if n_exp >= 0 else None
        if listed and n_exp >= 0:
            print(f"\n  coverage: {n_exp} returned vs {len(listed)} listed"
                  f"  -> {'FULL CHAIN' if covers else 'PARTIAL — not a drop-in'}")

        # --- per-expiration, for comparison --------------------------------
        targets = listed if args.sample <= 0 else listed[:args.sample]
        if not targets:
            print("\n  no expirations to time per-expiration against")
            verdicts.append((tk, True, w["secs"], None))
            continue
        print(f"\n  timing {len(targets)} per-expiration call(s)"
              f"{' (sampled)' if args.sample > 0 else ''} ...")
        per = []
        for i, e in enumerate(targets, 1):
            r = call(e, tk, fmt)
            if r["error"] is None:
                per.append(r["secs"])
            if i % 10 == 0 or i == len(targets):
                print(f"    {i}/{len(targets)}")
        if not per:
            print("  every per-expiration call failed")
            verdicts.append((tk, True, w["secs"], None))
            continue

        mean_one = statistics.mean(per)
        measured = sum(per)
        full = mean_one * len(listed)
        print(f"\n  per-expiration   mean {mean_one:>6.2f}s   "
              f"median {statistics.median(per):>6.2f}s   n={len(per)}")
        print(f"  measured total   {measured:>7.2f}s over {len(per)} call(s)")
        if args.sample > 0 and len(listed) > len(targets):
            print(f"  full chain       {full:>7.2f}s  (EXTRAPOLATED from the "
                  f"sample x {len(listed)} expirations)")
        print(f"\n  wildcard {w['secs']:.2f}s  vs  full chain {full:.2f}s"
              f"   -> {full / max(w['secs'], 1e-9):.1f}x")
        verdicts.append((tk, True, w["secs"], full))

    print("\n" + "=" * 68)
    print("VERDICT")
    print("=" * 68)
    for tk, ok, wc, full in verdicts:
        if not ok:
            print(f"  {tk:<6} wildcard REJECTED — per-expiration stays the "
                  f"only option")
        elif full is None:
            print(f"  {tk:<6} wildcard OK ({wc:.2f}s), no per-expiration "
                  f"baseline")
        else:
            print(f"  {tk:<6} wildcard {wc:.2f}s vs {full:.2f}s "
                  f"({full / max(wc, 1e-9):.1f}x)")
    if any(ok for _, ok, _, _ in verdicts):
        print("\n  A wildcard that succeeds is only a drop-in if it also "
              "returned the")
        print("  FULL expiration set and every required field above. Check "
              "both before")
        print("  changing fetch_chain_snapshots — its per-expiration design "
              "exists")
        print("  because the HISTORY variant rejects the wildcard.")
    print(f"\nLog: {log_path()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
