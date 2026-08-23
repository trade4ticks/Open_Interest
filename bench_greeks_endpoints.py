"""
bench_greeks_endpoints.py — first_order vs implied_volatility, head to head.

    /v3/option/history/greeks/first_order          (what the fetchers use now)
    /v3/option/history/greeks/implied_volatility   (unmeasured)

Same tickers, same expirations, same session, same time window, same interval.
Reports mean / median / p90 wall time per request.

WHY THIS IS NOT ONLY A LATENCY QUESTION
---------------------------------------
The decision it feeds is "do we compute greeks ourselves", and latency alone
cannot answer that. Two production consumers read vendor greek columns:

    lib/surface_fit.build_smile_points   uses `vega` to weight the spline.
                                         Without it the weighting falls back
                                         to w * (spread/mid), which is a
                                         different — and untested — fit.
    lib/clean_chain._add_gamma           derives gamma as d(delta)/d(strike).
                                         Without vendor `delta` that column is
                                         NaN for every row.

So the benchmark also reports WHICH FIELDS each endpoint returns and names
what is lost by switching. A 2x latency win that silently empties the gamma
column and changes every spline weight is not a win.

It also reports rows and payload bytes, because a latency difference that is
purely payload size tells you the endpoints are returning different universes,
not that one is faster at the same work.

METHOD
------
Requests are INTERLEAVED and the leading endpoint alternates, so terminal
warm-up, cache state and any drift over the run hit both endpoints equally.
A naive "20 of A then 20 of B" would attribute all of that to whichever ran
second. Warm-up requests are discarded. Concurrency is pinned to 1: this is a
per-request latency measurement, and parallel requests would measure the
terminal's queueing instead.

Usage:
    python bench_greeks_endpoints.py --tickers SPY,AAPL --date 20260601
    python bench_greeks_endpoints.py --tickers SPY --date 20260601 --pairs 30
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
from lib.thetadata import (
    SNAPSHOT_TIMING, NoDataError, ThetaDataError, _get_with_retry,
    _parse_csv_frame, _parse_frame, describe_retry_policy,
    enumerate_expirations_eod, response_format, set_max_connections,
    test_connection,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bench_greeks")

FIRST_ORDER = "/v3/option/history/greeks/first_order"
IMPLIED_VOL = "/v3/option/history/greeks/implied_volatility"

# Columns production actually consumes from the vendor. If an endpoint omits
# one of these, switching to it has a cost beyond latency.
CONSUMED = {
    "implied_vol": "the smile itself (surface_fit.build_smile_points)",
    "iv":          "alias for implied_vol",
    "vega":        "spline weighting (surface_fit.build_smile_points)",
    "delta":       "gamma derivation (clean_chain._add_gamma)",
    "bid":         "mid_price / spread (clean_chain)",
    "ask":         "mid_price / spread (clean_chain)",
    "underlying_price": "moneyness, intrinsic (clean_chain)",
}


def call(endpoint: str, symbol: str, expiration, day,
         start_time: str, end_time: str, interval: str,
         fmt: str) -> dict:
    """One request. Returns timing, rows, bytes and the field set.

    `fmt` is threaded through explicitly and defaults to response_format(),
    which is "csv". Measuring the JSON path while production runs CSV would
    benchmark a code path nothing uses — the CSV reader parses in pyarrow's
    C++ and releases the GIL, so the two are not interchangeable for timing.
    """
    date_str = day.strftime("%Y%m%d")
    params = {
        "symbol":     symbol.upper(),
        "expiration": expiration.strftime("%Y%m%d"),
        "start_date": date_str,
        "end_date":   date_str,
        "interval":   interval,
        "start_time": start_time,
        "end_time":   end_time,
    }
    label = f"{endpoint.rsplit('/', 1)[-1]} {symbol} {expiration} {day}"
    bytes_before = SNAPSHOT_TIMING["http_bytes"]
    t0 = time.monotonic()
    try:
        data = _get_with_retry(endpoint, params, 60, label, fmt=fmt)
        # Same branch production takes: CSV comes back as undecoded bytes for
        # pyarrow's reader, JSON as a parsed dict.
        df = _parse_csv_frame(data) if fmt == "csv" else _parse_frame(data)
        err = None
    except NoDataError:
        df, err = pd.DataFrame(), None
    except Exception as exc:                              # noqa: BLE001
        df, err = pd.DataFrame(), f"{type(exc).__name__}: {exc}"
    secs = time.monotonic() - t0
    nbytes = SNAPSHOT_TIMING["http_bytes"] - bytes_before
    return {"secs": secs, "rows": len(df), "bytes": nbytes,
            "cols": sorted(df.columns) if not df.empty else [],
            "error": err}


def summarise(name: str, samples: list) -> dict:
    ok = [s for s in samples if s["error"] is None]
    times = [s["secs"] for s in ok]
    if not times:
        return {"name": name, "n": 0}
    times_sorted = sorted(times)
    p90 = times_sorted[min(len(times_sorted) - 1,
                           int(round(0.90 * (len(times_sorted) - 1))))]
    return {
        "name": name,
        "n": len(ok),
        "failed": len(samples) - len(ok),
        "mean": statistics.mean(times),
        "median": statistics.median(times),
        "p90": p90,
        "min": min(times),
        "max": max(times),
        "rows": statistics.mean([s["rows"] for s in ok]),
        "mb": statistics.mean([s["bytes"] for s in ok]) / 1e6,
        "empty": sum(1 for s in ok if s["rows"] == 0),
    }


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Benchmark first_order vs implied_volatility.")
    ap.add_argument("--tickers", default="SPY,AAPL",
                    help="comma-separated (default SPY,AAPL)")
    ap.add_argument("--date", required=True, help="YYYYMMDD session to sample")
    ap.add_argument("--pairs", type=int, default=24,
                    help="(ticker, expiration) pairs to sample; each is one "
                         "request PER endpoint, so 24 gives 48 requests "
                         "(default 24, minimum 20 after warm-up)")
    ap.add_argument("--warmup", type=int, default=2,
                    help="leading pairs discarded from the statistics")
    ap.add_argument("--start-time", default="09:35:00")
    ap.add_argument("--end-time", default="16:00:00")
    ap.add_argument("--interval", default="5m")
    ap.add_argument("--format", dest="fmt",
                    choices=["csv", "json", "both"], default=None,
                    help="response format (default: whatever "
                         "lib.thetadata.response_format() returns, currently "
                         "csv). 'both' measures each endpoint twice, which is "
                         "how to confirm the CSV path's win independently of "
                         "the endpoint question.")
    args = ap.parse_args()

    log_file = setup_file_logging("bench_greeks_endpoints")
    print("=== first_order vs implied_volatility ===")
    print(f"Log: {log_file}\n")
    log.info("argv: %s", " ".join(sys.argv[1:]))

    # Sequential on purpose: this measures per-request latency, and parallel
    # requests would measure the terminal's queueing instead.
    set_max_connections(1)

    formats = (["csv", "json"] if args.fmt == "both"
               else [args.fmt or response_format()])
    day = datetime.strptime(args.date, "%Y%m%d").date()
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    print("Checking ThetaData ...", end=" ", flush=True)
    if not test_connection():
        raise SystemExit("FAILED — terminal not reachable.")
    print("OK")
    print(f"Retry: {describe_retry_policy()}\n")

    # Same expirations for both endpoints, enumerated once.
    pairs = []
    for tk in tickers:
        try:
            exps = sorted(e for e in enumerate_expirations_eod(tk, day, day)
                          if e >= day)
        except Exception as exc:                          # noqa: BLE001
            log.error("  %s: enumeration failed — %s", tk, exc)
            continue
        if not exps:
            log.warning("  %s: no expirations listed on %s", tk, day)
            continue
        # Spread across the term structure rather than taking the front N,
        # so the sample is not all weeklies.
        step = max(1, len(exps) // max(1, args.pairs // max(1, len(tickers))))
        pairs.extend((tk, e) for e in exps[::step])
    pairs = pairs[:args.pairs]
    if len(pairs) < args.warmup + 20:
        raise SystemExit(
            f"only {len(pairs)} (ticker, expiration) pair(s) available; "
            f"need {args.warmup + 20} for 20 measured requests per endpoint. "
            f"Add tickers or raise --pairs.")

    n_req = len(pairs) * 2 * len(formats)
    print(f"{len(pairs)} pair(s) x 2 endpoints x {len(formats)} format(s) "
          f"= {n_req} requests")
    print(f"format(s): {', '.join(formats)}")
    print(f"window {args.start_time}..{args.end_time} @{args.interval}, "
          f"session {day}")
    print(f"discarding the first {args.warmup} pair(s) as warm-up\n")

    buckets = {(e, f): [] for e in (FIRST_ORDER, IMPLIED_VOL)
               for f in formats}
    for i, (tk, exp) in enumerate(pairs):
        # Alternate which endpoint leads, so warm cache and drift are shared.
        combos = [(e, f) for e in (FIRST_ORDER, IMPLIED_VOL) for f in formats]
        if i % 2:
            combos.reverse()
        for endpoint, fmt in combos:
            r = call(endpoint, tk, exp, day, args.start_time, args.end_time,
                     args.interval, fmt)
            r["ticker"], r["expiration"] = tk, exp
            buckets[(endpoint, fmt)].append(r)
        if i == args.warmup - 1:
            for k in buckets:
                buckets[k] = []      # drop warm-up
        done = i + 1
        if done % 5 == 0 or done == len(pairs):
            print(f"  {done}/{len(pairs)} pairs")

    prod_fmt = formats[0]
    fo = buckets[(FIRST_ORDER, prod_fmt)]
    iv = buckets[(IMPLIED_VOL, prod_fmt)]
    s_fo, s_iv = summarise("first_order", fo), summarise("implied_volatility", iv)

    print("\n" + "=" * 68)
    print("WALL TIME PER REQUEST")
    print("=" * 68)
    print(f"  {'endpoint':<24}{'n':>4}{'mean':>9}{'median':>9}{'p90':>9}"
          f"{'min':>8}{'max':>8}")
    for s in (s_fo, s_iv):
        if not s.get("n"):
            print(f"  {s['name']:<24}{0:>4}   no successful requests")
            continue
        print(f"  {s['name']:<24}{s['n']:>4}{s['mean']:>9.3f}"
              f"{s['median']:>9.3f}{s['p90']:>9.3f}{s['min']:>8.3f}"
              f"{s['max']:>8.3f}")
    if len(formats) > 1:
        print("\n  by response format:")
        for (endpoint, fmt), rows_ in sorted(buckets.items()):
            st = summarise(f"{endpoint.rsplit('/', 1)[-1]} [{fmt}]", rows_)
            if st.get("n"):
                print(f"    {st['name']:<34}{st['n']:>4}"
                      f"{st['mean']:>9.3f}{st['median']:>9.3f}")

    if s_fo.get("n") and s_iv.get("n"):
        print(f"\n  median ratio  first_order / implied_volatility = "
              f"{s_fo['median'] / max(s_iv['median'], 1e-9):.2f}x")
        print(f"  mean   ratio                                     = "
              f"{s_fo['mean'] / max(s_iv['mean'], 1e-9):.2f}x")

    print("\n" + "=" * 68)
    print("PAYLOAD — a latency gap that is just payload means the two are")
    print("returning different universes, not doing the same work faster.")
    print("=" * 68)
    for s in (s_fo, s_iv):
        if s.get("n"):
            print(f"  {s['name']:<24}{s['rows']:>10.0f} rows"
                  f"{s['mb']:>9.2f} MB   empty responses: {s['empty']}")

    print("\n" + "=" * 68)
    print("FIELDS — what switching would cost")
    print("=" * 68)
    cols_fo = set().union(*[set(s["cols"]) for s in fo if s["cols"]] or [set()])
    cols_iv = set().union(*[set(s["cols"]) for s in iv if s["cols"]] or [set()])
    print(f"  first_order        ({len(cols_fo)}): {', '.join(sorted(cols_fo))}")
    print(f"  implied_volatility ({len(cols_iv)}): {', '.join(sorted(cols_iv))}")
    lost = sorted(cols_fo - cols_iv)
    print(f"\n  present in first_order, ABSENT from implied_volatility: "
          f"{', '.join(lost) if lost else '(none)'}")
    breaks = [c for c in lost if c in CONSUMED]
    if breaks:
        print("\n  Of those, production reads:")
        for c in breaks:
            print(f"    {c:<20}{CONSUMED[c]}")
        print("\n  VERDICT: switching is NOT a drop-in. Each field above needs")
        print("  either a replacement source or a computed substitute, and the")
        print("  substitute has to be validated against the current output.")
    else:
        print("\n  VERDICT: implied_volatility carries every field production")
        print("  reads, so the switch is a drop-in and the latency numbers")
        print("  above decide it on their own.")

    errs = [s for s in fo + iv if s["error"]]
    if errs:
        print(f"\n  {len(errs)} request(s) FAILED. First few:")
        for e in errs[:5]:
            print(f"    {e['ticker']} {e['expiration']}: {e['error'][:70]}")
        if all(e["error"] for e in iv):
            print("\n  Every implied_volatility request failed — the history "
                  "variant of")
            print("  that endpoint may not exist. lib/thetadata.py only knows "
                  "the SNAPSHOT")
            print("  form (/v3/option/snapshot/greeks/implied_volatility).")

    print(f"\nLog: {log_path()}")
    return 0 if (s_fo.get("n") and s_iv.get("n")) else 1


if __name__ == "__main__":
    sys.exit(main())
