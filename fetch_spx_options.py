"""
fetch_spx_options.py — raw open interest and trade-based EOD option volume for
the SPX and SPXW index-option roots.

    python fetch_spx_options.py fetch --start 20200102 --end 20201231
    python fetch_spx_options.py fetch --start 20200102 --end 20201231 --data oi
    python fetch_spx_options.py fetch --start 20261030 --end 20261030 --data oi --oi-snapshot-today
    python fetch_spx_options.py probe --date 20261026          # endpoint check, writes nothing

Then: python verify_spx_fetch.py --start ... --end ...

NO NEW OI FETCH LOGIC
---------------------
OI goes through fetch_oi.fetch_ticker unchanged — the same call
run_pipeline.py makes for the 120 tickers. The only difference is the
destination: _redirect_oi_store() re-points lib.parquet_store.OI_RAW_DIR (the
global its writer reads at call time) at SPX_OI_RAW_DIR, so every write lands
in {SPX_OI_RAW_DIR}/{root}/{year}.parquet. _assert_oi_target() re-checks the
writer's resolved path before every OI fetch call, and
_assert_prod_tree_clean() fails the run if SPX/ or SPXW/ ever appears under
the production OI_RAW_DIR, before and after the fetch. The root is the directory name, as the ticker is
there; it is materialised as a column at read time.

SPX cannot simply be added to OI_RAW_DIR: that directory's listing IS the
120-ticker universe (lib.parquet_store.list_tickers), and anything in it is
picked up by the chain_eod greeks fetch, the chain snapshot / intraday / live
fetchers, equity_1min and trade_paths.

VOLUME
------
The 120-ticker pipeline takes volume from the EOD greeks endpoint (chain_eod),
which is out of scope here. Volume comes from the trade-based
/v3/option/history/eod endpoint via lib.thetadata.fetch_volume_eod, unchanged.
That function's only existing caller (fetch_volume_eod.py) aggregates against
a spot price and writes Postgres; nothing stored the raw rows, so they get a
new store: lib/spx_volume_store.py. Only volume and trade count are kept;
the feed's bid / ask / OHLC are end-of-day quotes and are not stored.

Each request covers ONE session (start_date = end_date = day) and every row is
stamped with that day, exactly as fetch_oi pins OI rows to the requested day.
fetch_volume_eod otherwise dates rows from the `last_trade`/`created`
timestamp, which over a multi-day range is ambiguous for any contract whose
last trade was not on the day of the report.

DATES
-----
Both stores keep the vendor's date: OI as ThetaData labels it (trade_date = T,
published ~06:30 ET on T, position as of T-1's close) and volume as the session
it traded in. Mapping session T-1 volume onto row T is the build's job.

IDEMPOTENCY
-----------
Work is done one calendar month at a time per root, and each month is written
before the next is fetched, so an interrupted backfill keeps what it finished.
OI uses lib.parquet_store's keep-last merge; volume replaces whole trade_dates.
Re-running any range converges to the same files. A requested trading day
with no rows in the store afterwards is reported as MISSING and the exit code
is 1 — both roots carry OI and volume every session, so a gap is a failure.
"""
from __future__ import annotations

# --- Destination redirect: must run before anything imports lib.parquet_store.
import argparse
import logging
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as dtime, timedelta
from itertools import groupby

import pandas as pd
import pyarrow.parquet as pq

import config
import fetch_oi
import fetch_oi_snapshot
import lib.parquet_store as oi_store
import lib.spx_volume_store as vol_store
from lib.market_hours import get_trading_days, last_trading_day
from lib.thetadata import (
    BadRequestError,
    TerminalServerError,
    TerminalTimeoutError,
    ThetaDataError,
    _ET,
    _get,
    _parse_rows,
    fetch_volume_eod,
    test_connection,
    today_et,
)

log = logging.getLogger(__name__)

# The production OI tree, captured before anything redirects it.
_PROD_OI_RAW_DIR = config.OI_RAW_DIR

MAX_WORKERS = 4
# A full SPXW chain for one session is ~20-40k rows; the default 60s is
# comfortable for that but not for a slow terminal on a busy evening.
VOL_TIMEOUT = 120
# The EOD report is generated ~17:15 ET; before that the current session's
# request is refused (HTTP 400).
EOD_READY_ET = dtime(17, 30)
# OI for T is published ~06:30 ET on T.
OI_READY_ET = dtime(7, 0)


# --- Helpers ---------------------------------------------------------------

def _parse_day(s: str) -> date:
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    raise argparse.ArgumentTypeError(f"bad date {s!r}; use YYYYMMDD")


def _months(days: list[date]) -> list[list[date]]:
    return [list(g) for _, g in groupby(days, key=lambda d: (d.year, d.month))]


def _dates_in_store(root_dir, start: date, end: date) -> set[date]:
    """trade_dates present in {root_dir}/{year}.parquet within [start, end].
    Reads only the trade_date column."""
    out: set[date] = set()
    for y in range(start.year, end.year + 1):
        p = root_dir / f"{y}.parquet"
        if not p.exists():
            continue
        col = pq.read_table(p, columns=["trade_date"]).column("trade_date").to_pylist()
        out.update(d for d in col if start <= d <= end)
    return out


# --- OI destination redirect -------------------------------------------------
#
# lib.parquet_store does `from config import OI_RAW_DIR`, which copies the
# path into its own namespace at import time; ticker_dir() — and through it
# every write — reads that module global at CALL time. So the redirect sets
# lib.parquet_store.OI_RAW_DIR directly, at run time, after every import has
# happened. That makes it independent of import order: re-pointing
# config.OI_RAW_DIR alone is a no-op whenever lib.parquet_store was imported
# first, and an import tidy-up must not be able to change where OI lands.

def _redirect_oi_store() -> None:
    for name, d in (("SPX_OI_RAW_DIR", config.SPX_OI_RAW_DIR),
                    ("SPX_VOLUME_DIR", config.SPX_VOLUME_DIR)):
        if d == _PROD_OI_RAW_DIR or _PROD_OI_RAW_DIR in d.parents:
            raise SystemExit(f"{name}={d} is inside OI_RAW_DIR={_PROD_OI_RAW_DIR}; "
                             "it would join the 120-ticker universe. Refusing.")
    config.OI_RAW_DIR = config.SPX_OI_RAW_DIR
    oi_store.OI_RAW_DIR = config.SPX_OI_RAW_DIR


def _assert_oi_target(root: str) -> None:
    """Abort unless an OI write for `root` would land under SPX_OI_RAW_DIR.
    Checks the path the writer will actually use, not just config."""
    spx = config.SPX_OI_RAW_DIR
    problems = []
    if config.OI_RAW_DIR != spx:
        problems.append(f"config.OI_RAW_DIR={config.OI_RAW_DIR}")
    if oi_store.OI_RAW_DIR != spx:
        problems.append(f"lib.parquet_store.OI_RAW_DIR={oi_store.OI_RAW_DIR}")
    target = oi_store.ticker_dir(root)
    if target.parent != spx:
        problems.append(f"lib.parquet_store.ticker_dir({root!r})={target}")
    if problems:
        raise SystemExit("OI redirect not in effect — refusing to fetch. Expected "
                         f"SPX_OI_RAW_DIR={spx}, got: " + "; ".join(problems)
                         + f". Production OI_RAW_DIR is {_PROD_OI_RAW_DIR}.")


def _assert_prod_tree_clean() -> None:
    """The failure signature is an SPX/ or SPXW/ directory in the production
    OI tree. Checks only those names; never walks the 120-ticker tree."""
    leaked = [_PROD_OI_RAW_DIR / r for r in config.SPX_ROOTS
              if (_PROD_OI_RAW_DIR / r).exists()]
    if leaked:
        quarantine = (_PROD_OI_RAW_DIR.parent
                      / f"spx_leak_quarantine_{datetime.now():%Y%m%d_%H%M%S}")
        moves = "\n".join(f"  mv '{p}' '{quarantine}/'" for p in leaked)
        raise SystemExit(
            "SPX data in the PRODUCTION OI tree: "
            + ", ".join(map(str, leaked))
            + ".\nThese directories enrol SPX into the 120-ticker pipeline "
            "(chain_eod greeks fetch included). Before the next cron run, MOVE "
            "them out of the tree — do not delete them; keep what was written "
            "so it can be inspected:\n"
            f"  mkdir -p '{quarantine}'\n{moves}")


# --- Volume ----------------------------------------------------------------

def _fetch_volume_day(root: str, day: date) -> tuple[pd.DataFrame, int]:
    """One session's traded contracts for one root, stamped `day`.
    Returns (rows with volume > 0, count of traded rows the vendor dated
    differently from the requested day)."""
    raw = fetch_volume_eod(root, day, day, timeout=VOL_TIMEOUT)
    if raw.empty:
        return raw, 0
    raw = raw[raw["volume"] > 0].rename(columns={"count": "trade_count"})
    if "trade_count" not in raw.columns:
        raw["trade_count"] = pd.NA
    n_off = int((raw["trade_date"] != day).sum())
    raw["trade_date"] = day
    n = len(raw)
    raw = raw.groupby(vol_store.KEYS, as_index=False)[["volume", "trade_count"]].max()
    if len(raw) < n:
        log.warning("  DEDUP   %s %s volume: %d -> %d rows (max kept)",
                    root, day, n, len(raw))
    return raw, n_off


def fetch_volume_month(root: str, days: list[date]) -> int:
    frames: list[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_volume_day, root, d): d for d in days}
        for fut in as_completed(futures):
            d = futures[fut]
            try:
                df, n_off = fut.result()
            except (TerminalTimeoutError, TerminalServerError,
                    BadRequestError, ThetaDataError) as exc:
                log.warning("  FAIL    %s %s volume: %s", root, d, exc)
                continue
            if n_off:
                log.warning("  DATED   %s %s volume: %d traded rows carried a "
                            "different vendor date; stamped %s", root, d, n_off, d)
            if not df.empty:
                frames.append(df)
    if not frames:
        return 0
    combined = pd.concat(frames, ignore_index=True)
    vol_store.write_rows(root, combined)
    return len(combined)


# --- fetch command ---------------------------------------------------------

def cmd_fetch(args) -> int:
    _redirect_oi_store()
    _assert_prod_tree_clean()
    now_et = datetime.now(_ET)
    t_et = today_et()

    print("=== SPX / SPXW fetch ===")
    print(f"  roots          : {', '.join(args.roots)}")
    print(f"  data           : {', '.join(args.data)}")
    print(f"  SPX_OI_RAW_DIR : {config.SPX_OI_RAW_DIR}")
    print(f"  SPX_VOLUME_DIR : {config.SPX_VOLUME_DIR}")
    print(f"  (production OI_RAW_DIR {_PROD_OI_RAW_DIR} is not written)")

    # OI history for today's date is refused by the wildcard endpoint; today's
    # OI comes only from --oi-snapshot-today.
    oi_end = min(args.end, last_trading_day(t_et - timedelta(days=1)))
    vol_end = min(args.end, last_trading_day(t_et))
    if vol_end == t_et and now_et.time() < EOD_READY_ET:
        vol_end = last_trading_day(t_et - timedelta(days=1))

    print("Checking ThetaData ...", end=" ", flush=True)
    if not test_connection():
        print("FAILED — terminal not reachable.")
        return 2
    print("OK\n")

    # (data, root) -> [requested days, rows fetched]
    requested: dict[tuple[str, str], list[date]] = {}
    fetched: dict[tuple[str, str], int] = defaultdict(int)

    if "oi" in args.data:
        days = get_trading_days(args.start, oi_end) if oi_end >= args.start else []
        if oi_end < args.end:
            print(f"OI history capped at {oi_end} (the history endpoint has no "
                  "row for today yet)")
        for root in args.roots:
            requested[("oi", root)] = days
            for month in _months(days):
                print(f"--- OI {root} {month[0]:%Y-%m} ({len(month)} days) ---")
                _assert_oi_target(root)
                fetched[("oi", root)] += fetch_oi.fetch_ticker(root, month)

    if "oi" in args.data and args.oi_snapshot_today:
        if last_trading_day(t_et) != t_et or now_et.time() < OI_READY_ET:
            print(f"--oi-snapshot-today skipped: {t_et} is not a trading day or "
                  f"it is before {OI_READY_ET:%H:%M} ET")
        elif args.start <= t_et <= args.end:
            for root in args.roots:
                print(f"--- OI {root} snapshot, trade_date {t_et} ---")
                _assert_oi_target(root)
                n = fetch_oi_snapshot.fetch_ticker(root, t_et)
                fetched[("oi", root)] += n
                requested[("oi", root)] = requested.get(("oi", root), []) + [t_et]
        else:
            print(f"--oi-snapshot-today skipped: {t_et} is outside --start/--end")

    if "volume" in args.data:
        days = get_trading_days(args.start, vol_end) if vol_end >= args.start else []
        if vol_end < args.end:
            print(f"Volume capped at {vol_end} (last session with a generated "
                  "EOD report)")
        for root in args.roots:
            requested[("volume", root)] = days
            for month in _months(days):
                print(f"--- volume {root} {month[0]:%Y-%m} ({len(month)} days) ---")
                fetched[("volume", root)] += fetch_volume_month(root, month)

    # Where did the OI go? Both halves fail loudly.
    _assert_prod_tree_clean()
    for (data, root), days in requested.items():
        if data == "oi" and days and not any(oi_store.ticker_dir(root).glob("*.parquet")):
            raise SystemExit(f"OI fetched for {root} but no parquet under "
                             f"{config.SPX_OI_RAW_DIR / root}.")

    # Report from the store, not from the fetch: what matters is what landed.
    print("\n=== Summary (read back from the stores) ===")
    print(f"{'data':<7}{'root':<6}{'span requested':<26}{'days':>6}"
          f"{'present':>9}{'rows fetched':>14}")
    any_missing = False
    for (data, root), days in requested.items():
        if not days:
            print(f"{data:<7}{root:<6}{'(no trading days in range)':<26}")
            continue
        store_root = (oi_store.ticker_dir(root) if data == "oi"
                      else vol_store.root_dir(root))
        present = _dates_in_store(store_root, min(days), max(days))
        missing = sorted(set(days) - present)
        span = f"{min(days)} → {max(days)}"
        print(f"{data:<7}{root:<6}{span:<26}{len(days):>6}"
              f"{len(set(days) & present):>9}{fetched[(data, root)]:>14,}")
        if missing:
            any_missing = True
            shown = ", ".join(str(d) for d in missing[:20])
            more = f" … (+{len(missing) - 20})" if len(missing) > 20 else ""
            print(f"    MISSING {len(missing)}: {shown}{more}")

    if any_missing:
        print("\nSome requested days have no rows. Re-run the same range to retry.")
        return 1
    print("\nAll requested days present.")
    return 0


# --- probe command ---------------------------------------------------------

def _probe(label: str, endpoint: str, params: dict, day: date) -> None:
    print(f"\n--- {label}: {endpoint} {params}")
    try:
        data = _get(endpoint, params, timeout=VOL_TIMEOUT)
    except Exception as exc:
        print(f"  ERROR {type(exc).__name__}: {exc}")
        return
    rows = _parse_rows(data)
    print(f"  rows: {len(rows):,}")
    if not rows:
        return
    print(f"  fields: {list(rows[0].keys())}")
    for r in rows[:3]:
        print(f"  sample: {r}")
    for field in ("date", "created", "last_trade", "timestamp"):
        if field in rows[0]:
            prefixes = pd.Series([str(r.get(field))[:10] for r in rows]).value_counts()
            print(f"  `{field}` date prefixes (top 5): {prefixes.head(5).to_dict()}")
    if "volume" in rows[0]:
        vols = pd.to_numeric(pd.Series([r.get("volume") for r in rows]), errors="coerce")
        print(f"  volume > 0 rows: {(vols > 0).sum():,}   total volume: {int(vols.sum()):,}")
        if "count" in rows[0]:
            cnt = pd.to_numeric(pd.Series([r.get("count") for r in rows]), errors="coerce")
            print(f"  `count` (trades) total: {int(cnt.sum()):,}")
    if "last_trade" in rows[0] and "volume" in rows[0]:
        off = [r for r in rows
               if str(r.get("last_trade"))[:10] != day.isoformat()
               and (pd.to_numeric(r.get("volume"), errors="coerce") or 0) > 0]
        print(f"  traded rows whose last_trade is not {day}: {len(off):,}")
    if "open_interest" in rows[0]:
        oi = pd.to_numeric(pd.Series([r.get("open_interest") for r in rows]), errors="coerce")
        print(f"  open_interest > 0 rows: {(oi > 0).sum():,}   total OI: {int(oi.sum()):,}")


def cmd_probe(args) -> int:
    d = args.date.strftime("%Y%m%d")
    for root in args.roots:
        _probe(f"{root} trade EOD", "/v3/option/history/eod",
               {"symbol": root, "expiration": "*", "start_date": d, "end_date": d},
               args.date)
        _probe(f"{root} open interest", "/v3/option/history/open_interest",
               {"symbol": root, "expiration": "*", "date": d}, args.date)
    return 0


# --- Main ------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    f = sub.add_parser("fetch", help="fetch OI and/or volume for a date range")
    f.add_argument("--start", type=_parse_day, required=True)
    f.add_argument("--end", type=_parse_day, required=True)
    f.add_argument("--data", default="oi,volume",
                   type=lambda s: [x.strip() for x in s.split(",") if x.strip()])
    f.add_argument("--roots", default=",".join(config.SPX_ROOTS),
                   type=lambda s: [x.strip().upper() for x in s.split(",") if x.strip()])
    f.add_argument("--oi-snapshot-today", action="store_true",
                   help="also capture today's OI from the snapshot endpoint "
                        "(the history endpoint has no row for today yet)")

    pr = sub.add_parser("probe", help="print raw endpoint fields for one day; writes nothing")
    pr.add_argument("--date", type=_parse_day, required=True)
    pr.add_argument("--roots", default=",".join(config.SPX_ROOTS),
                    type=lambda s: [x.strip().upper() for x in s.split(",") if x.strip()])

    args = p.parse_args()
    if args.cmd == "fetch":
        if args.end < args.start:
            p.error("--end must be >= --start")
        bad = set(args.data) - {"oi", "volume"}
        if bad:
            p.error(f"--data accepts oi,volume; got {sorted(bad)}")
        bad = set(args.roots) - set(config.SPX_ROOTS)
        if bad:
            p.error(f"--roots accepts {','.join(config.SPX_ROOTS)}; got {sorted(bad)}")
        return cmd_fetch(args)
    return cmd_probe(args)


if __name__ == "__main__":
    sys.exit(main())
