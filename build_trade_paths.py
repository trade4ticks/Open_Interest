"""
build_trade_paths.py — resolve exit outcomes for every possible entry.

For every (ticker, trade_date, entry_anchor) the script walks the forward
one-minute price path and records, for each exit rule at each parameter value,
the bar that rule would have fired on and the return actually realised there.
The dashboard then combines rules with a min() over the exit-bar columns and
reads the return belonging to whichever rule won, instead of simulating.

Not purely exits — entry timing is part of it, hence "paths".

--- Entry universe: daily_features, never equity_1min ----------------------

The dashboard joins trade_paths to tt_bins / is_bins on (ticker, trade_date)
to slice entries into heatmap cells, so the key has to line up exactly.
build_bin_tables derives tt_bins rows from `SELECT DISTINCT ticker FROM
daily_features` and then that ticker's trade_dates, so daily_features is the
spine here too. Building the universe from whatever happens to exist in
equity_1min would silently drop rows out of heatmap cells.

Entries with no minute data still get a row, with NULL exits and
path_status = 'no_minute_data'. Visible, never dropped.

--- Price basis -------------------------------------------------------------

equity_1min is stored as-traded (adjusted=false), and holds of 5-20 days will
span splits. A raw calculation reads a 10:1 split as a -90% trade, which does
not merely produce one bad row: it fires a stop that should never have fired
and corrupts the exit statistics. Split factors are applied at read time from
the same make_split_factors machinery the option strikes use.

ATR(14), MA10/MA20 and the swing lows are derived from THIS series rather than
from underlying_ohlc, because they set price levels applied to this path. A
level computed in one vendor's basis and compared against another's is a
systematically misplaced stop. It also sidesteps underlying_ohlc's mixed-basis
exposure (yfinance adjusts to fetch date; the cron only refetches 10 days).

--- Sessions ---------------------------------------------------------------

Stops trigger on REGULAR-session bars only, by default. Both anchors are
regular-session prints, broker stop orders are regular-hours-only unless
explicitly enabled, and extended-hours prints are thin enough that a 100-share
tick would fire stops no real order could have hit — which would overstate
stop frequency and understate returns. Overnight moves are still handled
correctly, and more realistically, by the gap-through rule: a gap below the
stop fills at the 09:30 open, the most fillable price in the system.
--session-filter all overrides, so the alternative is testable without a
second implementation.

Usage:
    python build_trade_paths.py --dry-run
    python build_trade_paths.py
    python build_trade_paths.py --tickers AAPL,SPY --force
"""
from __future__ import annotations

import argparse
import io
import logging
import sys
import time
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

from lib.chain_fetch_common import (
    TIMING, log_path, print_timing_summary, set_local_busy, setup_file_logging,
    start_sampler, stop_background_threads,
)
from lib.market_hours import get_trading_days
from lib.trade_path_rules import (
    MAX_HORIZON_SESSIONS, NEVER, REGISTRY, evaluate,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("build_trade_paths")

# Entry anchors. The value goes in the KEY, so a third (e.g. an ~08:00
# premarket anchor) is new rows and a new entry here — no schema change.
ANCHORS = {
    "open":  "09:30",   # the auction print; the most fillable price in the system
    "t1000": "10:00",
}

# Entries per vectorised block. The (E x B) path arrays are the memory bound:
# at B = 7,800 bars, E = 400 is ~25 MB per array and ~200 MB peak across the
# working set. build_bin_tables.py was OOM-killed at 3.2 GB on this 7.8 GB
# box, so this is sized deliberately rather than left to chance.
BLOCK_ENTRIES = 400

ATR_WINDOW = 14
MA_WINDOWS = (10, 20)
SWING_WINDOWS = (1, 3, 5)


# --- Loading ----------------------------------------------------------------

def load_entry_universe(conn, tickers=None, start=None, end=None) -> pd.DataFrame:
    """(ticker, trade_date) from daily_features — the tt_bins spine."""
    from db import read_sql_df
    where, params = [], {}
    if tickers:
        where.append("ticker = ANY(%(tk)s)")
        params["tk"] = list(tickers)
    if start:
        where.append("trade_date >= %(s)s")
        params["s"] = start
    if end:
        where.append("trade_date <= %(e)s")
        params["e"] = end
    # DISTINCT is insurance, not a fix: daily_features is keyed on
    # (ticker, trade_date) so it cannot duplicate today. A duplicated staged
    # row would make the upsert fail with "cannot affect row a second time",
    # which is an unhelpful way to learn the spine changed shape.
    sql = "SELECT DISTINCT ticker, trade_date FROM daily_features"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ticker, trade_date"
    df = read_sql_df(conn, sql, params or None)
    if not df.empty:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return df


def load_bars(conn, ticker: str, session_filter: str = "regular") -> pd.DataFrame:
    """Split-adjusted minute bars for one ticker, ascending.

    Split factors come from the same make_split_factors used for option
    strikes; prices are multiplied by the factor so a pre-split bar is
    expressed in today's basis and a hold spanning the split is continuous.
    """
    from lib.equity_1min_store import list_years, read_year
    from lib.split_factors import load_splits, make_split_factor_map

    cols = ["trade_date", "session", "timestamp", "open", "high", "low", "close"]
    frames = [read_year(ticker, y, columns=cols) for y in list_years(ticker)]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=cols)
    df = pd.concat(frames, ignore_index=True)
    if session_filter != "all":
        df = df[df["session"] == "regular"]
    if df.empty:
        return df
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df = df.sort_values("timestamp").reset_index(drop=True)

    splits = load_splits(conn, ticker)
    if splits.empty:
        log.info("  %s: no split events — no price adjustment needed", ticker)
        return df

    # inclusive=False is REQUIRED here. The default boundary adjusts
    # trade_date <= split_date, which is right for OI (1-day publication lag,
    # so the ex-date's OI is still pre-split) and wrong for a traded price
    # series, which is already post-split on the ex-date. With the inclusive
    # boundary the ex-date gets divided by the split ratio, which reads as a
    # ~+3 return for entries on that date and — where a hold's exit lands on
    # it — as a return numerically identical to no adjustment, because the two
    # equal factors cancel. Both were observed on AAPL 2020-08-31.
    fmap = make_split_factor_map(splits, sorted(df["trade_date"].unique()),
                                 inclusive=False)
    f = df["trade_date"].map(fmap).astype(float).values
    if not np.isfinite(f).all():
        raise ValueError(
            f"{ticker}: {int((~np.isfinite(f)).sum())} bar(s) got no split "
            f"factor — the factor map does not cover every bar date."
        )
    for c in ("open", "high", "low", "close"):
        df[c] = df[c].values * f
    n_adj = int((f != 1.0).sum())
    log.info("  %s: applied %d split event(s) (%s); %d/%d bars rescaled",
             ticker, len(splits),
             ", ".join(f"{r.trade_date}x{r.splits:g}"
                       for r in splits.itertuples()),
             n_adj, len(f))
    return df


def daily_frame(bars: pd.DataFrame) -> pd.DataFrame:
    """Per-session OHLC from the minute bars, plus ATR / MA / swing levels.

    ATR uses the same definition as build_features.py's intermediate:
        TR_t    = max(H-L, |H - C_{t-1}|, |L - C_{t-1}|)
        atr_14d = mean(TR over T-14 .. T-1)        inclusive, ending at T-1
    Ending at T-1 is what makes it knowable at 09:30 of T. atr_14d is NOT a
    stored daily_features column — only atr_normalized_ret_5d is — so it is
    recomputed here rather than read.
    """
    g = bars.groupby("trade_date", sort=True)
    d = pd.DataFrame({
        "high":  g["high"].max(),
        "low":   g["low"].min(),
        "close": g["close"].last(),
    }).reset_index()

    pc = d["close"].shift(1)
    tr = np.maximum.reduce([
        (d["high"] - d["low"]).values,
        (d["high"] - pc).abs().values,
        (d["low"] - pc).abs().values,
    ])
    d["tr"] = tr
    # shift(1) then rolling: the window must END at T-1, not T.
    d["atr_14d"] = d["tr"].shift(1).rolling(ATR_WINDOW, min_periods=ATR_WINDOW).mean()
    for w in MA_WINDOWS:
        # MA over the w sessions ENDING at that session inclusive — this is the
        # series the ma_close_below rule compares each session's close against.
        d[f"ma{w}"] = d["close"].rolling(w, min_periods=w).mean()
    for n in SWING_WINDOWS:
        # Low of the n sessions BEFORE entry; shift(1) excludes the entry
        # session, whose low is not knowable at the open.
        d[f"swing_low_{n}"] = d["low"].shift(1).rolling(n, min_periods=n).min()
    return d


# --- Path assembly ----------------------------------------------------------

def _anchor_bar_index(bars: pd.DataFrame, sess_start: dict, anchor: str):
    """First bar index at or after the anchor time, per session."""
    hh, mm = ANCHORS[anchor].split(":")
    tod = bars["timestamp"].dt.hour * 60 + bars["timestamp"].dt.minute
    target = int(hh) * 60 + int(mm)
    ok = tod >= target
    out: dict = {}
    td = bars["trade_date"].values
    idx = np.arange(len(bars))
    for d, (lo, hi) in sess_start.items():
        seg = ok.values[lo:hi + 1]
        w = np.argmax(seg) if seg.any() else -1
        if w >= 0 and seg[w]:
            out[d] = lo + int(w)
    return out


def build_ticker(conn, ticker: str, entries: pd.DataFrame, anchor: str,
                 session_filter: str, block: int) -> tuple:
    """Compute all rule outcomes for one (ticker, anchor). Returns (rows, stats)."""
    bars = load_bars(conn, ticker, session_filter)
    if bars.empty:
        return [], {"status": "no_minute_data", "n": len(entries), "resolved": 0}

    daily = daily_frame(bars)
    dpos = {d: i for i, d in enumerate(daily["trade_date"].tolist())}

    # Session -> [first_bar, last_bar] index range.
    td = bars["trade_date"].values
    starts = np.searchsorted(td, np.unique(td), side="left")
    ends = np.searchsorted(td, np.unique(td), side="right") - 1
    sess_dates = list(np.unique(td))
    sess_range = {d: (int(a), int(b)) for d, a, b in zip(sess_dates, starts, ends)}
    sess_order = {d: i for i, d in enumerate(sess_dates)}
    sess_last = np.array([sess_range[d][1] for d in sess_dates])

    anchor_idx = _anchor_bar_index(bars, sess_range, anchor)

    o = bars["open"].values.astype(np.float64)
    h = bars["high"].values.astype(np.float64)
    lo_ = bars["low"].values.astype(np.float64)
    c = bars["close"].values.astype(np.float64)
    n_bars_total = len(bars)

    # Widest horizon in bars, used as the padded block width.
    B = 0
    for i in range(len(sess_dates)):
        j = min(i + MAX_HORIZON_SESSIONS - 1, len(sess_dates) - 1)
        B = max(B, int(sess_last[j]) - int(sess_range[sess_dates[i]][0]) + 1)
    B = max(B, 1)

    ent_dates = [d for d in entries["trade_date"].tolist() if d in anchor_idx]
    rows: list = []
    resolved = 0

    for b0 in range(0, len(ent_dates), block):
        chunk = ent_dates[b0:b0 + block]
        E = len(chunk)
        start_idx = np.array([anchor_idx[d] for d in chunk])
        si = np.array([sess_order[d] for d in chunk])

        # Last bar of the 20th session after entry (clipped to available data).
        end_sess = np.minimum(si + MAX_HORIZON_SESSIONS - 1, len(sess_dates) - 1)
        end_idx = sess_last[end_sess]
        path_len = (end_idx - start_idx + 1).astype(np.int64)
        full = (si + MAX_HORIZON_SESSIONS - 1) <= (len(sess_dates) - 1)

        # Padded (E, B) block. Pads can never trigger: low = +inf never crosses
        # a stop, high = -inf never crosses a target.
        cols = np.arange(B)[None, :]
        take = np.minimum(start_idx[:, None] + cols, n_bars_total - 1)
        valid = cols < path_len[:, None]
        path = {
            "open":  np.where(valid, o[take], np.nan),
            "high":  np.where(valid, h[take], -np.inf),
            "low":   np.where(valid, lo_[take], np.inf),
            "close": np.where(valid, c[take], np.nan),
        }

        entry_price = o[start_idx]

        # Session-end bar index RELATIVE to each entry, for the time rules.
        sess_end_rel = np.full((E, MAX_HORIZON_SESSIONS), NEVER, dtype=np.int64)
        for k in range(MAX_HORIZON_SESSIONS):
            kk = np.minimum(si + k, len(sess_dates) - 1)
            avail = (si + k) <= (len(sess_dates) - 1)
            rel = sess_last[kk] - start_idx
            sess_end_rel[:, k] = np.where(avail & (rel >= 0) & (rel < B), rel, NEVER)

        di = np.array([dpos[d] for d in chunk])
        atr = daily["atr_14d"].values[di]
        ctx = {
            "entry_price": entry_price,
            "atr": atr,
            "session_end_idx": sess_end_rel,
            "path_len": path_len,
        }
        for n in SWING_WINDOWS:
            ctx[f"swing_low_{n}"] = daily[f"swing_low_{n}"].values[di]

        # close < MA per session of the hold, evaluated on the daily frame.
        for w in MA_WINDOWS:
            ma = daily[f"ma{w}"].values
            cl = daily["close"].values
            below = np.zeros((E, MAX_HORIZON_SESSIONS), dtype=bool)
            for k in range(MAX_HORIZON_SESSIONS):
                kk = np.minimum(di + k, len(daily) - 1)
                avail = (di + k) <= (len(daily) - 1)
                with np.errstate(invalid="ignore"):
                    below[:, k] = avail & np.isfinite(ma[kk]) & (cl[kk] < ma[kk])
            ctx[f"_ma_below_{w}"] = below

        res = evaluate(path, ctx, REGISTRY)

        for e in range(E):
            d = chunk[e]
            status = "ok" if full[e] else "truncated"
            if status == "ok":
                resolved += 1
            row = {
                "ticker": ticker, "trade_date": d, "entry_anchor": anchor,
                "entry_price": float(entry_price[e]),
                # numpy datetime64 stringifies with 'T' and nanoseconds
                # ('2019-01-02T14:30:00.000000000'), which is past the
                # microsecond precision a Postgres TIMESTAMP accepts. Convert
                # once here rather than relying on the parser to round.
                "entry_bar_ts": pd.Timestamp(
                    bars["timestamp"].values[start_idx[e]]).to_pydatetime(),
                "atr_14d": _f(atr[e]),
                "swing_low_1": _f(ctx["swing_low_1"][e]),
                "swing_low_3": _f(ctx["swing_low_3"][e]),
                "swing_low_5": _f(ctx["swing_low_5"][e]),
                "n_bars": int(path_len[e]),
                "n_sessions": int(min(MAX_HORIZON_SESSIONS,
                                      len(sess_dates) - si[e])),
                "path_status": status,
            }
            for r in REGISTRY:
                bar, ret = res[r.key]
                row[r.bar_col] = None if bar[e] < 0 else int(bar[e])
                row[r.ret_col] = _f(ret[e])
            rows.append(row)

    return rows, {"status": "ok", "n": len(ent_dates), "resolved": resolved}


def _f(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if not np.isfinite(f) else f


# --- Write ------------------------------------------------------------------

def write_rows(conn, rows: list) -> int:
    """COPY into a temp table, then upsert. execute_values on 131 columns x
    450k rows is minutes slower for no benefit.

    INCLUDING DEFAULTS is load-bearing. A bare `LIKE trade_paths` copies column
    types and NOT NULL constraints but NOT defaults, so the staged
    built_at — which this function deliberately does not supply, leaving it to
    the table — arrives NULL and violates its own NOT NULL. Carrying the
    defaults across makes the stage table behave like the target it mirrors.
    """
    if not rows:
        return 0
    cols = list(rows[0].keys())
    assert "built_at" not in cols, (
        "built_at is supplied by the table default, not by the row dicts; "
        "if that changes, drop it from the DO UPDATE SET below too."
    )
    buf = io.StringIO()
    for r in rows:
        buf.write("\t".join(
            "\\N" if r[c] is None else str(r[c]) for c in cols) + "\n")
    buf.seek(0)
    collist = ", ".join(cols)
    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE _tp_stage "
                    "(LIKE trade_paths INCLUDING DEFAULTS) ON COMMIT DROP")
        cur.copy_expert(
            f"COPY _tp_stage ({collist}) FROM STDIN WITH (FORMAT text)", buf)
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols
                            if c not in ("ticker", "trade_date", "entry_anchor"))
        # built_at is not in `cols`, so a rebuild would otherwise keep the
        # first build's timestamp and misreport when this row was computed.
        updates += ", built_at = now()"
        cur.execute(
            f"INSERT INTO trade_paths ({collist}) "
            f"SELECT {collist} FROM _tp_stage "
            f"ON CONFLICT (ticker, trade_date, entry_anchor) DO UPDATE SET {updates}"
        )
    conn.commit()
    return len(rows)


def record_manifest(conn, ticker: str, anchor: str, stats: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO trade_paths_manifest "
            "(ticker, entry_anchor, status, n_entries, n_resolved, built_at, note) "
            "VALUES (%s,%s,%s,%s,%s,now(),%s) "
            "ON CONFLICT (ticker, entry_anchor) DO UPDATE SET "
            "status=EXCLUDED.status, n_entries=EXCLUDED.n_entries, "
            "n_resolved=EXCLUDED.n_resolved, built_at=now(), note=EXCLUDED.note",
            (ticker, anchor, stats["status"], stats.get("n"),
             stats.get("resolved"), stats.get("note")),
        )
    conn.commit()


def completed(conn) -> set:
    with conn.cursor() as cur:
        cur.execute("SELECT ticker, entry_anchor FROM trade_paths_manifest "
                    "WHERE status = 'ok'")
        return {(r[0], r[1]) for r in cur.fetchall()}


# --- Coverage ---------------------------------------------------------------

def coverage_report(conn) -> None:
    """tt_bins vs trade_paths on the shared key, both sides of the difference.

    Reported because renamed tickers and 2020-21 listings will legitimately
    differ, and the number should be a known quantity rather than a surprise
    when a heatmap cell comes up short.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM tt_bins")
        n_tt = cur.fetchone()[0]
        cur.execute("SELECT count(DISTINCT (ticker, trade_date)) FROM trade_paths")
        n_tp = cur.fetchone()[0]
        cur.execute("""
            SELECT count(*) FROM tt_bins b
            WHERE NOT EXISTS (SELECT 1 FROM trade_paths p
                              WHERE p.ticker = b.ticker
                                AND p.trade_date = b.trade_date)
        """)
        only_tt = cur.fetchone()[0]
        cur.execute("""
            SELECT count(*) FROM (
                SELECT DISTINCT ticker, trade_date FROM trade_paths) p
            WHERE NOT EXISTS (SELECT 1 FROM tt_bins b
                              WHERE b.ticker = p.ticker
                                AND b.trade_date = p.trade_date)
        """)
        only_tp = cur.fetchone()[0]
        cur.execute("SELECT path_status, count(*) FROM trade_paths "
                    "GROUP BY path_status ORDER BY 2 DESC")
        by_status = cur.fetchall()
        cur.execute("""
            SELECT b.ticker, count(*) AS missing FROM tt_bins b
            WHERE NOT EXISTS (SELECT 1 FROM trade_paths p
                              WHERE p.ticker = b.ticker
                                AND p.trade_date = b.trade_date)
            GROUP BY b.ticker ORDER BY missing DESC LIMIT 10
        """)
        worst = cur.fetchall()

    print("\n" + "=" * 64)
    print("JOIN COVERAGE — trade_paths vs tt_bins on (ticker, trade_date)")
    print("=" * 64)
    print(f"  tt_bins (ticker, trade_date)          {n_tt:>12,}")
    print(f"  trade_paths (distinct)                {n_tp:>12,}")
    print(f"  in tt_bins, NOT in trade_paths        {only_tt:>12,}")
    print(f"  in trade_paths, NOT in tt_bins        {only_tp:>12,}")
    print("\n  trade_paths rows by status:")
    for s, n in by_status:
        print(f"    {s:<20}{n:>12,}")
    if worst:
        print("\n  Tickers with the most tt_bins dates absent from trade_paths:")
        for t, n in worst:
            print(f"    {t:<8}{n:>8,}")
        print("  (renames and 2020-21 listings legitimately appear here — "
              "compare against audit_symbol_gaps.py)")


# --- Main -------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Precompute trade exit paths.")
    ap.add_argument("--tickers", help="comma-separated; default = all")
    ap.add_argument("--anchors", default=",".join(ANCHORS),
                    help=f"comma-separated; default {','.join(ANCHORS)}")
    ap.add_argument("--start", help="YYYYMMDD entry-date floor")
    ap.add_argument("--end", help="YYYYMMDD entry-date ceiling")
    ap.add_argument("--session-filter", choices=["regular", "all"],
                    default="regular",
                    help="which bars can trigger a stop (default regular)")
    ap.add_argument("--block", type=int, default=BLOCK_ENTRIES,
                    help=f"entries per vectorised block (default {BLOCK_ENTRIES})")
    ap.add_argument("--force", action="store_true",
                    help="rebuild tickers already marked ok in the manifest")
    ap.add_argument("--dry-run", action="store_true",
                    help="report the work and exit without computing")
    args = ap.parse_args()

    log_file = setup_file_logging("build_trade_paths")
    print("=== Open_Interest — trade path precompute ===")
    print(f"Log: {log_file}\n")
    log.info("argv: %s", " ".join(sys.argv[1:]))

    from db import get_connection
    from lib.trade_path_schema import (
        SchemaNotInitialised, sync_rule_catalog, sync_trade_paths_schema,
    )

    anchors = [a.strip() for a in args.anchors.split(",") if a.strip()]
    bad = [a for a in anchors if a not in ANCHORS]
    if bad:
        raise SystemExit(f"unknown anchor(s): {bad}. Known: {list(ANCHORS)}")

    start = datetime.strptime(args.start, "%Y%m%d").date() if args.start else None
    end = datetime.strptime(args.end, "%Y%m%d").date() if args.end else None
    tickers = ([t.strip().upper() for t in args.tickers.split(",") if t.strip()]
               if args.tickers else None)

    run_t0 = time.monotonic()
    with get_connection() as conn:
        print("Syncing trade_paths schema ...", end=" ", flush=True)
        try:
            n_added, _ = sync_trade_paths_schema(conn)
            n_rules = sync_rule_catalog(conn)
        except SchemaNotInitialised as exc:
            print("FAILED")
            raise SystemExit(f"\n{exc}")
        print(f"OK ({n_added} new column pair(s), {n_rules} rules catalogued)")

        universe = load_entry_universe(conn, tickers, start, end)
        if universe.empty:
            raise SystemExit("daily_features returned no entries for these filters.")
        all_tickers = sorted(universe["ticker"].unique())
        n_entries = len(universe)

        print(f"\n{len(all_tickers)} tickers x {n_entries:,} entry dates "
              f"x {len(anchors)} anchor(s) = {n_entries * len(anchors):,} rows")
        print(f"{len(REGISTRY)} rules -> {len(REGISTRY) * 2} exit columns")
        print(f"session filter: {args.session_filter}, block: {args.block}\n")

        if args.dry_run:
            print("DRY RUN — nothing computed, nothing written.")
            return 0

        done = set() if args.force else completed(conn)
        start_sampler()
        TIMING.startup = time.monotonic() - run_t0

        total = 0
        failed: list = []
        from tqdm import tqdm
        with tqdm(total=len(all_tickers) * len(anchors), unit="tk",
                  ncols=90, desc="paths") as bar:
            for tk in all_tickers:
                ent = universe[universe["ticker"] == tk]
                for anchor in anchors:
                    if (tk, anchor) in done:
                        bar.update(1)
                        continue
                    t0 = time.monotonic()
                    try:
                        try:
                            set_local_busy(True)
                            rows, stats = build_ticker(
                                conn, tk, ent, anchor,
                                args.session_filter, args.block)
                        finally:
                            # Without the finally, a raising build leaves the
                            # occupancy sampler believing local work is still
                            # running for the rest of the run.
                            set_local_busy(False)
                        TIMING.local_compute += time.monotonic() - t0
                        w0 = time.monotonic()
                        total += write_rows(conn, rows)
                        TIMING.parquet_write += time.monotonic() - w0
                        TIMING.writes.append((time.monotonic() - w0, len(rows)))
                        record_manifest(conn, tk, anchor, stats)
                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except Exception as exc:
                        # ROLLBACK FIRST. A failed COPY leaves the connection in
                        # an aborted transaction, so recording the manifest
                        # before clearing it fails too — which is how a write
                        # failure ended up unrecorded, with a traceback as the
                        # only evidence. One ticker-anchor failing must record
                        # and continue, exactly as fetch_equity_1min does.
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        failed.append(f"{tk}/{anchor}")
                        log.error("  FAIL %s/%s: %s: %s — recorded, continuing",
                                  tk, anchor, type(exc).__name__, exc)
                        log.debug("  traceback for %s/%s", tk, anchor,
                                  exc_info=True)
                        try:
                            record_manifest(conn, tk, anchor, {
                                "status": "failed", "n": len(ent),
                                "resolved": 0,
                                "note": f"{type(exc).__name__}: {exc}"[:300]})
                        except Exception as exc2:
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            log.error("  %s/%s: could not record the failure "
                                      "either (%s) — it will simply be retried "
                                      "on the next run", tk, anchor, exc2)
                    bar.update(1)

        stop_background_threads()
        print(f"\n{total:,} rows written to trade_paths")
        print(f"Log written to {log_path()}")
        print_timing_summary(time.monotonic() - run_t0,
                             query_label="ticker-anchor builds",
                             timing={}, retry_policy="n/a (no network)",
                             connections=1)
        coverage_report(conn)

        if failed:
            print(f"\n{len(failed)} ticker-anchor build(s) FAILED: "
                  f"{', '.join(failed[:10])}")
            print("Recorded in trade_paths_manifest; a plain re-run retries them.")
            return 1
    print("\nNext: python audit_trade_paths.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
