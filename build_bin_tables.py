"""
build_bin_tables.py — Build / refresh wf_bins and tt_thresholds.

Hooks into the existing two-batch cadence:
  Evening batch:  python build_bin_tables.py --tier EVENING [--build-tt]
  Morning batch:  python build_bin_tables.py --tier MORNING

CRITICAL contract (mirrors the two-cron daily_features upsert pattern):
  The per-tier upsert SET clause names ONLY the running tier's columns
  (frac_<m>, bin20_<m> for metrics with tier == TIER) and never touches the
  other tier's columns.  Morning and evening batches write disjoint columns
  of the SAME (ticker, trade_date) row.  A DELETE before INSERT would wipe
  the other tier's data — DO NOT add one.

The wf_bins rebuild is full-history per tier (~225k rows × tier's metrics).
Walk-forward semantics make incremental-by-date fragile (a prior date's value
being edited propagates forward), so full rebuild keeps the contract simple.
Per-tier runtime is ~1-3 minutes for ~70 metrics × 125 tickers × ~1800 dates.

tt_thresholds rebuild is full (all metrics × all tickers) but cheap (~18k
small array rows).  Run on the evening batch via --build-tt.

Usage:
    python build_bin_tables.py --tier {MORNING,EVENING}
    python build_bin_tables.py --build-tt
    python build_bin_tables.py --tier EVENING --build-tt    (combined)
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date

import pandas as pd
import psycopg2.extras

from db import get_connection, read_sql_df
from lib.bin_compute import (
    TRAIN_TEST_CUTOFF_DEFAULT,
    TT_MIN_TRAIN_DEFAULT,
    in_sample_series,
    train_test_history,
    train_test_series,
    walk_forward_series,
)
from lib.bin_schema import (
    existing_daily_features_columns,
    get_metrics_by_tier,
    sync_is_bins_schema,
    sync_tt_bins_schema,
    sync_wf_bins_schema,
)

# Cutoff stored as a date object for tt_bins (the bin compute compares date
# objects, not strings).  Same calendar value as TRAIN_TEST_CUTOFF_DEFAULT.
TT_CUTOFF_DATE = date.fromisoformat(TRAIN_TEST_CUTOFF_DEFAULT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("build_bin_tables")


# Commit per N-ticker batch.  Lower = lower peak memory; higher = fewer
# commits.  Sized for a ~8GB VPS with no swap — the previous unchunked
# upsert built a 221k-row × 192-col DataFrame and then a 221k-tuple rows
# list in memory simultaneously, peaking at ~3.2GB rss and getting
# SIGKILL'd by the OOM-killer.  At batch=5 (each ticker contributes
# ~1800 rows × ~190 cells), peak per-batch overhead stays well under
# 150MB on top of the source df.
UPSERT_TICKER_BATCH = 5


def _flush_upsert(conn, insert_sql: str, write_cols: list, frames: list) -> int:
    """Concatenate a batch of per-ticker DataFrames, materialise the rows
    list, upsert via execute_values, commit, return rows upserted.

    All intermediate objects (batch_df, batch_rows) go out of scope on
    return, so the caller's GC can reclaim them before the next batch
    builds its own.  This is the memory-bounding contract.
    """
    if not frames:
        return 0
    batch_df = pd.concat(frames, ignore_index=True)
    batch_rows = [tuple(_pgify(r.get(c)) for c in write_cols)
                  for r in batch_df.to_dict(orient="records")]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, insert_sql, batch_rows, page_size=500)
    conn.commit()
    return len(batch_rows)


# Walk-forward build ---------------------------------------------------------

def _build_wf_bins_for_tier(conn, tier: str) -> int:
    """Full rebuild of wf_bins for one tier's eligible metrics.  Returns the
    number of (ticker, trade_date) rows upserted."""
    tier = tier.upper()
    if tier not in ("MORNING", "EVENING"):
        raise ValueError(f"tier must be MORNING or EVENING; got {tier!r}")

    log.info("Syncing wf_bins schema ...")
    sync_wf_bins_schema(conn)

    metrics_by_tier = get_metrics_by_tier(conn)
    metrics = metrics_by_tier.get(tier, [])
    if not metrics:
        log.warning("No eligible metrics for tier %s — nothing to do.", tier)
        return 0
    log.info("Tier %s: %d eligible metric(s).", tier, len(metrics))

    df_cols = existing_daily_features_columns(conn)
    metrics = [m for m in metrics if m in df_cols]
    if not metrics:
        log.warning("All %s-tier metrics are absent from daily_features. "
                    "Aborting.", tier)
        return 0

    cols_sql = ", ".join(metrics)
    log.info("Reading daily_features (ticker, trade_date, %d metric cols) ...",
             len(metrics))
    t0 = time.time()
    df = read_sql_df(
        conn,
        f"SELECT ticker, trade_date, {cols_sql} FROM daily_features "
        f"ORDER BY ticker, trade_date"
    )
    log.info("  loaded %d rows in %.1fs.", len(df), time.time() - t0)
    if df.empty:
        log.warning("daily_features returned no rows — nothing to bin.")
        return 0

    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    # Build the tier-scoped upsert SQL once.  Names ONLY this tier's columns
    # in DO UPDATE SET — does not touch the other tier's columns of the same
    # (ticker, trade_date) row.  No DELETE; the two-cron write contract
    # depends on neither tier's path ever wiping the other.
    write_cols = ["ticker", "trade_date"]
    for m in metrics:
        write_cols.append(f"frac_{m}")
        write_cols.append(f"bin20_{m}")
    set_clauses = []
    for m in metrics:
        set_clauses.append(f"frac_{m} = EXCLUDED.frac_{m}")
        set_clauses.append(f"bin20_{m} = EXCLUDED.bin20_{m}")
    set_sql = ",\n        ".join(set_clauses)
    insert_sql = (
        f"INSERT INTO wf_bins ({', '.join(write_cols)}) VALUES %s\n"
        f"ON CONFLICT (ticker, trade_date) DO UPDATE SET\n"
        f"    {set_sql}"
    )

    # Compute + upsert per ticker batch.  Each batch commits independently
    # so peak memory stays bounded; ON CONFLICT DO UPDATE SET makes the
    # operation idempotent on rerun if a later batch fails.
    total_tickers = df["ticker"].nunique()
    log.info("Computing + upserting wf_bins (%s) — %d metrics × %d tickers, "
             "commit per %d-ticker batch ...",
             tier, len(metrics), total_tickers, UPSERT_TICKER_BATCH)
    t0 = time.time()

    pending: list = []
    total_rows = 0
    n_tickers_done = 0

    for ticker, sub in df.groupby("ticker", sort=False):
        sub = sub.sort_values("trade_date").reset_index(drop=True)
        dates = sub["trade_date"].tolist()
        cols: dict = {"ticker": [ticker] * len(dates), "trade_date": dates}
        for m in metrics:
            vals = sub[m].tolist()
            fracs, bin20s = walk_forward_series(vals, dates)
            cols[f"frac_{m}"] = fracs
            cols[f"bin20_{m}"] = bin20s
        pending.append(pd.DataFrame(cols))

        if len(pending) >= UPSERT_TICKER_BATCH:
            total_rows += _flush_upsert(conn, insert_sql, write_cols, pending)
            n_tickers_done += len(pending)
            log.info("  ... %d/%d tickers, %d rows upserted",
                     n_tickers_done, total_tickers, total_rows)
            pending = []

    # Final partial batch.
    if pending:
        total_rows += _flush_upsert(conn, insert_sql, write_cols, pending)
        n_tickers_done += len(pending)
        log.info("  ... %d/%d tickers, %d rows upserted",
                 n_tickers_done, total_tickers, total_rows)

    log.info("  wf_bins (%s) done: %d rows in %.1fs.",
             tier, total_rows, time.time() - t0)
    return total_rows


# In-sample build ------------------------------------------------------------

def _build_is_bins_for_tier(conn, tier: str) -> int:
    """Full rebuild of is_bins for one tier's eligible metrics.

    In-sample semantics: each trade's bin is computed by ranking its metric
    value against that ticker's ENTIRE history (full date range, fixed
    population).  No warmup — every row with a valid metric value gets a
    frac and bin20.

    Mirrors _build_wf_bins_for_tier exactly except:
      - Uses in_sample_series instead of walk_forward_series.
      - Calls sync_is_bins_schema and writes to is_bins, not wf_bins.

    Returns the number of (ticker, trade_date) rows upserted.
    """
    tier = tier.upper()
    if tier not in ("MORNING", "EVENING"):
        raise ValueError(f"tier must be MORNING or EVENING; got {tier!r}")

    log.info("Syncing is_bins schema ...")
    sync_is_bins_schema(conn)

    metrics_by_tier = get_metrics_by_tier(conn)
    metrics = metrics_by_tier.get(tier, [])
    if not metrics:
        log.warning("No eligible metrics for tier %s — nothing to do.", tier)
        return 0
    log.info("Tier %s: %d eligible metric(s).", tier, len(metrics))

    df_cols = existing_daily_features_columns(conn)
    metrics = [m for m in metrics if m in df_cols]
    if not metrics:
        log.warning("All %s-tier metrics are absent from daily_features. "
                    "Aborting.", tier)
        return 0

    cols_sql = ", ".join(metrics)
    log.info("Reading daily_features (ticker, trade_date, %d metric cols) ...",
             len(metrics))
    t0 = time.time()
    df = read_sql_df(
        conn,
        f"SELECT ticker, trade_date, {cols_sql} FROM daily_features "
        f"ORDER BY ticker, trade_date"
    )
    log.info("  loaded %d rows in %.1fs.", len(df), time.time() - t0)
    if df.empty:
        log.warning("daily_features returned no rows — nothing to bin.")
        return 0

    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    # Build the tier-scoped upsert SQL once.  Same two-cron contract as
    # wf_bins — names ONLY this tier's columns in DO UPDATE SET.
    write_cols = ["ticker", "trade_date"]
    for m in metrics:
        write_cols.append(f"frac_{m}")
        write_cols.append(f"bin20_{m}")
    set_clauses = []
    for m in metrics:
        set_clauses.append(f"frac_{m} = EXCLUDED.frac_{m}")
        set_clauses.append(f"bin20_{m} = EXCLUDED.bin20_{m}")
    set_sql = ",\n        ".join(set_clauses)
    insert_sql = (
        f"INSERT INTO is_bins ({', '.join(write_cols)}) VALUES %s\n"
        f"ON CONFLICT (ticker, trade_date) DO UPDATE SET\n"
        f"    {set_sql}"
    )

    # Compute + upsert per ticker batch.  Mirrors wf_bins's path.
    total_tickers = df["ticker"].nunique()
    log.info("Computing + upserting is_bins (%s) — %d metrics × %d tickers, "
             "commit per %d-ticker batch ...",
             tier, len(metrics), total_tickers, UPSERT_TICKER_BATCH)
    t0 = time.time()

    pending: list = []
    total_rows = 0
    n_tickers_done = 0

    for ticker, sub in df.groupby("ticker", sort=False):
        sub = sub.sort_values("trade_date").reset_index(drop=True)
        dates = sub["trade_date"].tolist()
        cols: dict = {"ticker": [ticker] * len(dates), "trade_date": dates}
        for m in metrics:
            vals = sub[m].tolist()
            fracs, bin20s = in_sample_series(vals, dates)
            cols[f"frac_{m}"] = fracs
            cols[f"bin20_{m}"] = bin20s
        pending.append(pd.DataFrame(cols))

        if len(pending) >= UPSERT_TICKER_BATCH:
            total_rows += _flush_upsert(conn, insert_sql, write_cols, pending)
            n_tickers_done += len(pending)
            log.info("  ... %d/%d tickers, %d rows upserted",
                     n_tickers_done, total_tickers, total_rows)
            pending = []

    # Final partial batch.
    if pending:
        total_rows += _flush_upsert(conn, insert_sql, write_cols, pending)
        n_tickers_done += len(pending)
        log.info("  ... %d/%d tickers, %d rows upserted",
                 n_tickers_done, total_tickers, total_rows)

    log.info("  is_bins (%s) done: %d rows in %.1fs.",
             tier, total_rows, time.time() - t0)
    return total_rows


# tt_bins build (train-test, replaces what the dashboard reads from tt_thresholds)

def _build_tt_bins(conn, cutoff: date = TT_CUTOFF_DATE,
                   min_train: int = TT_MIN_TRAIN_DEFAULT) -> int:
    """Full rebuild of tt_bins for the given cutoff.

    Single-pass: all eligible metrics across all tickers, no tier separation.
    Per (ticker, metric): build the frozen ruler from pre-cutoff valid
    values, apply it to BOTH train (pre-cutoff) and test (post-cutoff) rows.
    See lib.bin_compute.train_test_series for the algorithm.

    Per-ticker safety: a ticker with no valid pre-cutoff rows for any metric
    (e.g. a post-cutoff listing like QBTS) still gets its rows written, with
    all bin20_<m> = 0.  train_test_series guards the empty-train_sorted /
    n_train < min_train case before any bisect or division.

    Mirrors the chunked-upsert pattern from wf/is builds — commit per
    UPSERT_TICKER_BATCH tickers — so peak memory stays bounded.
    """
    log.info("Syncing tt_bins schema ...")
    sync_tt_bins_schema(conn)

    metrics_by_tier = get_metrics_by_tier(conn)
    metrics = sorted(set(metrics_by_tier["MORNING"]) | set(metrics_by_tier["EVENING"]))
    if not metrics:
        log.warning("No eligible metrics — nothing to do.")
        return 0

    df_cols = existing_daily_features_columns(conn)
    metrics = [m for m in metrics if m in df_cols]
    log.info("tt_bins: %d eligible metric(s), cutoff = %s, min_train = %d.",
             len(metrics), cutoff, min_train)

    cols_sql = ", ".join(metrics)

    # Load the ticker list separately (cheap), then STREAM daily_features
    # per-ticker inside the loop below.  Loading the whole table at once
    # (~221k rows × 144 metric cols) OOM-killed this build on the
    # 7.8GB-with-no-swap VPS — see commit history.  The per-ticker
    # algorithm doesn't need cross-ticker state (binning is per-ticker
    # against the ticker's own pre-cutoff history), so streaming costs
    # nothing semantically; only one ticker × all metrics sits in memory
    # at any moment, freed before the next iteration.
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT ticker FROM daily_features ORDER BY ticker")
        all_tickers = [r[0] for r in cur.fetchall()]
    total_tickers = len(all_tickers)
    log.info("Tickers to process: %d", total_tickers)
    if total_tickers == 0:
        log.warning("daily_features has no tickers — nothing to bin.")
        return 0

    # Build the upsert SQL once.  tt_bins is single-tier (no MORNING/EVENING
    # split); the SET clause names cutoff_date + frac_<m> + bin20_<m> for
    # every eligible metric.  Same frac+bin20 pair shape as wf_bins / is_bins.
    write_cols = ["ticker", "trade_date", "cutoff_date"]
    for m in metrics:
        write_cols.append(f"frac_{m}")
        write_cols.append(f"bin20_{m}")
    set_clauses = ["cutoff_date = EXCLUDED.cutoff_date"]
    for m in metrics:
        set_clauses.append(f"frac_{m} = EXCLUDED.frac_{m}")
        set_clauses.append(f"bin20_{m} = EXCLUDED.bin20_{m}")
    set_sql = ",\n        ".join(set_clauses)
    insert_sql = (
        f"INSERT INTO tt_bins ({', '.join(write_cols)}) VALUES %s\n"
        f"ON CONFLICT (ticker, trade_date) DO UPDATE SET\n"
        f"    {set_sql}"
    )

    log.info("Computing + upserting tt_bins — %d metrics × %d tickers, "
             "streaming reads, commit per %d-ticker batch ...",
             len(metrics), total_tickers, UPSERT_TICKER_BATCH)
    t0 = time.time()

    pending: list = []
    total_rows = 0
    n_tickers_done = 0
    n_thin_tickers = 0   # tickers where EVERY metric returned all-zero (insufficient train)

    for ticker in all_tickers:
        # Stream ONE ticker's full series.  WHERE ticker = %s lets Postgres
        # use the (ticker, trade_date) PK index so only this ticker's rows
        # cross the wire — not all 124 tickers' worth.  This is THE memory
        # bound: peak ≈ one ticker's row count × len(metrics).
        sub = read_sql_df(
            conn,
            f"SELECT trade_date, {cols_sql} FROM daily_features "
            f"WHERE ticker = %(t)s ORDER BY trade_date",
            {"t": ticker},
        )
        if sub.empty:
            continue
        sub["trade_date"] = pd.to_datetime(sub["trade_date"]).dt.date

        dates = sub["trade_date"].tolist()
        cols: dict = {
            "ticker":      [ticker] * len(dates),
            "trade_date":  dates,
            "cutoff_date": [cutoff] * len(dates),
        }
        n_metrics_with_any_bin = 0
        for m in metrics:
            vals = sub[m].tolist()
            fracs, bin20s = train_test_series(vals, dates, cutoff, min_train)
            cols[f"frac_{m}"]  = fracs
            cols[f"bin20_{m}"] = bin20s
            if any(b > 0 for b in bin20s):
                n_metrics_with_any_bin += 1
        if n_metrics_with_any_bin == 0:
            n_thin_tickers += 1
            log.info("  thin ticker (no binnable metric, all bin20=0): %s", ticker)
        pending.append(pd.DataFrame(cols))

        # Explicitly drop the raw read so peak memory stays at one ticker's
        # worth before the next SELECT lands.  (Python rebinding `sub` on
        # the next iteration would do the same, but `del` makes the intent
        # visible and the GC fire sooner.)
        del sub

        if len(pending) >= UPSERT_TICKER_BATCH:
            total_rows += _flush_upsert(conn, insert_sql, write_cols, pending)
            n_tickers_done += len(pending)
            log.info("  ... %d/%d tickers, %d rows upserted",
                     n_tickers_done, total_tickers, total_rows)
            pending = []

    if pending:
        total_rows += _flush_upsert(conn, insert_sql, write_cols, pending)
        n_tickers_done += len(pending)
        log.info("  ... %d/%d tickers, %d rows upserted",
                 n_tickers_done, total_tickers, total_rows)

    log.info("  tt_bins done: %d rows in %.1fs.  "
             "Thin tickers (zero binnable metrics): %d.",
             total_rows, time.time() - t0, n_thin_tickers)
    return total_rows


# Train-test build -----------------------------------------------------------

def _build_tt_thresholds(conn, cutoff: str = TRAIN_TEST_CUTOFF_DEFAULT) -> int:
    """Full rebuild of tt_thresholds for one cutoff.  Returns row count."""
    log.info("Building tt_thresholds for cutoff_date = %s (training: trade_date < cutoff).",
             cutoff)
    metrics_by_tier = get_metrics_by_tier(conn)
    eligible = sorted(set(metrics_by_tier["MORNING"]) | set(metrics_by_tier["EVENING"]))
    if not eligible:
        log.warning("No eligible metrics — nothing to do.")
        return 0
    df_cols = existing_daily_features_columns(conn)
    eligible = [m for m in eligible if m in df_cols]
    log.info("  %d eligible metric(s).", len(eligible))

    # All tickers in daily_features — used to emit a row for tickers with zero
    # pre-cutoff data (n_train = 0, history_vals = []).  Without this step,
    # tickers that started trading on/after the cutoff (e.g. APLD) would have
    # no tt_thresholds rows at all, and the dashboard's read query would have
    # to defensively treat "row missing" the same as "row present, n_train < k".
    # Emitting empty rows keeps that contract single-source: the read side
    # always finds a row and applies n_train < k → None uniformly.
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT ticker FROM daily_features ORDER BY ticker")
        all_tickers = [r[0] for r in cur.fetchall()]
    log.info("  %d ticker(s) in daily_features.", len(all_tickers))

    cols_sql = ", ".join(eligible)
    t0 = time.time()
    df = read_sql_df(
        conn,
        f"SELECT ticker, trade_date, {cols_sql} FROM daily_features "
        f"WHERE trade_date < %(cutoff)s "
        f"ORDER BY ticker, trade_date",
        {"cutoff": cutoff},
    )
    log.info("  loaded %d pre-cutoff rows in %.1fs.", len(df), time.time() - t0)

    # Group pre-cutoff data by ticker (empty dict if no pre-cutoff rows at all).
    by_ticker: dict = {}
    if not df.empty:
        for ticker, sub in df.groupby("ticker", sort=False):
            by_ticker[ticker] = sub

    t0 = time.time()
    records: list = []
    n_empty = 0
    for ticker in all_tickers:
        sub = by_ticker.get(ticker)
        for m in eligible:
            if sub is None:
                history_vals: list = []
                n_train = 0
            else:
                history_vals, n_train = train_test_history(sub[m].tolist())
            if n_train == 0:
                n_empty += 1
            records.append({
                "metric":       m,
                "ticker":       ticker,
                "cutoff_date":  cutoff,
                "history_vals": history_vals,
                "n_train":      n_train,
            })
    log.info("  computed %d (metric, ticker) rows in %.1fs "
             "(%d with n_train=0).",
             len(records), time.time() - t0, n_empty)

    upsert_sql = """
    INSERT INTO tt_thresholds
        (metric, ticker, cutoff_date, history_vals, n_train)
    VALUES %s
    ON CONFLICT (metric, ticker, cutoff_date) DO UPDATE SET
        history_vals = EXCLUDED.history_vals,
        n_train      = EXCLUDED.n_train
    """
    t0 = time.time()
    rows = [(r["metric"], r["ticker"], r["cutoff_date"],
             r["history_vals"], r["n_train"]) for r in records]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, upsert_sql, rows, page_size=500)
    conn.commit()
    log.info("  upserted %d rows in %.1fs.", len(rows), time.time() - t0)
    return len(rows)


# Helpers --------------------------------------------------------------------

def _pgify(v):
    """numpy/pandas → native; NaN/NaT/None → None."""
    if v is None:
        return None
    if isinstance(v, float):
        return None if v != v else v
    try:
        import numpy as np
        if isinstance(v, np.floating):
            f = float(v)
            return None if f != f else f
        if isinstance(v, np.integer):
            return int(v)
        if isinstance(v, np.bool_):
            return bool(v)
    except ImportError:
        pass
    return v


# CLI ------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tier", choices=["MORNING", "EVENING"], default=None,
                    help="Rebuild wf_bins and is_bins for this tier's metrics.")
    ap.add_argument("--build-tt-bins", action="store_true",
                    help="(Re)build tt_bins for the standard cutoff "
                         "(in-sample bin table the dashboard reads).")
    ap.add_argument("--build-tt", action="store_true",
                    help="(Re)build legacy tt_thresholds for the standard "
                         "cutoff.  Dashboard reads tt_bins instead; this is "
                         "kept for the underlying threshold artifact if needed.")
    args = ap.parse_args()

    if not args.tier and not args.build_tt and not args.build_tt_bins:
        ap.print_help()
        return 1

    overall_t0 = time.time()
    with get_connection() as conn:
        if args.tier:
            n = _build_wf_bins_for_tier(conn, args.tier)
            log.info("wf_bins (%s): %d rows upserted.", args.tier, n)
            n = _build_is_bins_for_tier(conn, args.tier)
            log.info("is_bins (%s): %d rows upserted.", args.tier, n)
        if args.build_tt_bins:
            n = _build_tt_bins(conn)
            log.info("tt_bins: %d rows upserted.", n)
        if args.build_tt:
            n = _build_tt_thresholds(conn)
            log.info("tt_thresholds: %d rows upserted.", n)
    log.info("Total runtime: %.1fs.", time.time() - overall_t0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
