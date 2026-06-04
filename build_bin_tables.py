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
    in_sample_series,
    train_test_history,
    walk_forward_series,
)
from lib.bin_schema import (
    existing_daily_features_columns,
    get_metrics_by_tier,
    sync_is_bins_schema,
    sync_wf_bins_schema,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("build_bin_tables")


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

    # Compute walk-forward per (ticker, metric).  Result accumulates as a wide
    # frame with one row per (ticker, trade_date) and two cols per metric.
    log.info("Computing walk-forward bins for %d metrics × %d tickers ...",
             len(metrics), df["ticker"].nunique())
    t0 = time.time()
    out_frames: list = []
    for ticker, sub in df.groupby("ticker", sort=False):
        sub = sub.sort_values("trade_date").reset_index(drop=True)
        row = {"ticker": ticker, "trade_date": sub["trade_date"].tolist()}
        dates = sub["trade_date"].tolist()
        for m in metrics:
            vals = sub[m].tolist()
            fracs, bin20s = walk_forward_series(vals, dates)
            row[f"frac_{m}"] = fracs
            row[f"bin20_{m}"] = bin20s
        # Expand into rows.
        n = len(dates)
        ticker_df = pd.DataFrame({
            "ticker":     [ticker] * n,
            "trade_date": dates,
            **{f"frac_{m}":  row[f"frac_{m}"]  for m in metrics},
            **{f"bin20_{m}": row[f"bin20_{m}"] for m in metrics},
        })
        out_frames.append(ticker_df)
    result = pd.concat(out_frames, ignore_index=True)
    log.info("  computed %d rows × %d metric pairs in %.1fs.",
             len(result), len(metrics), time.time() - t0)

    # Upsert.  Tier-specific SET clause; never touches the other tier's cols.
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

    log.info("Upserting to wf_bins ...")
    t0 = time.time()
    rows = [tuple(_pgify(r.get(c)) for c in write_cols)
            for r in result.to_dict(orient="records")]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=500)
    conn.commit()
    log.info("  upserted %d rows in %.1fs.", len(rows), time.time() - t0)
    return len(rows)


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

    # Compute in-sample bins per (ticker, metric).  The ranking population is
    # the ticker's full history — no expanding window, no warmup.
    log.info("Computing in-sample bins for %d metrics × %d tickers ...",
             len(metrics), df["ticker"].nunique())
    t0 = time.time()
    out_frames: list = []
    for ticker, sub in df.groupby("ticker", sort=False):
        sub   = sub.sort_values("trade_date").reset_index(drop=True)
        dates = sub["trade_date"].tolist()
        row: dict = {"ticker": ticker, "trade_date": dates}
        for m in metrics:
            vals = sub[m].tolist()
            fracs, bin20s = in_sample_series(vals, dates)
            row[f"frac_{m}"]  = fracs
            row[f"bin20_{m}"] = bin20s
        n = len(dates)
        ticker_df = pd.DataFrame({
            "ticker":     [ticker] * n,
            "trade_date": dates,
            **{f"frac_{m}":  row[f"frac_{m}"]  for m in metrics},
            **{f"bin20_{m}": row[f"bin20_{m}"] for m in metrics},
        })
        out_frames.append(ticker_df)
    result = pd.concat(out_frames, ignore_index=True)
    log.info("  computed %d rows × %d metric pairs in %.1fs.",
             len(result), len(metrics), time.time() - t0)

    # Upsert.  Tier-specific SET clause; never touches the other tier's cols.
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

    log.info("Upserting to is_bins ...")
    t0 = time.time()
    rows = [tuple(_pgify(r.get(c)) for c in write_cols)
            for r in result.to_dict(orient="records")]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=500)
    conn.commit()
    log.info("  upserted %d rows in %.1fs.", len(rows), time.time() - t0)
    return len(rows)


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
                    help="Rebuild wf_bins for this tier's metrics.")
    ap.add_argument("--build-tt", action="store_true",
                    help="(Re)build tt_thresholds for the standard cutoff.")
    args = ap.parse_args()

    if not args.tier and not args.build_tt:
        ap.print_help()
        return 1

    overall_t0 = time.time()
    with get_connection() as conn:
        if args.tier:
            n = _build_wf_bins_for_tier(conn, args.tier)
            log.info("wf_bins (%s): %d rows upserted.", args.tier, n)
            n = _build_is_bins_for_tier(conn, args.tier)
            log.info("is_bins (%s): %d rows upserted.", args.tier, n)
        if args.build_tt:
            n = _build_tt_thresholds(conn)
            log.info("tt_thresholds: %d rows upserted.", n)
    log.info("Total runtime: %.1fs.", time.time() - overall_t0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
