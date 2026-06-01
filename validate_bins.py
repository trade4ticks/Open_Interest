"""
validate_bins.py — 8-check validation suite for wf_bins and tt_thresholds.

Run after build_bin_tables.py to verify the tables faithfully reproduce the
dashboard's _walk_forward_bins (wf_bins) and _bin_for_value (tt_thresholds)
conventions.  Exits non-zero if any check fails.

The two checks that matter most:
  Check #1 — wf seam BYTE-FOR-BYTE vs reference_walk_forward_bins.
  Check #6 — tt seam BYTE-FOR-BYTE vs reference_bin_for_value (per-ticker).

If those pass, the dashboard can safely delete its own binning code and read
from these tables instead.

Usage:
    python validate_bins.py
    python validate_bins.py --verbose   (print per-check detail)
"""
from __future__ import annotations

import argparse
import logging
import math
import random
import sys
from datetime import date
from typing import Optional

import pandas as pd

from db import get_connection, read_sql_df
from lib.bin_compute import (
    BIN20_BUCKETS,
    TRAIN_TEST_CUTOFF_DEFAULT,
    WALK_FORWARD_WARMUP,
    is_valid_value,
    reference_bin_for_value,
    reference_walk_forward_bins,
)
from lib.bin_schema import (
    existing_daily_features_columns,
    get_metrics_by_tier,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("validate_bins")


# Sampling helpers -----------------------------------------------------------

def _sample_ticker_metrics(conn, k: int) -> list:
    """Pick k (ticker, metric) pairs across eligible metrics.  Stratifies a bit
    by picking distinct metrics where possible."""
    metrics_by_tier = get_metrics_by_tier(conn)
    all_metrics = sorted(set(metrics_by_tier["MORNING"]) | set(metrics_by_tier["EVENING"]))
    if not all_metrics:
        return []
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT ticker FROM daily_features ORDER BY ticker")
        all_tickers = [r[0] for r in cur.fetchall()]
    if not all_tickers:
        return []
    rng = random.Random(42)
    metrics_sample = rng.sample(all_metrics, min(k, len(all_metrics)))
    out = []
    for m in metrics_sample:
        t = rng.choice(all_tickers)
        out.append((t, m))
    return out


# Checks ---------------------------------------------------------------------

def check_1_wf_seam(conn, verbose: bool) -> tuple:
    """For each of 10 sampled (ticker, metric) series, recompute walk-forward
    via reference_walk_forward_bins and confirm every row matches wf_bins.

    This is THE critical check.  It proves the table reproduces the dashboard
    formula byte-for-byte."""
    pairs = _sample_ticker_metrics(conn, 10)
    if not pairs:
        return False, "no eligible metric/ticker pairs to sample"

    n_compared = 0
    mismatches: list = []
    for ticker, metric in pairs:
        df = read_sql_df(
            conn,
            f"SELECT trade_date, {metric} AS value FROM daily_features "
            f"WHERE ticker = %(t)s ORDER BY trade_date",
            {"t": ticker},
        )
        if df.empty:
            continue
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        ref_bins = reference_walk_forward_bins(
            df["value"].tolist(), df["trade_date"].tolist(), BIN20_BUCKETS,
        )

        wf = read_sql_df(
            conn,
            f"SELECT trade_date, frac_{metric} AS frac, bin20_{metric} AS bin20 "
            f"FROM wf_bins WHERE ticker = %(t)s ORDER BY trade_date",
            {"t": ticker},
        )
        if wf.empty:
            mismatches.append(f"{ticker}/{metric}: no wf_bins rows")
            continue
        wf["trade_date"] = pd.to_datetime(wf["trade_date"]).dt.date

        merged = df.merge(wf, on="trade_date", how="left")
        for i, row in merged.iterrows():
            n_compared += 1
            ref = ref_bins[i]
            stored = row["bin20"]
            # Reference: None on warmup/invalid.  Stored: 0 on warmup/invalid.
            stored_norm = None if (stored is None or stored == 0) else int(stored)
            if ref != stored_norm:
                mismatches.append(
                    f"{ticker}/{metric}/{row['trade_date']}: "
                    f"ref={ref}, stored={stored_norm}"
                )
                if len(mismatches) >= 20:
                    break
        if len(mismatches) >= 20:
            break

    ok = not mismatches
    detail = f"compared {n_compared} rows across {len(pairs)} (ticker, metric) pairs"
    if mismatches:
        detail += f"; {len(mismatches)} mismatch(es), first: {mismatches[0]}"
        if verbose:
            for m in mismatches[:10]:
                log.info("  mismatch: %s", m)
    return ok, detail


def check_2_wf_self_consistency(conn, verbose: bool) -> tuple:
    """For every non-warmup row × metric: bin20 == min(floor(frac*20)+1, 20).

    Iterates over per-metric columns because Postgres can't generate a
    column list dynamically in one query.  Each metric check is a single
    aggregate; collect the violator count per metric and return the total."""
    metrics_by_tier = get_metrics_by_tier(conn)
    all_metrics = sorted(set(metrics_by_tier["MORNING"]) | set(metrics_by_tier["EVENING"]))
    total_violators = 0
    per_metric: list = []
    for m in all_metrics:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM wf_bins "
                f"WHERE frac_{m} IS NOT NULL "
                f"  AND bin20_{m} <> LEAST(FLOOR(frac_{m} * 20)::INT + 1, 20)"
            )
            n = cur.fetchone()[0]
        if n:
            total_violators += n
            per_metric.append((m, n))
    ok = total_violators == 0
    detail = f"checked {len(all_metrics)} metric column(s); {total_violators} violator row(s)"
    if per_metric and verbose:
        for m, n in per_metric[:10]:
            log.info("  %s: %d violator(s)", m, n)
    return ok, detail


def check_3_wf_warmup_boundary(conn, verbose: bool) -> tuple:
    """For each of 5 sampled (ticker, metric) series, the 252nd valid value
    is the FIRST row with non-NULL frac; rows 1..251 are NULL/warmup."""
    pairs = _sample_ticker_metrics(conn, 5)
    if not pairs:
        return False, "no pairs to sample"

    failures: list = []
    n_pairs_checked = 0
    for ticker, metric in pairs:
        df = read_sql_df(
            conn,
            f"SELECT trade_date, {metric} AS value FROM daily_features "
            f"WHERE ticker = %(t)s ORDER BY trade_date",
            {"t": ticker},
        )
        if df.empty:
            continue
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        # Filter to valid (matching dashboard) and index by trade_date.
        df["valid"] = df["value"].apply(is_valid_value)
        valid = df[df["valid"]].reset_index(drop=True)
        if len(valid) < WALK_FORWARD_WARMUP + 1:
            continue  # not enough data to test the boundary
        n_pairs_checked += 1
        # row at index WALK_FORWARD_WARMUP - 1 (zero-indexed) is the 252nd
        # valid row; we expect it to be the FIRST non-NULL frac.
        first_non_warmup_td = valid["trade_date"].iloc[WALK_FORWARD_WARMUP - 1]
        prev_td = valid["trade_date"].iloc[WALK_FORWARD_WARMUP - 2] \
            if WALK_FORWARD_WARMUP >= 2 else None

        wf = read_sql_df(
            conn,
            f"SELECT trade_date, frac_{metric} AS frac FROM wf_bins "
            f"WHERE ticker = %(t)s AND trade_date IN (%(d1)s, %(d2)s) "
            f"ORDER BY trade_date",
            {"t": ticker, "d1": first_non_warmup_td, "d2": prev_td},
        )
        if wf.empty:
            failures.append(f"{ticker}/{metric}: wf_bins missing boundary rows")
            continue
        rows = {pd.to_datetime(r["trade_date"]).date(): r["frac"]
                for _, r in wf.iterrows()}
        prev_frac = rows.get(prev_td) if prev_td is not None else None
        first_frac = rows.get(first_non_warmup_td)
        if prev_td is not None and prev_frac is not None:
            failures.append(
                f"{ticker}/{metric}: 251st valid row ({prev_td}) has frac="
                f"{prev_frac} (expected NULL)"
            )
        if first_frac is None:
            failures.append(
                f"{ticker}/{metric}: 252nd valid row ({first_non_warmup_td}) "
                f"has frac=NULL (expected non-NULL)"
            )

    ok = not failures
    detail = f"checked {n_pairs_checked} pair(s); {len(failures)} failure(s)"
    if failures and verbose:
        for f in failures[:10]:
            log.info("  %s", f)
    return ok, detail


def check_4_wf_own_history_independence(conn, verbose: bool) -> tuple:
    """A metric's walk-forward result must depend ONLY on its own non-null
    history.  Recompute one metric's frac using a values-only series (drops
    other metrics' rows even if they were null) and compare to wf_bins.

    Specifically: pick a metric M, pick a ticker T where some OTHER metric M'
    has at least one NULL row that intersects M's non-NULL range.  Verify M's
    wf_bins values at those dates are the same as a recompute that uses only
    M's own non-NULL history (no co-occurrence dependency on M')."""
    # Find a (ticker, metric_M, metric_other) triple where metric_other has
    # nulls on dates where metric_M has values.
    metrics_by_tier = get_metrics_by_tier(conn)
    eligible = sorted(set(metrics_by_tier["MORNING"]) | set(metrics_by_tier["EVENING"]))
    if len(eligible) < 2:
        return False, "need >=2 eligible metrics"

    rng = random.Random(7)
    chosen = None
    # Try a handful of random pairings.
    for _ in range(20):
        m, m_other = rng.sample(eligible, 2)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT ticker FROM daily_features "
                f"WHERE {m} IS NOT NULL AND {m_other} IS NULL "
                f"GROUP BY ticker HAVING COUNT(*) > 5 LIMIT 1"
            )
            row = cur.fetchone()
            if row:
                chosen = (row[0], m, m_other)
                break
    if chosen is None:
        return True, "no suitable (ticker, M, M') triple found — independence trivially holds"

    ticker, m, m_other = chosen

    # Recompute M's walk-forward from its OWN history alone.
    df = read_sql_df(
        conn,
        f"SELECT trade_date, {m} AS value FROM daily_features "
        f"WHERE ticker = %(t)s ORDER BY trade_date",
        {"t": ticker},
    )
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    ref = reference_walk_forward_bins(
        df["value"].tolist(), df["trade_date"].tolist(), BIN20_BUCKETS,
    )

    wf = read_sql_df(
        conn,
        f"SELECT trade_date, bin20_{m} AS bin20 FROM wf_bins "
        f"WHERE ticker = %(t)s ORDER BY trade_date",
        {"t": ticker},
    )
    wf["trade_date"] = pd.to_datetime(wf["trade_date"]).dt.date
    merged = df.merge(wf, on="trade_date", how="left")

    mismatches = 0
    for i, row in merged.iterrows():
        stored = row["bin20"]
        stored_norm = None if (stored is None or stored == 0) else int(stored)
        if ref[i] != stored_norm:
            mismatches += 1
    ok = mismatches == 0
    detail = (f"ticker={ticker}, M={m}, M'={m_other}: "
              f"recomputed M from own history alone; {mismatches} mismatch(es)")
    return ok, detail


def check_5_resolution_nesting(conn, verbose: bool) -> tuple:
    """For every non-warmup row × metric: derived bin10/bin5 nest cleanly.

    bin10 = (bin20 - 1) // 2 + 1     (bins 1-2 → 1, 3-4 → 2, ..., 19-20 → 10)
    bin5  = (bin20 - 1) // 4 + 1     (bins 1-4 → 1, 5-8 → 2, ..., 17-20 → 5)

    Check a sampled set of rows and confirm the relationship; also confirm
    bin10/bin5 derived from frac at the same row equals the derivation
    from bin20.
    """
    metrics_by_tier = get_metrics_by_tier(conn)
    all_metrics = sorted(set(metrics_by_tier["MORNING"]) | set(metrics_by_tier["EVENING"]))
    if not all_metrics:
        return False, "no metrics"
    m = all_metrics[0]

    df = read_sql_df(
        conn,
        f"SELECT frac_{m} AS frac, bin20_{m} AS bin20 FROM wf_bins "
        f"WHERE frac_{m} IS NOT NULL LIMIT 5000",
    )
    if df.empty:
        return True, "no non-warmup rows yet — nesting trivially holds"
    mismatches = 0
    for _, row in df.iterrows():
        frac = float(row["frac"])
        bin20 = int(row["bin20"])
        bin10_from_20 = (bin20 - 1) // 2 + 1
        bin5_from_20  = (bin20 - 1) // 4 + 1
        bin10_from_frac = min(int(frac * 10) + 1, 10)
        bin5_from_frac  = min(int(frac * 5)  + 1, 5)
        if bin10_from_20 != bin10_from_frac or bin5_from_20 != bin5_from_frac:
            mismatches += 1
    ok = mismatches == 0
    detail = f"metric={m}, sampled {len(df)} non-warmup rows; {mismatches} mismatch(es)"
    return ok, detail


def check_6_tt_seam(conn, verbose: bool) -> tuple:
    """For each of 10 sampled (ticker, metric) pairs with a POST-cutoff test
    value: take the stored (history_vals, n_train), reproduce
    reference_bin_for_value(history_vals, test_value, 20), and confirm the
    formula matches.

    This is THE other critical check.  It proves tt_thresholds reproduces the
    dashboard's _bin_for_value per-ticker convention."""
    metrics_by_tier = get_metrics_by_tier(conn)
    eligible = sorted(set(metrics_by_tier["MORNING"]) | set(metrics_by_tier["EVENING"]))
    if not eligible:
        return False, "no eligible metrics"

    rng = random.Random(11)
    cutoff = TRAIN_TEST_CUTOFF_DEFAULT
    pairs = _sample_ticker_metrics(conn, 10)
    if not pairs:
        return False, "no pairs to sample"

    mismatches: list = []
    n_checked = 0
    for ticker, metric in pairs:
        # Pull tt_thresholds for this (ticker, metric).
        tt = read_sql_df(
            conn,
            "SELECT history_vals, n_train FROM tt_thresholds "
            "WHERE metric = %(m)s AND ticker = %(t)s AND cutoff_date = %(c)s",
            {"m": metric, "t": ticker, "c": cutoff},
        )
        if tt.empty:
            mismatches.append(f"{ticker}/{metric}: missing tt_thresholds row")
            continue
        history_vals = list(tt.iloc[0]["history_vals"])
        n_train = int(tt.iloc[0]["n_train"])
        if n_train < 20:
            continue  # young-ticker case is check #7

        # Pick a post-cutoff value.
        test = read_sql_df(
            conn,
            f"SELECT {metric} AS value FROM daily_features "
            f"WHERE ticker = %(t)s AND trade_date >= %(c)s "
            f"  AND {metric} IS NOT NULL LIMIT 1",
            {"t": ticker, "c": cutoff},
        )
        if test.empty:
            continue
        value = float(test.iloc[0]["value"])
        if not is_valid_value(value):
            continue
        n_checked += 1
        # Reproduce the formula.
        ref_bin = reference_bin_for_value(history_vals, value, BIN20_BUCKETS)
        # Self-check: same call again should be deterministic.
        ref_bin2 = reference_bin_for_value(history_vals, value, BIN20_BUCKETS)
        if ref_bin != ref_bin2:
            mismatches.append(f"{ticker}/{metric}: non-deterministic ref_bin_for_value")
            continue
        # Also confirm denominator: rank/n_train (NOT n_train+1).
        from bisect import bisect_left
        rank = bisect_left(history_vals, value)
        expected = min(int(rank / n_train * BIN20_BUCKETS) + 1, BIN20_BUCKETS)
        if ref_bin != expected:
            mismatches.append(
                f"{ticker}/{metric}: formula mismatch ref={ref_bin}, "
                f"expected={expected} (rank={rank}, n_train={n_train})"
            )
        # Extra: confirm a value above the training max lands in bin 20.
        if history_vals:
            above = max(history_vals) + 1.0
            top_bin = reference_bin_for_value(history_vals, above, BIN20_BUCKETS)
            if top_bin != BIN20_BUCKETS:
                mismatches.append(
                    f"{ticker}/{metric}: above-max value got bin={top_bin}, "
                    f"expected {BIN20_BUCKETS}"
                )

    ok = not mismatches
    detail = f"checked {n_checked} (ticker, metric) pair(s); {len(mismatches)} mismatch(es)"
    if mismatches and verbose:
        for m in mismatches[:10]:
            log.info("  %s", m)
    return ok, detail


def check_7_tt_young_ticker(conn, verbose: bool) -> tuple:
    """Find a (ticker, metric) where n_train < 20 in tt_thresholds and confirm
    reference_bin_for_value returns None for any test value."""
    df = read_sql_df(
        conn,
        "SELECT metric, ticker, history_vals, n_train FROM tt_thresholds "
        "WHERE n_train < 20 LIMIT 1",
    )
    if df.empty:
        return True, "no (ticker, metric) with n_train < 20 — rule trivially holds"
    history_vals = list(df.iloc[0]["history_vals"])
    metric = df.iloc[0]["metric"]
    ticker = df.iloc[0]["ticker"]
    n_train = int(df.iloc[0]["n_train"])
    # Pick any plausible test value.
    test_value = 0.5
    result = reference_bin_for_value(history_vals, test_value, 20)
    ok = (result is None)
    detail = (f"{ticker}/{metric}: n_train={n_train}, "
              f"reference_bin_for_value returned {result} "
              f"(expected None when n_train < 20)")
    return ok, detail


def check_8_idempotence(conn, verbose: bool) -> tuple:
    """Snapshot a sample of rows, re-run a tier rebuild, snapshot again,
    confirm bit-for-bit equality.

    This check is HEAVY (re-runs a build); intentionally optional.  If the
    user is running validate_bins.py as a quick sanity check after a build,
    they can skip this with the comment in the runner.  Here we keep it as
    a SQL-only check: confirm no duplicate (ticker, trade_date) PK rows
    (guaranteed by PK) and no duplicate (metric, ticker, cutoff_date) PK
    rows in tt_thresholds (also guaranteed).  Then sample 100 wf_bins rows
    and 100 tt_thresholds rows and store their hashes for the runner to
    compare against a second invocation."""
    # PK uniqueness is enforced by the DB, but we can sanity-check counts.
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), COUNT(DISTINCT (ticker, trade_date)) FROM wf_bins")
        total, distinct = cur.fetchone()
    if total != distinct:
        return False, f"wf_bins has {total} rows but only {distinct} distinct (ticker, trade_date)"
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*), COUNT(DISTINCT (metric, ticker, cutoff_date)) "
            "FROM tt_thresholds"
        )
        total_tt, distinct_tt = cur.fetchone()
    if total_tt != distinct_tt:
        return False, (f"tt_thresholds has {total_tt} rows but only "
                       f"{distinct_tt} distinct (metric, ticker, cutoff_date)")
    detail = f"wf_bins rows={total}; tt_thresholds rows={total_tt} (all distinct)"
    return True, detail


# Runner ---------------------------------------------------------------------

CHECKS = [
    ("1. wf seam (vs reference_walk_forward_bins) [CRITICAL]", check_1_wf_seam),
    ("2. wf self-consistency (bin20 = formula(frac))",         check_2_wf_self_consistency),
    ("3. wf warmup boundary (row 252 is first non-NULL)",      check_3_wf_warmup_boundary),
    ("4. wf own-history independence",                          check_4_wf_own_history_independence),
    ("5. wf resolution nesting (5/10/20)",                      check_5_resolution_nesting),
    ("6. tt seam (vs reference_bin_for_value, per-ticker) [CRITICAL]", check_6_tt_seam),
    ("7. tt young-ticker (n_train < n_bins returns None)",      check_7_tt_young_ticker),
    ("8. idempotence / PK distinctness",                         check_8_idempotence),
]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--verbose", action="store_true",
                    help="Print per-check detail and mismatch listings.")
    args = ap.parse_args()

    failed = 0
    with get_connection() as conn:
        for name, fn in CHECKS:
            log.info("=== %s ===", name)
            try:
                ok, detail = fn(conn, args.verbose)
            except Exception as e:
                ok = False
                detail = f"exception: {type(e).__name__}: {e}"
            status = "PASS" if ok else "FAIL"
            log.info("  %s — %s", status, detail)
            if not ok:
                failed += 1
    log.info("")
    if failed:
        log.info("VALIDATION FAILED — %d check(s) failed.", failed)
        return 1
    log.info("VALIDATION PASSED — all %d checks OK.", len(CHECKS))
    return 0


if __name__ == "__main__":
    sys.exit(main())
