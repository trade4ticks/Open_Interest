"""
iv25d_quality_report.py — Read-only diagnostic measuring 25-delta IV data
quality in data/chain_eod/, to inform whether the 7 dormant skew metrics
(iv_25d_call_30d, iv_25d_put_30d, rr_25d_30d, bf_25d_30d, skew_25p_atm_30d,
skew_atm_25c_30d, zscore_rr_25d_30d) can be safely re-enabled.

Method (mirrors how the actual interpolation would consume the chain, so
the numbers reflect the metric's real inputs — NOT a looser proxy):

  Strike selection — per (ticker, trade_date, expiration), sort by strike;
  locate the adjacent strike pair where stored `delta` brackets +0.25 on
  the call side or -0.25 on the put side.  Stored delta is the same field
  the production metric would use; mild circularity is acknowledged and a
  BS-inverse sanity flag is computed alongside (independent of stored
  delta, using r=0 flat-vol BS with atm_iv_30d as sigma).

  DTE selection — per (ticker, trade_date), find exp_lower (max DTE <= 30)
  and exp_upper (min DTE > 30).  These are the two expirations the 30d
  IV interpolation would actually consume.  Measure iv_error at those
  expirations only, not at a coarser DTE bucket.

  Bracketability — straddling +/-0.25 alone is too loose (delta=0.05 to
  delta=0.45 would extrapolate through the smile).  Report at three
  closeness tolerances (0.03, 0.05, 0.10): a strike pair is "bracketable
  @ tol" iff straddle holds AND the closer-side |delta - target| <= tol.

  Liquidity tiering — median total_vol (last 90 days) per ticker, with
  quartile assignment across the universe.  Also tiered by total_oi from
  daily_features for comparison (since the user asked for both if trivial).

  Split-adjustment — uses the same chain_adj view as build_features.py
  (strike * split_factor), so measured strikes match what the metric
  would consume across split boundaries.

Outputs:
  data/diagnostics/iv25d_quality_report.csv         — aggregated slices
  data/diagnostics/iv25d_quality_report_detail.csv  — per-(ticker, date)

Write contract: NONE.  Reads chain_eod parquet + daily_features.  Touches
no feature tables, no metric_classification flags, no parquet stores.
"""
from __future__ import annotations

import logging
import math
import time
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
from scipy.stats import norm

from db import get_connection, read_sql_df
from lib.chain_store import has_data as chain_has_data
from lib.chain_store import parquet_glob as chain_parquet_glob
from lib.split_factors import load_splits, make_split_factors

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("iv25d_qa")


# ---- Configuration ------------------------------------------------------

TARGET_DTE              = 30
TARGET_DELTA            = 0.25
DELTA_TOLERANCES        = [0.03, 0.05, 0.10]
IV_ERROR_THRESHOLDS     = [0.001, 0.005, 0.010]
DTE_WIDTH_BUCKETS       = [(0, 7), (7, 15), (15, 30), (30, 9999)]
LIQUIDITY_LOOKBACK_DAYS = 90

OUTPUT_DIR  = Path("data/diagnostics")
SLICE_FILE  = OUTPUT_DIR / "iv25d_quality_report.csv"
DETAIL_FILE = OUTPUT_DIR / "iv25d_quality_report_detail.csv"

# Inverse-normal at p=0.25 (~-0.67449) — used to back out the BS strike
# that would have flat-vol delta = +/-0.25.
_Z_25 = float(norm.ppf(0.25))


# ---- BS-inverse sanity locator ------------------------------------------

def bs_inverse_strike(spot: float, iv: float, dte_years: float,
                      side: str) -> float | None:
    """Return the strike where flat-vol BS-delta = +/-0.25.

    r=0, q=0, no smile — a deliberately simple analytic locator that
    does NOT depend on the chain's stored delta.  Used only as a
    cross-check: if stored-delta-selected bracket strikes don't
    contain this target, that's a signal the snapshot delta diverges
    from BS theory.  NOT used as the metric's locator unless the
    divergence rate is high.

    Returns None on ANY degenerate input — non-finite, non-positive,
    or values that would overflow math.exp (e.g. an illiquid contract
    reporting iv >> 1 making the BS exponent blow past ~709).  The
    sanity-flag is secondary to the rest of the per-row measurement;
    a single bad contract must NOT bubble up and lose the ticker's
    entire iv_error / bracketability data.
    """
    # Reject None, NaN, Inf, and non-positive values up front — keeps
    # math.sqrt/math.exp from raising on the easy cases.
    if spot is None or iv is None or dte_years is None:
        return None
    if not (math.isfinite(spot) and math.isfinite(iv)
            and math.isfinite(dte_years)):
        return None
    if spot <= 0 or iv <= 0 or dte_years <= 0:
        return None
    try:
        sigma_sqrt_t = iv * math.sqrt(dte_years)
        drift = 0.5 * iv * iv * dte_years
        if side == "call":
            # N(d1) = 0.25 -> d1 = Z_25 (~-0.6745); K = S * exp(-Z_25*sigma*sqrt(t) + drift).
            exponent = -_Z_25 * sigma_sqrt_t + drift
        elif side == "put":
            # N(d1) = 0.75 -> d1 = -Z_25; K = S * exp(Z_25*sigma*sqrt(t) + drift).
            exponent = _Z_25 * sigma_sqrt_t + drift
        else:
            raise ValueError(f"side must be 'call' or 'put'; got {side!r}")
        # Final guard before math.exp: extreme exponents (large iv*sqrt(t)
        # or huge drift) would overflow.  Cap at ~700 to stay inside
        # double-precision range; anything beyond is a degenerate input
        # whose sanity-flag isn't meaningful anyway.
        if not math.isfinite(exponent) or exponent > 700 or exponent < -700:
            return None
        return spot * math.exp(exponent)
    except (OverflowError, ValueError):
        return None


# ---- Bracket-finding (strike-sorted lists in, bracket pair out) ---------

def find_delta_bracket(strikes: list[float], deltas: list[float],
                       iv_errors: list[float], target: float
                       ) -> dict | None:
    """Adjacent strike pair where delta crosses `target`.

    target > 0 (call side): delta is non-increasing in strike (ITM=high
    delta, OTM=low delta), so look for delta_lo >= target >= delta_hi.
    target < 0 (put side): delta is non-decreasing in strike (ITM=-1,
    OTM=0), so look for delta_lo <= target <= delta_hi.
    Strikes/deltas/iv_errors must be sorted ascending by strike.
    """
    n = len(strikes)
    if n < 2:
        return None
    if target > 0:
        for i in range(n - 1):
            if deltas[i] is None or deltas[i + 1] is None:
                continue
            if deltas[i] >= target >= deltas[i + 1]:
                return _pack_bracket(strikes, deltas, iv_errors, i)
    else:
        for i in range(n - 1):
            if deltas[i] is None or deltas[i + 1] is None:
                continue
            if deltas[i] <= target <= deltas[i + 1]:
                return _pack_bracket(strikes, deltas, iv_errors, i)
    return None


def find_atm_bracket(strikes: list[float], deltas: list[float],
                     iv_errors: list[float], spot: float) -> dict | None:
    """Adjacent strike pair bracketing `spot` (used for ATM iv_error
    comparison; matches build_features.py's atm_iv_* convention: calls
    only, two strikes around spot, linear interp).
    """
    if spot is None or spot <= 0 or len(strikes) < 2:
        return None
    for i in range(len(strikes) - 1):
        if strikes[i] <= spot <= strikes[i + 1]:
            return _pack_bracket(strikes, deltas, iv_errors, i)
    return None


def _pack_bracket(strikes, deltas, iv_errors, i) -> dict:
    return {
        "strike_lo":   strikes[i],
        "delta_lo":    deltas[i],
        "iv_error_lo": iv_errors[i],
        "strike_hi":   strikes[i + 1],
        "delta_hi":    deltas[i + 1],
        "iv_error_hi": iv_errors[i + 1],
    }


# ---- Per-ticker measurement --------------------------------------------

def measure_ticker(pg_conn, ticker: str) -> pd.DataFrame:
    """Return one row per (trade_date) carrying all bracket measurements
    + iv_errors + BS sanity flags + total_vol for tier assignment.
    Empty DataFrame if no chain_eod data, no daily_features data, or no
    measurable (date, expiration) combinations.
    """
    if not chain_has_data(ticker):
        return pd.DataFrame()

    # spot_pc + atm_iv_30d (BS-inverse needs both).
    df_spots = read_sql_df(
        pg_conn,
        "SELECT trade_date, spot_pc, atm_iv_30d FROM daily_features "
        "WHERE ticker = %(t)s ORDER BY trade_date",
        {"t": ticker},
    )
    if df_spots.empty:
        return pd.DataFrame()
    df_spots["trade_date"] = pd.to_datetime(df_spots["trade_date"]).dt.date
    spots = df_spots.set_index("trade_date")

    splits_df = load_splits(pg_conn, ticker)

    con = duckdb.connect(database=":memory:")

    chain_dates = con.execute(
        f"SELECT DISTINCT trade_date FROM "
        f"read_parquet('{chain_parquet_glob(ticker)}') ORDER BY trade_date"
    ).fetchall()
    if not chain_dates:
        con.close()
        return pd.DataFrame()
    chain_dates_list = [r[0] for r in chain_dates]
    sf_df = make_split_factors(splits_df, chain_dates_list)
    con.register("split_factors", sf_df)

    # Mirror build_features.py:1253-1263 — same chain_adj view, same
    # split-adjustment (strike * adj_factor; volume / adj_factor; delta,
    # implied_vol, iv_error untouched).
    con.execute(
        f"CREATE OR REPLACE VIEW chain_adj AS "
        f"SELECT raw.trade_date, raw.expiration, "
        f"       raw.strike * COALESCE(sf.adj_factor, 1.0) AS strike, "
        f"       raw.option_type, "
        f"       raw.volume / COALESCE(sf.adj_factor, 1.0) AS volume, "
        f"       raw.implied_vol, raw.delta, raw.iv_error "
        f"FROM (SELECT * FROM read_parquet('{chain_parquet_glob(ticker)}')) raw "
        f"LEFT JOIN split_factors sf ON raw.trade_date = sf.trade_date"
    )

    # total_vol per trade_date — for the median-based liquidity tier.
    df_vol = con.execute(
        "SELECT trade_date, SUM(volume)::DOUBLE AS total_vol "
        "FROM chain_adj GROUP BY trade_date"
    ).df()
    df_vol["trade_date"] = pd.to_datetime(df_vol["trade_date"]).dt.date
    vols = df_vol.set_index("trade_date")["total_vol"]

    # Per (trade_date, expiration, option_type) — strike-sorted arrays of
    # (strike, delta, iv_error).  DuckDB LIST(... ORDER BY ...) keeps them
    # aligned.  Drop rows with NULL delta or non-positive IV upstream so
    # the bracket-finder doesn't have to.
    df_pe = con.execute(
        """
        SELECT
            trade_date, expiration, option_type,
            LIST(strike   ORDER BY strike) AS strikes,
            LIST(delta    ORDER BY strike) AS deltas,
            LIST(iv_error ORDER BY strike) AS iv_errors
        FROM chain_adj
        WHERE delta IS NOT NULL
          AND implied_vol IS NOT NULL AND implied_vol > 0
        GROUP BY trade_date, expiration, option_type
        """
    ).df()
    con.close()

    if df_pe.empty:
        return pd.DataFrame()

    df_pe["trade_date"] = pd.to_datetime(df_pe["trade_date"]).dt.date
    df_pe["expiration"] = pd.to_datetime(df_pe["expiration"]).dt.date
    df_pe["dte"] = df_pe.apply(lambda r: (r["expiration"] - r["trade_date"]).days,
                               axis=1)

    rows = []
    for tdate, g in df_pe.groupby("trade_date", sort=True):
        # Build the DTE-bracket pair around TARGET_DTE.  exp_lower = max
        # expiration with DTE <= 30; exp_upper = min expiration with DTE > 30.
        # Both must exist (matches the metric's "no nearest-neighbor fallback"
        # convention) or the row is dropped.
        exp_dte = g[["expiration", "dte"]].drop_duplicates().sort_values("dte")
        lower = exp_dte[exp_dte["dte"] <= TARGET_DTE]
        upper = exp_dte[exp_dte["dte"] >  TARGET_DTE]
        if lower.empty or upper.empty:
            continue
        exp_lower = lower.iloc[-1]["expiration"]
        dte_lower = int(lower.iloc[-1]["dte"])
        exp_upper = upper.iloc[ 0]["expiration"]
        dte_upper = int(upper.iloc[ 0]["dte"])

        spot   = spots.loc[tdate, "spot_pc"]   if tdate in spots.index else None
        atm_iv = spots.loc[tdate, "atm_iv_30d"] if tdate in spots.index else None
        total_vol = vols.loc[tdate] if tdate in vols.index else None

        row = {
            "ticker":            ticker,
            "trade_date":        tdate,
            "exp_lower":         exp_lower,
            "exp_upper":         exp_upper,
            "dte_lower":         dte_lower,
            "dte_upper":         dte_upper,
            "bracket_width_dte": dte_upper - dte_lower,
            "spot_pc":           spot,
            "atm_iv_30d":        atm_iv,
            "total_vol":         total_vol,
        }

        for exp_tag, exp_date, exp_dte_val in (
            ("lower", exp_lower, dte_lower),
            ("upper", exp_upper, dte_upper),
        ):
            for side_tag, target, opt_type in (
                ("call_25d", +TARGET_DELTA, "C"),
                ("put_25d",  -TARGET_DELTA, "P"),
            ):
                sub = g[(g["expiration"] == exp_date)
                        & (g["option_type"] == opt_type)]
                if sub.empty:
                    continue
                strikes = list(sub["strikes"].iloc[0])
                deltas  = list(sub["deltas"].iloc[0])
                iv_errs = list(sub["iv_errors"].iloc[0])
                br = find_delta_bracket(strikes, deltas, iv_errs, target)
                if br is None:
                    continue
                pfx = f"{side_tag}_{exp_tag}_"
                row[pfx + "strike_lo"]   = br["strike_lo"]
                row[pfx + "delta_lo"]    = br["delta_lo"]
                row[pfx + "iv_error_lo"] = br["iv_error_lo"]
                row[pfx + "strike_hi"]   = br["strike_hi"]
                row[pfx + "delta_hi"]    = br["delta_hi"]
                row[pfx + "iv_error_hi"] = br["iv_error_hi"]
                # BS-inverse sanity flag (only if atm_iv is usable).
                if (atm_iv is not None and atm_iv > 0
                        and spot is not None and spot > 0):
                    bs_k = bs_inverse_strike(
                        spot, atm_iv, exp_dte_val / 365.0,
                        "call" if target > 0 else "put",
                    )
                    if bs_k is not None:
                        row[pfx + "bs_agree"] = (
                            br["strike_lo"] <= bs_k <= br["strike_hi"]
                        )

            # ATM bracket (calls only, around spot — matches atm_iv_* logic).
            csub = g[(g["expiration"] == exp_date) & (g["option_type"] == "C")]
            if not csub.empty and spot is not None and spot > 0:
                strikes = list(csub["strikes"].iloc[0])
                deltas  = list(csub["deltas"].iloc[0])
                iv_errs = list(csub["iv_errors"].iloc[0])
                ab = find_atm_bracket(strikes, deltas, iv_errs, spot)
                if ab is not None:
                    pfx = f"atm_{exp_tag}_"
                    row[pfx + "iv_error_lo"] = ab["iv_error_lo"]
                    row[pfx + "iv_error_hi"] = ab["iv_error_hi"]

        rows.append(row)

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ---- Tier assignment ----------------------------------------------------

def assign_tiers(detail_df: pd.DataFrame, pg_conn) -> pd.DataFrame:
    """Add vol_tier and oi_tier columns (Q1-Q4) based on median total_vol
    (from detail_df, last 90 days) and median total_oi (from
    daily_features, last 90 days)."""
    if detail_df.empty:
        return detail_df

    # vol tiers: median total_vol per ticker over the last 90 days.
    cutoff = detail_df["trade_date"].max() - timedelta(days=LIQUIDITY_LOOKBACK_DAYS)
    recent = detail_df[detail_df["trade_date"] >= cutoff]
    vol_med = (recent.groupby("ticker")["total_vol"].median()
                     .rename("median_vol").reset_index())
    vol_med["vol_tier"] = _to_quartile(vol_med["median_vol"])

    # oi tiers: median total_oi per ticker over the last 90 days, from
    # daily_features (already populated).
    df_oi = read_sql_df(
        pg_conn,
        f"""
        WITH recent AS (
            SELECT ticker, trade_date, total_oi
            FROM daily_features
            WHERE trade_date >= CURRENT_DATE - INTERVAL '{LIQUIDITY_LOOKBACK_DAYS} days'
              AND total_oi IS NOT NULL
        )
        SELECT ticker,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_oi) AS median_oi
        FROM recent
        GROUP BY ticker
        """,
    )
    df_oi["oi_tier"] = _to_quartile(df_oi["median_oi"])

    out = detail_df.merge(vol_med[["ticker", "vol_tier"]], on="ticker", how="left")
    out = out.merge(df_oi[["ticker", "oi_tier"]],          on="ticker", how="left")
    return out


def _to_quartile(series: pd.Series) -> pd.Series:
    """Quartile labels Q1-Q4 by rank.  duplicates='drop' protects against
    flat-tail ties causing qcut to error."""
    try:
        cats = pd.qcut(series.rank(method="first"), q=4,
                       labels=["Q1", "Q2", "Q3", "Q4"])
        return cats.astype(str)
    except ValueError:
        # Fallback: too few unique values to form 4 quantiles.  Pool all
        # into Q4 (highest tier, conservative — won't underestimate noise
        # by lumping low-liquidity tickers with high-liquidity ones).
        return pd.Series(["Q4"] * len(series), index=series.index)


# ---- Aggregation --------------------------------------------------------

def _dte_bucket(width: int) -> str:
    for lo, hi in DTE_WIDTH_BUCKETS:
        if lo <= width < hi:
            return f"[{lo}-{hi})"
    return f"[{DTE_WIDTH_BUCKETS[-1][0]}+]"


def _bracket_ok(slice_df: pd.DataFrame, side: str, exp_tag: str,
                target: float, tol: float) -> pd.Series:
    """Per-row boolean: bracket exists AND closer-side |delta - target| <= tol."""
    lo_col = f"{side}_{exp_tag}_delta_lo"
    hi_col = f"{side}_{exp_tag}_delta_hi"
    if lo_col not in slice_df.columns or hi_col not in slice_df.columns:
        return pd.Series(False, index=slice_df.index)
    lo = slice_df[lo_col]
    hi = slice_df[hi_col]
    closer = pd.concat([(lo - target).abs(), (hi - target).abs()],
                       axis=1).min(axis=1)
    return lo.notna() & hi.notna() & (closer <= tol)


def _all_iv_errs_below(slice_df: pd.DataFrame, side: str,
                       threshold: float) -> pd.Series:
    """Per-row boolean: all 4 iv_errors (2 strikes x 2 expirations) <=
    threshold AND all present."""
    cols = [
        f"{side}_lower_iv_error_lo", f"{side}_lower_iv_error_hi",
        f"{side}_upper_iv_error_lo", f"{side}_upper_iv_error_hi",
    ]
    missing = [c for c in cols if c not in slice_df.columns]
    if missing:
        return pd.Series(False, index=slice_df.index)
    sub = slice_df[cols]
    return (sub.notna().all(axis=1) & (sub <= threshold).all(axis=1))


def aggregate_slices(detail_df: pd.DataFrame) -> pd.DataFrame:
    """Pool iv_errors + bracketability + survival per
    (tier_metric, tier, dte_bucket, side) slice."""
    if detail_df.empty:
        return pd.DataFrame()

    df = detail_df.copy()
    df["dte_bucket"] = df["bracket_width_dte"].apply(_dte_bucket)

    rows = []
    for tier_metric in ("vol", "oi"):
        tier_col = f"{tier_metric}_tier"
        if tier_col not in df.columns:
            continue
        for tier in ("Q1", "Q2", "Q3", "Q4"):
            for db in sorted(df["dte_bucket"].dropna().unique()):
                slice_ = df[(df[tier_col] == tier) & (df["dte_bucket"] == db)]
                if slice_.empty:
                    continue
                for side in ("call_25d", "put_25d"):
                    target = +TARGET_DELTA if side == "call_25d" else -TARGET_DELTA
                    row = {
                        "tier_metric": tier_metric,
                        "tier":        tier,
                        "dte_bucket":  db,
                        "side":        side,
                        "n_dates":     len(slice_),
                    }

                    # (a) iv_error percentiles at the 25d bracketing strikes.
                    cols_25d = [
                        f"{side}_lower_iv_error_lo", f"{side}_lower_iv_error_hi",
                        f"{side}_upper_iv_error_lo", f"{side}_upper_iv_error_hi",
                    ]
                    iv_errs_25d = pd.concat(
                        [slice_[c].dropna() for c in cols_25d
                         if c in slice_.columns], ignore_index=True,
                    )
                    row["n_iv_errors_25d"] = len(iv_errs_25d)
                    for p in (50, 90, 95, 99):
                        row[f"iv_err_25d_p{p}"] = (
                            iv_errs_25d.quantile(p / 100)
                            if len(iv_errs_25d) else None
                        )

                    # (a-comparison) ATM iv_error percentiles at the same exp pair.
                    atm_cols = [
                        "atm_lower_iv_error_lo", "atm_lower_iv_error_hi",
                        "atm_upper_iv_error_lo", "atm_upper_iv_error_hi",
                    ]
                    iv_errs_atm = pd.concat(
                        [slice_[c].dropna() for c in atm_cols
                         if c in slice_.columns], ignore_index=True,
                    )
                    row["n_iv_errors_atm"] = len(iv_errs_atm)
                    for p in (50, 90, 95, 99):
                        row[f"iv_err_atm_p{p}"] = (
                            iv_errs_atm.quantile(p / 100)
                            if len(iv_errs_atm) else None
                        )

                    # (b) Bracketability rate per tolerance — both lower
                    # AND upper exp brackets must be valid (the metric needs
                    # both to interpolate across DTE).
                    n = len(slice_)
                    for tol in DELTA_TOLERANCES:
                        ok = (_bracket_ok(slice_, side, "lower", target, tol)
                              & _bracket_ok(slice_, side, "upper", target, tol))
                        row[f"bracketable_pct_tol_{int(tol*100):02d}"] = (
                            ok.sum() / n if n else None
                        )

                    # (c) Survival rate: bracketable @ tol AND all 4 strikes'
                    # iv_error <= threshold.  3x3 grid (3 tols x 3 thresholds).
                    for tol in DELTA_TOLERANCES:
                        for thr in IV_ERROR_THRESHOLDS:
                            ok = (_bracket_ok(slice_, side, "lower", target, tol)
                                  & _bracket_ok(slice_, side, "upper", target, tol)
                                  & _all_iv_errs_below(slice_, side, thr))
                            row[(f"survival_pct_tol_{int(tol*100):02d}"
                                 f"_thr_{int(thr*1000):03d}")] = (
                                ok.sum() / n if n else None
                            )

                    # BS-inverse sanity-flag rate: among rows where both
                    # exp brackets exist at default tol=0.05, what fraction
                    # had BOTH bs_agree = True?  Low rate = stored delta
                    # diverges from BS theory (consider switching locator).
                    bs_lo = f"{side}_lower_bs_agree"
                    bs_hi = f"{side}_upper_bs_agree"
                    if bs_lo in slice_.columns and bs_hi in slice_.columns:
                        ok_05 = (_bracket_ok(slice_, side, "lower", target, 0.05)
                                 & _bracket_ok(slice_, side, "upper", target, 0.05))
                        denom = ok_05.sum()
                        if denom > 0:
                            bs_both_ok = (slice_[bs_lo].fillna(False)
                                          & slice_[bs_hi].fillna(False))
                            row["bs_agree_pct_among_bracketable_05"] = (
                                (ok_05 & bs_both_ok).sum() / denom
                            )
                        else:
                            row["bs_agree_pct_among_bracketable_05"] = None
                    else:
                        row["bs_agree_pct_among_bracketable_05"] = None

                    rows.append(row)

    return pd.DataFrame(rows)


# ---- Main ---------------------------------------------------------------

def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=" * 60)
    log.info("IV 25-delta data-quality report")
    log.info("=" * 60)

    with get_connection() as conn:
        tickers = read_sql_df(
            conn, "SELECT DISTINCT ticker FROM daily_features ORDER BY ticker"
        )["ticker"].tolist()
        log.info("Tickers in scope: %d", len(tickers))

        frames = []
        t_total = time.time()
        for i, t in enumerate(tickers, 1):
            t0 = time.time()
            try:
                df = measure_ticker(conn, t)
            except Exception as exc:
                log.warning("  %d/%d %s: measure failed — %s",
                            i, len(tickers), t, exc)
                continue
            elapsed = time.time() - t0
            if df.empty:
                log.info("  %d/%d %s: 0 rows (%.1fs)", i, len(tickers), t, elapsed)
                continue
            frames.append(df)
            log.info("  %d/%d %s: %d rows (%.1fs)",
                     i, len(tickers), t, len(df), elapsed)

        if not frames:
            log.error("No tickers produced rows. Output files not written.")
            return 1

        detail = pd.concat(frames, ignore_index=True)
        log.info("Detail rows: %d  (total measure time %.1fs)",
                 len(detail), time.time() - t_total)

        detail = assign_tiers(detail, conn)

    log.info("Writing detail CSV → %s", DETAIL_FILE)
    detail.to_csv(DETAIL_FILE, index=False)

    log.info("Aggregating slices ...")
    slices = aggregate_slices(detail)
    log.info("Slice rows: %d", len(slices))

    if not slices.empty:
        print()
        sort_cols = ["tier_metric", "tier", "dte_bucket", "side"]
        slices_sorted = slices.sort_values(sort_cols).reset_index(drop=True)
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", 240)
        pd.set_option("display.float_format",
                      lambda x: f"{x:.4f}" if pd.notna(x) else "—")
        print(slices_sorted.to_string(index=False))
        print()

    log.info("Writing slice CSV → %s", SLICE_FILE)
    slices.to_csv(SLICE_FILE, index=False)

    log.info("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
