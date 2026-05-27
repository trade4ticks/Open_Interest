"""
test_iv_endpoint.py — Standalone diagnostic for ThetaData's 15:45 greeks
endpoint, evaluating whether 25-delta OTM data is reliable across the full
project history (2019-2026).

This script is throwaway. It must not be wired into the pipeline. It:
  * does NOT import from fetch_iv_chain.py, build_features.py, lib/thetadata.py,
    or any other project module (uses raw HTTP via `requests` directly)
  * does NOT read or write to option_iv_daily, daily_features, or any other
    Postgres table
  * does NOT write to the parquet OI store

What it does:
  Samples ~1 trade_date per month across the full 2019..today window for three
  tickers (AAPL, AAL, XLK), hits /v3/option/history/greeks/first_order at
  15:45 ET on each sample date, and computes two metrics per (ticker, date)
  cell:

    atm_iv_30d        - representative ATM metric (close-to-the-money,
                        forgiving of wing data quality)
    rr_25d_30d        - 25-delta risk reversal at 30 DTE. Needs BOTH wings
                        to interpolate cleanly, so it stress-tests the
                        endpoint's OTM data quality (which is the actual
                        question this script exists to answer).

  Each cell is classified into one of:
    ok                          - all metrics computed, sanity-clean
    no_expirations_in_window    - list/expirations had nothing in [+7, +90] days
    total_outage                - HTTP succeeded but both expirations returned empty
    http_error_exp_{a,b}        - the actual HTTP call failed
    sparse_otm_strikes          - data returned, deltas valid, but 25-delta
                                  strike not bracketed in the chain
    garbage_data_exp_{a,b}      - deltas or IVs are out of plausible ranges
                                  (e.g. call delta > 1, IV < 0, IV > 500%)
    unexpected_error            - any other exception

  For comparison, each cell also fetches /v3/option/history/greeks/eod and
  computes atm_iv_30d the same way — the diff (15:45 ATM vs EOD ATM) shows
  whether the two endpoints agree on the easy ATM number, which is a
  prerequisite for trusting the harder 25-delta numbers from 15:45.

Output: iv_endpoint_test_results.csv (in cwd) with one row per cell.
        Summary printed to stdout: pass rate overall, failure category
        counts, and a per-year breakdown so you can spot regime clusters.

How to run:
    # Set the ThetaData base URL (matches the production env var)
    export THETADATA_BASE_URL=http://100.76.94.99:25503
    python test_iv_endpoint.py

Runtime: roughly 89 months * 3 tickers = ~267 cells. Each cell makes 2
greeks/first_order calls + 1 greeks/eod call + (cached) list/expirations.
At ~1-2s per call serial, expect 15-25 minutes total. No concurrency is
used (avoids rate-limit headaches; this is throwaway diagnostic code).

Rate limits: the ThetaData subscription is 4 concurrent requests; this
script issues 1 at a time, well under the limit. No sleeps needed unless
the terminal starts returning 429s — in which case the script just logs
the failure and moves on (the bad cell shows up in the CSV).
"""
from __future__ import annotations

import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import pandas_market_calendars as mcal
import requests

# ---------------------------------------------------------------------------
# Constants — edit here to change the sample.
# ---------------------------------------------------------------------------
BASE_URL     = os.environ.get("THETADATA_BASE_URL", "http://localhost:25503")
TICKERS      = ["AAPL", "AAL", "XLK"]
START_DATE   = date(2019, 1, 1)
END_DATE     = date.today()
TARGET_DOM   = 15           # day-of-month to aim for; snapped to nearest trading day
TARGET_DTE   = 30           # interpolate IVs to this DTE across two expirations
STRIKE_RANGE = 30           # 61 strikes around ATM — wide enough for 25-delta on these names
TIMEOUT      = 60           # per-call HTTP timeout (seconds)
OUT_CSV      = Path("iv_endpoint_test_results.csv")

# ---------------------------------------------------------------------------
# Minimal HTTP / parse helpers — duplicated from lib/thetadata.py on purpose,
# so this script cannot be silently broken by changes to production code.
# ---------------------------------------------------------------------------

def _get(endpoint: str, params: dict, timeout: int = TIMEOUT):
    """GET <BASE_URL><endpoint>?<params>&format=json. Returns parsed JSON.
    Builds the query string manually so * is not percent-encoded."""
    full = {**params, "format": "json"}
    qs = "&".join(f"{k}={v}" for k, v in full.items())
    url = f"{BASE_URL}{endpoint}?{qs}"
    resp = requests.get(url, timeout=timeout)
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
    return resp.json()


def _parse_rows(data) -> list[dict]:
    """Normalize ThetaData v3 response shapes to a list of dicts.
    Mirrors lib/thetadata.py:_parse_rows behavior but standalone."""
    if not data:
        return []
    if isinstance(data, dict) and "header" in data and "response" in data:
        fields = data["header"].get("format", []) or []
        return [dict(zip(fields, row)) for row in (data.get("response") or []) if row]
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    if isinstance(data, dict):
        # Some endpoints return single-dict or {symbol: rows} shapes — best-effort.
        keys = list(data.keys())
        if keys and isinstance(data[keys[0]], list):
            return [r for r in data[keys[0]] if isinstance(r, dict)]
    return []


def _parse_ymd(s) -> Optional[date]:
    """Parse YYYYMMDD or YYYY-MM-DD into a date. Returns None on failure."""
    if s is None:
        return None
    s = str(s).strip()
    if len(s) == 8 and s.isdigit():
        try:
            return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        except ValueError:
            return None
    try:
        return date.fromisoformat(s[:10])
    except (ValueError, IndexError):
        return None


# ---------------------------------------------------------------------------
# ThetaData calls — bare and explicit, no retry logic on purpose.
# A failing cell IS the signal we're looking for.
# ---------------------------------------------------------------------------

def list_expirations(ticker: str) -> tuple[list[date], Optional[str]]:
    """All expirations the terminal knows for this symbol. Returns ([], err) on failure."""
    try:
        data = _get("/v3/option/list/expirations", {"symbol": ticker.upper()})
    except Exception as e:
        return [], str(e)[:200]
    out: list[date] = []
    for r in _parse_rows(data):
        d = _parse_ymd(r.get("expiration") if isinstance(r, dict) else r)
        if d:
            out.append(d)
    return sorted(set(out)), None


def fetch_first_order(ticker: str, expiration: date, trade_date: date) -> list[dict]:
    """Fetch 15:45 first-order greeks for one (ticker, expiration, date)."""
    params = {
        "symbol":       ticker.upper(),
        "expiration":   expiration.strftime("%Y%m%d"),
        "strike_range": STRIKE_RANGE,
        "start_date":   trade_date.strftime("%Y%m%d"),
        "end_date":     trade_date.strftime("%Y%m%d"),
        "interval":     "5m",
        "start_time":   "15:45",
        "end_time":     "15:45",
    }
    data = _get("/v3/option/history/greeks/first_order", params)
    return _parse_rows(data)


def fetch_eod_chain(ticker: str, trade_date: date) -> list[dict]:
    """Fetch EOD greeks across the entire chain on one trade_date.
    Used only to compute the ATM-IV-EOD comparison number."""
    params = {
        "symbol":     ticker.upper(),
        "expiration": "*",
        "start_date": trade_date.strftime("%Y%m%d"),
        "end_date":   trade_date.strftime("%Y%m%d"),
    }
    data = _get("/v3/option/history/greeks/eod", params, timeout=120)
    return _parse_rows(data)


# ---------------------------------------------------------------------------
# Chain parsing and metric computation
# ---------------------------------------------------------------------------

def parse_chain(rows: list[dict]) -> pd.DataFrame:
    """Convert raw rows to DataFrame[expiration, strike, option_type, implied_vol, delta]."""
    out = []
    for r in rows:
        # ThetaData uses 'right' or 'option_type'; values vary between C/P and call/put.
        otype = (r.get("option_type") or r.get("right") or "").strip().upper()
        if otype in ("CALL", "C"):
            otype = "C"
        elif otype in ("PUT", "P"):
            otype = "P"
        else:
            continue
        out.append({
            "expiration":  _parse_ymd(r.get("expiration")),
            "strike":      r.get("strike"),
            "option_type": otype,
            "implied_vol": r.get("implied_vol") if r.get("implied_vol") is not None else r.get("iv"),
            "delta":       r.get("delta"),
        })
    df = pd.DataFrame(out)
    if df.empty:
        return df
    for col in ("strike", "implied_vol", "delta"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["strike", "implied_vol", "delta", "expiration"])
    df = df.drop_duplicates(subset=["expiration", "strike", "option_type"])
    return df


def check_garbage(df: pd.DataFrame) -> Optional[str]:
    """If >50% of deltas are out-of-range or >30% of IVs are out-of-range, return a reason string."""
    if df.empty:
        return None  # emptiness is handled elsewhere
    calls = df[df["option_type"] == "C"]
    puts  = df[df["option_type"] == "P"]
    if not calls.empty:
        bad = ((calls["delta"] < -0.05) | (calls["delta"] > 1.05)).sum()
        if bad > len(calls) * 0.5:
            return f"call_deltas_out_of_range:{bad}/{len(calls)}"
    if not puts.empty:
        bad = ((puts["delta"] < -1.05) | (puts["delta"] > 0.05)).sum()
        if bad > len(puts) * 0.5:
            return f"put_deltas_out_of_range:{bad}/{len(puts)}"
    bad_iv = ((df["implied_vol"] <= 0) | (df["implied_vol"] > 5.0)).sum()
    if bad_iv > len(df) * 0.3:
        return f"ivs_out_of_range:{bad_iv}/{len(df)}"
    return None


def interp_iv_at_delta(side_df: pd.DataFrame, target_delta: float) -> tuple[Optional[float], Optional[str]]:
    """Linearly interpolate IV vs delta at target_delta within one option_type slice.
    Returns (iv, reason) — reason is None on success, else a short failure code."""
    if side_df.empty or len(side_df) < 2:
        return None, "too_few_strikes"
    df = side_df.sort_values("delta").reset_index(drop=True)
    above = df[df["delta"] >= target_delta]
    below = df[df["delta"] <= target_delta]
    if above.empty or below.empty:
        # target delta isn't in the range covered by the returned chain
        return None, "delta_out_of_range"
    d_hi = float(above.iloc[0]["delta"])
    iv_hi = float(above.iloc[0]["implied_vol"])
    d_lo = float(below.iloc[-1]["delta"])
    iv_lo = float(below.iloc[-1]["implied_vol"])
    if d_hi == d_lo:
        return iv_hi, None
    iv = iv_lo + (iv_hi - iv_lo) * (target_delta - d_lo) / (d_hi - d_lo)
    return iv, None


def compute_one_expiration(chain_df: pd.DataFrame) -> dict:
    """Compute ATM IV, 25-delta call IV, 25-delta put IV for one expiration's chain."""
    calls = chain_df[chain_df["option_type"] == "C"]
    puts  = chain_df[chain_df["option_type"] == "P"]

    atm,    atm_reason = interp_iv_at_delta(calls, 0.50)
    iv_25c, c_reason   = interp_iv_at_delta(calls, 0.25)
    iv_25p, p_reason   = interp_iv_at_delta(puts, -0.25)

    return {
        "atm_iv":          atm,
        "iv_25c":          iv_25c,
        "iv_25p":          iv_25p,
        "atm_reason":      atm_reason,
        "iv_25c_reason":   c_reason,
        "iv_25p_reason":   p_reason,
        "n_calls":         len(calls),
        "n_puts":          len(puts),
        "min_call_delta":  float(calls["delta"].min()) if not calls.empty else None,
        "max_call_delta":  float(calls["delta"].max()) if not calls.empty else None,
        "min_put_delta":   float(puts["delta"].min())  if not puts.empty  else None,
        "max_put_delta":   float(puts["delta"].max())  if not puts.empty  else None,
    }


def interp_to_dte(dte_a, val_a, dte_b, val_b, target_dte: int = TARGET_DTE):
    """Linear interpolate val at target_dte using two (dte, val) pairs.
    Falls back gracefully if only one side is populated."""
    if val_a is None and val_b is None:
        return None
    if val_a is None:
        return val_b
    if val_b is None:
        return val_a
    if dte_a == dte_b:
        return (val_a + val_b) / 2.0
    return val_a + (val_b - val_a) * (target_dte - dte_a) / (dte_b - dte_a)


def pick_bracket_expirations(expirations: list[date], trade_date: date):
    """Pick the two expirations bracketing trade_date + TARGET_DTE,
    restricted to [trade_date + 7, trade_date + 90]."""
    target_exp = trade_date + timedelta(days=TARGET_DTE)
    candidates = [e for e in expirations
                  if 7 <= (e - trade_date).days <= 90]
    if not candidates:
        return None, None
    before = [e for e in candidates if e <= target_exp]
    after  = [e for e in candidates if e >  target_exp]
    e_before = max(before) if before else None
    e_after  = min(after)  if after  else None
    return e_before, e_after


def sanity_flags(atm, iv_25c, iv_25p, rr) -> str:
    """Return ';'-joined flags ('ok' if none triggered)."""
    flags = []
    for name, v in [("atm", atm), ("iv_25c", iv_25c), ("iv_25p", iv_25p)]:
        if v is not None and (v <= 0 or v > 5.0):
            flags.append(f"{name}_implausible({v:.3f})")
    if rr is not None and abs(rr) > 0.30:
        flags.append(f"rr_extreme({rr:.3f})")
    return ";".join(flags) if flags else "ok"


# ---------------------------------------------------------------------------
# Date sampling
# ---------------------------------------------------------------------------

def pick_target_dates() -> list[date]:
    """One trade_date per month, snapped to nearest NYSE trading day to TARGET_DOM."""
    nyse = mcal.get_calendar("NYSE")
    sched = nyse.schedule(start_date=START_DATE, end_date=END_DATE)
    trading_days = [d.date() for d in sched.index]
    if not trading_days:
        return []

    out: list[date] = []
    cur = date(START_DATE.year, START_DATE.month, 1)
    while cur <= END_DATE:
        target = date(cur.year, cur.month, min(TARGET_DOM, 28))
        # Nearest trading day on or after target (within a week)
        on_or_after = [d for d in trading_days
                       if d >= target and (d - target).days <= 7]
        if on_or_after:
            out.append(on_or_after[0])
        else:
            # Fall back to nearest before
            before = [d for d in trading_days
                      if d < target and (target - d).days <= 7]
            if before:
                out.append(before[-1])
        # Next month
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return out


# ---------------------------------------------------------------------------
# Per-cell test
# ---------------------------------------------------------------------------

EMPTY_ROW_TEMPLATE = {
    "ticker":              None,
    "trade_date":          None,
    # 15:45 first_order results
    "atm_iv_30d_1545":     None,
    "iv_25d_call_30d":     None,
    "iv_25d_put_30d":      None,
    "rr_25d_30d":          None,
    # EOD comparison
    "atm_iv_30d_eod":      None,
    "atm_diff_1545_eod":   None,
    # Bracket expirations chosen
    "exp_a":               None, "exp_b":               None,
    "dte_a":               None, "dte_b":               None,
    # Per-expiration chain shape
    "atm_iv_a":            None, "atm_iv_b":            None,
    "iv_25c_a":            None, "iv_25c_b":            None,
    "iv_25p_a":            None, "iv_25p_b":            None,
    "n_calls_a":           None, "n_calls_b":           None,
    "n_puts_a":            None, "n_puts_b":            None,
    "min_call_delta_a":    None, "min_call_delta_b":    None,
    "max_call_delta_a":    None, "max_call_delta_b":    None,
    "min_put_delta_a":     None, "min_put_delta_b":     None,
    "max_put_delta_a":     None, "max_put_delta_b":     None,
    # Per-side interpolation reasons
    "iv_25c_reason_a":     None, "iv_25c_reason_b":     None,
    "iv_25p_reason_a":     None, "iv_25p_reason_b":     None,
    # Top-line classification
    "failure_reason":      None,
    "sanity":              None,
}


def test_one_cell(ticker: str, trade_date: date, all_expirations: list[date]) -> dict:
    row = dict(EMPTY_ROW_TEMPLATE)
    row["ticker"] = ticker
    row["trade_date"] = trade_date

    e_a, e_b = pick_bracket_expirations(all_expirations, trade_date)
    if e_a is None and e_b is None:
        row["failure_reason"] = "no_expirations_in_window"
        return row
    row["exp_a"] = e_a
    row["exp_b"] = e_b
    if e_a is not None:
        row["dte_a"] = (e_a - trade_date).days
    if e_b is not None:
        row["dte_b"] = (e_b - trade_date).days

    # Fetch each expiration's 15:45 chain
    metrics_by_label: dict[str, Optional[dict]] = {"a": None, "b": None}
    garbage_reasons: dict[str, Optional[str]] = {"a": None, "b": None}

    for label, exp in [("a", e_a), ("b", e_b)]:
        if exp is None:
            continue
        try:
            raw_rows = fetch_first_order(ticker, exp, trade_date)
        except Exception as e:
            row["failure_reason"] = f"http_error_exp_{label}:{str(e)[:120]}"
            return row
        df = parse_chain(raw_rows)
        if df.empty:
            metrics_by_label[label] = None
            continue
        # Restrict to the requested expiration (defensive — should already match)
        df = df[df["expiration"] == exp]
        if df.empty:
            metrics_by_label[label] = None
            continue
        g = check_garbage(df)
        if g:
            garbage_reasons[label] = g
            # Still compute metrics so we can see what came back, but flag it
        metrics_by_label[label] = compute_one_expiration(df)

    # Populate per-expiration columns
    for label in ("a", "b"):
        m = metrics_by_label[label]
        if m is None:
            continue
        row[f"atm_iv_{label}"]         = m["atm_iv"]
        row[f"iv_25c_{label}"]         = m["iv_25c"]
        row[f"iv_25p_{label}"]         = m["iv_25p"]
        row[f"n_calls_{label}"]        = m["n_calls"]
        row[f"n_puts_{label}"]         = m["n_puts"]
        row[f"min_call_delta_{label}"] = m["min_call_delta"]
        row[f"max_call_delta_{label}"] = m["max_call_delta"]
        row[f"min_put_delta_{label}"]  = m["min_put_delta"]
        row[f"max_put_delta_{label}"]  = m["max_put_delta"]
        row[f"iv_25c_reason_{label}"]  = m["iv_25c_reason"]
        row[f"iv_25p_reason_{label}"]  = m["iv_25p_reason"]

    # Classification
    a, b = metrics_by_label["a"], metrics_by_label["b"]
    if a is None and b is None:
        row["failure_reason"] = "total_outage"
        return row

    # Garbage check: if either present expiration has bad-looking data, flag that
    garbage_seen = [(l, g) for l, g in garbage_reasons.items() if g is not None]
    if garbage_seen:
        # Mark as garbage but still allow the metrics through so the user can see them
        row["failure_reason"] = ";".join(f"garbage_data_exp_{l}:{g}" for l, g in garbage_seen)

    # Interpolate to 30 DTE across whichever expirations we got
    atm = interp_to_dte(row["dte_a"], a["atm_iv"] if a else None,
                        row["dte_b"], b["atm_iv"] if b else None)
    iv_25c = interp_to_dte(row["dte_a"], a["iv_25c"] if a else None,
                           row["dte_b"], b["iv_25c"] if b else None)
    iv_25p = interp_to_dte(row["dte_a"], a["iv_25p"] if a else None,
                           row["dte_b"], b["iv_25p"] if b else None)
    rr = (iv_25c - iv_25p) if (iv_25c is not None and iv_25p is not None) else None

    row["atm_iv_30d_1545"] = atm
    row["iv_25d_call_30d"] = iv_25c
    row["iv_25d_put_30d"]  = iv_25p
    row["rr_25d_30d"]      = rr

    # Sparse OTM strikes check — even with valid deltas, target delta wasn't bracketed
    if iv_25c is None or iv_25p is None:
        reasons = []
        for label, m in [("a", a), ("b", b)]:
            if m is None:
                continue
            if m["iv_25c_reason"]:
                reasons.append(f"exp_{label}_call:{m['iv_25c_reason']}")
            if m["iv_25p_reason"]:
                reasons.append(f"exp_{label}_put:{m['iv_25p_reason']}")
        sparse_msg = "sparse_otm_strikes" + ((":" + ";".join(reasons)) if reasons else "")
        # Only set if we don't already have garbage (garbage is more diagnostic)
        if row["failure_reason"] is None:
            row["failure_reason"] = sparse_msg

    # EOD ATM comparison (best-effort — failures here don't affect classification)
    try:
        eod_rows = fetch_eod_chain(ticker, trade_date)
        eod_df = parse_chain(eod_rows)
        if not eod_df.empty:
            eod_exps = sorted(eod_df["expiration"].dropna().unique())
            ea, eb = pick_bracket_expirations(eod_exps, trade_date)
            atm_a = atm_b = None
            da = db = None
            for lbl, e in [("a", ea), ("b", eb)]:
                if e is None:
                    continue
                sub = eod_df[eod_df["expiration"] == e]
                if sub.empty:
                    continue
                m = compute_one_expiration(sub)
                if lbl == "a":
                    atm_a, da = m["atm_iv"], (e - trade_date).days
                else:
                    atm_b, db = m["atm_iv"], (e - trade_date).days
            row["atm_iv_30d_eod"] = interp_to_dte(da, atm_a, db, atm_b)
    except Exception:
        pass  # EOD comparison is best-effort

    # Diff between 15:45 ATM and EOD ATM (vol-point difference)
    if row["atm_iv_30d_1545"] is not None and row["atm_iv_30d_eod"] is not None:
        row["atm_diff_1545_eod"] = row["atm_iv_30d_1545"] - row["atm_iv_30d_eod"]

    row["sanity"] = sanity_flags(atm, iv_25c, iv_25p, rr)

    if row["failure_reason"] is None:
        row["failure_reason"] = "ok"

    return row


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print(f"=== IV endpoint diagnostic test ===")
    print(f"Base URL    : {BASE_URL}")
    print(f"Tickers     : {TICKERS}")
    print(f"Date range  : {START_DATE} -> {END_DATE}")
    print(f"Target DTE  : {TARGET_DTE}")
    print(f"Strike range: +/-{STRIKE_RANGE}")
    print(f"Output      : {OUT_CSV.resolve()}")
    print()

    print("Sampling trade dates ...", end=" ", flush=True)
    target_dates = pick_target_dates()
    print(f"{len(target_dates)} dates (~1/month)")

    cells = [(t, d) for t in TICKERS for d in target_dates]
    print(f"Total cells : {len(cells)}\n")

    print("Listing expirations per ticker ...")
    exp_cache: dict[str, list[date]] = {}
    for ticker in TICKERS:
        print(f"  {ticker} ...", end=" ", flush=True)
        exps, err = list_expirations(ticker)
        if err:
            print(f"FAILED: {err}")
            print(f"\nCan't continue without expirations list for {ticker}. Aborting.")
            return 1
        exp_cache[ticker] = exps
        print(f"{len(exps)} expirations known to terminal")
    print()

    started = time.time()
    results: list[dict] = []
    for i, (ticker, td) in enumerate(cells, 1):
        t0 = time.time()
        try:
            row = test_one_cell(ticker, td, exp_cache[ticker])
        except Exception as e:
            row = dict(EMPTY_ROW_TEMPLATE)
            row["ticker"] = ticker
            row["trade_date"] = td
            row["failure_reason"] = f"unexpected_error:{type(e).__name__}:{str(e)[:200]}"
        results.append(row)
        dur = time.time() - t0
        reason = row.get("failure_reason", "?")
        # Truncate long reasons for the live log; full text lands in the CSV.
        short = reason if len(reason) <= 60 else reason[:57] + "..."
        print(f"[{i:>4}/{len(cells)}] {ticker:<5} {td}  {dur:5.2f}s  {short}")

    df = pd.DataFrame(results)
    df.to_csv(OUT_CSV, index=False)

    elapsed = time.time() - started
    print(f"\nWrote {len(df)} rows to {OUT_CSV} in {elapsed/60:.1f} min.")

    # ----- Summary -----
    print("\n=== Summary ===")
    fr = df["failure_reason"].fillna("ok")
    n = len(df)
    print(f"Total cells: {n}")

    # Top-level outcome counts
    ok_mask = fr == "ok"
    n_ok = int(ok_mask.sum())
    print(f"  ok                          : {n_ok:>4}  ({100 * n_ok / n:5.1f}%)")

    def count_prefix(prefix: str) -> int:
        return int(fr.str.startswith(prefix).sum() - (1 if prefix == "ok" else 0)
                   if prefix == "ok" else fr.str.startswith(prefix).sum())

    for cat in ["no_expirations_in_window", "total_outage", "http_error",
                "garbage_data", "sparse_otm_strikes", "unexpected_error"]:
        c = int(fr.str.startswith(cat).sum())
        if c:
            print(f"  {cat:<28}: {c:>4}  ({100 * c / n:5.1f}%)")

    # Sanity flags on the cells that DID produce values
    have_metrics = df["rr_25d_30d"].notna()
    if have_metrics.any():
        good_sanity = (df.loc[have_metrics, "sanity"] == "ok").sum()
        print(f"\nCells with computed rr_25d_30d : {int(have_metrics.sum())}")
        print(f"  ... clean sanity              : {int(good_sanity)}")
        rr = df.loc[have_metrics, "rr_25d_30d"]
        print(f"  rr_25d_30d  median (vol pts)  : {rr.median():.4f}")
        print(f"              p10 / p90         : {rr.quantile(0.10):.4f} / {rr.quantile(0.90):.4f}")

    # 15:45 vs EOD ATM agreement
    diff = df["atm_diff_1545_eod"].dropna()
    if not diff.empty:
        print(f"\n15:45 ATM vs EOD ATM diff (vol points):")
        print(f"  n        : {len(diff)}")
        print(f"  median   : {diff.median():+.4f}")
        print(f"  |median| : {diff.abs().median():.4f}")
        print(f"  p10/p90  : {diff.quantile(0.10):+.4f} / {diff.quantile(0.90):+.4f}")

    # Failure clustering by year
    df["year"] = pd.to_datetime(df["trade_date"]).dt.year
    print("\nResults by year (n_ok / n_total):")
    for yr, grp in df.groupby("year"):
        n_yr = len(grp)
        ok_yr = int((grp["failure_reason"] == "ok").sum())
        print(f"  {int(yr)}: {ok_yr:>3} / {n_yr:>3}  ({100 * ok_yr / n_yr:5.1f}% ok)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
