"""
backtest_spreads.py — Backtest call debit spreads from a predefined trade list.

Input CSV columns used: ticker, trade_date, exit_date, spot_entry, spot_exit,
fired_systems.  (ret_pct is ignored — this backtest uses option mid-prices.)

Per-trade logic
---------------
1. Expiration: soonest date on or after exit_date from the OI parquet store.
2. Entry (09:35 on trade_date):
   - Use spot_entry (CSV open price) to locate strikes.
   - Fetch option quotes at 09:35 (interval=5m) for the target expiration.
   - long_strike  = first call strike strictly above spot_entry.
   - short_strike = next call strike above long_strike.
   - Entry mid-prices determine net_entry_debit.
3. Sizing: qty = floor($1,000 / (net_entry_debit * 100)).
4. Exit (15:30 on exit_date):
   - Fetch option quotes at 15:30 (interval=30m) for same expiration & strikes.
   - net_exit_value = long_mid - short_mid at exit.
5. P&L = (net_exit_value - net_entry_debit) * qty * 100.
   pnl_pct = P&L / capital_deployed * 100.

Results upserted to backtest_call_spread (one row per trade).
Run init_db.py first to create the table if it doesn't exist.
"""
from __future__ import annotations

import logging
import math
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from db import get_connection, read_sql_df
from lib.parquet_store import read_range as read_oi_range
from lib.thetadata import (
    TerminalServerError,
    TerminalTimeoutError,
    fetch_option_quotes_at,
    test_connection,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

CSV_PATH = Path(__file__).parent / "portfolio_3_union_2026-05-18.csv"
CAPITAL  = 1_000.0   # dollars allocated per trade

_NONE_FIELDS = [
    "expiration",
    "long_strike", "short_strike",
    "long_entry_bid", "long_entry_ask", "long_entry_mid", "long_entry_spread",
    "short_entry_bid", "short_entry_ask", "short_entry_mid", "short_entry_spread",
    "net_entry_debit",
    "long_exit_bid", "long_exit_ask", "long_exit_mid", "long_exit_spread",
    "short_exit_bid", "short_exit_ask", "short_exit_mid", "short_exit_spread",
    "net_exit_value",
    "max_risk_per_contract", "max_profit_per_contract",
    "qty", "capital_deployed", "total_pnl", "pnl_pct",
]

_UPSERT_SQL = """
INSERT INTO backtest_call_spread (
    ticker, trade_date, exit_date, fired_systems,
    spot_entry_open, spot_exit_close, expiration,
    long_strike, short_strike,
    long_entry_bid, long_entry_ask, long_entry_mid, long_entry_spread,
    short_entry_bid, short_entry_ask, short_entry_mid, short_entry_spread,
    net_entry_debit,
    long_exit_bid, long_exit_ask, long_exit_mid, long_exit_spread,
    short_exit_bid, short_exit_ask, short_exit_mid, short_exit_spread,
    net_exit_value,
    max_risk_per_contract, max_profit_per_contract,
    qty, capital_deployed, total_pnl, pnl_pct, status
) VALUES (
    %(ticker)s, %(trade_date)s, %(exit_date)s, %(fired_systems)s,
    %(spot_entry_open)s, %(spot_exit_close)s, %(expiration)s,
    %(long_strike)s, %(short_strike)s,
    %(long_entry_bid)s, %(long_entry_ask)s, %(long_entry_mid)s, %(long_entry_spread)s,
    %(short_entry_bid)s, %(short_entry_ask)s, %(short_entry_mid)s, %(short_entry_spread)s,
    %(net_entry_debit)s,
    %(long_exit_bid)s, %(long_exit_ask)s, %(long_exit_mid)s, %(long_exit_spread)s,
    %(short_exit_bid)s, %(short_exit_ask)s, %(short_exit_mid)s, %(short_exit_spread)s,
    %(net_exit_value)s,
    %(max_risk_per_contract)s, %(max_profit_per_contract)s,
    %(qty)s, %(capital_deployed)s, %(total_pnl)s, %(pnl_pct)s, %(status)s
)
ON CONFLICT (ticker, trade_date, exit_date) DO UPDATE SET
    fired_systems           = EXCLUDED.fired_systems,
    spot_entry_open         = EXCLUDED.spot_entry_open,
    spot_exit_close         = EXCLUDED.spot_exit_close,
    expiration              = EXCLUDED.expiration,
    long_strike             = EXCLUDED.long_strike,
    short_strike            = EXCLUDED.short_strike,
    long_entry_bid          = EXCLUDED.long_entry_bid,
    long_entry_ask          = EXCLUDED.long_entry_ask,
    long_entry_mid          = EXCLUDED.long_entry_mid,
    long_entry_spread       = EXCLUDED.long_entry_spread,
    short_entry_bid         = EXCLUDED.short_entry_bid,
    short_entry_ask         = EXCLUDED.short_entry_ask,
    short_entry_mid         = EXCLUDED.short_entry_mid,
    short_entry_spread      = EXCLUDED.short_entry_spread,
    net_entry_debit         = EXCLUDED.net_entry_debit,
    long_exit_bid           = EXCLUDED.long_exit_bid,
    long_exit_ask           = EXCLUDED.long_exit_ask,
    long_exit_mid           = EXCLUDED.long_exit_mid,
    long_exit_spread        = EXCLUDED.long_exit_spread,
    short_exit_bid          = EXCLUDED.short_exit_bid,
    short_exit_ask          = EXCLUDED.short_exit_ask,
    short_exit_mid          = EXCLUDED.short_exit_mid,
    short_exit_spread       = EXCLUDED.short_exit_spread,
    net_exit_value          = EXCLUDED.net_exit_value,
    max_risk_per_contract   = EXCLUDED.max_risk_per_contract,
    max_profit_per_contract = EXCLUDED.max_profit_per_contract,
    qty                     = EXCLUDED.qty,
    capital_deployed        = EXCLUDED.capital_deployed,
    total_pnl               = EXCLUDED.total_pnl,
    pnl_pct                 = EXCLUDED.pnl_pct,
    status                  = EXCLUDED.status
"""


# ---------------------------------------------------------------------------

def _find_expiration(ticker: str, exit_date: date) -> date | None:
    """Soonest expiration on or after exit_date from the OI parquet store."""
    oi = read_oi_range(ticker, exit_date, exit_date + timedelta(days=60))
    if oi.empty:
        return None
    oi["expiration"] = pd.to_datetime(oi["expiration"]).dt.date
    candidates = sorted(e for e in oi["expiration"].unique() if e >= exit_date)
    return candidates[0] if candidates else None


def _split_factor(conn, ticker: str, trade_date: date) -> float:
    """
    Cumulative split factor from trade_date forward.
    Multiply adjusted spot_entry by this to get the unadjusted (historical) price
    that ThetaData's option strikes reflect.

    Example: AAPL 4-for-1 split on 2020-08-31 → factor = 4.0 for any trade
    before that date, so spot 38.72 → 154.88 (the actual pre-split price).
    """
    df = read_sql_df(
        conn,
        "SELECT splits FROM underlying_ohlc "
        "WHERE ticker = %(t)s AND trade_date >= %(d)s AND splits != 0 AND splits IS NOT NULL",
        {"t": ticker, "d": trade_date},
    )
    if df.empty:
        return 1.0
    factor = 1.0
    for v in df["splits"]:
        factor *= float(v)
    return factor


def _leg_quotes(chain: pd.DataFrame, strike: float, option_type: str) -> dict:
    """Extract bid/ask/mid/spread for one strike from a chain DataFrame."""
    row = chain[(chain["strike"] == strike) & (chain["option_type"] == option_type)]
    if row.empty:
        return {"bid": None, "ask": None, "mid": None, "spread": None}
    bid = float(row.iloc[0]["bid"])
    ask = float(row.iloc[0]["ask"])
    return {"bid": bid, "ask": ask, "mid": (bid + ask) / 2.0, "spread": ask - bid}


def _run_trade(conn, ticker: str, trade_date: date, exit_date: date,
               spot_entry: float, expiration: date) -> dict:
    """Fetch quotes and compute all spread metrics for one trade."""

    # Unadjust spot: CSV prices are split-adjusted; ThetaData strikes are not.
    factor = _split_factor(conn, ticker, trade_date)
    spot_unadj = spot_entry * factor
    if factor != 1.0:
        log.info("  split factor=%.4f  spot %.2f → %.2f (unadjusted)",
                 factor, spot_entry, spot_unadj)

    # --- Entry: 09:35 on trade_date, 5-minute interval ---
    entry_chain = fetch_option_quotes_at(
        ticker, expiration, trade_date, time_str="09:35", interval="5m", timeout=120
    )
    if entry_chain.empty:
        return {"status": "no_entry_data"}

    calls = entry_chain[entry_chain["option_type"] == "C"].sort_values("strike")
    above = calls[calls["strike"] > spot_unadj]
    if len(above) < 2:
        return {"status": "insufficient_strikes"}

    long_strike  = float(above.iloc[0]["strike"])
    short_strike = float(above.iloc[1]["strike"])

    le = _leg_quotes(entry_chain, long_strike,  "C")
    se = _leg_quotes(entry_chain, short_strike, "C")

    if le["mid"] is None or se["mid"] is None:
        return {"status": "missing_entry_quotes"}

    net_entry_debit = le["mid"] - se["mid"]
    if net_entry_debit <= 0:
        log.warning("  invalid_entry_debit: long_strike=%.2f mid=%.4f  "
                    "short_strike=%.2f mid=%.4f  debit=%.4f",
                    long_strike, le["mid"], short_strike, se["mid"], net_entry_debit)
        return {"status": "invalid_entry_debit"}

    max_risk   = net_entry_debit * 100
    max_profit = (short_strike - long_strike - net_entry_debit) * 100
    qty        = math.floor(CAPITAL / max_risk)
    if qty < 1:
        return {"status": "insufficient_capital"}

    capital_deployed = net_entry_debit * qty * 100

    # --- Exit: 15:30 on exit_date, 30-minute interval ---
    exit_chain = fetch_option_quotes_at(
        ticker, expiration, exit_date, time_str="15:30", interval="30m", timeout=120
    )

    lx = _leg_quotes(exit_chain, long_strike,  "C")
    sx = _leg_quotes(exit_chain, short_strike, "C")

    if lx["mid"] is None or sx["mid"] is None:
        net_exit_value = None
        total_pnl      = None
        pnl_pct        = None
        status         = "no_exit_data"
    else:
        net_exit_value = lx["mid"] - sx["mid"]
        total_pnl      = (net_exit_value - net_entry_debit) * qty * 100
        pnl_pct        = total_pnl / capital_deployed * 100
        status         = "ok"

    return {
        "long_strike":             long_strike,
        "short_strike":            short_strike,
        "long_entry_bid":          le["bid"],
        "long_entry_ask":          le["ask"],
        "long_entry_mid":          le["mid"],
        "long_entry_spread":       le["spread"],
        "short_entry_bid":         se["bid"],
        "short_entry_ask":         se["ask"],
        "short_entry_mid":         se["mid"],
        "short_entry_spread":      se["spread"],
        "net_entry_debit":         net_entry_debit,
        "long_exit_bid":           lx["bid"],
        "long_exit_ask":           lx["ask"],
        "long_exit_mid":           lx["mid"],
        "long_exit_spread":        lx["spread"],
        "short_exit_bid":          sx["bid"],
        "short_exit_ask":          sx["ask"],
        "short_exit_mid":          sx["mid"],
        "short_exit_spread":       sx["spread"],
        "net_exit_value":          net_exit_value,
        "max_risk_per_contract":   max_risk,
        "max_profit_per_contract": max_profit,
        "qty":                     qty,
        "capital_deployed":        capital_deployed,
        "total_pnl":               total_pnl,
        "pnl_pct":                 pnl_pct,
        "status":                  status,
    }


# ---------------------------------------------------------------------------

def main() -> None:
    print("=== Call Debit Spread Backtest ===\n")

    if not CSV_PATH.exists():
        raise SystemExit(f"CSV not found: {CSV_PATH}")

    trades = pd.read_csv(CSV_PATH, parse_dates=["trade_date", "exit_date"])
    trades["trade_date"] = pd.to_datetime(trades["trade_date"]).dt.date
    trades["exit_date"]  = pd.to_datetime(trades["exit_date"]).dt.date

    print(f"Loaded {len(trades)} trades from {CSV_PATH.name}")

    # Optional date range filter
    raw_start = input("Start trade_date (YYYY-MM-DD, blank = all): ").strip()
    raw_end   = input("End   trade_date (YYYY-MM-DD, blank = all): ").strip()
    if raw_start:
        start_filter = pd.to_datetime(raw_start).date()
        trades = trades[trades["trade_date"] >= start_filter]
    if raw_end:
        end_filter = pd.to_datetime(raw_end).date()
        trades = trades[trades["trade_date"] <= end_filter]
    trades = trades.reset_index(drop=True)
    print(f"{len(trades)} trades in range.\n")

    print("Checking ThetaData ...", end=" ", flush=True)
    if not test_connection():
        raise SystemExit("FAILED — terminal not reachable.")
    print("OK\n")

    counts = {"ok": 0, "partial": 0, "skipped": 0, "error": 0}

    with get_connection() as conn:
        with conn.cursor() as cur:
            for i, row in trades.iterrows():
                ticker     = str(row["ticker"]).upper()
                trade_date = row["trade_date"]
                exit_date  = row["exit_date"]
                spot_entry = float(row["spot_entry"])
                spot_exit  = float(row["spot_exit"]) if pd.notna(row["spot_exit"]) else None
                fired      = str(row["fired_systems"]) if pd.notna(row.get("fired_systems")) else None

                log.info("[%d/%d] %s  %s → %s  spot=%.2f",
                         i + 1, len(trades), ticker, trade_date, exit_date, spot_entry)

                expiration = _find_expiration(ticker, exit_date)
                if expiration is None:
                    log.warning("  no expiration found in parquet — skipping")
                    rec = {"status": "no_expiration"}
                    counts["skipped"] += 1
                else:
                    log.info("  expiration=%s", expiration)
                    try:
                        rec = _run_trade(conn, ticker, trade_date, exit_date,
                                         spot_entry, expiration)
                    except (TerminalTimeoutError, TerminalServerError) as exc:
                        log.warning("  API error: %s", exc)
                        rec = {"status": "api_error"}
                        counts["error"] += 1
                    else:
                        s = rec.get("status")
                        if s == "ok":
                            counts["ok"] += 1
                            log.info("  P&L=%.2f  pnl_pct=%.1f%%  qty=%d",
                                     rec["total_pnl"], rec["pnl_pct"], rec["qty"])
                        elif s == "no_exit_data":
                            counts["partial"] += 1
                        else:
                            counts["skipped"] += 1
                            log.warning("  status=%s", s)

                # Build full record — fill any missing keys with None.
                full_rec = {f: None for f in _NONE_FIELDS}
                full_rec.update(rec)
                full_rec.update({
                    "ticker":          ticker,
                    "trade_date":      trade_date,
                    "exit_date":       exit_date,
                    "fired_systems":   fired,
                    "spot_entry_open": spot_entry,
                    "spot_exit_close": spot_exit,
                    "expiration":      full_rec.get("expiration") or expiration,
                })

                cur.execute(_UPSERT_SQL, full_rec)
                conn.commit()

    total = sum(counts.values())
    print(f"\nDone.  {total} trades processed.")
    print(f"  ok={counts['ok']}  partial(no exit)={counts['partial']}  "
          f"skipped={counts['skipped']}  errors={counts['error']}")


if __name__ == "__main__":
    main()
