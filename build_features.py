"""
build_features.py — Recompute the daily_features table from option_oi_surface
and underlying_ohlc.

Cheap to re-run: it truncates daily_features for the affected tickers and
rebuilds in a single SQL pass.

Usage:
    python build_features.py
    (prompts for tickers — leave blank for "all tickers in the surface table")
"""
from __future__ import annotations

import logging

from db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# Single-pass rebuild for one ticker. Runs entirely in SQL.
#
# Returns the list of trading days driven by underlying_ohlc (so realized vol
# and forward returns use trading-day spacing, not calendar-day spacing).
REBUILD_SQL = """
WITH spot AS (
    SELECT
        ticker,
        trade_date,
        close,
        LN(NULLIF(close, 0) / NULLIF(LAG(close) OVER w_t, 0)) AS log_ret
    FROM underlying_ohlc
    WHERE ticker = %(ticker)s
    WINDOW w_t AS (PARTITION BY ticker ORDER BY trade_date)
),
spot_lags AS (
    SELECT
        ticker, trade_date, close AS spot_close,
        STDDEV_SAMP(log_ret) OVER w5
            * SQRT(252)                               AS rv_5d,
        STDDEV_SAMP(log_ret) OVER w20
            * SQRT(252)                               AS rv_20d,
        (LEAD(close, 1)  OVER w_t) / NULLIF(close, 0) - 1 AS ret_1d_fwd,
        (LEAD(close, 5)  OVER w_t) / NULLIF(close, 0) - 1 AS ret_5d_fwd,
        (LEAD(close, 20) OVER w_t) / NULLIF(close, 0) - 1 AS ret_20d_fwd
    FROM spot
    WINDOW
        w_t AS (PARTITION BY ticker ORDER BY trade_date),
        w5  AS (PARTITION BY ticker ORDER BY trade_date
                ROWS BETWEEN 4  PRECEDING AND CURRENT ROW),
        w20 AS (PARTITION BY ticker ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
),
agg AS (
    SELECT
        ticker,
        trade_date,
        SUM(open_interest)                                                  AS total_oi,
        SUM(CASE WHEN right = 'C' THEN open_interest ELSE 0 END)            AS call_oi,
        SUM(CASE WHEN right = 'P' THEN open_interest ELSE 0 END)            AS put_oi,
        SUM(CASE WHEN ABS(moneyness) <= 0.05 THEN open_interest ELSE 0 END) AS oi_within_5pct,
        SUM(CASE WHEN ABS(moneyness) <= 0.10 THEN open_interest ELSE 0 END) AS oi_within_10pct,
        SUM(strike * open_interest)::DOUBLE PRECISION
            / NULLIF(SUM(open_interest), 0)                                 AS oi_weighted_strike_all,
        SUM(CASE WHEN right = 'C' THEN strike * open_interest ELSE 0 END)::DOUBLE PRECISION
            / NULLIF(SUM(CASE WHEN right = 'C' THEN open_interest ELSE 0 END), 0)
                                                                            AS oi_weighted_strike_call,
        SUM(CASE WHEN right = 'P' THEN strike * open_interest ELSE 0 END)::DOUBLE PRECISION
            / NULLIF(SUM(CASE WHEN right = 'P' THEN open_interest ELSE 0 END), 0)
                                                                            AS oi_weighted_strike_put
    FROM option_oi_surface
    WHERE ticker = %(ticker)s
    GROUP BY ticker, trade_date
),
max_call AS (
    SELECT DISTINCT ON (ticker, trade_date)
        ticker, trade_date, strike AS max_oi_strike_call
    FROM option_oi_surface
    WHERE ticker = %(ticker)s AND right = 'C'
    ORDER BY ticker, trade_date, open_interest DESC, strike
),
max_put AS (
    SELECT DISTINCT ON (ticker, trade_date)
        ticker, trade_date, strike AS max_oi_strike_put
    FROM option_oi_surface
    WHERE ticker = %(ticker)s AND right = 'P'
    ORDER BY ticker, trade_date, open_interest DESC, strike
),
front_expiry AS (
    SELECT ticker, trade_date, MIN(expiration) AS front_exp
    FROM option_oi_surface
    WHERE ticker = %(ticker)s AND dte >= 0
    GROUP BY ticker, trade_date
),
front_oi AS (
    SELECT
        s.ticker,
        s.trade_date,
        SUM(s.open_interest) AS front_total_oi
    FROM option_oi_surface s
    JOIN front_expiry f
      ON  f.ticker     = s.ticker
      AND f.trade_date = s.trade_date
      AND f.front_exp  = s.expiration
    GROUP BY s.ticker, s.trade_date
),
oi_lags AS (
    SELECT
        ticker, trade_date, total_oi,
        total_oi - LAG(total_oi, 1)  OVER w AS d1_total_oi_change,
        total_oi - LAG(total_oi, 5)  OVER w AS d5_total_oi_change,
        total_oi - LAG(total_oi, 20) OVER w AS d20_total_oi_change
    FROM agg
    WINDOW w AS (PARTITION BY ticker ORDER BY trade_date)
)
INSERT INTO daily_features (
    ticker, trade_date, spot_close,
    total_oi, call_oi, put_oi, put_call_oi_ratio,
    max_oi_strike_call, max_oi_strike_put,
    oi_weighted_strike_call, oi_weighted_strike_put, oi_weighted_strike_all,
    oi_within_5pct, oi_within_10pct, pct_oi_in_front_expiry,
    d1_total_oi_change, d5_total_oi_change, d20_total_oi_change,
    rv_5d, rv_20d,
    ret_1d_fwd, ret_5d_fwd, ret_20d_fwd
)
SELECT
    a.ticker,
    a.trade_date,
    sl.spot_close,
    a.total_oi,
    a.call_oi,
    a.put_oi,
    a.put_oi::DOUBLE PRECISION / NULLIF(a.call_oi, 0)        AS put_call_oi_ratio,
    mc.max_oi_strike_call,
    mp.max_oi_strike_put,
    a.oi_weighted_strike_call,
    a.oi_weighted_strike_put,
    a.oi_weighted_strike_all,
    a.oi_within_5pct,
    a.oi_within_10pct,
    fo.front_total_oi::DOUBLE PRECISION / NULLIF(a.total_oi, 0)
                                                              AS pct_oi_in_front_expiry,
    ol.d1_total_oi_change,
    ol.d5_total_oi_change,
    ol.d20_total_oi_change,
    sl.rv_5d,
    sl.rv_20d,
    sl.ret_1d_fwd,
    sl.ret_5d_fwd,
    sl.ret_20d_fwd
FROM agg       a
LEFT JOIN max_call  mc USING (ticker, trade_date)
LEFT JOIN max_put   mp USING (ticker, trade_date)
LEFT JOIN front_oi  fo USING (ticker, trade_date)
LEFT JOIN oi_lags   ol USING (ticker, trade_date)
LEFT JOIN spot_lags sl USING (ticker, trade_date)
ON CONFLICT (ticker, trade_date) DO UPDATE SET
    spot_close              = EXCLUDED.spot_close,
    total_oi                = EXCLUDED.total_oi,
    call_oi                 = EXCLUDED.call_oi,
    put_oi                  = EXCLUDED.put_oi,
    put_call_oi_ratio       = EXCLUDED.put_call_oi_ratio,
    max_oi_strike_call      = EXCLUDED.max_oi_strike_call,
    max_oi_strike_put       = EXCLUDED.max_oi_strike_put,
    oi_weighted_strike_call = EXCLUDED.oi_weighted_strike_call,
    oi_weighted_strike_put  = EXCLUDED.oi_weighted_strike_put,
    oi_weighted_strike_all  = EXCLUDED.oi_weighted_strike_all,
    oi_within_5pct          = EXCLUDED.oi_within_5pct,
    oi_within_10pct         = EXCLUDED.oi_within_10pct,
    pct_oi_in_front_expiry  = EXCLUDED.pct_oi_in_front_expiry,
    d1_total_oi_change      = EXCLUDED.d1_total_oi_change,
    d5_total_oi_change      = EXCLUDED.d5_total_oi_change,
    d20_total_oi_change     = EXCLUDED.d20_total_oi_change,
    rv_5d                   = EXCLUDED.rv_5d,
    rv_20d                  = EXCLUDED.rv_20d,
    ret_1d_fwd              = EXCLUDED.ret_1d_fwd,
    ret_5d_fwd              = EXCLUDED.ret_5d_fwd,
    ret_20d_fwd             = EXCLUDED.ret_20d_fwd
"""

CLEAR_SQL = "DELETE FROM daily_features WHERE ticker = %(ticker)s"


def prompt_tickers(conn) -> list[str]:
    raw = input(
        "Tickers (comma-separated; blank = all tickers in option_oi_surface): "
    ).strip()
    if raw:
        return [t.strip().upper() for t in raw.split(",") if t.strip()]
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT ticker FROM option_oi_surface ORDER BY ticker")
        return [r[0] for r in cur.fetchall()]


def main() -> None:
    print("=== OI_Research — Build daily_features ===\n")
    with get_connection() as conn:
        tickers = prompt_tickers(conn)
        if not tickers:
            print("No tickers found in option_oi_surface — nothing to do.")
            return
        print(f"\nRebuilding features for: {', '.join(tickers)}\n")

        with conn.cursor() as cur:
            for t in tickers:
                log.info("  %s ...", t)
                cur.execute(CLEAR_SQL,   {"ticker": t})
                cur.execute(REBUILD_SQL, {"ticker": t})
                log.info("    %d rows", cur.rowcount)
        conn.commit()
    print("\nDone.")


if __name__ == "__main__":
    main()
