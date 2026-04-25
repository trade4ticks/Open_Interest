# OI_Research

Research-only project for testing whether daily option open-interest (OI)
surfaces have any useful relationship to future stock/ETF price movement,
pinning, or realized volatility.

Isolated from the main SPX project — uses its own Postgres database
(`open_interest`) so it can be dropped without side effects.

## Layout

```
.
├── config.py            env-driven settings (Postgres + ThetaData + filter thresholds)
├── db.py                psycopg2 connection helper
├── init_db.py           one-time: applies sql/01_schema.sql + sql/02_views.sql
├── fetch_ohlc.py        yfinance daily OHLC -> underlying_ohlc
├── fetch_oi.py          ThetaData daily OI -> option_oi_raw, then rebuilds option_oi_surface
├── build_features.py    aggregates option_oi_surface + underlying_ohlc -> daily_features
├── lib/
│   ├── thetadata.py     v3 client (open_interest endpoint only)
│   └── market_hours.py  NYSE trading-day helpers
└── sql/
    ├── 00_create_database.sql   run once as superuser
    ├── 01_schema.sql            tables
    └── 02_views.sql             helper views for AI explorer
```

## First-time setup (on the VPS)

```bash
git clone <repo>
cd OI_Research
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# (edit .env if any value differs from your VPS conventions)

# Create the database (one-time, as a superuser):
psql -U postgres -f sql/00_create_database.sql

# Create the tables and views:
python init_db.py
```

## Daily workflow

```bash
python fetch_ohlc.py     # prompts for tickers + date range
python fetch_oi.py       # prompts for tickers + date range
python build_features.py # prompts for tickers (blank = all)
```

`fetch_oi.py` runs the surface-filter pass automatically after each ticker.
`build_features.py` is a separate step because it benefits from being run
*after* both OHLC and OI are loaded for the date range.

## Tables

| Table               | Grain                                              |
|---------------------|----------------------------------------------------|
| `underlying_ohlc`   | (ticker, trade_date)                               |
| `option_oi_raw`     | (ticker, trade_date, expiration, strike, option_type) |
| `option_oi_surface` | same grain, filtered by OI_MIN / OI_MAX_DTE / OI_MAX_MONEYNESS, joined to spot |
| `daily_features`    | (ticker, trade_date)                               |

## Views (for AI explorer)

| View                        | Purpose                                                     |
|-----------------------------|-------------------------------------------------------------|
| `v_oi_surface_latest`       | Full filtered surface for the most recent trade_date        |
| `v_oi_top_nodes_latest`     | Top 25 OI strikes per ticker on the latest day              |
| `v_oi_changes_daily`        | Per-contract OI delta vs the previous trade_date            |
| `v_oi_concentration`        | Per (ticker, date, expiration) totals + OI-weighted strike  |
| `v_features_with_returns`   | `daily_features` + spot for one-shot correlation queries    |
| `v_pin_candidates`          | Near-spot strikes in the front expiration ranked by OI      |

## Surface filter (.env)

`option_oi_surface` keeps a row from `option_oi_raw` only if all hold:

```
open_interest >= OI_MIN              (default 100)
0 <= dte      <= OI_MAX_DTE          (default 365)
|strike/spot - 1| <= OI_MAX_MONEYNESS (default 0.50)
```

Tune these in `.env` and re-run `fetch_oi.py` (the surface is rebuilt for
the touched dates each run; the raw table is preserved either way).

## Cleanup

```sql
DROP DATABASE open_interest;
```
