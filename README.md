# OI_Research

Research-only project for testing whether daily option open-interest (OI)
surfaces have any useful relationship to future stock/ETF price movement,
pinning, or realized volatility.

Isolated from the main SPX project — uses its own Postgres database
(`open_interest`) so it can be dropped without side effects.

## Storage layout (current)

| Layer                   | Where                                              |
|-------------------------|----------------------------------------------------|
| Raw OI                  | parquet at `data/oi_raw/{ticker}/{year}.parquet`   |
| Underlying OHLC         | Postgres `underlying_ohlc`                         |
| Derived daily features  | Postgres `daily_features`                          |

The earlier `option_oi_raw` and `option_oi_surface` Postgres tables are
**no longer populated**. They remain in the schema as historical snapshots
until you choose to drop them (see "Migration" below).

## Project layout

```
.
├── config.py                env-driven settings (Postgres + ThetaData + OI_RAW_DIR)
├── db.py                    psycopg2 connection helper
├── init_db.py               applies sql/01_schema.sql + sql/02_views.sql (idempotent)
├── fetch_ohlc.py            yfinance daily OHLC -> underlying_ohlc
├── fetch_oi.py              ThetaData daily OI -> data/oi_raw/{ticker}/{year}.parquet
├── build_features.py        parquet (OI) + Postgres (OHLC) -> daily_features (DuckDB)
├── export_raw_to_parquet.py one-time migration: option_oi_raw -> parquet
├── lib/
│   ├── thetadata.py         v3 client (open_interest endpoint)
│   ├── parquet_store.py     read/write per-(ticker, year) parquet files
│   ├── expirations.py       3rd-Friday helper, snaps to listed expirations
│   └── market_hours.py      NYSE trading-day helpers
└── sql/
    ├── 00_create_database.sql   one-time, run as superuser
    ├── 01_schema.sql            tables (incl. ALTER TABLE adds for new feature cols)
    └── 02_views.sql             helper views (read frozen surface — see note below)
```

## First-time setup (on the VPS)

```bash
git clone <repo>
cd OI_Research
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
psql -U postgres -f sql/00_create_database.sql   # one-time
python init_db.py                                # creates tables + views, idempotent
```

## Migration from Postgres-raw to parquet (one-time)

If you previously loaded data into `option_oi_raw`:

```bash
python export_raw_to_parquet.py
# Validate the parquet files (script logs row counts vs Postgres).
# When you're satisfied, drop the table yourself:
psql -d open_interest -c "DROP TABLE option_oi_raw;"
```

`option_oi_surface` can also be dropped manually if you don't plan to use it.

## Daily workflow

```bash
python fetch_ohlc.py     # prompts for tickers + date range
python fetch_oi.py       # prompts for tickers + date range; writes parquet
python build_features.py # prompts for tickers (blank = all); reads parquet + ohlc -> daily_features
```

`fetch_oi.py` no longer rebuilds a surface table — derived metrics are
computed on demand by `build_features.py` directly from the raw parquet.

## daily_features columns

OI aggregates (full unfiltered chain):
`spot_close`, `total_oi`, `call_oi`, `put_oi`, `put_call_oi_ratio`,
`max_oi_strike_call`, `max_oi_strike_put`,
`oi_weighted_strike_call/put/all`,
`oi_weighted_strike_*_minus_spot`, `oi_weighted_strike_*_div_spot`,
`oi_within_5pct`, `oi_within_10pct`, `pct_oi_in_front_expiry`,
`oi_above_spot`, `oi_below_spot`, `oi_above_below_ratio`,
`oi_weighted_strike_*_0_30d`, `oi_weighted_strike_*_31_90d` (each with `_div_spot`),
`d1_total_oi_change`, `d5_total_oi_change`, `d20_total_oi_change`.

OHLC-derived:
`rv_5d`, `rv_20d`,
`ret_{1,3,5,7,10,20}d_fwd_cc`,
`ret_{1,3,5,7,10,20}d_fwd_oc` (entry = next-day open since OI is released ~6:30am ET).

New percentage / aggregate features (denominator = full unfiltered chain):
`pct_oi_within_5pct`, `pct_oi_within_10pct`,
`pct_oi_above_spot`, `pct_oi_below_spot`,
`top5_strikes_pct_total_oi`, `top10_strikes_pct_total_oi`,
`weighted_avg_dte`,
`pct_oi_0_30d`, `pct_oi_31_90d`, `pct_oi_91_365d`,
`pct_oi_next_monthly`, `oi_weighted_strike_next_monthly_div_spot`.

Top-N strikes aggregate OI **across all expirations** at each strike level
(matches the "price magnet" reading). "Next monthly" is the next 3rd-Friday
expiration on/after `trade_date`, snapped to the actually-listed expiration
in the parquet (handles Good Friday automatically).

## Views (`sql/02_views.sql`)

The OI-related views read from `option_oi_surface`, which is now a frozen
historical snapshot. They'll work but won't reflect any data fetched after
the migration. `v_features_with_returns` reads from `daily_features` and
remains live.

## Cleanup

```sql
DROP DATABASE open_interest;
```
```bash
rm -rf data/oi_raw/
```
