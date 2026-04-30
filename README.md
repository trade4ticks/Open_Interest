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

## Daily workflow (interactive)

```bash
python fetch_ohlc.py         # prompts for tickers + date range (blank ticker = all already-loaded)
python fetch_oi.py           # prompts for tickers + date range — history endpoint (lags ~1 day)
python fetch_oi_snapshot.py  # prompts for tickers — captures TODAY's chain via the snapshot endpoint
python build_features.py     # prompts for tickers + date range (blank = full history rebuild)
```

`fetch_oi.py` no longer rebuilds a surface table — derived metrics are
computed on demand by `build_features.py` directly from the raw parquet.

The history endpoint (`/v3/option/history/open_interest`) typically lags
by ~1 day, so today's chain isn't in it yet at 7am ET. The snapshot
endpoint (`/v3/option/snapshot/open_interest`) returns the current OI
for every contract in one call per ticker. Both write to the same
parquet store; the writer dedupes on
`(trade_date, expiration, strike, option_type)` keeping last, so when
tomorrow's history fetch finally returns today, it overwrites the
snapshot value cleanly.

## Daily workflow (cron)

`run_pipeline.py` runs the four steps in sequence over rolling windows:

- OHLC:        rolling 10 calendar days (~7 trading days)
- OI history:  rolling 10 calendar days (fills/refreshes the prior week)
- OI snapshot: today only (or last_trading_day on weekends / holidays)
- Features:    rolling 45 calendar days (~30 trading days — keeps recent
  rows current and lets older `ret_*_fwd_oc` fill in as new OHLC arrives)

Operates on every ticker already in `underlying_ohlc` / `OI_RAW_DIR`; does
not add new tickers (run the interactive scripts manually for that).

Cron entry (Mon-Fri at 7am ET, with a flock to prevent overlapping runs):

```
TZ=America/New_York 0 7 * * 1-5 flock -n /tmp/oi_research.lock /Open_Interest/.venv/bin/python /Open_Interest/run_pipeline.py >> /Open_Interest/logs/pipeline.log 2>&1
```

Today's row at 7am will have OI-side features (totals, percentages, top-N
strikes, DTE buckets, OI-weighted strikes, d1/d5 changes, z-scores of
OI-only ratios, etc.) populated, and spot-derived ones (`pct_oi_within_*pct`,
`*_div_spot`, `oi_above/below_spot`, forward returns, realised vol) NULL
until tomorrow's run picks up today's OHLC and recomputes the row.

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
`ret_{1,3,5,7,10,20}d_fwd_oc` — entry = open of `trade_date`, exit = close
of `trade_date + (N-1)`. (OI for `trade_date` is published overnight and
visible on broker platforms when the market opens, so the realistic entry
is that day's open. `ret_1d_fwd_oc` is therefore the intraday open-to-close
on `trade_date` itself.)

Percentage / aggregate features (denominator = full unfiltered chain):
`pct_oi_within_5pct`, `pct_oi_within_10pct`,
`pct_oi_above_spot`, `pct_oi_below_spot`,
`top5_strikes_pct_total_oi`, `top10_strikes_pct_total_oi`,
`weighted_avg_dte`,
`pct_oi_0_30d`, `pct_oi_31_90d`, `pct_oi_91_365d`,
`pct_oi_next_monthly`, `oi_weighted_strike_next_monthly_div_spot`.

Pct changes / derived-ratio changes / 60-trading-day (~3-month) z-scores —
each z-score is NULL until at least 60 prior observations exist for that
column, so early rows in the series are NULL by design:
`d1_total_oi_pct_change`, `d5_total_oi_pct_change`,
`d1_d5_ratio_total_oi_pct_change`,
`d1_oi_weighted_strike_all_div_spot_change`, `d5_oi_weighted_strike_all_div_spot_change`,
`d1_put_call_oi_ratio_change`, `d5_put_call_oi_ratio_change`,
`zscore_d1_oi_change_3m`, `zscore_d5_oi_change_3m`,
`zscore_oi_weighted_strike_all_div_spot_3m`,
`zscore_put_call_oi_ratio_3m`,
`zscore_oi_above_below_ratio_3m`.

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
