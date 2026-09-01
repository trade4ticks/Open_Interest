# migrations/

One-time scripts, kept for the record. **None of these are part of any
pipeline** — nothing imports them, no cron entry runs them, and each has
already served its purpose against the live database.

They are retained rather than deleted because each one documents *how* a store
moved, which is the thing that is hard to reconstruct later from a schema file
that no longer mentions the old table.

| script | what it did | status |
|---|---|---|
| `export_raw_to_parquet.py` | Copied Postgres `option_oi_raw` → `{OI_RAW_DIR}/{ticker}/{year}.parquet` | **Complete.** Table dropped 2026-09-01. |

`option_oi_surface` was dropped in the same session. It had no migration
script of its own because it was always derived — see below.

## export_raw_to_parquet.py

Its source table no longer exists, so **it cannot be run again** — it will
fail at `SELECT DISTINCT ticker FROM option_oi_raw`. That is fine and is the
point: the migration finished.

Verified a clean superset before the drop, per ticker:

| ticker | Postgres | parquet |
|---|---|---|
| AAPL | 2,949,149 | 3,507,279 |
| GLD | 4,615,492 | 5,534,688 |
| IWM | 5,479,524 | 6,320,726 |
| JPM | 1,680,670 | 1,973,186 |
| SPY | 11,769,923 | 14,005,923 |
| TSLA | 7,298,229 | 8,263,399 |
| XOM | 1,331,711 | 1,586,098 |

Parquet also starts a year earlier (2019-01-02 vs 2020-01-02) and runs through
2026-08-31 rather than stopping at 2026-04-24. Nothing was in Postgres that
was not in parquet. Recovered 5.1 GB.


---

# option_oi_surface — dropped 2026-09-01

**15,303,161 rows. Not empty.** It held real data and the five views that read
it would have returned results. It was dropped because it is *reconstructible*,
not because it was hollow.

Same grain as `option_oi_raw` — one row per contract per day, primary key
`(ticker, trade_date, expiration, strike, option_type)` — so **filtered, not
aggregated**. 15.3M rows against the raw store's 35.1M is the filter's work.

| | option_oi_surface | raw parquet |
|---|---|---|
| rows | 15,303,161 | 35,148,299 |
| tickers | 7 | same 7 |
| range | 2020-01-02 → 2026-04-24 | 2019-01-02 → 2026-08-31 |

## The filter definition

Recovered from `fetch_oi.py` at `85d3ccb^` — the commit before the parquet
migration removed it. This is the whole of what produced 15.3M rows from
35.1M, and it is the one thing dropping the table would otherwise have lost.

```sql
INSERT INTO option_oi_surface
    (ticker, trade_date, expiration, dte, strike, option_type, open_interest,
     spot_close, moneyness)
SELECT
    r.ticker,
    r.trade_date,
    r.expiration,
    (r.expiration - r.trade_date)::INTEGER  AS dte,
    r.strike,
    r.option_type,
    r.open_interest,
    o.close                                 AS spot_close,
    (r.strike / o.close) - 1.0              AS moneyness
FROM option_oi_raw r
JOIN underlying_ohlc o
  ON  o.ticker     = r.ticker
  AND o.trade_date = r.trade_date
WHERE r.ticker     = %(ticker)s
  AND r.trade_date = %(trade_date)s
  AND r.open_interest >= %(oi_min)s
  AND (r.expiration - r.trade_date) BETWEEN 0 AND %(max_dte)s
  AND ABS((r.strike / o.close) - 1.0) <= %(max_moneyness)s
  AND o.close IS NOT NULL AND o.close > 0;
```

Three thresholds, from `config.py` at the same commit. They were removed from
`config.py` by the migration, so **git is now the only source**:

```python
OI_MIN           = int(os.environ.get("OI_MIN", "100"))
OI_MAX_DTE       = int(os.environ.get("OI_MAX_DTE", "365"))
OI_MAX_MONEYNESS = float(os.environ.get("OI_MAX_MONEYNESS", "0.50"))
```

⚠️ **These are the DEFAULTS, not necessarily what ran.** All three read from
the environment, so if the VPS `.env` set any of them, the table was built with
those values instead and the row count reflects them. `.env` is gitignored, so
the repo cannot answer this. If an exact reproduction ever matters, check
whether that `.env` still has `OI_MIN`, `OI_MAX_DTE` or `OI_MAX_MONEYNESS` in
it before assuming 100 / 365 / 0.50.

Four filter clauses, in effect:

1. `open_interest >= 100` — drop near-dead contracts
2. `0 <= dte <= 365` — drop expired and beyond one year
3. `|strike/spot − 1| <= 0.50` — drop strikes more than 50% from spot
4. an **inner** join to `underlying_ohlc` with a positive close — a day with no
   OHLC row contributed nothing, which is a filter in its own right and easy
   to overlook

The three derived columns are all recomputable: `dte` is a date difference,
`spot_close` is the OHLC join, `moneyness` is `(strike / close) - 1.0`.

## Rebuilding it from parquet

Nothing needs it today. If the per-node surface is ever wanted back, the shape
is a DuckDB query over `{OI_RAW_DIR}/{ticker}/{year}.parquet` joined to
`underlying_ohlc`, applying the four clauses above. The parquet store is a
superset of what the table held — an extra year at the start, four extra
months at the end — so a rebuild would cover *more* than the original.

## What was dropped with it

Five views in `sql/02_views.sql`, all of which read the surface:
`v_oi_surface_latest`, `v_oi_top_nodes_latest`, `v_oi_changes_daily`,
`v_oi_concentration`, `v_pin_candidates`. No Python in the repo queried any of
them. `v_features_with_returns` reads `daily_features` and survives.
