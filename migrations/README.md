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
