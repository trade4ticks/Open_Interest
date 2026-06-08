# Open_Interest — agent guidance

## Authoritative reference: the data dictionary

Before any change touching cron tiers, trade_date semantics, build ordering, or how
`daily_features` rows are keyed/populated, first read
[`docs/daily_features_data_dictionary.md`](docs/daily_features_data_dictionary.md) —
especially the **"As-of semantics / universal row invariant"** and **"Cron tier"**
sections.

The row's `trade_date` is defined by tradability / knowledge-at-9:30am of session T,
not by fetch order. A `daily_features` row for date T is a composite, unified by what
is tradable or knowable on T:

- EVENING-tier columns (OHLC, vol, IV) derive from session T−1's close and are
  computable the evening before T. The EVENING cron writes them to the T row via
  `feature_date = next_trading_day`.
- The OI block is stamped `trade_date = T` and is published by CBOE/OCC at ~6:30am
  on T. It represents position as of T−1's 4pm close, but is not knowable to anyone
  until the ~6:30am release on T — so OI is correctly stamped T and only becomes
  available the morning of T.
- Therefore the EVENING cron always runs **before** the MORNING cron for any given
  trade_date. The EVENING run creates the next trading day's row at a time when OI
  for that date does not yet exist and could not exist. This is the normal
  steady-state sequence, not an edge case.

The scoped-upsert design (`MORNING_UPSERT_SQL` / `EVENING_UPSERT_SQL` writing
disjoint column sets to the same `(ticker, trade_date)` row) exists to support this
out-of-order fill. Never make the existence of a `daily_features` row conditional on
any single family (OI, OHLC, or chain) — the row key for T must be the union of
trade_dates present across all contributing families.
