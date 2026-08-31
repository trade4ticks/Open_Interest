# scalp — equities scalp ticker ranking

Ranks a candidate universe by how tradeable each name is for a passive scalping
strategy: post inside the best bid, wait for a fill, exit near the offer. Median
hold ~8 seconds, one position at a time.

This is an **opportunity ranker, not a profit predictor**. Earlier analysis of
live fills found tape features predict *whether a fill happens*, not *whether
it is profitable*. Nothing predicted profit per trade.

## Self-contained on purpose

Nothing in `scalp/` imports from the project root or from `../lib`. The
ThetaData client here is a deliberate copy of the patterns in
`../lib/thetadata.py` — typed status exceptions, the jittered retry ladder, the
connection semaphore, the streaming GET with a hard total-duration deadline,
the CSV-via-pyarrow parse path — not an import of it.

The stock subscription may be dropped. The options pipeline has to keep working
untouched if it is, so **`rm -rf scalp/` must leave the options code completely
unaffected**. A few hundred duplicated lines is the price of that guarantee.
Do not refactor the two into a shared module.

## Nothing here runs on a schedule

No cron entries are installed by anything in this directory. Every script is
run by hand.

## Step 0 — discovery, before any pipeline code

Run in order. **Stop after s0 and report.**

| script | question |
|---|---|
| `s0_availability.py` | Is `trade_quote` on the Standard plan? **GATE** |
| `s1_venue_check.py` | Which endpoints actually need `venue=utp_cta`? |
| `s2_one_day.py` | Row count, real schema, wall clock, parquet size |
| `s3_multiday_timing.py` | Does a 544-symbol backfill need concurrency? |
| `s4_conditions.py` | Which trade condition codes to exclude |
| `s5_quote_emission.py` | Change-only emission, or periodic samples? |

```
python -m scalp.step0.s0_availability
```

Each script prints a `WHAT A BAD RESULT LOOKS LIKE` section in its docstring
and a verdict at the end. Nothing writes to Postgres; nothing writes outside
`config.STEP0_DIR`, which is safe to delete.

### s0 gates everything

If `trade_quote` is Pro-only, the pipeline as designed cannot be built and the
remaining scripts are pointless. There is a specific reason to doubt the
answer: `lib/thetadata.py`'s `fetch_underlying_snapshot` docstring states the
subscription "doesn't include regular-stock endpoints" and works around it by
reading `underlying_price` off an options endpoint. That comment may predate a
subscription change, but it is the only first-hand evidence in the codebase and
it disagrees with the plan. (Leave that docstring alone — it belongs to the
options project.)

### s1 gates the metrics

`venue=utp_cta` is **not** hardcoded anywhere. `config.VENUE_BY_ENDPOINT` holds
a per-endpoint policy:

- `snapshot/ohlc` → `utp_cta`. **Measured**: the default `nqb` returned 44% of
  true volume and omitted ~10,000 symbols; `utp_cta` matched FDX's EOD figure
  of 956,900 shares exactly.
- `history/trade_quote`, `history/quote` → **unresolved, sending nothing**.
  Nothing was ever established about the history endpoints. Theta's docs
  describe a 15-minute delayed feed from all three SIP networks alongside a
  real-time Nasdaq Basic feed, which suggests these may already be
  consolidated — in which case the parameter is redundant or not even accepted.

`s1_venue_check.py` pulls the same symbol-day with and without the parameter,
sums trade sizes, and compares both against 956,900. Then set the table and set
`VENUE_POLICY_VERIFIED = True`. The fetch scripts refuse to run a bulk pull
while it is `False`, so a multi-hour backfill cannot be launched against an
unverified venue assumption.

A silently Nasdaq-only spread measurement would look completely plausible and
there is no EOD figure downstream to catch it. This is the only point in the
pipeline where the check is possible.

## Design constraints carried forward

- **Phase 2 is intraday.** Metric computation takes arbitrary start/end bounds
  rather than assuming a full session, so the same functions serve a mid-session
  re-rank. The intraday path is not being built now, just not walled off.
- **Parquet is the record**, under `config.DATA_DIR`, partitioned by symbol and
  date. Postgres holds derived metrics only — no tick data ever.
- **Every nightly ranking is retained**, not overwritten. In a month that is a
  feature history to test against actual fills.
- Scripts are independently runnable, idempotent, resumable, and incremental by
  default.
