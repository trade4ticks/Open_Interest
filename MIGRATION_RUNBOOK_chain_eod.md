# Migration runbook — vol + IV → raw chain refactor

This document covers the operational steps for migrating from the
pre-aggregated `option_volume_daily` / `option_iv_daily` Postgres tables
to a unified raw-chain parquet store at `data/chain_eod/`.

## What this PR changes

| Area | Before | After |
|---|---|---|
| Raw chain storage | None (raw data discarded after aggregation) | `data/chain_eod/{ticker}/{year}.parquet`, 10 columns per row |
| Aggregation site | `fetch_volume_eod.py:_aggregate` (Python) | `VOL_FEATURES_SQL` in `build_features.py` (DuckDB) |
| ATM IV interpolation | `fetch_iv_chain.py:_atm_iv_for_expiration` (Python) | `IV_FEATURES_SQL` in `build_features.py` (DuckDB SQL) |
| Strike split adjustment | At write time in both fetchers | At read time in `chain_adj` view (same pattern as OI) |
| Future splits | Required full historical re-fetch from ThetaData | No-op for chain — just rebuild `daily_features` |
| Output schema | All 163 `daily_features` columns unchanged | All 163 `daily_features` columns unchanged |

## Behavioural deltas to know about

These are deliberate changes from legacy behaviour, decided during planning:

1. **No boundary fallback in ATM interpolation.** When the chain's strikes don't bracket spot, `atm_iv_*d` is `NULL` rather than the nearest-strike IV. The legacy code's nearest-strike fallback was what masked the split-bug class. With split adjustment correct, missing-bracket should be rare; when it happens, the NULL is a data-quality signal.
2. **`iv_error` filter deferred.** For the initial backfill and first verification rebuild, no filter is applied — this isolates the refactor as the only variable. After the AAPL verification passes, analyze `iv_error` distribution and apply the filter at the `calls` CTE in `IV_FEATURES_SQL`.
3. **`source_session` column** added to the parquet schema (alongside `trade_date` and `feature_date`). In the EOD store `source_session == trade_date`; included for forward-compat with the future 15:45 endpoint and as an invariant we can check.

## Critical pre-conditions

- **PAUSE THE DAILY CRON** before starting. `build_features.py` now reads from `chain_adj` (parquet) instead of `option_volume_daily` / `option_iv_daily` (Postgres). Until the chain backfill completes, every `build_features` run will leave vol/IV columns NULL for any ticker without chain data. The cron should resume only after Step 3 verification passes and Step 5 (run_pipeline.py update) is in place.
- Have at least **30 GB free** on the disk hosting `data/chain_eod/`. Chains include zero-volume strikes (unlike OI), so this store will be 2–3× the size of `data/oi_raw/`.
- ThetaData terminal reachable from the VPS (existing `THETADATA_BASE_URL` env var).

## Step 0 — Pull the code

```bash
cd /Open_Interest
git pull
```

Confirm the new files landed:

```bash
ls -la fetch_chain_eod.py lib/chain_store.py MIGRATION_RUNBOOK_chain_eod.md
```

No schema migration needed — the refactor doesn't add or modify any Postgres tables in this PR.

## Step 1 — Backfill the chain parquet store

Use the new fetcher. It is **resumable per (ticker, date)** — interrupt with Ctrl-C and re-run; dates already present are skipped.

### Single-ticker smoke test first

Before the full run, verify the fetcher works end-to-end with one ticker:

```bash
python fetch_chain_eod.py
# Tickers: AAPL
# Fetch start date: 20240101
# Fetch end   date: 20240105
```

Then inspect the resulting parquet:

```bash
python - <<'EOF'
import pandas as pd
df = pd.read_parquet("data/chain_eod/AAPL/2024.parquet")
print(df.shape)
print(df.dtypes)
print(df.head(3))
print()
print("date sanity:")
print("  trade_date range:", df["trade_date"].min(), "→", df["trade_date"].max())
print("  feature_date - trade_date gap (trading-day count):")
print((pd.to_datetime(df["feature_date"]) - pd.to_datetime(df["trade_date"])).dt.days.value_counts().head())
EOF
```

You should see:
- ~1500–2500 rows per trade_date (all strikes × both option_types)
- gap days mostly 1 (Tue→Mon, Wed→Tue, etc.), 3 (Mon→Fri), 4+ around holidays
- `option_type` = `"C"` or `"P"` only
- `iv_error` either populated (likely) or all NaN with a one-line warning printed at fetch time

If the smoke test looks wrong, **stop and report** before kicking off the full backfill.

### Full backfill

Run in tmux. Expect 8–16 hours wall-clock for all ~125 tickers × 7 years at 4 concurrent.

```bash
tmux new -s chain_backfill
cd /Open_Interest
python fetch_chain_eod.py
# Tickers: (blank — uses all tickers from data/oi_raw/)
# Fetch start date: 20190101
# Fetch end   date: 20260527   (or whatever the most recent published session is)
# Ctrl-B then D to detach
```

Reattach with `tmux attach -t chain_backfill`. The script logs progress per ticker via tqdm and per-day failures inline.

### Per-parquet verification after backfill

```bash
python - <<'EOF'
from pathlib import Path
import pandas as pd

bad = []
total_size = 0
for tdir in sorted(Path("data/chain_eod").iterdir()):
    if not tdir.is_dir(): continue
    for pq in sorted(tdir.glob("*.parquet")):
        size = pq.stat().st_size
        total_size += size
        try:
            df = pd.read_parquet(pq)
        except Exception as e:
            bad.append((pq, f"read failed: {e}"))
            continue
        if df.empty:
            bad.append((pq, "empty")); continue
        if list(df.columns) != ["trade_date","source_session","feature_date","expiration",
                                "strike","option_type","volume","implied_vol","delta","iv_error"]:
            bad.append((pq, f"wrong columns: {list(df.columns)}")); continue
        # plausible row count: at least ~100 rows per trading day, more for liquid tickers
        n_days = df["trade_date"].nunique()
        n_rows = len(df)
        if n_days > 0 and n_rows / n_days < 50:
            bad.append((pq, f"sparse: {n_rows} rows / {n_days} days = {n_rows/n_days:.0f} per day"))

print(f"Total parquet size: {total_size / 1e9:.2f} GB")
print(f"Bad/suspect files: {len(bad)}")
for p, why in bad[:20]:
    print(f"  {p}: {why}")
EOF
```

Suspect rows (sparse days, etc.) are not necessarily bugs — illiquid tickers genuinely have fewer strikes. Use this as a smell test, not a hard gate.

## Step 2 — Verification gate #1 (AAPL)

Rebuild `daily_features` for **AAPL only** using the new code path, compare to your previously-verified AAPL row.

```bash
python build_features.py
# Tickers: AAPL
# Start date: 20190101
# End   date: 20260527
```

Expected log lines per ticker include "computing vol features ..." and "computing IV features ..." — confirming the new chain-driven SQL is being used. If you see "no chain_eod parquet" then the backfill didn't land for that ticker.

Then in psql:

```sql
-- Spot-check the previously verified AAPL row (date and predicted values from
-- your independent recalc — substitute the actual date you have verification for).
SELECT trade_date, atm_iv_30d, vrp_30d,
       vol_weighted_all_div_spot_pc, vol_above_below_ratio_pc,
       put_call_ratio_vol
FROM   daily_features
WHERE  ticker = 'AAPL' AND trade_date = '2019-07-02';

-- 2019-07-02 is the AAPL date you used for the split-fix verification.
-- Values should match the prediction table from that audit.
-- If atm_iv_30d is in the ~0.20–0.30 range (not the bug-era ~0.55), the
-- IV refactor is producing sane values. Cross-check vol_weighted_all_div_spot_pc
-- against the predicted value too.
```

**Decision point**: if values match the verified row, proceed to Step 3. If not, debug before scaling.

Common things to check if values differ:
- AAPL chain parquet has 2019 data: `ls data/chain_eod/AAPL/2019.parquet` and inspect with the snippet above
- `chain_adj` view is being created: look for "computing vol features ..." in the build_features log
- Split factors look right: `SELECT * FROM underlying_ohlc WHERE ticker='AAPL' AND splits != 0 ORDER BY trade_date;` should show the 2020-08-31 4:1 (ratio = 4.0)

## Step 3 — Verification gate #2 (cross-ticker)

Rebuild for a small set of tickers covering split / no-split / pre-split / post-split / split-boundary regimes:

```bash
python build_features.py
# Tickers: NVDA,SPY,XLK,RIOT,AAL
# (Adjust to taste; NVDA and AAPL both had splits; SPY/XLK/AAL have none; RIOT had a reverse.)
# Start date: 20190101
# End   date: 20260527
```

Then spot-check across split boundaries:

```sql
-- Smoothness check: pick a ticker with a known split (e.g. NVDA 2024-06-10 10:1).
-- vol_weighted_all_div_spot_pc should NOT show a 10x / 0.1x discontinuity
-- across the split day.
SELECT trade_date,
       vol_weighted_all_div_spot_pc,
       pct_vol_within_5pct_pc,
       atm_iv_30d
FROM   daily_features
WHERE  ticker = 'NVDA'
  AND  trade_date BETWEEN '2024-06-05' AND '2024-06-14'
ORDER  BY trade_date;

-- 2019 AAPL: atm_iv_30d should be in the ~0.20–0.30 range, not the bug-era
-- ~0.55+ readings. Monthly aggregates:
SELECT date_trunc('month', trade_date) AS mo,
       AVG(atm_iv_30d) AS mean_iv,
       MAX(atm_iv_30d) AS max_iv,
       COUNT(*) FILTER (WHERE atm_iv_30d IS NULL) AS n_null
FROM   daily_features
WHERE  ticker = 'AAPL' AND trade_date BETWEEN '2019-01-01' AND '2019-12-31'
GROUP  BY 1 ORDER BY 1;

-- NULL-rate sanity. ATM IV should be NULL only when the chain genuinely
-- didn't bracket spot (rare). Track this per ticker:
SELECT ticker,
       COUNT(*)                            AS n_rows,
       COUNT(*) FILTER (WHERE atm_iv_30d IS NULL) AS n_null_iv,
       ROUND(100.0 * COUNT(*) FILTER (WHERE atm_iv_30d IS NULL) / COUNT(*), 2) AS pct_null
FROM   daily_features
WHERE  trade_date >= '2019-01-01'
GROUP  BY ticker
ORDER  BY pct_null DESC
LIMIT  20;
```

Acceptance criteria:
- No discontinuity at split boundaries
- AAPL 2019 atm_iv_30d in plausible range (~0.20–0.35 monthly mean)
- ATM IV NULL rate per ticker generally under a few percent; outliers worth investigating but not blocking

If anything looks off, debug before scaling to the full universe.

## Step 4 — Full rebuild

```bash
python build_features.py
# Tickers: (blank — all tickers in OI store)
# Start date: 20190101
# End   date: 20260527
```

Wall-clock: several hours; ~minutes per ticker depending on history depth.

## Step 5 — Update `run_pipeline.py` for the new fetcher

Replace the two old fetcher calls (steps 2c and 2d) with one new call to `fetch_chain_eod`. I have NOT made this change in the current PR because the cron should not run the new code path until backfill + verification complete.

Replace **steps 2c and 2d** in `run_pipeline.py` with a single step that calls `fetch_chain_eod.fetch_ticker` per ticker over the same `last_trading_day(today - timedelta(days=1))`-anchored 10-day rolling window. Remove the `run_vol_eod_fetch` and `run_iv_chain_fetch` imports.

Once the cron is updated, re-enable it.

## Step 6 — Drop legacy tables

Only after Steps 2–5 all pass.

```bash
# First, find any remaining code references to the old tables:
grep -rn "option_volume_daily\|option_iv_daily" \
    --include="*.py" --include="*.sql" \
    --exclude-dir=".venv" --exclude-dir=".git" \
    /Open_Interest

# Expected remaining references after this PR:
#   sql/03_new_metrics.sql       — CREATE TABLE definitions (leave for fresh-install reference, or delete the blocks)
#   sql/01_schema.sql section 10 — source_session ADD COLUMN migration (leave; idempotent on missing tables)
#   backfill_source_session.py   — the one-time backfill (delete or archive)
#
# Anything in fetch_volume_eod.py / fetch_iv_chain.py / run_pipeline.py is a
# deprecation residual; those files can be deleted entirely.
```

Once you've reviewed the grep output:

```sql
-- Drop the legacy tables. backfill_source_session.py and the source_session
-- migration silently no-op if the table is gone (IF NOT EXISTS guards).
DROP TABLE IF EXISTS option_volume_daily;
DROP TABLE IF EXISTS option_iv_daily;
```

Then delete the deprecated Python files:

```bash
git rm fetch_volume_eod.py fetch_iv_chain.py backfill_source_session.py
```

And clean up the CREATE TABLE blocks in `sql/03_new_metrics.sql` (the two tables and the section 10 ALTER TABLE in `sql/01_schema.sql` for `source_session`). Commit as a cleanup PR.

## Future splits

After this refactor, a new split in `underlying_ohlc.splits` is fully handled by re-running `build_features.py` for the affected ticker over the full history. The split factor changes for pre-split sessions, the `chain_adj` view recomputes adjusted strikes, and all metrics update.

No re-fetch from ThetaData required. No `option_volume_daily` / `option_iv_daily` re-population required (those tables no longer exist).

This is the architectural payoff of the refactor.

## Rollback

If verification fails and you need to revert before backfill completes:

```bash
cd /Open_Interest
git revert <this-PR-commit-hash>
# Run init_db.py is unnecessary — no schema changes in this PR.
```

The legacy `option_volume_daily` / `option_iv_daily` tables still have all their data; the old code paths can resume immediately after revert. Backfilled chain parquet under `data/chain_eod/` does no harm if left in place — it just becomes orphaned data that nothing reads.
