# VPS State Capture — Open_Interest

Written 2026-09-14. The .txt and .sql files in this folder are real output
copied from the VPS. Treat those as ground truth. This README is notes, not
verified fact — where source code contradicts anything written here, the code
is right and this file should be corrected.

## Scope

This capture is about the Open_Interest project only.

Open_Interest shares a VPS and a single Postgres server with several unrelated
projects. Two consequences matter for the inventory:

- **This repo writes two databases:** `open_interest` (the main project) and
  `equities_scalp` (from `scalp/`). Always write tables as `database.table`.
- **The `open_interest` database is not exclusively ours.** A separate
  codebase also writes into it. Which tables those are is TBD — derive it from
  the code rather than assuming from table names. A table written here with no
  reader in this repo is not necessarily dead; its consumer may be external.

Other databases, directories, and cron jobs visible in these captures belong
to other projects and are out of scope. Don't infer owners for them.

## Files in this folder

| File | What it is |
|---|---|
| `crontab.txt` | `crontab -l`. The only scheduler on the box — includes jobs from other projects; the Open_Interest ones are commented as such. |
| `timers.txt` | `systemctl list-timers`. All stock Ubuntu housekeeping, nothing project-related. Confirmed negative. |
| `databases.txt` | Databases on the shared Postgres server. |
| `all_databases_tables.txt` | Table listings. Most rows are partition children — `open_interest` has ~40 real tables, not 124. |
| `table_stats.txt` | Row counts and sizes. The `last_stats` column is an ANALYZE timestamp, NOT a write time — do not read it as freshness. |
| `bin_freshness.txt` | `max(trade_date)` per bin table. This IS real freshness evidence. |
| `oi_schema_trimmed.sql` | Full `open_interest` schema, partition children excluded. |
| `env_paths.txt` | Resolved parquet paths from the VPS `.env`. Secrets redacted. |
| `storage_layout.txt` | Docker mounts, volume contents, `/data` sizes. |

The captures above date from 2026-09-14 and predate the retention work below:
`crontab.txt` has no `prune_equity_surface.py` line and
`all_databases_tables.txt` has no `equity_retention_log`. Re-capture both after
deploying.

## Resolved 2026-09-14

**tt_bins scheduling.** `tt_bins` had drifted a month behind because nothing
in cron ever passed `--build-tt-bins`. Now built incrementally in both the
07:00 premarket and 09:35 morning runs, via a flag added to the existing
`build_bin_tables.py` subprocess call in `run_pipeline_early.py` and
`run_pipeline.py`. No crontab change was needed.

- New flag `--build-tt-bins-incremental` rebuilds only rows at or after
  `--since`, defaulting to today − 45 days to match the window
  `build_features` re-upserts. ~3,900 rows and ~70s, versus ~228,000 rows and
  ~480s for the full rebuild.
- Safe because the train-test ruler is frozen at the 2024-01-01 cutoff, so a
  post-cutoff row's bin depends only on its own value and pre-cutoff history.
  Re-binning older rows would reproduce values they already hold. This is NOT
  true of `wf_bins` (expanding window) or `is_bins` (whole-history
  population), which is why neither has an incremental mode.
- Premarket gives pre-open bins from the provisional open; the 09:35 run
  overwrites the same rows with authoritative values.
- Deliberately NOT built at EVENING: that run creates the next trading day's
  row with MORNING columns still NULL, so binning it would write the
  (NULL, 0) sentinel for every MORNING metric.
- Verified 2026-09-14: full rebuild produced identical row counts before and
  after an incremental run, at every 5-ticker checkpoint.

## Retention — `equity_surface` (2026-10-25)

**The disk emergency.** The root disk hit 100% on 2026-10-25. Cause:
`open_interest.equity_surface` grew from ~350 MB/month to 3.3 GB (Aug), 11 GB
(Sep) and 9 GB (Oct, partial) once `fetch_live_surface.py` started writing
the full 5-minute grid on 2026-08-24 — 2,356,350 rows for a September session
against 64,923 for a June one. Nothing had ever purged it.
`equity_surface_diagnostics` (unpartitioned, ~9M rows, 454 sessions back to
Jan 2025) grows at the same rate.

**Manual slimming, same day.** `equity_surface_202608` and `_202609` were cut
by hand to `snapshot IN ('0945', '1545')` — create a copy table, insert the
kept rows, then detach / drop / rename / attach. Only the surface was
slimmed; their diagnostics rows were not touched. All of September was
slimmed, including Sep 25-30, which the 30-day rule below would still have
kept at full resolution.

**What the purge does now.** `prune_equity_surface.py` (dry-run unless
`--delete`; details in `docs/equity_surface.md` § Retention):

1. `equity_surface_diagnostics` — batched `DELETE` of intraday rows older than
   the cutoff (today − 30 days), then `VACUUM (ANALYZE)`.
2. `equity_surface` — every monthly partition whose whole range is older than
   the cutoff is rewritten to the `0945`/`1545` rows by the same
   detach/drop/rename/attach swap, in one transaction, and recorded in
   `open_interest.equity_retention_log`. A partition already free of intraday
   rows is logged as `already_slim` without a rewrite.

Partitioning stayed monthly, deliberately. The monthly unit was inherited from
`interpolate_SPX` rather than chosen for the 5-minute workload, but it was not
changed while volume storage is also being added.

**The window is 30-61 days, not 30.** A month is slimmed only when every day
in it is past the cutoff, so October keeps full 5-minute data until Dec 1,
November until Jan 1. Space comes back once a month, ~10 GB at a time, and
just before a swap two full months sit on disk.

**HAZARD — never run `build_equity_metrics.py batch --rebuild` over a slimmed
month.** The surface is gone for every intraday bucket there, but
`equity_atm` still holds them all, so `compute_metrics` returns a row with the
ATM values set and every skew / delta-node column NULL, and the upsert writes
it over the good `equity_metrics` row. Two defences:

- Diagnostics are purged first, and `--rebuild` takes its targets from
  diagnostics, so a purged bucket is normally never offered.
- `build_equity_metrics.py` skips any (date, snapshot) inside a month in
  `equity_retention_log` and logs `RETENTION GUARD` with the count.

Both depend on the purge having run. **The hand-slimmed Aug/Sep months are
unprotected until the first `--delete` run logs them**, and their intraday
diagnostics still exist. That first run must use
`--before 2026-10-01` so September is eligible and its Sep 25-30 diagnostics
go too.

`build_equity_surface.py --source intraday` is also refused inside a slimmed
month, since it would restore the rows retention removed.

**Not purged:** `equity_atm`, `equity_metrics`, `equity_metrics_z`. The
dashboard's intraday `/series` (`spx_analysis_dashboard`) reads them at every
bucket over its full history window. Together they grow roughly 1.8 GB/month
(extrapolated from the Sep 1-11 sizes in `table_stats.txt`).

## Known open issues

- **Pre-cutoff backfill obligation.** `--build-tt-bins-incremental` cannot
  detect edits to `daily_features` rows before 2024-01-01 — there is no
  timestamp column to detect them from. Such an edit moves the train-test
  ruler and invalidates every post-cutoff bin. **After any backfill touching
  trade_date < 2024-01-01, run a full `--build-tt-bins` by hand.**
- **Bin sentinel ambiguity (all three bin tables).** Between the EVENING run
  and 07:00, MORNING metrics for the new trade_date read as (NULL, 0), which
  is byte-identical to the "genuinely unrankable" sentinel. Nothing in the
  repo — build, validation, or consumer — can tell them apart. Not currently
  an active problem: the dashboard is only used after premarket completes
  (~07:15), by which point MORNING columns are populated. Note that the legacy
  `tt_thresholds` carried `n_train` per (metric, ticker, cutoff) so the read
  side could disambiguate; `tt_bins` dropped it. Restoring a count column is
  the likely fix if this ever matters.
- `FEATURES_LOOKBACK_DAYS = 45` is duplicated in `build_bin_tables.py` and
  `run_pipeline.py` — deliberately, to avoid a circular import. If one
  changes, change the other. The bin window must never be shorter than the
  feature re-upsert window.
- `CHAIN_INTRADAY_DIR` is set in `.env` but the directory doesn't exist. The
  scripts using it are manual-only and appear superseded by
  `fetch_live_surface.py` / `CHAIN_LIVE_DIR`.
- The five `research_*` tables in `open_interest` have never had a row
  inserted. Same names in another database hold real data. Nothing in this
  repo touches them.
- `/mnt/trading_volume_3/quote_ticks/`, `ext_trades/`, `backup/` — no writer
  in this project. Out of scope; don't guess.