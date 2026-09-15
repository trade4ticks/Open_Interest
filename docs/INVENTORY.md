# Open_Interest — Inventory

Generated 2026-09-14. Read-only pass; nothing in the project was modified.

**Verification basis.** Script facts (entry point, flags, reads, writes) are read
from source in the working tree. Database facts are read from `docs/vps_state/`,
which is authoritative. Anything not established by one of those two is marked
`UNCERTAIN` with the reason. Tables are always written `database.table`.

**Scope of this file so far:** Section 1a only — root, `lib/`, `analysis/`,
`migrations/`. `scalp/` and `Cowork/metrics_panel` are deliberately excluded and
will be added as Section 1b. Sections 2 and 3 are not yet written.

---

## Revision log

| Date | Change |
|---|---|
| 2026-09-14 (first pass) | Sections 1a written against an 8-file `vps_state/` with a truncated `crontab.txt`. |
| 2026-09-14 (revision 1) | `vps_state/README.md` supplied; `crontab.txt` re-supplied complete (8 jobs, was 6); `bin_freshness.txt` added; `batabases.txt` renamed `databases.txt`. Items revised below are tagged **[rev1]**. Section 2 added. |
| 2026-09-14 (revision 2) | Precedence rule changed: **source code beats `vps_state/README.md`**; only the machine-generated capture files are truth. Both §2a conflicts confirmed as README errors and applied. External-writer list re-derived by grep rather than taken from README:19. Section 1b added. Tagged **[rev2]**. |

## Precedence rule (current)

1. **Machine-verified captures** — `crontab.txt`, `oi_schema_trimmed.sql`,
   `table_stats.txt`, `env_paths.txt`, `storage_layout.txt`,
   `bin_freshness.txt`. Truth, no exceptions.
2. **Source code in this repo.** Beats all prose, including
   `vps_state/README.md`.
3. **`vps_state/README.md` prose** — orientation notes. Useful for intent and
   for out-of-scope boundaries; not authoritative on what writes what.
4. **Project-root docs** (`README.md`, `CLAUDE.md`, `PIPELINE_BRIEF.md`) — lowest.

Where 2 beats 3, the README is listed as needing correction, not as a finding.

## Gaps in the supplied VPS state

| Expected | Actual | Effect on this inventory |
|---|---|---|
| `docs/vps_state/README.md` | **[rev1] Now present.** | Resolved. Scope, ownership and known-issues notes now available. Used as the authority for which `open_interest` tables have external writers. |
| `docs/vps_state/databases.txt` | **[rev1] Now present** under the correct name; `batabases.txt` removed | Resolved. |
| `docs/vps_state/crontab.txt` | **[rev1] Was truncated** — first two jobs missing, so the file opened mid-command on a `>> /Thetadata_Raw_SPX/logs/pipeline.log` fragment. Now complete: **8 jobs**, 5 of them this project's | Conflict 3 and question Q4 revised below. No Section 1a script row changes: the two recovered jobs belong to other projects and invoke nothing in this repo. |
| `table_stats.txt` database scope | Header omits the database name | Row counts assumed to be `open_interest` (every relname matches that DB's table list and none match the others). `UNCERTAIN` only in that the file does not say so. |
| `table_stats.txt` `last_stats` column | 100+ rows share timestamps in a 6-minute band on 2026-09-01 | This is an ANALYZE sweep, not a last-write time. **`last_stats` is not evidence of when a table was last written** — only of when statistics were last collected. Rows with later timestamps were written after the sweep; rows still at 2026-09-01 may or may not have been. Used only as a weak lower bound below. |

---

## Findings that should not wait for Section 3

### 1. `build_features.py` is destroyed in the working tree

The file is 2 bytes: a newline and the character `q`. `git diff` reports
**1755 deletions, 1 insertion**, uncommitted. The committed version at `HEAD` is
89,858 bytes and contains `MORNING_UPSERT_SQL` / `EVENING_UPSERT_SQL`,
`build_for_ticker`, and the whole feature SQL body.

This looks like a vi `q` typed into an editor that was not in command mode, then
saved. `run_pipeline.py:57` and `run_pipeline_early.py:49` both do
`from build_features import build_for_ticker` at module import time, so **any
local run of either pipeline fails at import**. The VPS runs from its own
checkout, so cron is unaffected as long as the VPS has not pulled this state.

Not acting on it — this is a read-only pass — but it is the most consequential
thing in the tree. Recovery is `git checkout -- build_features.py`, which
discards only the 1-character edit.

All `build_features.py` rows below are read from `HEAD`, not the working tree,
and are marked accordingly.

### 2. `open_interest.tt_bins` has no caller anywhere

`build_bin_tables.py:566` defines `--build-tt-bins` to build
`open_interest.tt_bins`. `run_pipeline.py:237` appends `--build-tt` — a
*different flag* that builds `open_interest.tt_thresholds`. No script, cron
entry, or timer passes `--build-tt-bins`. Detail in the answers below.

---

## Conflicts between project docs and `docs/vps_state/` (vps_state wins)

You said you already know all three are wrong about cron. Listing every conflict
found anyway, since these docs need fixing afterward.

| # | Doc | Doc says | VPS truth | Severity |
|---|---|---|---|---|
| 1 | `README.md:119-122` | One cron entry: `0 7 * * 1-5 … run_pipeline.py` with no tier flag | `crontab.txt` has **five** Open_Interest entries. `run_pipeline.py` is never invoked without `--tier`; 07:00 runs `run_pipeline_early.py` instead | High — the documented command would crash (`--tier` is `required=True`, `run_pipeline.py:255`) |
| 2 | `README.md:106` heading "Daily workflow (cron)" | Describes a single daily run of four steps | Three daily tiers (18:00 EVENING, 07:00 PREMARKET, 09:35 MORNING) plus a 5-minute intraday loop and a 20:30 earnings job | High |
| 3 | `PIPELINE_BRIEF.md:244` | "Nothing runs on a schedule unless I add it to cron myself." | **[rev1]** Eight cron jobs are active, five of them this project's (the other three: a Portfolio_Dashboard trade fetch every 5 min, the Thetadata_Raw_SPX pipeline every 5 min offset by 1, and the 02:00 `pg_backup.sh`) | High — reads as "no automation exists" |
| 4 | `CLAUDE.md:19-24` | EVENING and MORNING tiers described; asserts "the EVENING cron always runs **before** the MORNING cron for any given trade_date" | True as written, but the doc does not mention the **PREMARKET tier at 07:00**, which also calls `build_features` with `tier="MORNING"` (`run_pipeline_early.py:188`). So two different crons write MORNING columns for the same trade_date | Medium — the two-tier invariant is really a three-cron, two-tier-of-columns invariant |
| 5 | `build_bin_tables.py:2` docstring | "Build / refresh wf_bins and tt_thresholds" | Also builds `open_interest.is_bins` (line 266) and `open_interest.tt_bins` (line 377) | Medium — docstring predates half the file |
| 6 | `build_bin_tables.py:5-6` usage block | `--tier EVENING [--build-tt]` / `--tier MORNING` | Accurate, but omits `--build-tt-bins` entirely, which is the only way `open_interest.tt_bins` is ever written | High — this omission is why the table is stale |
| 7 | `README.md:32` | `config.py` described as "Postgres + ThetaData + OI_RAW_DIR" | `config.py` defines six parquet roots plus Polygon credentials | Low |
| 8 | `sql/01_schema.sql:10` | "`{OI_RAW_DIR}/{ticker}/{year}.parquet`, which build_features.py reads directly" | True at `HEAD`; the working-tree file reads nothing | Low (artifact of finding 1) |
| 9 | `run_live_pipeline.py:45-49` | Gives the cron line as "replaces the fetch_live_surface entry" | Matches `crontab.txt:16` exactly | **No conflict** — noted because it is the one doc that is right |
| 10 | `migrations/README.md:13` | `option_oi_raw` "Table dropped 2026-09-01" | Confirmed: `option_oi_raw` is absent from `open_interest` in `all_databases_tables.txt` | **No conflict** — confirms the doc |

---

## Section 1a — Script inventory (root, `lib/`, `analysis/`, `migrations/`)

Legend — **Kind**: `entry` = has `if __name__ == "__main__"`; `lib` = imported
only. **Invoked by**: `cron` means a `crontab.txt` line reaches it, directly or
through an import/subprocess chain.

### Pipeline orchestrators

| Path | Purpose (from docstring) | Kind | CLI flags | Invoked by | Git | READS | WRITES |
|---|---|---|---|---|---|---|---|
| `run_pipeline.py` | "Daily two-tier pipeline." | entry | `--tier {MORNING,EVENING}` (required) | **cron** — `crontab.txt:7` 18:00 EVENING, `:13` 09:35 MORNING, both under `flock /tmp/oi_research.lock` | 2026-06-10 | `open_interest.underlying_ohlc` (ticker list, :91); `OI_RAW_DIR` (ticker list); ThetaData `/v3` (reachability) | Nothing directly. Orchestrates `fetch_ohlc`, `fetch_chain_eod`, `fetch_oi`, `fetch_oi_snapshot`, `build_features`, then subprocess `build_bin_tables.py --tier <tier>` (+`--build-tt` on EVENING) |
| `run_pipeline_early.py` | "7 am pre-open pipeline." | entry | none | **cron** — `crontab.txt:10` 07:00, same lock | 2026-06-07 | `open_interest.underlying_ohlc` (:80); `OI_RAW_DIR` | Nothing directly. Calls `fetch_ohlc_premarket.run`, `fetch_oi_snapshot`, `build_features.build_for_ticker(tier="MORNING")`, then subprocess `build_bin_tables.py --tier MORNING` (:112) |
| `run_live_pipeline.py` | "live capture, then metrics for what it just captured." | entry | `--skip-metrics`, `--no-z`, plus all of `fetch_live_surface.build_parser()`: `--tickers --connections --workers --max-inflight --no-persist --force --lock --no-lock` | **cron** — `crontab.txt:16` `*/5 9-16 * * 1-5`, `flock /tmp/live_surface.lock` | 2026-08-25 | via stage 1 and 2 below | via stage 1 and 2 below |

### Fetchers

| Path | Purpose (from docstring) | Kind | CLI flags | Invoked by | Git | READS | WRITES |
|---|---|---|---|---|---|---|---|
| `fetch_ohlc.py` | "Pull daily OHLC … from yfinance and upsert into underlying_ohlc." | entry | none (interactive prompt) | `run_pipeline.py` both tiers → **cron** | 2026-06-07 | yfinance API; `open_interest.underlying_ohlc` (:53) | `open_interest.underlying_ohlc` (:29, `UPSERT_SQL`) |
| `fetch_ohlc_premarket.py` | "Fetch a premarket open-price proxy from ThetaData's IV snapshot endpoint and write it to underlying_ohlc.open with provenance." | entry | none | `run_pipeline_early.py:150` → **cron** | 2026-06-09 | ThetaData IV snapshot endpoint | `open_interest.underlying_ohlc` (:48, `PREMARKET_UPSERT_SQL` — `open` column only) |
| `fetch_oi.py` | "Pull daily Open Interest … and write to parquet at `{OI_RAW_DIR}/{ticker}/{year}.parquet`." | entry | none (interactive prompt) | `run_pipeline.py` MORNING → **cron** | 2026-09-01 | ThetaData OI history endpoint; `OI_RAW_DIR` (ticker list) | `OI_RAW_DIR/{TICKER}/{year}.parquet` via `lib/parquet_store.write_rows` |
| `fetch_oi_snapshot.py` | "Pull TODAY's OI snapshot … and write to parquet at `{OI_RAW_DIR}/…`." | entry | none (interactive prompt) | `run_pipeline.py` MORNING + `run_pipeline_early.py` → **cron** | 2026-05-19 | ThetaData `/v3/option/snapshot/open_interest` | `OI_RAW_DIR/{TICKER}/{year}.parquet` (same dedup key as `fetch_oi.py`) |
| `fetch_chain_eod.py` | "Pure I/O fetcher for ThetaData's EOD greeks chain. Replaces both fetch_volume_eod.py and fetch_iv_chain.py." | entry | none | `run_pipeline.py` EVENING → **cron** | 2026-06-08 | ThetaData EOD greeks; `OI_RAW_DIR` (ticker list) | `CHAIN_EOD_DIR/{TICKER}/{year}.parquet` via `lib/chain_store` |
| `fetch_live_surface.py` | "live intraday surface, every 5 minutes." | entry | `--tickers --connections --workers --max-inflight --no-persist --force --lock --no-lock` | `run_live_pipeline.py` → **cron** | 2026-08-25 | ThetaData first-order snapshot; `open_interest.equity_surface_diagnostics` (dedup, via `surface_store`) | `CHAIN_LIVE_DIR/{TICKER}/{YYYYMMDD}/{HHMM}.parquet` (:306, pre-clean); `open_interest.equity_surface`, `open_interest.equity_atm`, `open_interest.equity_surface_diagnostics` (:471 `write_snapshot`) |
| `fetch_earnings_calendar.py` | "nightly earnings calendar refresh from yfinance." | entry | `--tickers --delay --limit --init-db` | **cron** — `crontab.txt:23` 20:30 weekdays, `flock /tmp/earnings.lock` | 2026-08-25 | yfinance `get_earnings_dates`; `CHAIN_SNAPSHOTS_DIR` + `OI_RAW_DIR` ticker lists (:98-99) | `open_interest.earnings_calendar`, `open_interest.earnings_coverage` via `lib/earnings_store` |
| `fetch_chain_snapshots.py` | "Pure I/O fetcher for twice-daily intraday option chain snapshots (09:45:00 and 15:45:00 ET)." | entry | `--force --repair --batch-days --connections --write-queue --debug-response --resume --progress-file --tickers --start --end` | **manual only** — no cron entry, no in-project caller | 2026-08-26 | ThetaData; `OI_RAW_DIR` ticker list | `CHAIN_SNAPSHOTS_DIR/{TICKER}/{YYYYMM}.parquet` |
| `fetch_chain_intraday.py` | "full-day 5-minute intraday option chain bars. Sister script to fetch_chain_snapshots.py." | entry | `--force --batch-days --response-format --connections --debug-response --tickers --start --end` | **manual only** — see Q1 below | 2026-08-18 | ThetaData; `OI_RAW_DIR` ticker list | `CHAIN_INTRADAY_DIR/{TICKER}/{YYYYMMDD}.parquet` + `.manifest.json` |
| `fetch_equity_1min.py` | "1-minute equity bars from Polygon.io / Massive." | entry | `--dry-run --probe --force --repair --batch-chunks --connections --splice-renames --symbol-history-out --no-symbol-history --debug-response --tickers --start --end` | **manual only** | 2026-08-15 | Polygon/Massive REST; `OI_RAW_DIR` ticker list | `EQUITY_1MIN_DIR/{TICKER}/{year}.parquet` + `EQUITY_1MIN_DIR/_manifest/{TICKER}.parquet` |
| `fetch_iv_chain.py` | "Pull EOD greeks … and upsert ATM IV metrics to option_iv_daily." | entry | none | **nothing found** — superseded by `fetch_chain_eod.py` per that file's docstring | 2026-05-27 | ThetaData; `open_interest.underlying_ohlc` (:209); `OI_RAW_DIR` ticker list | `open_interest.option_iv_daily` (:68) |
| `fetch_volume_eod.py` | "Pull EOD option volume … and upsert aggregated metrics to option_volume_daily." | entry | none | **nothing found** — superseded by `fetch_chain_eod.py` | 2026-05-27 | ThetaData; `open_interest.underlying_ohlc` (:93, :199) | `open_interest.option_volume_daily` (:49) |

### Builders

| Path | Purpose (from docstring) | Kind | CLI flags | Invoked by | Git | READS | WRITES |
|---|---|---|---|---|---|---|---|
| `build_features.py` | **Working tree: 2 bytes, contents destroyed.** All facts below from `git show HEAD:build_features.py`. Builds the `daily_features` row per (ticker, trade_date) across MORNING/EVENING column sets. | entry (at HEAD) | `--tier` (at HEAD; `run_pipeline*` call `build_for_ticker()` directly, not the CLI) | `run_pipeline.py` both tiers + `run_pipeline_early.py` → **cron** | 2026-06-09 (last commit; working tree edit uncommitted) | `open_interest.underlying_ohlc` (:1273, :1290); `OI_RAW_DIR` parquet via DuckDB `read_parquet` (:1304, :1417, :1471); `CHAIN_EOD_DIR` parquet via DuckDB (:1500) | `open_interest.daily_features` (:1138, via `MORNING_UPSERT_SQL` :1261 / `EVENING_UPSERT_SQL` :1262, disjoint column sets) |
| `build_bin_tables.py` | "Build / refresh wf_bins and tt_thresholds." (docstring stale — see conflict 5) | entry | `--tier {MORNING,EVENING}`, `--build-tt-bins`, `--build-tt` | subprocess from `run_pipeline.py:235` (both tiers, `--build-tt` added on EVENING) and `run_pipeline_early.py:112` (`--tier MORNING`) → **cron** | 2026-08-16 | `open_interest.daily_features` (:129, :244, :356, :399, :474, :482); `open_interest.metric_classification` (via `lib/bin_schema.py:51`) | `--tier` → `open_interest.wf_bins` (:153) **and** `open_interest.is_bins` (:266); `--build-tt-bins` → `open_interest.tt_bins` (:377); `--build-tt` → `open_interest.tt_thresholds` (:520) |
| `build_equity_surface.py` | "stage 3 of the chain pipeline. fetch -> clean -> INTERPOLATE -> metrics" | entry | `--start --end --tickers --source --workers` | **manual only** — no cron entry, no in-project caller | 2026-08-26 | `CHAIN_SNAPSHOTS_DIR` (default) or `CHAIN_INTRADAY_DIR` (`--source intraday`), :61-63; `open_interest.equity_surface_diagnostics` (resume point) | `open_interest.equity_surface`, `open_interest.equity_atm`, `open_interest.equity_surface_diagnostics` via `lib/surface_store.write_snapshot` |
| `build_equity_metrics.py` | "derived vol metrics off the interpolated surface. Stage 4." | entry | `--start --end --tickers --snapshots --rebuild --no-z` | `run_live_pipeline.py:75` imports `run_for_snapshot` → **cron**; CLI form is manual | 2026-08-25 | `open_interest.equity_surface_diagnostics` (:155); `open_interest.equity_surface`, `open_interest.equity_atm`, `open_interest.underlying_ohlc` (via `lib/metrics_compute.py:230-326`); `open_interest.earnings_calendar` (via `lib/earnings_store`) | `open_interest.equity_metrics`, `open_interest.equity_metrics_z`, `open_interest.equity_metrics_catalog` (via `lib/metrics_store.py:37-39`) |
| `build_trade_paths.py` | "resolve exit outcomes for every possible entry." | entry | `--tickers --anchors --start --end --session-filter --block --force --dry-run` | **manual only** | 2026-09-05 | `open_interest.daily_features` (:124); `open_interest.tt_bins` (:473, :478, :487, :496); `EQUITY_1MIN_DIR` via `lib/equity_1min_store` (:141) | `open_interest.trade_paths` (:433, via `COPY _tp_stage` :426); `open_interest.trade_paths_manifest` (:444); `open_interest.trade_path_rules` (via `lib/trade_path_schema.py:114`) |

### Backfills and migrations

| Path | Purpose (from docstring) | Kind | CLI flags | Invoked by | Git | READS | WRITES |
|---|---|---|---|---|---|---|---|
| `backfill_call_price.py` | "populate equity_surface.call_price on rows written before that column existed, WITHOUT re-fitting." | entry | `--start --end --dry-run --verify` | manual (one-time) | 2026-08-23 | `open_interest.equity_surface` (:304, :320) | `open_interest.equity_surface` (:277, UPDATE — `call_price` only) |
| `backfill_days_to_earnings.py` | "populate days_to_earnings on existing rows." | entry | `--tickers --dry-run --report --verify --limit --all` | manual (one-time) | 2026-08-25 | `open_interest.earnings_calendar` (:58, :112); `open_interest.earnings_coverage` (:98); `open_interest.equity_metrics` (:72, :97, :154) | `open_interest.equity_metrics` (:67, UPDATE — `days_to_earnings` only) |
| `backfill_source_session.py` | "One-time UPDATE to populate source_session on every existing row of option_volume_daily and option_iv_daily." | entry | none | manual (one-time, complete) | 2026-05-27 | both tables below | `open_interest.option_volume_daily` (:125), `open_interest.option_iv_daily` (:126) — `source_session` only |
| `migrate_chain_snapshots_to_monthly.py` | "one-time layout change for the chain_snapshots store" — `{YYYY}.parquet` → `{YYYYMM}.parquet` | entry | `--dry-run --tickers --years --verify --keep-year-files --no-normalize --keep-going` | manual (one-time) | 2026-08-26 | `CHAIN_SNAPSHOTS_DIR/{TICKER}/{YYYY}.parquet` | `CHAIN_SNAPSHOTS_DIR/{TICKER}/{YYYYMM}.parquet` (deletes year files unless `--keep-year-files`) |
| `migrations/export_raw_to_parquet.py` | "One-time migration: Postgres `option_oi_raw` → `{OI_RAW_DIR}/{ticker}/{year}.parquet`" | entry | none | manual — **complete**; source table dropped 2026-09-01 | 2026-09-01 | `open_interest.option_oi_raw` (:30-49) — **table no longer exists**; script cannot run | `OI_RAW_DIR/{TICKER}/{year}.parquet` via `lib/parquet_store.write_year` |
| `init_db.py` | "One-time schema + view initialisation for the open_interest DB." | entry | none | manual | 2026-08-25 | `sql/*.sql` files | Applies DDL to `open_interest`; calls `lib/bin_schema` syncs (:47) and `lib/metrics_store.sync_all` (:74) |
| `migrations/drop_option_oi_surface.sql` | Drops the retired `option_oi_surface` table | sql | — | manual (applied) | 2026-09-01 | — | drops `open_interest.option_oi_surface` |
| `migrations/rebuild_intraday_metrics.sql` | Rebuilds partitioned `intraday_metrics` | sql | — | manual | 2026-09-07 | — | `equities_scalp.intraday_metrics` (`TRUNCATE` :46, :68) — **belongs to the `scalp` system, not this pipeline** |

### Audits, diagnostics, probes — all read-only unless noted

| Path | Purpose (from docstring) | Kind | CLI flags | Invoked by | Git | READS | WRITES |
|---|---|---|---|---|---|---|---|
| `audit_chain_snapshots.py` | "completeness audit of the chain_snapshots store. Read-only." | entry | `--tickers --start --end --max-gap --universe --no-enumerate --connections --out --summary-out` | manual | 2026-08-26 | `CHAIN_SNAPSHOTS_DIR`; `OI_RAW_DIR` ticker list; ThetaData (enumeration, unless `--no-enumerate`) | CSV files at `--out` / `--summary-out` |
| `audit_equity_1min.py` | "completeness audit of the equity_1min store. Read-only and fully offline." | entry | `--tickers --start --end --max-gap --regular-ratio --universe --no-leading-empty --leading-empty-out --out --summary-out` | manual | 2026-08-15 | `EQUITY_1MIN_DIR`; `OI_RAW_DIR` ticker list | CSV files at `--out` / `--summary-out` / `--leading-empty-out` |
| `audit_symbol_gaps.py` | "how much history the ticker-rename gap has already cost. READ-ONLY." | entry | `--tickers --history-start --no-vendor --known-renames --out` | manual | 2026-08-15 | `open_interest.daily_features` (:62); `open_interest.underlying_ohlc` (:70); `OI_RAW_DIR`; `CHAIN_EOD_DIR`; vendor API unless `--no-vendor` | CSV at `--out` |
| `audit_trade_paths.py` | "correctness and coverage checks for trade_paths. Read-only." | entry | `--tolerance --skip-coverage` | manual | 2026-08-15 | `open_interest.trade_paths` (:74 et al.); `open_interest.daily_features` (:75); `open_interest.underlying_ohlc` (:124, :178) | stdout only |
| `snapshot_null_audit.py` | "every column, NULL rate intraday vs at the close. Read-only." | entry | `--date --table {equity_metrics,equity_metrics_z,both} --threshold --by-bucket --verbose` | manual | 2026-08-25 | `open_interest.equity_metrics`, `open_interest.equity_metrics_z` | stdout only |
| `validate_bins.py` | "8-check validation suite for wf_bins and tt_thresholds." | entry | `--verbose` | manual, after `build_bin_tables.py` | 2026-06-01 | `open_interest.daily_features`; `open_interest.wf_bins`; `open_interest.tt_thresholds` | stdout only. **Does not check `is_bins` or `tt_bins`** — both postdate it |
| `zscore_migration_check.py` | "scope and preview the z-score redefinition. Read-only." | entry | `--scope --diff --ticker --limit` | manual | 2026-08-25 | `open_interest.equity_metrics` (:80); `open_interest.equity_metrics_z` (:44, :102) | stdout only |
| `rv_vrp_noise_report.py` | "quantify the short-tenor VRP noise. Read-only." | entry | `--snapshot --min-obs --ticker --ratio` | manual | 2026-08-25 | `open_interest.equity_metrics` (:70) | stdout only |
| `iv25d_quality_report.py` | "Read-only diagnostic measuring 25-delta IV data quality in data/chain_eod/" | entry | none | manual | 2026-06-09 | `CHAIN_EOD_DIR` parquet via DuckDB (:404, :423); `open_interest.daily_features` (:390, :531, :730) | stdout only |
| `phase1_iv25d_sample.py` | "Phase 1 verification of the proposed 25-delta IV metrics (14d and 30d tenors)." | entry | `--tickers --dates --n-dates` | manual (one-time study) | 2026-06-09 | `CHAIN_EOD_DIR` parquet via DuckDB (:376-425); `open_interest.underlying_ohlc` (:407) | stdout only |
| `verify_surface_fixes.py` | "attribute surface changes to a specific fix. Read-only. No database, no writes." | entry | `--ticker --date --snapshot --source --tenor --sweep-knots` | manual | 2026-08-26 | `CHAIN_INTRADAY_DIR` (:62), `CHAIN_LIVE_DIR` (:68), or `CHAIN_SNAPSHOTS_DIR` (:73) depending on `--source` | stdout only |
| `verify_csv_path.py` | "prove the CSV response path matches the JSON one. Read-only." | entry | `--ticker --date --expiration` | manual | 2026-08-18 | ThetaData (same request twice, CSV + JSON) | stdout only |
| `diagnose_transform.py` | "why is write_expiration ~15x slower on the VPS? Read-only diagnostic." | entry | `--ticker --date --expiration` | manual | 2026-08-18 | ThetaData (one enumeration + one interval call) | **Writes probe files** to `CHAIN_INTRADAY_DIR/_diag/` (:263-267) and system temp — the one "read-only diagnostic" that is not |
| `probe_session_gap.py` | "Why does audit_chain_snapshots see a session that fetch_chain_snapshots won't fetch?" | entry | `--ticker --date --window` | manual | 2026-09-07 | ThetaData; `CHAIN_SNAPSHOTS_DIR` | stdout only |
| `probe_snapshot_first_order.py` | "does /v3/option/snapshot/greeks/first_order accept expiration=*?" | entry | `--tickers --sample --format` | manual | 2026-08-23 | ThetaData | stdout only |
| `bench_greeks_endpoints.py` | "first_order vs implied_volatility, head to head." | entry | `--tickers --date --pairs --warmup --start-time --end-time --interval --format` | manual | 2026-08-23 | ThetaData (both endpoints) | stdout only |
| `backtest_spreads.py` | "Backtest call debit spreads from a predefined trade list." | entry | none | manual | 2026-05-19 | input CSV (`ticker, trade_date, exit_date, …`); `open_interest.underlying_ohlc` (:151); `OI_RAW_DIR` via `read_range` (:36) | `open_interest.backtest_call_spread` (:68) |
| `check.py` | "pre-push gates. Run this before every push." | entry | `--gate` | manual (developer) | 2026-08-16 | project source files | stdout / exit code |
| `deadlock_proof.py` | "Reproduce the run_cycle deadlock and prove the interleaved loop fixes it." | entry | none | manual (one-time proof) | 2026-08-24 | nothing | stdout only |
| `lockproof.py` | "Reproduce the outage mechanism and prove the fix. POSIX only." | entry | none | manual (one-time proof) | 2026-08-24 | temp files only | temp files only |

### `lib/` — all library modules, none has a `__main__`

| Path | Purpose (from docstring) | Kind | Invoked by | Git | READS | WRITES |
|---|---|---|---|---|---|---|
| `lib/parquet_store.py` | "Parquet storage for raw OI rows." Layout `{OI_RAW_DIR}/{ticker}/{year}.parquet` | lib | `fetch_oi`, `fetch_oi_snapshot`, `run_pipeline*`, `backtest_spreads`, `fetch_equity_1min`, `fetch_chain_*`, audits, `migrations/export_raw_to_parquet` | 2026-08-26 | `OI_RAW_DIR` | `OI_RAW_DIR` |
| `lib/chain_store.py` | "Parquet storage for raw EOD greeks chain rows." `{CHAIN_EOD_DIR}/{ticker}/{year}.parquet` | lib | `fetch_chain_eod`, `iv25d_quality_report`, `phase1_iv25d_sample`, `audit_symbol_gaps` | 2026-08-26 | `CHAIN_EOD_DIR` | `CHAIN_EOD_DIR` |
| `lib/chain_snapshot_store.py` | "Parquet storage for twice-daily intraday option-chain snapshots." `{CHAIN_SNAPSHOTS_DIR}/{ticker}/{YYYYMM}.parquet` | lib | `fetch_chain_snapshots`, `build_equity_surface`, `audit_chain_snapshots`, `migrate_chain_snapshots_to_monthly`, `fetch_earnings_calendar` | 2026-08-26 | `CHAIN_SNAPSHOTS_DIR` | `CHAIN_SNAPSHOTS_DIR` |
| `lib/chain_intraday_store.py` | "Parquet storage for full-day 5-minute intraday option-chain bars." `{CHAIN_INTRADAY_DIR}/{ticker}/{YYYYMMDD}.parquet` + `.manifest.json` | lib | `fetch_chain_intraday`, `diagnose_transform`, `verify_csv_path`, `verify_surface_fixes`, `lib/chain_live_store` | 2026-08-26 | `CHAIN_INTRADAY_DIR` | `CHAIN_INTRADAY_DIR` |
| `lib/chain_live_store.py` | "Parquet storage for live 5-minute chain captures." `{CHAIN_LIVE_DIR}/{TICKER}/{YYYYMMDD}/{HHMM}.parquet` | lib | `fetch_live_surface` → **cron**; `verify_surface_fixes` | 2026-08-26 | `CHAIN_LIVE_DIR` | `CHAIN_LIVE_DIR` |
| `lib/equity_1min_store.py` | "Parquet storage for 1-minute equity bars (Polygon / Massive)." | lib | `fetch_equity_1min`, `build_trade_paths`, `audit_equity_1min` | 2026-08-26 | `EQUITY_1MIN_DIR` | `EQUITY_1MIN_DIR` |
| `lib/parquet_schema.py` | "Canonical-type enforcement for every parquet store in this project." | lib | the store modules | 2026-08-26 | — | — |
| `lib/surface_store.py` | "Equity option surface: stage 5 (store)." | lib | `fetch_live_surface` → **cron**; `build_equity_surface`; `lib/metrics_store` | 2026-08-24 | `open_interest.equity_surface_diagnostics` (:122, :141) | `open_interest.equity_surface` (:110), `open_interest.equity_atm` (:112), `open_interest.equity_surface_diagnostics` (:114); calls `ensure_equity_surface_partition` / `ensure_equity_atm_partition` (:97-98) |
| `lib/surface_fit.py` | "Equity option surface: stages 1-4 (clean, fit, sample, greeks)." | lib | `fetch_live_surface`, `build_equity_surface` | 2026-08-24 | — (pure compute) | — |
| `lib/surface_config.py` | "Configuration for the equity option surface interpolation stage." | lib | surface fit/store, `fetch_live_surface` | 2026-08-24 | — | — |
| `lib/clean_chain.py` | "computed fields and data-quality flags for equity option chains." | lib | `fetch_live_surface`, `build_equity_surface` | 2026-08-22 | — (pure compute) | — |
| `lib/metrics_compute.py` | "Equity surface metrics: compute (pipeline stage 4)." | lib | `build_equity_metrics` → **cron** | 2026-08-25 | `open_interest.underlying_ohlc` (:230), `equity_atm` (:271, :314), `equity_surface` (:302), `equity_surface_diagnostics` (:326); `earnings_calendar` via `earnings_store` | — |
| `lib/metrics_config.py` | "Equity surface metrics: the column registry (pipeline stage 4)." | lib | `build_equity_metrics`, `lib/metrics_store` | 2026-08-25 | — | — |
| `lib/metrics_store.py` | "Equity surface metrics: schema, catalog, z-scores and writes (stage 4)." | lib | `build_equity_metrics` → **cron**; `zscore_migration_check`; `init_db` | 2026-08-25 | `open_interest.equity_surface_diagnostics` + `equity_metrics` (:406-407) | `open_interest.equity_metrics` (:37), `equity_metrics_z` (:38), `equity_metrics_catalog` (:39); applies `sql/09_equity_metrics.sql`, `11/12/13/14_*.sql` |
| `lib/earnings_store.py` | "Earnings calendar store: fetch from yfinance, upsert, and look up." | lib | `fetch_earnings_calendar` → **cron**; `backfill_days_to_earnings`; `lib/metrics_compute` | 2026-08-25 | yfinance; `open_interest.earnings_calendar` | `open_interest.earnings_calendar` (:20), `open_interest.earnings_coverage` (:21) |
| `lib/bin_compute.py` | "Pure compute helpers for wf_bins, is_bins, and tt_thresholds." | lib | `build_bin_tables` → **cron**; `validate_bins` | 2026-06-05 | — | — |
| `lib/bin_schema.py` | "Dynamic schema sync for wf_bins and is_bins." | lib | `build_bin_tables` → **cron**; `init_db`; `validate_bins` | 2026-06-05 | `open_interest.metric_classification` (:51) | DDL on `open_interest.wf_bins` (:145,:150), `is_bins` (:194,:199), `tt_bins` (:249,:254) |
| `lib/trade_path_rules.py` | "Exit-rule registry and vectorised evaluator for build_trade_paths.py." | lib | `build_trade_paths` | 2026-09-05 | — | — |
| `lib/trade_path_schema.py` | "Dynamic per-rule column sync for trade_paths." | lib | `build_trade_paths` | 2026-09-05 | — | DDL on `open_interest.trade_paths` (:90,:93); `INSERT INTO open_interest.trade_path_rules` (:114) |
| `lib/thetadata.py` | "ThetaData v3 client." | lib | every ThetaData fetcher; `run_pipeline*` reachability check | 2026-08-25 | ThetaData REST | — |
| `lib/polygon.py` | "Polygon.io / Massive REST client — 1-minute equity aggregates." | lib | `fetch_equity_1min` | 2026-08-15 | Polygon/Massive REST | — |
| `lib/polygon_symbols.py` | "Symbol-history resolution for tickers that were renamed." | lib | `fetch_equity_1min`, `audit_symbol_gaps` | 2026-08-15 | Polygon/Massive REST | — |
| `lib/chain_fetch_common.py` | "Shared infrastructure for the chain fetchers." (`ParquetWriterThread`, timing, file logging, `preflight_store`) | lib | all chain fetchers; `run_live_pipeline` | 2026-08-26 | — | `logs/*.log` |
| `lib/split_factors.py` | "Split-adjustment factors for converting raw … option strikes into split-adjusted units" | lib | surface/metrics path | 2026-08-15 | `open_interest.underlying_ohlc` (:32) | — |
| `lib/market_hours.py` | "NYSE trading-day helpers (thin wrapper over pandas_market_calendars)." | lib | `run_pipeline*`, `fetch_live_surface`, fetchers | 2026-08-15 | — | — |
| `lib/expirations.py` | "Standard-monthly expiration helper." | lib | **[rev3]** `build_features.py:30` — `from lib.expirations import build_next_monthly_lookup` | 2026-04-28 | — | — |

### `analysis/` — scalp episode study, four entry points

These read the **`equities_scalp`** database and the scalp parquet store, not the
Open_Interest pipeline. They are in this repo but belong to the scalp system.

| Path | Purpose (from docstring) | Kind | CLI flags | Invoked by | Git | READS | WRITES |
|---|---|---|---|---|---|---|---|
| `analysis/dump_fills.sql` | "Round-trip fills, one row per trip, for the episode analysis. READ-ONLY." | sql (psql `\copy`) | — | manual | 2026-09-08 | `equities_scalp.fills` — the file states this table "is owned by the dashboard (app/routers/equities_scalp.py)" | `fills_dump.csv` (local file) |
| `analysis/episode_shape.py` | "Step 1b: what the fills actually look like, and how much the episode boundary N matters. Reads the CSV from dump_fills.sql. Touches nothing else." | entry | positional `csv` (default `fills_dump.csv`) | manual | 2026-09-08 | `fills_dump.csv` | stdout only |
| `analysis/parquet_coverage.py` | "Step 1c: is the tape actually on disk for every ticker-day I traded… Read-only; opens no parquet, only stats the files." | entry | positional `csv` | manual | 2026-09-08 | `fills_dump.csv`; `scalp.store` paths (stat only) | stdout only |
| `analysis/episode_metrics.py` | "Step 2: one row per trading episode, with the tape measured over exactly the window I was exposed to" | entry | `--gap-minutes --lead-minutes --out --out-full --time-tol --price-tol` | manual | 2026-09-08 | `fills_dump.csv`; scalp parquet store via `scalp.{compute,config,metrics,quiet,store}` | two CSVs at `--out` / `--out-full` |
| `analysis/episode_correlate.py` | "Step 2b: the same correlations, at episode grain, with a chance table…" | entry | positional `csv`, `--permutations --seed --min-coverage` | manual | 2026-09-08 | episode CSV from `episode_metrics.py` | stdout only |

---

## Answers to your four questions

### Q1 — `CHAIN_INTRADAY_DIR` is set but the directory does not exist. What touches it, and is the path live?

`.env` sets `CHAIN_INTRADAY_DIR=/mnt/trading_volume_3/chain_intraday`
(`env_paths.txt:18`). `storage_layout.txt` lists `backup`, `chain_live`,
`chain_snapshots`, `equities_scalp`, `ext_trades`, `lost+found`, `quote_ticks`
on that volume — **no `chain_intraday`**. Its two siblings are there, so the
volume is mounted and writable; the directory was never created or was removed.

| Role | Code | Live? |
|---|---|---|
| Writer | `fetch_chain_intraday.py` → `lib/chain_intraday_store.py` (`SessionWriter`) | **Not scheduled.** No `crontab.txt` entry names it; no in-project script imports it |
| Writer (incidental) | `diagnose_transform.py:263-267` writes probe files to `CHAIN_INTRADAY_DIR/_diag/` | Manual diagnostic only |
| Reader | `build_equity_surface.py:61-63` when `--source intraday` | **Not scheduled**, and not the default — `--source` defaults to snapshots (`build_equity_surface.py:18`) |
| Reader | `verify_surface_fixes.py:62` when `--source intraday` | Manual |
| Reader | `lib/chain_live_store.py:75` imports `COLUMNS, SCHEMA, coerce` | Schema constants only — **no I/O against the directory** |
| Test | `test_equity_surface_parallel.py:86` sets `CHAIN_INTRADAY_DIR` to a temp dir | Test isolation, not the real store |

**Verdict: the code path is intact but dormant.** Every reader and writer is
manual-only; nothing on a schedule touches it. The live 5-minute intraday capture
goes to `CHAIN_LIVE_DIR` instead, via `fetch_live_surface.py` under
`run_live_pipeline.py`. So the missing directory breaks nothing today — the first
`fetch_chain_intraday.py` run would recreate it (`preflight_store` at
`fetch_chain_intraday.py:664` resolves and reports the store before fetching).

`UNCERTAIN` — whether the directory once held data that was deleted. Nothing in
the supplied state records that, and I cannot see the volume.

### Q2 — Nothing in cron calls `build_bin_tables.py`, yet `wf_bins` and `is_bins` updated today while `tt_bins` stopped. What builds each?

The premise is half right: cron never names `build_bin_tables.py`, but it reaches
it as a **subprocess two levels down**.

- `run_pipeline.py:235` → `build_bin_tables.py --tier {MORNING|EVENING}`, plus
  `--build-tt` on EVENING only (`:236-237`)
- `run_pipeline_early.py:112` → `build_bin_tables.py --tier MORNING`

So the chain is `crontab.txt` → `run_pipeline*.py` → subprocess.

**What each flag actually writes** (`build_bin_tables.py:575-591`):

| Flag | Writes | Reached by cron? | Consistent with `table_stats.txt`? |
|---|---|---|---|
| `--tier X` | `open_interest.wf_bins` (:153) **and** `open_interest.is_bins` (:266) — one flag, two tables | **Yes, 3×/weekday** (07:00, 09:35, 18:00) | Yes — both show 2026-09-14 13:47 / 13:52, minutes apart, exactly the sequential order of `main()` |
| `--build-tt` | `open_interest.tt_thresholds` (:520) | **Yes, EVENING only** | Consistent — 2026-09-11 |
| `--build-tt-bins` | `open_interest.tt_bins` (:377) | **No — nothing passes this flag** | Yes — frozen |

**`open_interest.tt_bins` is meant to be built by `python build_bin_tables.py
--build-tt-bins`, and nothing does it.** The flag exists, the builder is complete
and chunked for the VPS memory limit, but no cron entry, no `run_pipeline*`
branch, and no other script passes it. It is a manual command that stopped being
run.

This matters more than a stale table: `build_bin_tables.py:571-572` says the
dashboard reads `tt_bins`, and `--build-tt` is "kept for the underlying threshold
artifact if needed" — i.e. `tt_thresholds` is the *legacy* table and it is the one
cron refreshes nightly, while the table the dashboard actually reads is the one
nothing refreshes. `build_trade_paths.py` also reads `tt_bins` (:473-497), so
trade-path builds are consuming frozen bins.

**[rev1] The date is now settled.** `bin_freshness.txt` gives `max(trade_date)`
per table, which is real data recency:

| Table | Earliest | Latest | Tickers |
|---|---|---|---|
| `open_interest.tt_bins` | 2018-12-31 | **2026-08-14** | 121 |
| `open_interest.wf_bins` | 2018-12-31 | 2026-09-14 | 121 |
| `open_interest.is_bins` | 2018-12-31 | 2026-09-14 | 121 |
| `open_interest.daily_features` | 2018-12-31 | 2026-09-14 | 121 |

Your 2026-08-14 was right and the `table_stats.txt` figure I could not use
(2026-09-01) was indeed the ANALYZE sweep, not a write. The gap is **one month
of dates, not of tickers** — all four tables carry the same 121 tickers and the
same 2018-12-31 start, so nothing about coverage or backfill is wrong. Only the
tail is missing, exactly as an un-run `--build-tt-bins` would produce.

Consequence worth stating plainly: `build_trade_paths.py` reads
`open_interest.tt_bins` (:473-497), so any trade-path build since 2026-08-14 has
been joining current `daily_features` rows against bins that stop a month short.

### Q3 — The five `research_*` tables in `open_interest` are empty. Does anything in THIS project touch them?

**No. Zero references.** I grepped `research_runs|research_results|research_series|research_charts|research_followups|research_knowledge`
across every file in the repo — including `sql/`, `migrations/`, `scalp/`, and
all documentation — and got no matches at all.

So they are not created by this project's DDL, not written by it, and not read by
it. In `open_interest` they are `0 rows, 0 inserts ever` (`table_stats.txt:11-14,16`),
while the same five names in `spx_interpolated` sit alongside
`research_knowledge`, `research_backtest_staging`, `research_backtest_uploads`,
and `research_pnl_uploads` — a fuller set that `open_interest` does not have.

Reading that: something applied a subset of the research schema to
`open_interest` once and never wrote to it. The `n_tup_ins = 0` is strong — not
"emptied later", but "never received a row".

`UNCERTAIN — which codebase created them.` The dashboard is the obvious
candidate since it is the other writer in this cluster, but I cannot see that
codebase, and `docs/vps_state/README.md` (which would have told me how the four
systems relate) is missing. Section 2 will list them as "exists in DB, no
in-project writer" without naming an owner.

### Q4 — `/mnt/trading_volume_3` has `quote_ticks/`, `ext_trades/`, `backup/` and they are nowhere in `config.py`. Does any script here write them?

**No script in this project writes any of the three.** Grepping the whole repo
for `quote_ticks`, `ext_trades`, `/mnt/`, and `backup` returns only four hits,
all in `scalp/`, and all about a different directory:

- `scalp/README.md:242` — `export SCALP_DATA_DIR=/mnt/trading_volume_3/equities_scalp`
- `scalp/config.py:479`, `scalp/README.md:263` — the "an unmounted `/mnt/block3`
  is just an ordinary empty directory" warning
- `scalp/config.py:533` — a capacity comment: "block 3 (/mnt/trading_volume_3) 100 GB, 78% used, ~22 GB available"

So the only thing this repo writes on that volume is `equities_scalp/`, plus
`chain_live/` and `chain_snapshots/` from `.env`. `quote_ticks/`, `ext_trades/`
and `backup/` have no writer here.

**[rev1] Ownership inference withdrawn.** My first pass named
`Thetadata_Raw_SPX` as the "likely owner" of `quote_ticks/` and `ext_trades/`,
reasoning from the truncated `crontab.txt:1` fragment. The complete crontab does
confirm that project runs a 5-minute pipeline (`crontab.txt:19`), and
`vps_state/README.md` confirms the directories have no writer here — but the
README also says explicitly: *"Other databases, directories, and cron jobs
visible in these captures belong to other projects and are out of scope. Don't
infer owners for them,"* and names these three directories under known issues as
*"Out of scope; don't guess."*

So the corrected position is narrower than my first pass, not broader:

| Directory | Written by this project? | Owner |
|---|---|---|
| `/mnt/trading_volume_3/quote_ticks/` | **No** — verified by repo-wide grep | Out of scope; not determined |
| `/mnt/trading_volume_3/ext_trades/` | **No** — verified | Out of scope; not determined |
| `/mnt/trading_volume_3/backup/` | **No** — verified | Out of scope; not determined |

The only fact I will assert is the negative one, which is verified: no file in
this repo names any of the three. Their mtimes (`ext_trades/` 2026-09-10 20:24,
`quote_ticks/` 2026-09-10 13:57) show they are live rather than archival, which
matters only as a caution against treating them as reclaimable space.

One note on your framing of `spx_options`: `/mnt/trading_volume_2/spx_options`
(1005 entries) and `/mnt/volume1/spx_options` (669 entries) are the two halves of
the one logical store you described. `/data/spx_options` (395 MB,
`storage_layout.txt:31`) is a third location and **no script in this project
references it** — same grep result as above. It will appear in Section 2 as an
unexplained store with no in-project reader or writer.

---

## Section 2 — Data store inventory

Partition children (`*_YYYYMM`, `*_YYYYMMDD`) are excluded throughout;
`equity_surface`, `equity_atm`, `equity_metrics`, `equity_metrics_z` and
`equities_scalp.intraday_metrics` are each treated as one logical table.

**Ownership legend.** `in-repo` = a script in this repository writes it.
`external` = no writer in this repository; `vps_state/README.md` attributes it to
a separate codebase. `external consumer likely` marks in-repo writes with no
in-repo reader — per README these are read by the dashboard, **not orphans**.

### 2a. Ownership re-derived from source — **[rev2]**

**Precedence, per your ruling:** machine-verified `vps_state` captures
(`crontab.txt`, `oi_schema_trimmed.sql`, `table_stats.txt`, `env_paths.txt`,
`storage_layout.txt`, `bin_freshness.txt`) are truth. **Source code beats
`vps_state/README.md`**, whose prose sections are working notes. Where code
contradicts the README, the code wins and the README needs correcting.

Both conflicts I raised are confirmed as README errors, now applied:

| Table | README error | Corrected |
|---|---|---|
| `open_interest.trade_paths`, `.trade_paths_manifest`, `.trade_path_rules` | listed at README:19 as externally written | **Written by this repo.** `build_trade_paths.py:433,:444`, `lib/trade_path_schema.py:114`, DDL in `sql/07_trade_paths.sql`. Removed from the external list |
| `open_interest.underlying_ohlc` | README:22 gives it as the example of "read here but produced elsewhere" | **Produced here, read by the dashboard** — the direction was reversed. `fetch_ohlc.py:29` + `fetch_ohlc_premarket.py:48`, three cron runs a weekday |

#### The re-derived external list

Method: grep every one of the 40 logical `open_interest` table names across all
`.py` and `.sql` in the repo — root, `lib/`, `sql/`, `migrations/`, `analysis/`,
`scalp/`, `Cowork/` — excluding only `.venv/` and my own `docs/` output. A table
counts as having an in-repo writer if any source file contains an `INSERT`,
`UPDATE`, `COPY`, `TRUNCATE`, `CREATE TABLE`, or a store-module constant naming
it.

**19 tables have no writer in this repo** (README:19 named 16 of them):

| database.table | In README's list? |
|---|---|
| `open_interest.analyze_cache_outcome` | yes |
| `open_interest.analyze_cache_slim` | yes |
| `open_interest.analyze_cache_trade_meta` | yes |
| `open_interest.analyze_primary_cache` | yes |
| `open_interest.corner_scan_1f` | yes |
| `open_interest.corner_scan_2f` | yes |
| `open_interest.corner_scan_notes` | yes |
| `open_interest.ic_batch_cache` | yes |
| `open_interest.sec_scan_cache` | yes |
| `open_interest.ticker_analysis_chain_cache` | yes |
| `open_interest.ticker_analysis_layouts` | yes |
| `open_interest.research_charts` | yes |
| `open_interest.research_followups` | yes |
| `open_interest.research_results` | yes |
| `open_interest.research_runs` | yes |
| `open_interest.research_series` | yes |
| **`open_interest.global_bins_cache`** | **no — README missed it** |
| **`open_interest.equity_structure_presets`** | **no — README missed it** |
| **`open_interest.signals`** | **no — README missed it** |

So the README list was wrong in both directions: it **added** three tables this
repo demonstrably writes (`trade_paths*`) and **missed** three it does not
(`global_bins_cache`, `equity_structure_presets`, `signals`).

On `signals` specifically — the only source occurrences of that string anywhere
in the repo are the English word in comments (`sql/01_schema.sql:116` "OI
build-up signals", `iv25d_quality_report.py:468`, `scalp/metrics.py:443`). Never
an identifier. The table is external.

#### One table that is neither

`open_interest.metric_classification` does not fit either list and is worth
calling out:

- **No `CREATE TABLE` anywhere in this repo.** `sql/` creates 20 tables; this is
  not one of them. Its DDL comes from outside.
- **But this repo writes rows to it** — `sql/06_25d_skew_metrics.sql:47`
  `INSERT INTO metric_classification`, applied by `init_db.py`.
- **And reads it as a control table** — `lib/bin_schema.py:51` selects
  `metric, tier WHERE eligible_as_metric = TRUE`, which decides which columns
  every bin table gets.

So an externally-owned table governs what `build_bin_tables.py` produces, while
this repo also mutates it. That is a shared-write table with a control
relationship, and it belongs in Section 3.

#### DDL ownership, for reference

`sql/` creates exactly 20 tables: `underlying_ohlc`, `daily_features`,
`option_volume_daily`, `option_iv_daily`, `backtest_call_spread`, `wf_bins`,
`is_bins`, `tt_bins`, `tt_thresholds`, `trade_paths`, `trade_path_rules`,
`trade_paths_manifest`, `equity_surface`, `equity_atm`,
`equity_surface_diagnostics`, `equity_metrics`, `equity_metrics_z`,
`equity_metrics_catalog`, `earnings_calendar`, `earnings_coverage` — plus the
`ensure_*_partition` functions that create partition children on demand.

### 2b. `open_interest` — 40 logical tables

Partition children bring the raw count to 124; README:35 says "~40 real tables",
and the exact figure from `all_databases_tables.txt` minus children is **40**.

#### Written by this repo — the daily pipeline

| database.table | Written by (script + flag) | Read by (in repo) | Grain / PK | Rows | Notes |
|---|---|---|---|---|---|
| `open_interest.daily_features` | `build_features.py` via `run_pipeline.py --tier EVENING` (EVENING cols) and `--tier MORNING` + `run_pipeline_early.py` (MORNING cols) | `build_bin_tables.py`, `build_trade_paths.py`, `audit_symbol_gaps.py`, `audit_trade_paths.py`, `validate_bins.py`, `iv25d_quality_report.py` | PK `(ticker, trade_date)` | 200,677 | The composite row. 1,204,779 inserts vs 200,677 live rows = heavy upsert churn, as the two-tier design implies. Through 2026-09-14, 121 tickers |
| `open_interest.underlying_ohlc` | `fetch_ohlc.py` (cron ×2) and `fetch_ohlc_premarket.py` (`open` only, cron PREMARKET) | `build_features.py`, `run_pipeline*.py`, `backtest_spreads.py`, `fetch_iv_chain.py`, `fetch_volume_eod.py`, `audit_*`, `lib/metrics_compute.py`, `lib/split_factors.py`, `phase1_iv25d_sample.py` | PK `(ticker, trade_date)` | 227,434 | See conflict 2a |
| `open_interest.wf_bins` | `build_bin_tables.py --tier {MORNING,EVENING}` | `validate_bins.py` | PK `(ticker, trade_date)` | 262,440 | Walk-forward bins. Current to 2026-09-14 |
| `open_interest.is_bins` | `build_bin_tables.py --tier {MORNING,EVENING}` — **same flag as `wf_bins`** | nothing in repo | PK `(ticker, trade_date)` | 229,917 | **External consumer likely.** Current to 2026-09-14. `validate_bins.py` does not cover it |
| `open_interest.tt_bins` | `build_bin_tables.py --build-tt-bins` — **flag is never passed by anything** | `build_trade_paths.py:473-497` | PK `(ticker, trade_date)` | 225,204 | **STALE: data ends 2026-08-14** vs 2026-09-14 elsewhere. Dashboard reads this (per `build_bin_tables.py:571`) |
| `open_interest.tt_thresholds` | `build_bin_tables.py --build-tt` via `run_pipeline.py --tier EVENING` | `validate_bins.py` | PK `(metric, ticker, cutoff_date)` | 19,118 | Described in its own builder as the **legacy** artifact, yet it is the one cron refreshes |
| `open_interest.metric_classification` | `sql/06_25d_skew_metrics.sql:47` (`INSERT INTO`), applied by `init_db.py` | `lib/bin_schema.py:51` → drives which metrics `build_bin_tables.py` bins | PK `(metric)` | 172 | Control table: edits here change bin table columns |

#### Written by this repo — surface and metrics (intraday, 5-minute)

| database.table | Written by (script + flag) | Read by (in repo) | Grain / PK | Rows | Notes |
|---|---|---|---|---|---|
| `open_interest.equity_surface` | `fetch_live_surface.py` via `run_live_pipeline.py` (cron, every 5 min RTH); `build_equity_surface.py` (manual); `backfill_call_price.py` (UPDATE `call_price` only) | `lib/metrics_compute.py:302`, `backfill_call_price.py` | UNIQUE `(ticker, trade_date, snapshot, dte, put_delta)`; `PARTITION BY RANGE (trade_date)`, monthly | 14.4M in `_202608`, 19.1M in `_202609` | Largest store in the DB. Parent is empty by design (partitioned) |
| `open_interest.equity_atm` | same two writers as `equity_surface` | `lib/metrics_compute.py:271,314` | UNIQUE `(ticker, trade_date, snapshot, dte)`; `PARTITION BY RANGE (trade_date)`, monthly | 1.0M in `_202609` | |
| `open_interest.equity_surface_diagnostics` | same two writers | `build_equity_metrics.py:155`, `lib/metrics_store.py:406`, `lib/surface_store.py:122,141`, `lib/metrics_compute.py:326` | PK `(ticker, trade_date, snapshot, expiry)` | 3,979,247 | **Not partitioned** — 937 MB in one table, unlike its three siblings |
| `open_interest.equity_metrics` | `build_equity_metrics.py` via `run_live_pipeline.py` (cron); `backfill_days_to_earnings.py` (UPDATE `days_to_earnings` only) | `snapshot_null_audit.py`, `zscore_migration_check.py`, `rv_vrp_noise_report.py`, `lib/metrics_store.py:407`, `backfill_days_to_earnings.py` | PK `(ticker, trade_date, snapshot)`; `PARTITION BY RANGE (trade_date)`, monthly | 75,337 in `_202609` | |
| `open_interest.equity_metrics_z` | `build_equity_metrics.py` (suppressed by `--no-z` / `run_live_pipeline.py --no-z`) | `snapshot_null_audit.py`, `zscore_migration_check.py` | PK `(ticker, trade_date, snapshot)`; `PARTITION BY RANGE (trade_date)`, monthly | 75,360 in `_202609` | |
| `open_interest.equity_metrics_catalog` | `lib/metrics_store.py:146` regenerates from the registry, via `build_equity_metrics.py init-db` | nothing in repo | PK `(column_name)` | 767 | **External consumer likely** — `lib/metrics_config.py:9` says it is "the dashboard's metric picker" |

#### Written by this repo — earnings, trade paths, legacy

| database.table | Written by (script + flag) | Read by (in repo) | Grain / PK | Rows | Notes |
|---|---|---|---|---|---|
| `open_interest.earnings_calendar` | `fetch_earnings_calendar.py` (cron 20:30) via `lib/earnings_store.py:20` | `backfill_days_to_earnings.py`, `lib/metrics_compute.py` | PK `(ticker, earnings_date)` | 2,280 | |
| `open_interest.earnings_coverage` | same | `backfill_days_to_earnings.py:98` | PK `(ticker)` | 120 | Distinguishes "no earnings" from "not fetched" |
| `open_interest.trade_paths` | `build_trade_paths.py:433` (manual), staged via `COPY _tp_stage` | `audit_trade_paths.py` | PK `(ticker, trade_date, entry_anchor)` | 441,029 | See conflict 2a. Columns synced per-rule by `lib/trade_path_schema.py` |
| `open_interest.trade_paths_manifest` | `build_trade_paths.py:444` | `build_trade_paths.py:458` (own resume logic) | PK `(ticker, entry_anchor)` | 242 | |
| `open_interest.trade_path_rules` | `lib/trade_path_schema.py:114` | `lib/trade_path_rules.py` | PK `(rule_key)` | 143 | |
| `open_interest.option_iv_daily` | `fetch_iv_chain.py:68` — **no caller; superseded by `fetch_chain_eod.py`** | nothing in repo | PK `(ticker, trade_date)` | 215,060 | Legacy. Last touched by `backfill_source_session.py` (one-time, 2026-05-27) |
| `open_interest.option_volume_daily` | `fetch_volume_eod.py:49` — **no caller; superseded** | nothing in repo | PK `(ticker, trade_date)` | 215,582 | Legacy, same story |
| `open_interest.backtest_call_spread` | `backtest_spreads.py:68` (manual) | nothing in repo | PK `(id)`, UNIQUE `(ticker, trade_date, exit_date)` | 1,971 | |

#### Exists in `open_interest`, no writer in this repo — external

**[rev2]** Re-derived by grep, not taken from README:19 — see §2a for the method
and for the three tables the README missed. Listed so the map is complete;
**not candidates for removal.**

| database.table | Grain / PK | Rows | Attribution |
|---|---|---|---|
| `open_interest.analyze_cache_outcome` | PK `(cache_key, outcome)` | 104 (171 MB) | external cache |
| `open_interest.analyze_cache_slim` | PK `(cache_key)` | 8 | external cache |
| `open_interest.analyze_cache_trade_meta` | PK `(cache_key)` | 8 (32 MB) | external cache |
| `open_interest.analyze_primary_cache` | PK `(cache_key)` | 17 (37 MB) | external cache |
| `open_interest.corner_scan_1f` | PK `(metric, extreme, outcome, mode)` | 5,512 | external |
| `open_interest.corner_scan_2f` | PK `(primary_metric, secondary_metric, corner_direction, outcome, mode)` | 1,130,468 (401 MB) | external |
| `open_interest.corner_scan_notes` | PK `(primary_metric, secondary_metric, corner_direction, outcome)` | 35 | external annotations |
| `open_interest.global_bins_cache` | PK `(cache_key)` | 8 | external cache |
| `open_interest.ic_batch_cache` | PK `(cache_key)` | 3 | external cache |
| `open_interest.sec_scan_cache` | PK `(structural_key)` | 6 | external cache |
| `open_interest.ticker_analysis_chain_cache` | PK `(cache_key)` | 603 (11 MB) | external cache |
| `open_interest.ticker_analysis_layouts` | PK `(id)`, UNIQUE `(name)` | 0 | external UI state |
| `open_interest.equity_structure_presets` | PK `(id)`, UNIQUE `(name)` | 1 | external UI state |
| `open_interest.signals` | PK `(id)` | 22 | external |
| `open_interest.research_charts` | PK `(id)` | **0, never inserted** | external; see Q3 |
| `open_interest.research_followups` | PK `(id)` | **0, never inserted** | external; see Q3 |
| `open_interest.research_results` | PK `(id)` | **0, never inserted** | external; see Q3 |
| `open_interest.research_runs` | PK `(id)` | **0, never inserted** | external; see Q3 |
| `open_interest.research_series` | PK `(id)` | **0, never inserted** | external; see Q3 |

Dropped and confirmed absent: `open_interest.option_oi_raw` (migrated to
`OI_RAW_DIR` 2026-09-01), `open_interest.option_oi_surface` (dropped by
`migrations/drop_option_oi_surface.sql`).

### 2c. `equities_scalp` — 9 logical tables (7 written here, 2 external)

25 relations in `all_databases_tables.txt`; the other 16 are daily partition
children of `intraday_metrics`.

Written by `scalp/`, whose script-level inventory is Section 1b. Writer
attribution below is from `scalp/db.py` and its callers; PKs are from the
`CREATE TABLE` statements in `scalp/db.py`, not from a VPS schema dump —
**`UNCERTAIN` in that no `equities_scalp` schema capture was supplied**, so these
are what the code creates, which may differ from what is deployed. No row counts
either: `table_stats.txt` covers `open_interest` only.

| database.table | Written by (script → helper) | Read by (in repo) | Grain / PK | Notes |
|---|---|---|---|---|
| `equities_scalp.universe` | `scalp/update_universe.py:473` → `db.write_universe` (`db.py:763`) | `scalp/` internals | PK `(trade_date, symbol)` | |
| `equities_scalp.daily_metrics` | `scalp/compute.py:406` → `db.write_daily_metrics` | `scalp/` internals | PK `(trade_date, symbol, metric)` | Long/EAV shape via `_upsert_long` |
| `equities_scalp.provenance` | `scalp/compute.py:407` → `db.write_provenance` | `scalp/db.py:456` `provenance_wide` | PK `(trade_date, symbol, item)` | |
| `equities_scalp.intraday_metrics` | `scalp/compute.py:409` → `db.write_intraday_metrics` (`db.py:491`); partitions created at `db.py:321` | `scalp/` internals | PK `(trade_date, symbol, bucket_start)`; `PARTITION BY RANGE (trade_date)`, **daily** (`_YYYYMMDD`) | Also `TRUNCATE`d by `migrations/rebuild_intraday_metrics.sql:46,68`. 16 daily partitions present, 2026-08-17 → 2026-09-08 |
| `equities_scalp.intraday_monthly` | `scalp/compute.py:516` → `db.upsert_intraday_monthly` (`db.py:545`) | `scalp/` internals | PK `(month, symbol, bucket_time)` | Rollup of the above |
| `equities_scalp.fetch_runs` | `scalp/fetch.py:256` → `db.write_fetch_run` (`db.py:578`) | `scalp/` internals | PK `(run_ts, trade_date)` | Run ledger |
| `equities_scalp.rankings` | `db.write_ranking` (`db.py:787`) — **no caller anywhere in the repo** | nothing in repo | PK `(run_ts, trade_date, symbol, variant)` | Writer function exists and is never called. Section 3 candidate |
| `equities_scalp.fills` | **Not written here.** `analysis/dump_fills.sql:6` states the table "is owned by the dashboard (`app/routers/equities_scalp.py`)" | `analysis/dump_fills.sql` → `fills_dump.csv` → `analysis/episode_*.py` | UNCERTAIN — not in any supplied schema | Read-only dependency on an external producer |
| `equities_scalp.fills_daily` | **Not written here**; no reader here either | nothing in repo | UNCERTAIN | Present in `all_databases_tables.txt`; entirely external |

### 2d. Parquet stores

Resolved paths are from `env_paths.txt` (the VPS `.env`). Do not trust the
`PROJECT_ROOT/data/...` fallbacks in `config.py:29-78` — that file's own comments
(`:37-49`) say production always overrides them.

| Logical store | Env var | Resolved path | Volume | Partitioning | Written by | Read by | Status |
|---|---|---|---|---|---|---|---|
| Raw OI | `OI_RAW_DIR` | `/data/oi_raw` | `/data` (577 MB) | `{TICKER}/{year}.parquet`; dedupe key `(trade_date, expiration, strike, option_type)` | `fetch_oi.py`, `fetch_oi_snapshot.py` (both cron MORNING/PREMARKET) | `build_features.py` (DuckDB), `backtest_spreads.py`, and as the **ticker-universe source** for `run_pipeline*.py`, `fetch_chain_*`, `fetch_equity_1min.py`, `fetch_earnings_calendar.py`, the audits | **Active** |
| EOD greeks chain | `CHAIN_EOD_DIR` | `/data/chain_eod` | `/data` (2.0 GB) | `{TICKER}/{year}.parquet` | `fetch_chain_eod.py` (cron EVENING) | `build_features.py` (DuckDB), `iv25d_quality_report.py`, `phase1_iv25d_sample.py`, `audit_symbol_gaps.py` | **Active** |
| 1-minute equity bars | `EQUITY_1MIN_DIR` | `/data/equity_1min` | `/data` (5.0 GB) | `{TICKER}/{year}.parquet` + `_manifest/{TICKER}.parquet` | `fetch_equity_1min.py` (manual) | `build_trade_paths.py`, `audit_equity_1min.py` | **Active, manually fed** — largest `/data` store |
| Twice-daily chain snapshots | `CHAIN_SNAPSHOTS_DIR` | `/mnt/trading_volume_3/chain_snapshots` | block 3 (123 entries, mtime 2026-09-07) | `{TICKER}/{YYYYMM}.parquet` — monthly since `migrate_chain_snapshots_to_monthly.py`; legacy `{YYYY}.parquet` still detected by `build_equity_surface.py:381` | `fetch_chain_snapshots.py` (manual) | `build_equity_surface.py` (**default source**), `audit_chain_snapshots.py`, `probe_session_gap.py`, `verify_surface_fixes.py`, `fetch_earnings_calendar.py` (ticker list) | **Active, manually fed** |
| Live 5-min chain captures | `CHAIN_LIVE_DIR` | `/mnt/trading_volume_3/chain_live` | block 3 (123 entries, mtime 2026-08-24) | `{TICKER}/{YYYYMMDD}/{HHMM}.parquet` — one file per capture cycle | `fetch_live_surface.py` via `run_live_pipeline.py` (**cron, every 5 min RTH**) | `verify_surface_fixes.py` | **Active** — the only parquet store on a schedule |
| Full-day 5-min intraday bars | `CHAIN_INTRADAY_DIR` | `/mnt/trading_volume_3/chain_intraday` | block 3 — **directory does not exist** | `{TICKER}/{YYYYMMDD}.parquet` + `{YYYYMMDD}.manifest.json` | `fetch_chain_intraday.py` (manual); `diagnose_transform.py` writes probes to `_diag/` | `build_equity_surface.py --source intraday`, `verify_surface_fixes.py --source intraday` | **SUPERSEDED — PENDING DECISION.** Not active. Configured in `.env`, no data on disk, no scheduled reader or writer. `vps_state/README.md:50-52` records it as superseded by `fetch_live_surface.py` / `CHAIN_LIVE_DIR` |

#### Volume context

| Path | Contents | In scope? |
|---|---|---|
| `/data` | `oi_raw`, `chain_eod`, `equity_1min`, `equities_scalp` (4 KB, empty), `spx_options` (395 MB) | Partly — `spx_options` here is **not referenced by any script in this repo** and is the third location of a store whose two real halves live on volumes 1 and 2 |
| `/mnt/trading_volume_3` | `chain_live`, `chain_snapshots`, `equities_scalp`, plus `backup`, `ext_trades`, `quote_ticks` | Partly — the last three have no writer here; out of scope per README |
| `/mnt/trading_volume_2` | `spx_options` (1,005 entries, static, first 3-4 years) | No — other project |
| `/mnt/volume1` | `spx_options` (669 entries, recent through live) | No — other project |

Note on `SCALP_DATA_DIR`: `scalp/README.md:242` documents
`/mnt/trading_volume_3/equities_scalp` with layout
`<SCALP_DATA_DIR>/raw/<SYMBOL>/<YYYY-MM-DD>.parquet` (`scalp/store.py:7`). It is
**not** defined in `config.py` and not present in `env_paths.txt`, so its
resolved production value is `UNCERTAIN — set outside the captured .env`. A
same-named empty directory also exists at `/data/equities_scalp` (4 KB), which
is consistent with the unmounted-`/mnt` failure mode `scalp/config.py:479` warns
about. Full treatment in Section 1b.

---

## Section 1b — Script inventory (`scalp/`, `Cowork/`)

### `scalp/` — a self-contained second pipeline

Three structural facts that govern every row below:

1. **Nothing in `scalp/` is scheduled.** No `crontab.txt` entry names `scalp`,
   `-m scalp.*`, or any file under it. The whole subsystem is manual, run as
   `python -m scalp.<module>`.
2. **It is deliberately isolated from the root project.** `scalp/__init__.py`,
   `config.py`, `db.py` and `thetadata.py` each carry an explicit
   "SELF-CONTAINED — does not import the project-root module" note.
   `scalp/thetadata.py:2` says "DO NOT REFACTOR INTO A SHARED MODULE". So the
   root `config.py`/`db.py`/`lib/thetadata.py` and the `scalp/` equivalents are
   intentional duplicates, not drift.
3. **Postgres holds derived metrics only.** `scalp/store.py:4` — "Parquet is the
   record. Postgres holds derived metrics only — no tick data."

#### Pipeline entry points

| Path | Purpose (from docstring) | Kind | CLI flags | Invoked by | Git | READS | WRITES |
|---|---|---|---|---|---|---|---|
| `scalp/update_universe.py` | "Nightly candidate list. One API call." | entry (`-m scalp.update_universe`) | `--date --from-eod --candidates --eod-workers --dry-run --verbose` | **manual** — despite "nightly", no cron entry | 2026-09-06 | ThetaData stock EOD | `equities_scalp.universe` via `db.write_universe` (`db.py:763`) |
| `scalp/fetch.py` | "Fetch trade_quote to parquet. Resumable, incremental, space-aware." | entry | `--start --end --symbols --workers --plan --yes --confirm-threshold --allow-today --min-exchange-codes --verbose` | **manual** | 2026-09-01 | ThetaData `/v3/stock/history/trade_quote`; `equities_scalp.universe` | `SCALP_DATA_DIR/raw/{SYMBOL}/{YYYY-MM-DD}.parquet`; `equities_scalp.fetch_runs` (`db.py:578`) |
| `scalp/compute.py` | "Read parquet, compute metrics, write Postgres." | entry | `--start --end --symbols --no-intraday --no-quiet --replace --workers --print --verbose` | **manual** | 2026-09-07 | `SCALP_DATA_DIR` parquet | `equities_scalp.daily_metrics` (:406), `.provenance` (:407), `.intraday_metrics` (:409), `.intraday_monthly` (:516) |
| `scalp/prune.py` | "Delete old raw parquet. RUN BY HAND. Dry-run by default." | entry | `--intraday --intraday-before --older-than --before --symbols --delete` | **manual, by design** | 2026-09-01 | `SCALP_DATA_DIR`; `equities_scalp.intraday_metrics` partition list | **Destructive.** Deletes parquet files; with `--intraday --delete` issues `DROP TABLE` on `equities_scalp.intraday_metrics` daily partitions past retention (`prune.py:84`). Dry-run unless `--delete` |

#### Libraries (no `__main__`)

| Path | Purpose (from docstring) | Git | Notes |
|---|---|---|---|
| `scalp/config.py` | "Configuration for the equities-scalp pipeline. SELF-CONTAINED." | 2026-09-07 | 66 KB. Defines `SCALP_DATA_DIR`, retention, universe filters. `:479` carries the unmounted-`/mnt` warning; `:533` the volume capacity note |
| `scalp/db.py` | "Postgres access for the equities-scalp pipeline. SELF-CONTAINED." | 2026-09-07 | Owns all seven `CREATE TABLE`s and every write helper. See §2c |
| `scalp/store.py` | "Parquet store: layout, atomic writes, and what is already on disk." | 2026-08-31 | Layout `raw/{SYMBOL}/{YYYY-MM-DD}.parquet` |
| `scalp/thetadata.py` | "ThetaData v3 STOCK client. SELF-CONTAINED BY DESIGN." | 2026-08-31 | Stock endpoints; distinct from `lib/thetadata.py` (options) |
| `scalp/schema.py` | "Column names as the vendor actually returns them, and how to resolve them." | 2026-08-31 | |
| `scalp/metrics.py` | "Metric computation for the equities-scalp ranker." | 2026-09-08 | 49 KB, pure compute. Most recently edited file in the repo |
| `scalp/quiet.py` | "Quiet-window metrics: is the level shift small relative to the range I work?" | 2026-09-07 | Pure compute |
| `scalp/quality.py` | "Data-quality guards for the trade tape." (restatement detection) | 2026-08-31 | Has `--selftest`, so it is runnable, but exposes no pipeline action |
| `scalp/metric_docs.py` | "Metric name -> its definition and its section in METRICS.md." | 2026-09-07 | **External consumer likely** — docstring says it "exists so the dashboard can make every column header a link" |

#### Diagnostics and profilers

| Path | Purpose (from docstring) | Kind | CLI flags | Git | READS | WRITES |
|---|---|---|---|---|---|---|
| `scalp/audit_venue.py` | "Is the stored tape consolidated, or Nasdaq-only? Reads parquet, nothing else." | entry | `--start --end --sample --full --symbols --threshold --seed` | 2026-08-31 | `SCALP_DATA_DIR` parquet | stdout |
| `scalp/profile_compute.py` | "Where does compute.py's time per symbol-day go?" | entry | `--symbol --date --top --universe --days` | 2026-09-07 | `SCALP_DATA_DIR` parquet | stdout |
| `scalp/profile_quiet.py` | "Where does quiet_session's time go, and on which symbols?" | entry | `--date --symbols --top --bottom --all --with-compute --workers --csv` | 2026-09-07 | `SCALP_DATA_DIR` parquet | stdout; CSV at `--csv` |

#### `scalp/step0/` — vendor discovery probes, all one-time

Eight numbered scripts that answered a question about the ThetaData stock
endpoint before the pipeline was built. All are entry points, all read the
vendor API only, all write stdout. All dated 2026-08-31 — the whole directory is
one day's work and has not been touched since.

| Path | Question it answered | Flags |
|---|---|---|
| `scalp/step0/s0_availability.py` | "GATE: is `/v3/stock/history/trade_quote` on the Standard plan?" | `--symbol --date --verbose` |
| `scalp/step0/s1_venue_check.py` | "does it need `venue=utp_cta`?" | `--symbol --date --expected --tolerance --skip-control --verbose` |
| `scalp/step0/s2_one_day.py` | "one symbol, one day: shape, time and size" | `--symbol --date --verbose` |
| `scalp/step0/s3_multiday_timing.py` | "how long does a 544-symbol backfill actually take?" | `--symbol --end-date --days --compare-serial --universe-size --verbose` |
| `scalp/step0/s4_conditions.py` | "which trade condition codes are present, and which to exclude?" | `--symbols --date --verbose` |
| `scalp/step0/s5_quote_emission.py` | "are quote records emitted on every update, or only on change?" | `--symbol --date --start-time --end-time --interval --verbose` |
| `scalp/step0/s6_session_bounds.py` | "where are the missing 19% of shares?" | `--symbol --date --expected --verbose` |
| `scalp/step0/s7_quote_sizing.py` | "how big is a full-day quote-tick pull, and what does 1s cost?" | `--symbols --date --compare-symbol --universe-size --days --verbose` |
| `scalp/step0/_common.py` | library — "Shared helpers for the step 0 discovery scripts" | — |

#### `scalp/tests/`

| Path | Purpose | Kind | Git |
|---|---|---|---|
| `scalp/tests/check_references.py` | "Resolve every cross-module attribute reference in `scalp/` without importing." | entry | 2026-08-31 |
| `scalp/tests/smoke.py` | "Execute every entry point and the whole metric path. No socket, no database." | entry | 2026-09-07 |
| `scalp/tests/test_quiet.py` | "Hand-checked tests for `scalp/quiet.py` and the metric cut." | pytest | 2026-09-07 |
| `scalp/tests/test_schema_sync.py` | "The intraday column set must reach the table, and a failing run must stop." | pytest | 2026-09-07 |
| `scalp/tests/test_timestamp_collapse.py` | "Same-timestamp collapsing must happen BEFORE duration weighting." | entry | 2026-09-07 |
| `scalp/tests/test_universe_filters.py` | "Universe filter tests: the price band and the spread floor." | pytest | 2026-09-06 |
| `scalp/tests/test_worker_reaping.py` | "Do pool workers die when the parent is killed with -9?" | entry | 2026-09-01 |

#### `scalp/` documentation

`scalp/README.md` (24 KB, 2026-08-31) and `scalp/METRICS.md` (36 KB, 2026-09-08)
— the latter is what `metric_docs.py` maps column names into.

### `Cowork/metrics_panel` — not code

**There are no scripts here.** The directory contains nine parquet files and
nothing else:

```
Cowork/metrics_panel/year=2018/43fdd9b81f714ac3bfd944a40fe116c5-0.parquet
Cowork/metrics_panel/year=2019/…  through  year=2026/…
```

Read directly from the files:

| Property | Value |
|---|---|
| Layout | Hive-partitioned, `year=YYYY`, 2018-2026 |
| Files | 9, **all sharing the UUID prefix `43fdd9b8…`** — one single write job, not incremental appends |
| Rows | 220,232 total (2018: 47 · 2019: 26,712 · … · 2026: 12,099) |
| Columns | 158 |
| Size | ~297 MB |
| First columns | `ticker, trade_date, total_oi, call_oi, put_oi, put_call_oi_ratio, max_oi_strike_call, max_oi_strike_put, pct_oi_in_front_expiry, d1_total_oi_change, …, rv_5d, rv_20d, ret_1d_fwd_oc, …` |

Those column names are `open_interest.daily_features`. **This is a one-shot
export of `daily_features`, partitioned by year** — 220,232 rows against the
table's 200,677 live rows, over the same 2018-2026 span.

| Question | Answer |
|---|---|
| What writes it? | **Nothing in this repo.** Grep for `Cowork` and `metrics_panel` across all source returns zero hits |
| What reads it? | **Nothing in this repo.** Same grep |
| Is it tracked? | No — `Cowork/` is untracked in git (`?? Cowork/`) |
| Is it current? | 2026 partition exists, but a single-UUID write means it is a point-in-time dump, not a maintained store |

Section 3 candidate: ~297 MB of untracked, unreferenced duplicate of a live
Postgres table sitting in the repo root. Not acting on it.

---

## Section 3 — Orphans and conflicts

**Candidates for review. Nothing here has been acted on.** Ordered within each
category by how much evidence there is, not by severity.

Two things are deliberately **not** filed as problems, per your rules:

- **`scalp/`'s duplication of `config.py`, `db.py`, `thetadata.py`.**
  `scalp/thetadata.py:2` says "SELF-CONTAINED BY DESIGN — DO NOT REFACTOR INTO A
  SHARED MODULE" and the other two carry equivalent notes. Intentional.
- **Manual-by-design scripts.** Absence from cron is not evidence of
  abandonment. Where a script's own text says it is a one-time migration, a
  hand-run tool, or a diagnostic, it is filed as manual, not orphaned.

### 3.1 Scripts with no caller

#### Manual by design — the code says so

Not orphans. Listed for completeness, with the phrase that establishes intent.

| Script(s) | Evidence of intent |
|---|---|
| `scalp/prune.py` | docstring: "RUN BY HAND. Dry-run by default." |
| `check.py` | "pre-push gates. Run this before every push." |
| `init_db.py` | "One-time schema + view initialisation" |
| `migrate_chain_snapshots_to_monthly.py` | "one-time layout change" |
| `backfill_call_price.py`, `backfill_days_to_earnings.py`, `backfill_source_session.py` | each states the one-time population it performs; `backfill_source_session.py` is explicitly "One-time UPDATE" |
| `deadlock_proof.py`, `lockproof.py` | "Reproduce … and prove the fix" |
| `scalp/step0/s0`–`s7` (8 scripts) | numbered "Step 0.N —" discovery questions; whole directory dated 2026-08-31 |
| all `audit_*.py`, `probe_*.py`, `verify_*.py`, `bench_greeks_endpoints.py`, `snapshot_null_audit.py`, `rv_vrp_noise_report.py`, `zscore_migration_check.py`, `iv25d_quality_report.py`, `phase1_iv25d_sample.py` | each opens "Read-only" / "READ-ONLY" / "Read-only diagnostic" |
| `fetch_chain_snapshots.py`, `fetch_chain_intraday.py`, `fetch_equity_1min.py`, `build_equity_surface.py`, `build_trade_paths.py` | manual pipeline stages with full CLI surfaces |
| every `scalp/` entry point | no cron reaches `scalp/` at all; invoked as `python -m scalp.<module>` |

#### Appears abandoned — the code says superseded

| Script | Evidence | Note |
|---|---|---|
| `fetch_iv_chain.py` | `fetch_chain_eod.py:4` — "Replaces both fetch_volume_eod.py and fetch_iv_chain.py." No importer, no cron, no subprocess. Last touched 2026-05-27 | Still present and still runnable. Writes `open_interest.option_iv_daily`, which nothing in the repo reads |
| `fetch_volume_eod.py` | same sentence names it. No caller. Last touched 2026-05-27 | Writes `open_interest.option_volume_daily`, which nothing in the repo reads |
| `migrations/export_raw_to_parquet.py` | `migrations/README.md:13` "**Complete.** Table dropped 2026-09-01" | **Cannot run** — its source table `open_interest.option_oi_raw` no longer exists. Distinct from the others: not merely uncalled, but non-functional |
| `scalp/db.py:787` `write_ranking()` | Not a script — a writer function with **zero callers anywhere in the repo** | The only writer of `equities_scalp.rankings` |

#### Ambiguous — cannot tell from the code

| Script | Why it is unclear |
|---|---|
| `backtest_spreads.py` | Oldest file in the project (2026-05-19). Its docstring describes an ongoing capability ("Backtest call debit spreads from a predefined trade list"), not a one-time job, so it does not qualify as manual-by-design by the test used above. But nothing calls it, and `open_interest.backtest_call_spread` (1,971 rows) has no in-repo reader. **UNCERTAIN — the code does not say whether this is a live tool or a finished experiment** |
| `validate_bins.py` | Manual by design (post-build check), but see 3.4 — its coverage no longer matches the tables that exist |

### 3.2 Written but never read

#### Written here, no in-repo reader — external consumer likely, NOT orphans

Per `vps_state/README.md:19-20`, a table written here with no reader here is
consumed by the dashboard.

| database.table | Written by | Note |
|---|---|---|
| `open_interest.is_bins` | `build_bin_tables.py --tier`, 3×/weekday | Actively maintained, zero in-repo readers |
| `open_interest.equity_metrics_catalog` | `lib/metrics_store.py:146` | `lib/metrics_config.py:9` names the consumer: "the dashboard's metric picker" |
| `open_interest.tt_bins` | `build_bin_tables.py --build-tt-bins` — never invoked | `build_bin_tables.py:571` names the dashboard as consumer. Also read in-repo by `build_trade_paths.py` |
| `equities_scalp.intraday_monthly` | `scalp/compute.py:516` | Built by reading `intraday_metrics` (`scalp/db.py:554`); **never selected from anywhere in the repo** |
| `equities_scalp.daily_metrics`, `.provenance`, `.fetch_runs`, `.universe`, `.intraday_metrics` | `scalp/` | These **do** have in-repo readers (`scalp/db.py:459, 554, 588, 803-841, 862-882`) — listed only to record that they were checked |

#### Written here, no reader anywhere that can be established

| database.table | Written by | Note |
|---|---|---|
| `open_interest.option_iv_daily` | `fetch_iv_chain.py` (orphaned) | 215,060 rows. Writer superseded; no in-repo reader. Whether the dashboard reads it is unknown |
| `open_interest.option_volume_daily` | `fetch_volume_eod.py` (orphaned) | 215,582 rows. Same |
| `open_interest.backtest_call_spread` | `backtest_spreads.py` (uncalled) | 1,971 rows. Same |

#### Dead in both directions

| Store | Status |
|---|---|
| `equities_scalp.rankings` | **No writer** (its only writer function has no caller) and **no reader**. The one table in either database with nothing on either side |
| `CHAIN_INTRADAY_DIR` (`/mnt/trading_volume_3/chain_intraday`) | Configured in `.env`, **directory does not exist**, no scheduled writer, no scheduled reader |
| `Cowork/metrics_panel` | ~297 MB, 220,232 rows. Zero grep hits for `Cowork` or `metrics_panel` in any source file. Untracked in git |
| `/data/spx_options` (395 MB) | Referenced by no script in this repo. Third location of a store whose two real halves are on `volume1` and `trading_volume_2` |

#### Read here, produced elsewhere

| database.table | Read by | Producer |
|---|---|---|
| `equities_scalp.fills` | `analysis/dump_fills.sql` → `fills_dump.csv` → `analysis/episode_*.py` | `analysis/dump_fills.sql:6`: "owned by the dashboard (`app/routers/equities_scalp.py`)" |

#### Neither read nor written here

The 19 tables in §2a's re-derived list. They coexist in `open_interest` and this
repo never touches them in either direction. **Not listing owners**, per your
rule.

#### Actively written on a schedule, read only by a manual diagnostic

| Store | Note |
|---|---|
| `CHAIN_LIVE_DIR` | Written every 5 minutes during RTH by `fetch_live_surface.py` under cron — the **only parquet store on a schedule**. Its sole in-repo reader is `verify_surface_fixes.py`, a manual diagnostic. `fetch_live_surface.py:8-9` states the intent: "Not for the dashboard — for [archival]". So it is an archive by design, but nothing in the repo consumes the archive |

### 3.3 Two or more writers on the same table

#### By design and documented — recorded, not flagged

| database.table | Writers | Design note |
|---|---|---|
| `open_interest.daily_features` | `build_features.py` from three cron paths (EVENING, MORNING, PREMARKET) | `CLAUDE.md` and `run_pipeline.py:23-28`: disjoint column sets, no DELETE before upsert |
| `open_interest.wf_bins`, `.is_bins` | `build_bin_tables.py` MORNING and EVENING tiers | `build_bin_tables.py:8-13`: same disjoint-column contract, "DO NOT add [a DELETE]" |
| `open_interest.underlying_ohlc` | `fetch_ohlc.py` (full row) + `fetch_ohlc_premarket.py` (`open` only) | `fetch_ohlc_premarket.py:29` explains the overwrite ordering |

#### Multiple writers, no stated contract

| database.table | Writers | Why it is worth review |
|---|---|---|
| `open_interest.equity_surface` | 1. `fetch_live_surface.py` (cron, every 5 min) · 2. `build_equity_surface.py` (manual) · 3. `backfill_call_price.py` (UPDATE `call_price`) | Three writers on one UNIQUE key `(ticker, trade_date, snapshot, dte, put_delta)`. Writers 1 and 2 can target the same partitions — one live, one hand-run — with nothing documenting precedence |
| `open_interest.equity_atm` | `fetch_live_surface.py` + `build_equity_surface.py` | Same pairing |
| `open_interest.equity_surface_diagnostics` | `fetch_live_surface.py` + `build_equity_surface.py` | Same pairing |
| `open_interest.equity_metrics` | `build_equity_metrics.py` (cron) + `backfill_days_to_earnings.py` (UPDATE) | Column-scoped backfill against a live table |
| `equities_scalp.intraday_metrics` | 1. `scalp/compute.py` (INSERT) · 2. `migrations/rebuild_intraday_metrics.sql` (**TRUNCATE**) · 3. `scalp/prune.py --intraday --delete` (**DROP TABLE** on partitions) | **Three different mutators, two of them destructive**, in two different directories, one of them a `.sql` file under the root `migrations/` |
| `open_interest.metric_classification` | `sql/06_25d_skew_metrics.sql:47` (INSERT) + whatever external codebase created it | See 3.6 |

### 3.4 Name vs. behaviour mismatches

| # | Where | Name / doc says | Code does |
|---|---|---|---|
| 1 | `build_bin_tables.py:2` | "Build / refresh wf_bins and tt_thresholds" | Also builds `is_bins` (:266) and `tt_bins` (:377) — half the file is undocumented by its own docstring |
| 2 | `build_bin_tables.py` flags | `--build-tt` / `--build-tt-bins` | Nearly identical names for different tables. `--build-tt` → `tt_thresholds` (the **legacy** artifact, per :571). `--build-tt-bins` → `tt_bins` (what the dashboard reads). Cron passes only the first |
| 3 | `build_bin_tables.py` `--tier` | reads as "pick a tier" | Writes **two tables** per invocation, `wf_bins` and `is_bins`. Nothing in the flag name or help text says so |
| 4 | `validate_bins.py:2` | "8-check validation suite for wf_bins and tt_thresholds" | Accurate to its own name, but `is_bins` and `tt_bins` now exist and are **not validated by anything**. The suite silently covers half the bin tables |
| 5 | `diagnose_transform.py:4` | "Read-only diagnostic" | Writes probe parquet to `CHAIN_INTRADAY_DIR/_diag/` (:263-267) and to system temp. The only script whose read-only claim is false |
| 6 | `fetch_oi.py` / `fetch_oi_snapshot.py` | two names suggest two destinations | Both write the **same** store, `OI_RAW_DIR/{TICKER}/{year}.parquet`, with the same dedupe key. Documented in `run_pipeline.py:204-207`, but not at either script |
| 7 | `scalp/update_universe.py:1` | "Nightly candidate list." | Nothing runs nightly — `scalp/` has no cron entry at all |
| 8 | `migrations/rebuild_intraday_metrics.sql` | sits in the root `migrations/`, beside `open_interest` migrations | Targets `equities_scalp.intraday_metrics`. Only file in that directory pointing at the other database |
| 9 | `Cowork/metrics_panel` | name suggests a dashboard panel asset | Is a year-partitioned export of `open_interest.daily_features` |
| 10 | `fetch_chain_eod.py:4` | "Replaces both fetch_volume_eod.py and fetch_iv_chain.py" | Both replaced scripts are still present, still runnable, and still able to write their tables |
| 11 | `docs/daily_features_data_dictionary.md` vs `daily_features_data_dictionary.md` | `CLAUDE.md:8` points at the `docs/` copy | Two copies exist: `docs/` (40,543 bytes, 2026-06-08) and repo root (40,543 bytes, 2026-05-30, untracked). Same size, different mtimes — **UNCERTAIN whether content differs**; I did not diff them |

### 3.5 Dead code paths and superseded implementations still present

| # | What | Superseded by | Still present as |
|---|---|---|---|
| 1 | `fetch_iv_chain.py`, `fetch_volume_eod.py` | `fetch_chain_eod.py` | Both files, both runnable, both with live target tables |
| 2 | `CHAIN_INTRADAY_DIR` store + `fetch_chain_intraday.py` + `build_equity_surface.py --source intraday` | `CHAIN_LIVE_DIR` / `fetch_live_surface.py`, per `vps_state/README.md:50-52` | Env var set, code intact, directory absent |
| 3 | `open_interest.tt_thresholds` | `open_interest.tt_bins`, per `build_bin_tables.py:571-572` | **The legacy table is the one cron refreshes nightly; the current one has not been built since 2026-08-14** |
| 4 | legacy `{YYYY}.parquet` chain_snapshots layout | `{YYYYMM}.parquet` via `migrate_chain_snapshots_to_monthly.py` | `build_equity_surface.py:381` still calls `list_legacy_year_files` to detect the old layout |
| 5 | `open_interest.option_oi_raw` | `OI_RAW_DIR` parquet | Table dropped; `migrations/export_raw_to_parquet.py` still references it and cannot run |
| 6 | `open_interest.option_oi_surface` | dropped outright | `migrations/README.md:66` still shows an `INSERT INTO option_oi_surface` example |
| 7 | `scalp/db.py:787` `write_ranking()` + `equities_scalp.rankings` | nothing — never wired up | Function and table both exist, both unused |
| 8 | `build_features.py` working tree | — | 1,755 lines replaced by the character `q`, uncommitted. `HEAD` is intact |

### 3.6 `metric_classification` — externally-owned table controlling this repo's schema

Its own entry, because it is the only two-way dependency of this kind.

**Direction 1 — the table controls this repo's schema.**
`lib/bin_schema.py:44,51` reads `SELECT metric, tier FROM metric_classification
WHERE eligible_as_metric = TRUE`. That result set determines, at runtime, which
`frac_<m>` / `bin20_<m>` column pairs `sync_wf_bins_schema`,
`sync_is_bins_schema` and `sync_tt_bins_schema` create, and therefore which
columns `build_bin_tables.py` writes into `open_interest.wf_bins`,
`.is_bins` and `.tt_bins` on every cron run. `sql/05_bin_tables.sql:7` states
this explicitly: the bin tables' column set is "driven by
metric_classification (eligible_as_metric = TRUE)".

**Direction 2 — this repo mutates the table.**
`sql/06_25d_skew_metrics.sql:47` performs `INSERT INTO metric_classification`,
and per `sql/06_25d_skew_metrics.sql:5,10` it "flips
metric_classification.eligible_as_metric" via a conditional INSERT + UPDATE.
Applied by `init_db.py`.

**What is missing.** There is **no `CREATE TABLE metric_classification` anywhere
in this repo.** `sql/` creates 20 tables and this is not one of them. The DDL
comes from outside.

**Why it is worth review:** a table this repo does not own, cannot recreate, and
whose other writers it cannot see, decides the column set of three tables that
three cron runs a weekday write into. A row appearing or its
`eligible_as_metric` flipping externally changes what the pipeline produces, with
no signal in this repo. `lib/bin_schema.py:159,208,263` already logs "Skipped N
metric(s) listed in metric_classification but absent [from daily_features]",
so drift in that direction is at least noticed; the reverse is not.

Row count: 172 (`table_stats.txt`).

### 3.7 Structural inconsistencies noted in passing

| What | Detail |
|---|---|
| `open_interest.equity_surface_diagnostics` is not partitioned | Its three siblings written by the same `lib/surface_store.write_snapshot` call — `equity_surface`, `equity_atm` — are `PARTITION BY RANGE (trade_date)` monthly. This one is a single 3,979,247-row / 937 MB table. `lib/surface_store.py:97-98` calls `ensure_equity_surface_partition` and `ensure_equity_atm_partition`; there is no diagnostics equivalent |
| `daily_features` upsert churn | 1,204,779 inserts ever against 200,677 live rows — 6× turnover. Expected from the three-cron upsert design, recorded so it is not mistaken for a defect later |
| `docs/vps_state/` staged as empty blobs | All nine files are in git's index at blob `e69de29` (empty), with content present only in the working tree. A `git commit` now would commit nine empty files |

---

*End of inventory. Sections 1a, 1b, 2 and 3 complete.*
