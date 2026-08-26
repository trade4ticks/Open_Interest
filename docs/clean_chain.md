# `clean_chain` — option-chain cleaning stage

Stage two of the chain pipeline:

```
fetch (exists) -> CLEAN (this) -> interpolate (not built) -> metrics (not built)
```

Takes a DataFrame of raw option-chain quotes, returns it with computed fields
and data-quality flags added. Two entry points, in `lib/clean_chain.py`:

```python
clean_chain(df)   -> DataFrame with computed fields + flag columns
clean_summary(df) -> flag rates by ticker x DTE bucket x delta bucket
```

Both accept an optional `config=` (a dict or a path) which defaults to
`flag_config.yaml` at the project root.

## Contract

- **No file I/O.** Reads nothing, writes nothing. Callers decide what to load
  and whether to persist. The live intraday path cleans in memory and hands
  the result to interpolation, which owns the Postgres write; nothing reads
  cleaned columns back off disk. Writing to the parquet store mid-session
  would also collide with the fetcher, which appends row groups incrementally
  through the trading day.
- **Drops no rows.** Flags mark; filtering belongs to interpolation. Row
  order and the caller's index are preserved.
- **Idempotent.** Every computed column derives from raw columns only, never
  from another computed one, so cleaning an already-cleaned frame is a no-op.
- The input frame is never mutated.

Works unchanged on both stores — the 5-minute
(`chain_intraday/<TICKER>/<YYYYMMDD>.parquet`) and twice-daily
(`chain_snapshots/<TICKER>/<YYYYMM>.parquet`) — because they share one 20-column
schema. There is no store-specific branching.

## Thresholds are inherited and known mis-tuned

**Every value in `flag_config.yaml` except `iv_error_high` came from an
index-options implementation and is wrong for single-name equities.** They are
left unchanged deliberately: flags drop no rows, so a bad threshold costs a
noisy column and nothing else, and the intent is to retune from observed rates
via `clean_summary` rather than guess.

Expect specifically:

| Threshold | Expected misbehaviour on equities |
|---|---|
| `wide_spread_abs: 5.0` | $5 is large relative to equity option prices — should fire almost never |
| `wide_spread_pct: 0.20` | high fire rate even on liquid near-the-money strikes; ~25% of AAPL 7-DTE near-the-money quotes |
| `deep_otm: 0.85 / 1.15` | a fixed ±15% band across a universe spanning ~12-vol to ~80-vol names. At 30 DTE and 25 vol that is under 2 sigma — inside normal trading range |
| `iv_error_high: 0.05` | pure placeholder, no basis yet |

`clean_summary` exists to fix this: it answers "what fraction of 10-delta puts
on this ticker trip each flag", with `n_rows` per cell so a rate is
interpretable — a 100% rate over four rows is not a finding.

## Conventions worth knowing

**Moneyness is inverted from the usual convention, on purpose.**

```
moneyness     = underlying / strike     (S/K, not K/S)
log_moneyness = log(S/K)                (negative of the usual log(K/F))
```

This matches a sibling project so metrics stay comparable. Do not "fix" it.
The consequence is that the sign reads backwards from habit: `moneyness > 1`
means the strike is **below** spot (OTM put / ITM call); `moneyness < 1` means
the strike is **above** spot (OTM call / ITM put). `flag_deep_otm` is a band
around 1, so it catches both tails.

**Gamma is derived, not vendor-supplied**, as a finite difference of delta
against strike, grouped by `(timestamp, expiration, option_type)`.

`expiration` in that key is load-bearing. A single file holds every listed
expiration and their strike ladders overlap heavily, so grouping without it
differences delta values from *different* expirations that happen to sit at
adjacent strikes — plausible-looking numbers, silently wrong, nothing raised.
`test_clean_chain.py` asserts this against hand-computed values on two
expirations with fully overlapping ladders.

Groups with fewer than two rows get `NaN`. A zero denominator (two identical
strikes in one group, which the stores' dedupe keys make impossible) also
yields `NaN` rather than `inf`, so it cannot propagate silently.

**Flag columns are plain `bool`, never nullable `boolean`.** Downstream
filtering does `~df[flag].astype(bool)`, and on a nullable column an NA becomes
`True` under that expression — silently dropping rows that should have been
kept. Every flag is forced through a coercion that turns NA into `False`, so a
missing `implied_vol` or `delta` reads as "did not trip", not "unknown".

**`spread_pct` is `NaN`, not `inf`, when `mid_price <= 0`.** An infinite
spread_pct would survive every threshold comparison and poison any aggregate
built on it.

**`bdte`** counts NYSE sessions in `(trade_date, expiration]` — exclusive of
the start. It builds one session index per call and uses `searchsorted`; the
naive one-`.schedule()`-per-trade_date approach is ~250 calendar calls for a
year-spanning snapshot file, which does not scale to 121 tickers.

**`flag_stale_underlying`** looks for a frozen underlying across **3 or more
consecutive distinct timestamps**, then marks every row at those timestamps.
Runs are found over distinct timestamps, not rows — a single timestamp holds
the whole chain, so counting rows would call any wide chain a long run. On
5-minute data a run of 3 is 15 minutes without a tick, meaning every IV and
greek stamped there was priced against a possibly-stale underlying. A missing
underlying price never forms a run: an absent price is not evidence the feed
is stuck.

## `clean_summary` output

One row per `(ticker, dte_bucket, delta_bucket)`:

- `n_rows`
- `rate_<flag>` for each of the 13 flags plus `rate_any` — the fraction of
  rows in that cell where the flag is `True`

Buckets: DTE `0-7 / 8-21 / 22-45 / 46-90 / 91+`, `abs(delta)`
`0-0.10 / 0.10-0.25 / 0.25-0.40 / 0.40-0.60 / 0.60+`.

Rows whose DTE or delta cannot be bucketed land in an explicit `missing`
bucket rather than being dropped by the groupby, so `n_rows` always sums to
`len(df)`. Negative DTE, if any ever appears, falls into `0-7`.

Flags are recomputed from the raw columns inside `clean_summary`, so passing a
frame already cleaned under different thresholds still gives rates for the
thresholds you asked for.

## Performance

1.2M rows (roughly one 5-minute session for a wide chain, 3,744 gamma groups):

```
clean_chain     2.41s   (2.0 us/row)
clean_summary   2.97s
```

Gamma uses a vectorised `groupby().shift()` construction rather than a
per-group Python loop — group count is large (many expirations x many
timestamps per file) and a loop with per-group `.loc` assignment is orders of
magnitude slower.

## Out of scope

Reading or writing files; dropping rows; solving forward price or rate from
put-call parity; corporate actions and adjusted deliverables; fitting a
volatility smile. The first two are permanent; the rest belong to later
stages.

## Tests

`python test_clean_chain.py` — 47 assertions, all hand-computed rather than
snapshotted. Covers gamma not leaking across expirations (or option types),
single-strike groups yielding NaN instead of raising, `spread_pct` NaN not inf,
idempotence, flag dtype and NA-freedom, the stale-underlying run logic, and
`clean_summary` accounting for every row.
