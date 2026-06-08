# `daily_features` — Data Dictionary

Reference for every column in the `daily_features` table: formula, inputs, OHLC
dependency, and cron tier. Reflects the **post-fix** schema (the changes agreed
during the audit: `_co`→`_pc` volume renames, six new `ret_*_fwd_cc` columns,
the `atr_normalized_ret_5d` units fix, and `weighted_avg_dte_vol` surfaced).

> **Status.** All schema and pipeline changes have been implemented and
> confirmed in code, and verified by independent recalculation against raw data
> (AAPL 2019-07-02, all metrics reconciled). This document reflects the current
> production pipeline, including the raw-chain storage architecture and the
> universal split adjustment described in the next section. Remaining judgment
> calls and known limitations are listed at the end.

---

## Conventions and legend

- **T** — the row's `trade_date`, an NYSE trading session.
- **C_t, O_t, H_t, L_t, V_t** — close, open, high, low, volume on session *t*.
- **C^spy_t** — SPY's close on *t*.
- **OI** — open interest per option contract (from the raw parquet store).
- **DTE** — days to expiration: `expiration − T`.
- **moneyness_x** — `strike / spot_x − 1`, where *x* is `pc` or `co`.
- **log_ret_t** — `LN(C_t / C_{t-1})`.
- **σ_n[X]** — sample standard deviation of series X over the trailing *n*
  sessions inclusive of T (divisor *n−1*; DuckDB `STDDEV_SAMP`, = Excel
  `STDEV.S`).
- **μ_n[X]** — arithmetic mean of X over the same *n* sessions.
- **next_monthly** — the first third-Friday expiration on or after T, snapped to
  an actually-listed expiration.
- **Σ(… WHERE cond)** — sum over option-chain rows on T meeting the condition.

### Spot definitions

Two spot prices are carried so spot-referenced metrics can be expressed against
either. The project standardizes on **prior close** for analysis, but both are
retained for the OI block.

- **spot_pc** = `C_{T-1}` — prior session's close. Known overnight; the price a
  trader sees pre-market.
- **spot_co** = `O_T` — the open of session T. Not known until the opening
  print.

### Cron tier

Each column is computed by one of two cron jobs, which write disjoint column
sets to the same `(ticker, trade_date)` row via a scoped upsert.

- **EVENING** — computable after T-1's close (~5:30pm). OHLC-derived metrics,
  IV metrics, and pure volume metrics.
- **MORNING** — requires OI data, published next morning (~7am). The OI block,
  plus vol/OI cross-metrics that reference `total_oi` / `call_oi` / `put_oi` /
  `d1_total_oi_change`.

**Tier rule:** a metric is MORNING-tier if and only if its formula references
`total_oi`, `call_oi`, `put_oi`, or `d1_total_oi_change`. Everything else is
EVENING-tier.

### Rolling z-scores — shared behavior

All rolling z-scores follow the same construction:

- Window: 60 trailing trading days inclusive of T
  (`ROWS BETWEEN 59 PRECEDING AND CURRENT ROW`).
- `zscore = (X_T − μ_60[X]) / σ_60[X]`, sample stdev (`STDDEV_SAMP`).
- Returns `NULL` until the window holds 60 non-null observations.
- A `NULLIF` guard on the denominator returns `NULL` when X is constant over
  the window.

Note: `X_T` denotes the value stored in row T of the underlying series. For
OHLC-derived series, that value is itself "as of start of session T" (computed
from closes through T-1) per the universal invariant; the z-score inherits
that timing from its input and needs no separate adjustment.

Note: `_3m` in a column name denotes this 60-session window. It is ~3 months,
not a literal 63-trading-day calendar quarter — a deliberate, consistently
applied convention.

---

## Data architecture

All four metric blocks are computed in a single `build_features.py` run, each
reading raw per-contract data and applying any adjustments at read time. No
metric reads pre-aggregated values from a database; aggregation happens inside
`build_features.py` SQL so it is inspectable and replayable.

### "As of" semantics — the universal row invariant

**Row trade_date T in `daily_features` represents the trader's knowledge at
9:30am of session T** — the OC entry time. Every feature in the row is computed
from data available at or before this moment, and forward returns enter from
this moment forward. Concretely, this means each block uses inputs ending one
session earlier than T:

- **OHLC features**: use data through session T-1's close (4pm of T-1).
- **OI features**: use OI labeled trade_date T, which ThetaData publishes at
  ~7am of T and represents the EOD position from session T-1's close. Despite
  the trade_date label matching T, the underlying position is T-1's.
- **Chain features (vol, IV)**: use chain data from session T-1's close, routed
  to the daily_features row via the `feature_date = next_trading_day(trade_date)`
  offset described below.
- **Forward returns**: enter at T's 9:30am open (`_oc`) or T-1's close (`_cc`)
  and exit at a later close.

This invariant is what eliminates lookahead bias: nothing in the feature row
depends on data the trader couldn't have at 9:30am of T. It was instituted
after an audit found the OHLC block had been using through-T's close (a
6.5-hour lookahead window relative to the 9:30am OC entry); see the change
history.

### Block-specific sourcing

- **OI block** — reads the raw OI parquet store (`data/oi_raw/{ticker}/{year}.parquet`),
  per-(strike, expiration, option_type) grain, raw unadjusted strikes and counts.
- **OHLC block** — reads `underlying_ohlc` (yfinance). Close/open/high/low are
  already split-adjusted by the source; verified empirically (e.g. AAPL across
  the 2020-08-31 4:1 split: prior close ~124.81 vs next close ~129.04, a ~1.03
  ratio, not 4×). They are NOT dividend-adjusted (`adj_close` carries that).
  OHLC feature formulas in Families 6, 8, 9, 10, 11 use indices ending at T-1
  rather than T, per the "as of" invariant above.
- **Vol and IV blocks** — read a single consolidated raw chain parquet store
  (`data/chain_eod/{ticker}/{year}.parquet`), per-(strike, expiration,
  option_type) grain. One fetch (the EOD greeks endpoint) supplies both volume
  and implied vol; the volume endpoint was retired after confirming the greeks
  endpoint returns the full chain with matching volume. Columns stored:
  `trade_date`, `source_session`, `feature_date`, `expiration`, `strike`
  (raw), `option_type`, `volume`, `implied_vol`, `delta`, `iv_error`.

### Date semantics for the chain store

The chain parquet carries three dates, because vol/IV data describes session
T-1 but is consumed in the `daily_features` row for session T:

- **`trade_date`** — the actual session the data is from (e.g. 2019-07-01).
- **`source_session`** — equal to `trade_date`; retained for audit symmetry and
  to support a `next_trading_day(trade_date) == feature_date` invariant check.
- **`feature_date`** — `next_trading_day(trade_date)`, computed at write time.
  This is the key `build_features.py` joins on. A chain row from session T-1 has
  `feature_date = T`, so its IV and volume land in the `daily_features` row at
  trade_date T — aligned with the universal "as of start of session T"
  invariant.

### Split adjustment — two universal scalings

Two independent, universal scalings are applied at read time, in the DuckDB
views, to every row — never per-metric. Both are built from the same
`split_factors` machinery (cumulative product across all splits on-or-after a
row's date, so multi-split histories compose correctly).

1. **Strike scaling** — `strike × strike_factor` (e.g. ×¼ for a 4:1 split), so
   that raw chain/OI strikes (which the vendors store unadjusted) are on the
   same basis as the split-adjusted spot from `underlying_ohlc`. Required for
   any strike-vs-spot comparison (moneyness, weighted strikes, IV interpolation).

2. **Count scaling** — `open_interest × count_factor` and `volume ×
   count_factor` (e.g. ×4 for a 4:1 split — the reciprocal of the strike
   factor), expressing historical contract counts in current post-split units
   so count series are continuous across splits.

**Why universal, not per-metric.** Applying the count factor to *every* row,
unconditionally, is algebraically safe: any metric that is a ratio, percentage,
or weighted average has the count in both numerator and denominator, so the
factor cancels exactly and the metric is unchanged. Only raw-count metrics
change — which is the intent. This removes any need to classify metrics into
"adjust" vs "don't adjust" buckets, and removes the risk of misclassifying one.

**The general rule:**

- **Count-unit metrics** (`total_oi`, `call_oi`, `put_oi`, `oi_within_5/10pct`,
  `oi_above/below_spot`, the `d1/d5/d20_total_oi_change` family) are scaled by
  the count factor — their values are expressed in current contract units, so a
  pre-split level reads larger than the raw historical count (e.g. AAPL 2019
  `total_oi` ≈ 13.1M, which is the raw ~3.28M × 4). The raw parquet retains the
  true as-of-date counts underneath.
- **Ratio / percentage / weighted metrics** are unaffected — the count factor
  cancels. (`put_call_oi_ratio`, all `pct_oi_*`, `oi_above_below_ratio`,
  `top5/10_strikes_pct`, all `oi_weighted_*`, `vol_oi_ratio_*`,
  `vol_weighted_*`, etc.)
- **Strike-referenced metrics** use the strike factor regardless of the above.
- A metric can need both — the moneyness-filtered counts use the strike factor
  to decide *which* contracts fall in the band and the count factor to scale
  *how many* the sum represents.

Both OI and volume counts must be scaled together: `vol_oi_ratio_*` =
volume / OI, and if only one side were scaled the ratio would step at splits.
Scaling both keeps it continuous via cancellation. Regression check after any
change to this logic: every non-count metric must be byte-identical before and
after; only the raw-count metrics may move.

### ATM IV interpolation method

`atm_iv_7d/30d/90d` are interpolated from the raw chain (calls only, IV > 0):
for each target DTE, find the two bracketing expirations; within each, linearly
interpolate IV across the two strikes bracketing spot (prior close, `spot_pc`);
then linearly interpolate the two expirations' ATM IVs across DTE. **No boundary
fallback:** if the chain lacks strikes on both sides of spot, the result is
`NULL`, not the nearest-strike IV. (The previous nearest-strike fallback was
what masked the split-vs-strike bug — it returned plausible-but-wrong deep-ITM
IVs. NULL is the honest signal; watch the NULL rate as a data-quality monitor.)
An `iv_error` convergence filter is available in the source data but is applied
conservatively; tune against the observed `iv_error` distribution before
relying on it.

---

## Family 1 — Identity

| Column | Formula / meaning | Tier |
|---|---|---|
| `ticker` | Identifier. | both (key) |
| `trade_date` | T — the NYSE session the row describes and the session forward returns are anchored to. Not the cron run date and not the data-arrival date. | both (key) |

---

## Family 2 — Spot snapshots

| Column | Formula | OHLC | Tier |
|---|---|---|---|
| `spot_pc` | `C_{T-1}` (ASOF join: most recent close strictly before T) | close T-1 | MORNING |
| `spot_co` | `O_T` (equality join on T) | open T | MORNING |

---

## Family 3 — Pure OI aggregates

No spot dependency. All are sums/ratios over the option chain on T.

| Column | Formula | Tier |
|---|---|---|
| `total_oi` | Σ(OI) over all contracts on T | MORNING |
| `call_oi` | Σ(OI WHERE type='C') | MORNING |
| `put_oi` | Σ(OI WHERE type='P') | MORNING |
| `put_call_oi_ratio` | `put_oi / call_oi` | MORNING |
| `max_oi_strike_call` | strike of the call with the largest Σ(OI by strike); ties → lowest strike | MORNING |
| `max_oi_strike_put` | same for puts | MORNING |
| `oi_weighted_call` | `Σ(strike·OI WHERE type='C') / Σ(OI WHERE type='C')` | MORNING |
| `oi_weighted_put` | `Σ(strike·OI WHERE type='P') / Σ(OI WHERE type='P')` | MORNING |
| `oi_weighted_all` | `Σ(strike·OI) / Σ(OI)` | MORNING |
| `oi_weighted_all_0_30d` | `oi_weighted_all` restricted to `0≤DTE≤30` | MORNING |
| `oi_weighted_call_0_30d` | same, calls only | MORNING |
| `oi_weighted_put_0_30d` | same, puts only | MORNING |
| `oi_weighted_all_31_90d` | `oi_weighted_all` restricted to `31≤DTE≤90` | MORNING |
| `oi_weighted_call_31_90d` | same, calls only | MORNING |
| `oi_weighted_put_31_90d` | same, puts only | MORNING |
| `weighted_avg_dte` | `Σ(DTE·OI) / Σ(OI)` — OI-weighted average days to expiration | MORNING |
| `pct_oi_in_front_expiry` | `Σ(OI WHERE expiration = front_exp) / total_oi`, front_exp = `MIN(expiration WHERE DTE≥0)` | MORNING |
| `pct_oi_0_30d` | `Σ(OI WHERE 0≤DTE≤30) / total_oi` | MORNING |
| `pct_oi_31_90d` | `Σ(OI WHERE 31≤DTE≤90) / total_oi` | MORNING |
| `pct_oi_91_365d` | `Σ(OI WHERE 91≤DTE≤365) / total_oi` | MORNING |
| `pct_oi_next_monthly` | `Σ(OI WHERE expiration = next_monthly) / total_oi` | MORNING |
| `top5_strikes_pct_total_oi` | (Σ OI of the 5 strikes with largest per-strike Σ OI) `/ total_oi` | MORNING |
| `top10_strikes_pct_total_oi` | same for the top 10 strikes | MORNING |

---

## Family 4 — OI × spot

Each metric has a `_pc` variant (spot = `C_{T-1}`) and a `_co` variant
(spot = `O_T`). Both are retained. The numerator in every case is a pure-OI
quantity from Family 3; only the spot reference differs.

| Column pair | Formula | Spot / OHLC | Tier |
|---|---|---|---|
| `oi_weighted_call_minus_spot_pc` / `_co` | `oi_weighted_call − spot` | C_{T-1} / O_T | MORNING |
| `oi_weighted_put_minus_spot_pc` / `_co` | `oi_weighted_put − spot` | C_{T-1} / O_T | MORNING |
| `oi_weighted_all_minus_spot_pc` / `_co` | `oi_weighted_all − spot` | C_{T-1} / O_T | MORNING |
| `oi_weighted_call_div_spot_pc` / `_co` | `oi_weighted_call / spot` | C_{T-1} / O_T | MORNING |
| `oi_weighted_put_div_spot_pc` / `_co` | `oi_weighted_put / spot` | C_{T-1} / O_T | MORNING |
| `oi_weighted_all_div_spot_pc` / `_co` | `oi_weighted_all / spot` | C_{T-1} / O_T | MORNING |
| `oi_weighted_all_0_30d_div_spot_pc` / `_co` | `oi_weighted_all_0_30d / spot` | C_{T-1} / O_T | MORNING |
| `oi_weighted_call_0_30d_div_spot_pc` / `_co` | `oi_weighted_call_0_30d / spot` | C_{T-1} / O_T | MORNING |
| `oi_weighted_put_0_30d_div_spot_pc` / `_co` | `oi_weighted_put_0_30d / spot` | C_{T-1} / O_T | MORNING |
| `oi_weighted_all_31_90d_div_spot_pc` / `_co` | `oi_weighted_all_31_90d / spot` | C_{T-1} / O_T | MORNING |
| `oi_weighted_call_31_90d_div_spot_pc` / `_co` | `oi_weighted_call_31_90d / spot` | C_{T-1} / O_T | MORNING |
| `oi_weighted_put_31_90d_div_spot_pc` / `_co` | `oi_weighted_put_31_90d / spot` | C_{T-1} / O_T | MORNING |
| `oi_weighted_next_monthly_div_spot_pc` / `_co` | `[Σ(strike·OI WHERE exp=next_monthly) / Σ(OI WHERE exp=next_monthly)] / spot` | C_{T-1} / O_T | MORNING |
| `oi_within_5pct_pc` / `_co` | `Σ(OI WHERE |moneyness| ≤ 0.05)` | C_{T-1} / O_T | MORNING |
| `oi_within_10pct_pc` / `_co` | `Σ(OI WHERE |moneyness| ≤ 0.10)` | C_{T-1} / O_T | MORNING |
| `oi_above_spot_pc` / `_co` | `Σ(OI WHERE strike > spot)` | C_{T-1} / O_T | MORNING |
| `oi_below_spot_pc` / `_co` | `Σ(OI WHERE strike < spot)` | C_{T-1} / O_T | MORNING |
| `oi_above_below_ratio_pc` / `_co` | `oi_above_spot / oi_below_spot` | C_{T-1} / O_T | MORNING |
| `pct_oi_within_5pct_pc` / `_co` | `oi_within_5pct / total_oi` | C_{T-1} / O_T | MORNING |
| `pct_oi_within_10pct_pc` / `_co` | `oi_within_10pct / total_oi` | C_{T-1} / O_T | MORNING |
| `pct_oi_above_spot_pc` / `_co` | `oi_above_spot / total_oi` | C_{T-1} / O_T | MORNING |
| `pct_oi_below_spot_pc` / `_co` | `oi_below_spot / total_oi` | C_{T-1} / O_T | MORNING |

> **Moneyness NULL-guard.** When a spot is unknown (e.g. `spot_co` for today
> before the OHLC arrives), the moneyness-dependent sums return `NULL` rather
> than `0`, so a missing spot reads as "unknown" instead of "zero OI in band."

---

## Family 5 — OI changes and OI z-scores

| Column | Formula | Tier |
|---|---|---|
| `d1_total_oi_change` | `total_oi_T − total_oi_{T-1}` | MORNING |
| `d5_total_oi_change` | `total_oi_T − total_oi_{T-5}` | MORNING |
| `d20_total_oi_change` | `total_oi_T − total_oi_{T-20}` | MORNING |
| `d1_total_oi_pct_change` | `(total_oi_T − total_oi_{T-1}) / total_oi_{T-1}` | MORNING |
| `d5_total_oi_pct_change` | `(total_oi_T − total_oi_{T-5}) / total_oi_{T-5}` | MORNING |
| `d1_d5_ratio_total_oi_pct_change` | `d1_total_oi_pct_change / d5_total_oi_pct_change` — unstable by construction; see note | MORNING |
| `d1_put_call_oi_ratio_change` | `put_call_oi_ratio_T − put_call_oi_ratio_{T-1}` | MORNING |
| `d5_put_call_oi_ratio_change` | `put_call_oi_ratio_T − put_call_oi_ratio_{T-5}` | MORNING |
| `d1_oi_weighted_all_div_spot_change_pc` / `_co` | `oi_weighted_all_div_spot_T − …_{T-1}` | MORNING |
| `d5_oi_weighted_all_div_spot_change_pc` / `_co` | `oi_weighted_all_div_spot_T − …_{T-5}` | MORNING |
| `zscore_d1_oi_change_3m` | 60-day z-score of `d1_total_oi_change` | MORNING |
| `zscore_d5_oi_change_3m` | 60-day z-score of `d5_total_oi_change` | MORNING |
| `zscore_put_call_oi_ratio_3m` | 60-day z-score of `put_call_oi_ratio` | MORNING |
| `zscore_oi_weighted_all_div_spot_3m_pc` / `_co` | 60-day z-score of `oi_weighted_all_div_spot` | MORNING |
| `zscore_oi_above_below_ratio_3m_pc` / `_co` | 60-day z-score of `oi_above_below_ratio` | MORNING |

> **`d1_d5_ratio_total_oi_pct_change` caution.** A ratio of two pct-changes:
> it explodes when the 5-day denominator is near zero and flips sign when the
> two changes have opposite signs. Summarize with median, not mean; expect fat
> tails. Left unguarded by choice — a `NULLIF` would only convert explosions to
> NULLs without making the metric more usable; the instability is handled
> analytically instead.

---

## Family 6 — Realized volatility (close-derived)

| Column | Formula | OHLC | Tier |
|---|---|---|---|
| `rv_5d` | `σ[log_ret_t, t∈T-4…T-1] · √252` | close T-5 … T-1 | EVENING |
| `rv_20d` | `σ[log_ret_t, t∈T-19…T-1] · √252` | close T-20 … T-1 | EVENING |

> **Observation count.** `rv_5d` is a stdev of **5 log-returns** (4 degrees of
> freedom) — a deliberately short, noisy estimator. `rv_20d` uses 20 returns
> (19 d.o.f.). Each log-return at row t consumes closes `C_{t-1}` and `C_t`, so
> `rv_5d`'s 5 log returns end at log_ret_{T-1} (the last one knowable at the
> start of session T) and reach back through close `C_{T-5}` (6 closes total).

---

## Family 7 — Forward returns (the prediction targets)

Two anchor families, same exit closes. Suffix denotes the **entry anchor**, not
a spot: `_oc` = entered at the open of T; `_cc` = entered at the prior close.

| Column | Formula | Entry / exit | Tier |
|---|---|---|---|
| `ret_1d_fwd_oc` | `C_T / O_T − 1` | O_T → C_T | EVENING |
| `ret_3d_fwd_oc` | `C_{T+2} / O_T − 1` | O_T → C_{T+2} | EVENING |
| `ret_5d_fwd_oc` | `C_{T+4} / O_T − 1` | O_T → C_{T+4} | EVENING |
| `ret_7d_fwd_oc` | `C_{T+6} / O_T − 1` | O_T → C_{T+6} | EVENING |
| `ret_10d_fwd_oc` | `C_{T+9} / O_T − 1` | O_T → C_{T+9} | EVENING |
| `ret_20d_fwd_oc` | `C_{T+19} / O_T − 1` | O_T → C_{T+19} | EVENING |
| `ret_1d_fwd_cc` | `C_T / C_{T-1} − 1` | C_{T-1} → C_T | EVENING |
| `ret_3d_fwd_cc` | `C_{T+2} / C_{T-1} − 1` | C_{T-1} → C_{T+2} | EVENING |
| `ret_5d_fwd_cc` | `C_{T+4} / C_{T-1} − 1` | C_{T-1} → C_{T+4} | EVENING |
| `ret_7d_fwd_cc` | `C_{T+6} / C_{T-1} − 1` | C_{T-1} → C_{T+6} | EVENING |
| `ret_10d_fwd_cc` | `C_{T+9} / C_{T-1} − 1` | C_{T-1} → C_{T+9} | EVENING |
| `ret_20d_fwd_cc` | `C_{T+19} / C_{T-1} − 1` | C_{T-1} → C_{T+19} | EVENING |

> **Trailing NULLs are expected.** The longest-horizon forward returns reference
> closes up to 19 sessions ahead, which do not exist when the row is first
> written. They self-heal: each run recomputes a trailing window of recent rows,
> so a value resolves once the future closes land. The difference between an
> `_oc` and the matching `_cc` value is exactly the overnight gap
> `O_T / C_{T-1} − 1` — diagnostic for signal-correlated gap behavior.

---

## Family 8 — Backward returns (close-to-close, close-derived)

| Column | Formula | OHLC | Tier |
|---|---|---|---|
| `ret_5d` | `C_{T-1} / C_{T-6} − 1` | close T-6, T-1 | EVENING |
| `ret_10d` | `C_{T-1} / C_{T-11} − 1` | close T-11, T-1 | EVENING |
| `ret_20d` | `C_{T-1} / C_{T-21} − 1` | close T-21, T-1 | EVENING |

---

## Family 9 — Moving averages, trend, range position (close-derived)

| Column | Formula | OHLC | Tier |
|---|---|---|---|
| `pct_from_ma20` | `C_{T-1} / [(1/20) Σ_{T-20}^{T-1} C_t] − 1` | close T-20 … T-1 | EVENING |
| `pct_from_ma50` | `C_{T-1} / [(1/50) Σ_{T-50}^{T-1} C_t] − 1` | close T-50 … T-1 | EVENING |
| `pct_from_52w_high` | `C_{T-1} / MAX(C_t, t∈T-252…T-1) − 1` (uses MAX **close**, not high) | close T-252 … T-1 | EVENING |
| `pct_from_52w_low` | `C_{T-1} / MIN(C_t, t∈T-252…T-1) − 1` (uses MIN **close**, not low) | close T-252 … T-1 | EVENING |
| `donchian_pos_20d` | `(C_{T-1} − MIN(L_t, T-20…T-1)) / (MAX(H_t, T-20…T-1) − MIN(L_t, T-20…T-1))` | close T-1, high+low T-20…T-1 | EVENING |
| `ma20_slope_5d` | `[(1/20)Σ_{T-20}^{T-1} C_t] / [(1/20)Σ_{T-25}^{T-6} C_t] − 1` | close T-25 … T-1 | EVENING |
| `pct_up_days_20d` | (count of t∈T-20…T-1 with `log_ret_t > 0`) / 20 | close T-21 … T-1 | EVENING |
| `rv_ratio_5d_20d` | `rv_5d / rv_20d` | close (via rv inputs) | EVENING |
| `cum_signed_vol_20d` | `Σ_{T-20}^{T-1} sign(log_ret_t)·V_t / Σ_{T-20}^{T-1} V_t` | close + volume T-21 … T-1 | EVENING |
| `atr_normalized_ret_5d` | `ret_5d / (atr_14d / C_{T-1})` — see note | close, high, low | EVENING |

Where `atr_14d = (1/14) Σ_{T-14}^{T-1} TR_t` and
`TR_t = MAX(H_t − L_t, |H_t − C_{t-1}|, |L_t − C_{t-1}|)`.

> **`atr_normalized_ret_5d` — units fix.** Originally `ret_5d / atr_14d`, which
> divided a dimensionless return by a dollar-denominated ATR — the result scaled
> with share price and was not cross-ticker comparable. Corrected to normalize
> ATR by the most recent close first (`atr_14d / C_{T-1}`), making the metric
> dimensionless: "5-day return in units of typical daily range."

> **`cum_signed_vol_20d` note.** `SIGN(log_ret)` is `NULL` on a ticker's first
> historical row (no prior close), so the signed-volume numerator can cover one
> fewer day than the volume denominator at the very start of history. This
> affects only the first ~20 warmup rows per ticker, which are discarded in
> analysis — accepted as-is, not treated as a bug.

---

## Family 10 — Cross-asset (close-derived)

| Column | Formula | Tier |
|---|---|---|
| `relative_strength_vs_spy_20d` | `(C_{T-1}/C_{T-21} − 1) − (C^spy_{T-1}/C^spy_{T-21} − 1)`; `= 0` if ticker is SPY | EVENING |

Computed in Python after the DuckDB pass (joins the ticker's `ret_20d` against
SPY's 20-day return). Both legs of the difference use through-T-1 closes, in
line with the universal "as of" invariant.

---

## Family 11 — Underlying-price z-scores (close-derived)

| Column | Formula | OHLC reach | Tier |
|---|---|---|---|
| `zscore_price_vs_ma20` | 60-day z-score of `pct_from_ma20` | close T-79 … T-1 | EVENING |
| `zscore_price_vs_ma50` | 60-day z-score of `pct_from_ma50` | close T-109 … T-1 | EVENING |
| `zscore_underlying_vol_20d` | 60-day z-score of `rv_20d` | close T-80 … T-1 | EVENING |

> The OHLC reach exceeds 60 because the z-score's underlying series is itself a
> moving-window metric. For `zscore_price_vs_ma20`: 60-day z-score window − 1,
> plus the 20-day reach of the oldest `pct_from_ma20` in that window = 79 days
> back from T-1. First valid value at row 80 of a ticker's close series.

> **Z-scores don't carry separate timing.** Each z-score is a function of an
> already-correct underlying series. Once the input (e.g. `pct_from_ma20`) is
> shifted to end at T-1, every value in the 60-day rolling window is itself
> "as of the start of its own row's session," and the z-score at row T is a
> proper relativization of an already-correct value against a window of
> already-correct values. No second-order adjustment is needed in the z-score
> formula itself.

---

## Family 12 — Option volume metrics

Volume figures come from `option_volume_daily` (the EOD volume report covering
the **T-1 session**, stored under T). Pure-volume metrics are EVENING-tier;
those dividing by an OI quantity are MORNING-tier.

| Column | Formula | Tier |
|---|---|---|
| `put_call_ratio_vol` | `total_put_vol / total_call_vol` | EVENING |
| `vol_weighted_call_div_spot_pc` | `vol_weighted_strike_call / spot_pc` | EVENING |
| `vol_weighted_put_div_spot_pc` | `vol_weighted_strike_put / spot_pc` | EVENING |
| `vol_weighted_all_div_spot_pc` | `vol_weighted_strike_all / spot_pc` | EVENING |
| `vol_above_below_ratio_pc` | `vol_above_spot / vol_below_spot` (spot = `C_{T-1}`, computed upstream) | EVENING |
| `pct_vol_within_5pct_pc` | `vol_within_5pct / total_vol` (spot = `C_{T-1}` upstream) | EVENING |
| `pct_vol_within_10pct_pc` | `vol_within_10pct / total_vol` (spot = `C_{T-1}` upstream) | EVENING |
| `pct_vol_0_30d` | `vol_0_30d / total_vol` | EVENING |
| `pct_vol_31_90d` | `vol_31_90d / total_vol` | EVENING |
| `weighted_avg_dte_vol` | `Σ(DTE·volume) / Σ(volume)` — volume-weighted average DTE | EVENING |
| `zscore_put_call_ratio_vol` | 60-day z-score of `put_call_ratio_vol` | EVENING |
| `zscore_vol_above_below_ratio_pc` | 60-day z-score of `vol_above_below_ratio_pc` | EVENING |
| `vol_oi_ratio_all` | `total_vol / total_oi` | MORNING |
| `vol_oi_ratio_call` | `total_call_vol / call_oi` | MORNING |
| `vol_oi_ratio_put` | `total_put_vol / put_oi` | MORNING |
| `net_new_oi_div_vol` | `d1_total_oi_change / total_vol` | MORNING |
| `zscore_vol_oi_ratio_all` | 60-day z-score of `vol_oi_ratio_all` | MORNING |
| `zscore_vol_oi_ratio_call` | 60-day z-score of `vol_oi_ratio_call` | MORNING |
| `zscore_vol_oi_ratio_put` | 60-day z-score of `vol_oi_ratio_put` | MORNING |

> **Suffix correction.** `vol_above_below_ratio`, `pct_vol_within_5pct`,
> `pct_vol_within_10pct`, `zscore_vol_above_below_ratio`,
> `vol_weighted_*_div_spot` all now carry `_pc` and reference `C_{T-1}`. The
> `_pc` suffix is accurate: these are computed against the prior close. (The
> first three were always computed against `C_{T-1}` upstream — only the suffix
> was wrong before. The `vol_weighted_*` group additionally had its denominator
> changed from `spot_co` to `spot_pc`.)

> **MORNING vol/OI metrics — read source.** When the morning cron computes the
> vol/OI cross-metrics it reads `total_vol` / `total_call_vol` / `total_put_vol`
> directly from `option_volume_daily`, never from the (possibly NULL)
> `daily_features` row, so a missed evening run does not null them out.

---

## Family 13 — Implied volatility chain

IV is interpolated from the EOD greeks chain computed from **T-1 closing
prices**, stored under T. The entire family is EVENING-tier and prev-close
consistent. `atm_iv_*` is an interpolated value (bracket two expirations around
the target DTE, interpolate IV across the two strikes bracketing spot, then
interpolate by DTE) — the "formula" below is descriptive; the true definition is
the interpolation algorithm in `fetch_iv_chain.py`, which has boundary-fallback
branches.

| Column | Formula / meaning | Tier |
|---|---|---|
| `atm_iv_7d` | ATM-interpolated IV at 7-day target DTE | EVENING |
| `atm_iv_30d` | ATM-interpolated IV at 30-day target DTE | EVENING |
| `atm_iv_90d` | ATM-interpolated IV at 90-day target DTE | EVENING |
| `iv_25d_call_30d` | **NULL** — not populated (EOD OTM IV too noisy) | EVENING |
| `iv_25d_put_30d` | **NULL** — not populated | EVENING |
| `rr_25d_30d` | `iv_25d_call_30d − iv_25d_put_30d` — **NULL** (inputs NULL) | EVENING |
| `bf_25d_30d` | `0.5·(iv_25d_call_30d + iv_25d_put_30d) − atm_iv_30d` — **NULL** | EVENING |
| `skew_25p_atm_30d` | `iv_25d_put_30d − atm_iv_30d` — **NULL** | EVENING |
| `skew_atm_25c_30d` | `atm_iv_30d − iv_25d_call_30d` — **NULL** | EVENING |
| `term_7d_30d` | `atm_iv_7d − atm_iv_30d` | EVENING |
| `term_30d_90d` | `atm_iv_30d − atm_iv_90d` | EVENING |
| `vrp_30d` | `atm_iv_30d − rv_20d` — volatility risk premium | EVENING |
| `iv_rv_ratio_30d` | `atm_iv_30d / rv_20d` | EVENING |
| `d1_atm_iv_7d_change` | `atm_iv_7d_T − atm_iv_7d_{T-1}` | EVENING |
| `d5_atm_iv_7d_change` | `atm_iv_7d_T − atm_iv_7d_{T-5}` | EVENING |
| `d1_atm_iv_30d_change` | `atm_iv_30d_T − atm_iv_30d_{T-1}` | EVENING |
| `d5_atm_iv_30d_change` | `atm_iv_30d_T − atm_iv_30d_{T-5}` | EVENING |
| `zscore_iv_7d` | 60-day z-score of `atm_iv_7d` | EVENING |
| `zscore_iv_30d` | 60-day z-score of `atm_iv_30d` | EVENING |
| `zscore_iv_90d` | 60-day z-score of `atm_iv_90d` | EVENING |
| `zscore_rr_25d_30d` | 60-day z-score of `rr_25d_30d` — **NULL** (input NULL) | EVENING |
| `zscore_term_7d_30d` | 60-day z-score of `term_7d_30d` | EVENING |
| `zscore_term_30d_90d` | 60-day z-score of `term_30d_90d` | EVENING |
| `zscore_vrp_30d` | 60-day z-score of `vrp_30d` | EVENING |
| `zscore_iv_rv_ratio_30d` | 60-day z-score of `iv_rv_ratio_30d` | EVENING |

> **`vrp_30d` / `iv_rv_ratio_30d` and their z-scores** embed `rv_20d`, which is
> close-derived — so although nominally IV metrics, they carry a close
> dependency. They remain EVENING-tier (consistent with both IV and `rv_20d`
> being EVENING-tier).

---

## Data-source notes

- **`underlying_ohlc.close`** is **split-adjusted but not dividend-adjusted**
  (verified empirically). It is the correct series for price/momentum/vol
  features. No split-adjustment step is needed in the OHLC computations.
- **`underlying_ohlc.adj_close`** is split- *and* dividend-adjusted (total
  return), and is **stale between fetches** — frozen at each ticker's last fetch
  date. It is not used by any feature. Do not use it as a feature input without
  accounting for staleness.
- **OI and chain strikes** from the parquet stores are *not* split-adjusted by
  the vendor; the pipeline applies its own per-row strike factor at read time
  (`make_split_factors`, `bisect_left` boundary). Contract *counts* (OI and
  volume) get a separate, reciprocal count factor at read time. Both are
  universal read-time scalings — see *Data architecture → Split adjustment* for
  the full treatment. This is independent of the OHLC block, which needs no
  split step.

---

## Cron / write architecture

- Two cron jobs write disjoint column sets to the same `(ticker, trade_date)`
  row via `INSERT … ON CONFLICT (ticker, trade_date) DO UPDATE SET <own cols>`.
  No `DELETE`+`INSERT`.
- **EVENING** (~5:30pm, after T-1 close): all EVENING-tier columns.
- **MORNING** (~7am, after OI publish): all MORNING-tier columns.
- Order is not assumed — whichever cron runs first inserts the row; the other
  updates its own columns. A missed evening run leaves EVENING columns NULL but
  does not block the morning run.
- The evening cron re-writes a trailing window of recent `trade_date` rows each
  run so forward-return NULLs self-heal as future closes arrive.
- A NULL can therefore mean "genuinely absent" or "not yet populated";
  consumers must distinguish the two using column-tier metadata.

---

## Change history (effect on stored values)

The pipeline reached its current state through several rounds of changes. Their
cumulative effect on stored values:

- **`atr_normalized_ret_5d` units fix** — now `ret_5d / (atr_14d / C_{T-1})`,
  dimensionless. Any model trained on the old (price-scaled) values needs
  recalibration.
- **`_co` → `_pc` volume renames** — the spot-referenced volume columns are
  anchored to prior close (`spot_pc`), matching the upstream spot reference;
  the stale `_co` versions were removed.
- **Six `ret_*_fwd_cc` columns added** — forward returns entered at prior close
  (`C_{T-1}`), paired with the `_oc` family (entered at `O_T`) to isolate the
  overnight-gap component.
- **`weighted_avg_dte_vol` surfaced** as a column.
- **Vol/OI cross-metrics** (`vol_oi_ratio_*`, `net_new_oi_div_vol`,
  `zscore_vol_oi_ratio_*`) are MORNING-tier (they need T's OI).
- **Raw-chain architecture** — vol and IV moved from pre-aggregated Postgres
  tables to read-time aggregation over the raw `chain_eod` parquet (see Data
  architecture). This fixed two split-vs-strike bugs (a volume strike-weighting
  error and an IV interpolation error that selected wrong-strike contracts on
  pre-split dates).
- **Universal split count adjustment** — raw OI and volume counts are now
  expressed in current contract units, making count-level metrics continuous
  across splits. Ratio/weighted/normalized metrics are unchanged (the factor
  cancels); only raw-count columns moved.
- **OHLC lookahead bias fix** — every OHLC-derived feature was shifted to use
  data through `T-1`'s close (the most recent close available at 9:30am of T),
  rather than through `T`'s close. Affects Families 6, 8, 9, 10, and the
  z-scores in Family 11. The OI block, chain block, and forward returns were
  already correctly aligned and are unchanged. Before this fix, the row at
  trade_date T had OHLC features that included T's intraday and closing data
  (not available at the 9:30am OC entry time), producing a 6.5-hour lookahead
  window relative to the forward returns in the same row. Discovered by a
  hand-audit on AAPL 2019-07-02: `donchian_pos_20d` read 0.9246 (using closes
  through 2019-07-02) versus the lookahead-free 0.9019 (using closes through
  2019-07-01). All OHLC features carried the same bias; OI, vol, and IV were
  unaffected. Any backtest results, Score Matrix scores, or Signal Survey IC
  rankings produced before this fix were biased and should be considered
  invalidated until re-run against the corrected data.

### Migration order

`init_db.py` runs the SQL files as `[01_schema, 03_new_metrics, 02_views,
04_backtest]` — `02_views.sql` deliberately runs *after* `03_new_metrics.sql`
so the recreated `v_features_with_returns` view (`SELECT f.*`) captures the
full, final column set regardless of database state.

---

## Notes on remaining judgment calls

These are settled decisions, recorded for context — not open questions:

1. **`d1_d5_ratio_total_oi_pct_change`** is left without a `NULLIF` denominator
   guard. A guard would only turn explosions into NULLs; the metric's
   instability is handled analytically (median, not mean).
2. **`cum_signed_vol_20d`** warmup-row numerator/denominator day-count mismatch
   is accepted — it affects only the first ~20 rows per ticker, which are
   discarded as warmup.

---

## Known limitations and metric redundancies

Recorded so they are not rediscovered later. None are bugs; all are properties
to keep in mind when using the metrics for analysis.

### Split-boundary contamination of lookback metrics

The universal count adjustment makes raw-count *levels* continuous across
splits. But any metric built on a trailing window still carries a transient
artifact on the specific dates whose window straddles a split:

- `atr_normalized_ret_5d` — `atr_14d` blends ~14 sessions; contaminated for
  ~14 trading days around a split if the true-range terms span it.
- `d1/d5/d20_total_oi_change` and their pct-change forms — the change spans the
  split window for N = 1 / 5 / 20 days respectively. (Count adjustment removes
  the level step; what remains is only any residual window-edge effect.)
- The OI- and IV-change z-scores (`zscore_d1_oi_change_3m`,
  `zscore_iv_*`, etc.) — a contaminated input value sits in the 60-day rolling
  window, so the z-score is affected for up to ~60 trading days after a split.

A future general remedy, if desired, is a split-aware mask that NULLs any
rolling-window metric whose lookback crosses a split date — one mechanism
covering all of the above. Not currently implemented.

### Metric redundancies (for factor modeling, not pipeline bugs)

- **Above/below pairs are near-inverse.** `pct_oi_above_spot` and
  `pct_oi_below_spot` sum to ~1 (minus negligible exactly-at-spot OI), so they
  carry the same factor with a sign flip. Together with `oi_above_below_ratio`,
  there are *three* encodings of one "above/below OI skew" quantity. For factor
  modeling, keep one encoding; feeding all three is multicollinearity with no
  added information. The columns are retained in the table (storage is cheap,
  dropping is irreversible); prune at model-fit time.
- **`_pc` vs `_co` are NOT redundant** despite often coinciding on low-gap days
  (they were identical on the 2019-07-02 verification date). They diverge on
  overnight-gap days — earnings, gaps, shocks — which is precisely the signal
  the pair exists to capture. Keep both.
- **Raw relative-volume features were considered and declined.** A normalized
  volume-spike metric (relative volume / volume z-score) would be highly
  correlated with the existing `vol_oi_ratio_*` and `put_call_ratio_vol` and
  add little orthogonal signal; not worth a feature slot at the current count.

### Verification scope

The pipeline was reconciled by independent recalculation from raw data on
**AAPL 2019-07-02**, with each metric family verified separately. The audit
ran in two passes.

**Pass one — value reconciliation against the pipeline.** Recomputed every
metric from raw data and matched the pipeline's stored values. 152/152 metrics
agreed. This pass confirmed the formulas were implemented as documented but
did NOT independently audit whether the documented formulas themselves had the
right timing alignment.

**Pass two — timing-alignment audit (added later).** Manually walked through
"what data should be available at 9:30am of T?" for each metric block and
compared to what the pipeline was actually using. Findings:

- OI block: timing-correct. OI labeled trade_date T is the prior session's EOD
  position published at ~7am of T, properly available before the 9:30am OC
  entry. Verified by inspecting the ThetaData publication timestamp.
- Chain block (vol, IV): timing-correct via the `feature_date` offset. Verified
  by recomputing `put_call_ratio_vol` and `atm_iv_30d` from chain rows where
  `trade_date = 2019-07-01` (the source session feeding feature_date 2019-07-02)
  and matching the dashboard values.
- Forward returns: timing-correct. `ret_1d_fwd_oc` at trade_date 2019-07-02 was
  confirmed to be the 2019-07-02 open-to-close return (entry at T's open).
- OHLC block: **lookahead bias found and fixed** — see Change History. The
  pre-fix `donchian_pos_20d` for AAPL 2019-07-02 was 0.9246 (using closes
  through 2019-07-02); the correct lookahead-free value is 0.9019 (using closes
  through 2019-07-01). Every OHLC feature was uniformly affected; the fix
  shifted all OHLC indices back by one session.

Caveats on the audit's broader applicability:

- The IV-block values were checked against an independent re-implementation of
  the *same* interpolation method (calls-only, linear, prior-close spot). This
  confirms the method is implemented consistently but does not independently
  validate the method choice itself against a third-party IV source.
- 2019-07-02 was a low-overnight-gap day, so the `_pc`/`_co` distinction was not
  exercised by that single date.
- AAPL has a single split in range, so the multi-split cumulative-factor path
  and a non-split "negative control" ticker were reasoned through but not
  separately data-verified.

### Corporate actions beyond splits

The split adjustment handles forward and reverse stock splits recorded in
`underlying_ohlc.splits`. It does **not** capture non-split OCC contract
adjustments (special dividends triggering strike/multiplier changes, mergers,
spinoffs). For a universe of liquid large-caps, splits dominate and this covers
nearly all cases, but a ticker with an unusual corporate action could carry an
unadjusted discontinuity. The split-detection process (re-fetch / rebuild on a
new split) is keyed on the `splits` column only.

### IV data quality

EOD implied vol is an after-hours snapshot with wide spreads; it is accepted as
adequate for ATM interpolation but is unreliable in the OTM wings (the
`delta` values for far-OTM contracts are unreliable, which is why the
25-delta skew metrics — `iv_25d_*`, `rr_25d_30d`, `bf_25d_30d`,
`skew_*` — remain NULL by design pending a future intraday/15:45-endpoint
migration). 2020 is a known vendor bad-batch period for the IV feed.
