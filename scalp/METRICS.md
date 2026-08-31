# Metric definitions — for review before the backfill

Every metric `compute.py` writes, what it means, and what it is computed from.
Read this before the backfill runs; changing a definition afterwards means a
recompute, and changing the *source* means a re-pull.

## ⚠️ The central hypothesis is unvalidated

**`spread ÷ noise` as the ranking metric is a hypothesis, not a result.**

It comes from a single observation made while trading: that stocks moving
faster than their spread are untradeable. That observation may well be right.
It has never been tested against anything.

The entire pipeline is organised around it. Every noise variant exists to find
the best denominator for that ratio; the horizons, the bucketing, the
time-weighting all serve it. If the premise is wrong, the pipeline is a very
careful measurement of the wrong quantity.

Calibration against the 15 traded tickers is the first and only test it has
been put to. **If no variant separates the top of the realised results from
the bottom, that is the finding** — and it gets reported as one rather than
resolved by picking whichever ratio looks tidiest.

---

Metrics come from two entry points, deliberately not merged:

| function | source | drives |
|---|---|---|
| `metrics.compute_window` | `history/trade_quote` | spread, noise, flow |
| `metrics.compute_quote_window` | `history/quote` at tick | flicker, BBO lifetime |

Within each, the daily row, each 15-minute row and the future intraday re-rank
all call the same function with different bounds. There is no separate daily
path.

## The source, and what it cannot support

Input is `/v3/stock/history/trade_quote`: every trade paired with the
prevailing NBBO **at that trade**.

So the quote series is **sampled at trade times**, not the full quote stream.
That is fine for spread and midpoint levels — every observation is a real
NBBO. It is a genuine limitation for anything counting quote *events*, because
a quote that appeared and vanished between two trades is invisible.

**BBO lifetime now comes from the quote stream, which is the correct source.**
`bid_lifetime_ms_median` / `ask_lifetime_ms_median` measure how long a price
actually stood before being replaced, computed from tick quotes. The earlier
`bid_persist_ms_median_tradesampled` has been **removed**, not kept alongside:
a knowingly-worse metric sitting next to available correct data only invites
someone to use it. (The final run in each window is left-censored — it was
still standing when observation stopped — so it is excluded rather than
counted as having ended.)

**One metric still returns `NaN`:**

| metric | why |
|---|---|
| `bbo_change_without_trade_share` | Cancel vs consumption. Needs the quote stream *and* the trade tape joined — the quote side to see the BBO change, the trade side to say whether a trade met it at that price. Not implemented. |

✅ **Settled by `s7_quote_sizing.py`: tick resolution, full retention.**

Those two metrics need a second pull from `history/quote`, and s7 measured
both the size and the cost of coarsening it.

**Why tick, not 1s.** Sampling at 1s doesn't merely coarsen the flicker
metrics, it corrupts them:

| | tick | 1s | retained |
|---|---|---|---|
| `nbbo_changes_per_min` | 22.26 | 8.98 | 40% |
| `two_sided_change_share` | 0.06 | 0.18 | **inflated 3×** |

The second row is the disqualifying one. A second is long enough to contain a
bid move and an ask move that happened separately, and collapsing them into
one record turns two one-sided events into a single two-sided one. That
directly attacks the **1,266:1** one-sided ratio s5 measured — the entire
justification for computing bid-side and ask-side noise separately. 1s would
manufacture two-sided repricing that never happened.

**Why retain, not compute-and-discard.** 2.92 GB projected against 21.50 GB
free. It fits with room, so the raw ticks stay: keeping them costs nothing and
re-pulling costs a run.

Measured full-day zstd parquet per symbol-day (recorded in config so it is
never re-derived from a partial-session sample): FDX 0.56 MB, LLY 1.00,
LITE 2.14, DLTR 0.70 — mean 1.10 MB × 544 × 5 days = 2.92 GB.

**Not an option: a smaller symbol list.** Flicker is an *input* to the ranking,
not a refinement applied afterwards. Measuring it only on names a
flicker-blind ranking already selected cannot tell us what the filter should
have been — the names it would have promoted or demoted are exactly the ones
never measured. It runs on the full universe or it doesn't earn its place.

### Lookbacks differ by source

| source | window | why |
|---|---|---|
| `trade_quote` | `TRADE_QUOTE_LOOKBACK_DAYS` = 10 | spread, noise and flow move with the day's conditions |
| `history/quote` | `QUOTE_LOOKBACK_DAYS` = 5 | flicker is a book-structure property and should be more stable day to day |

## Windows and exclusions

- **RTH** 09:30–16:00 ET. The endpoint's default window is exactly this, but
  the bounds are passed explicitly so nothing depends on that default.
- **Auction edges** — the first and last `EXCLUDE_OPEN_MINUTES` /
  `EXCLUDE_CLOSE_MINUTES` (default 1 each) are trimmed **for spread and noise
  only**. FDX opened bid 330.45 / ask 336.15, a $5.70 spread that is an auction
  artifact and would distort a daily average. Trade counts and volume keep the
  full window, because an arrival is an arrival.
- **15-minute buckets do not trim.** The first and last buckets *are* the
  auction-adjacent periods, and trimming them would hide exactly what those
  rows exist to show.
- **Excluded prints** (`config.EXCLUDED_CONDITION_CODES` = `{4, 51, 66, 96,
  124}`) are dropped before any counting. Restatements re-report shares
  already counted; off-quote codes print systematically away from the NBBO.
  Odd lots (115) are **kept** — 75–86% of all trades at 1.2–2.8 bps off mid.

## Same-timestamp collapsing

49.8% of records share an instant with another. Before any duration weighting,
records at one instant collapse to the **last** — the state that stood when the
clock moved on — and each surviving observation's weight is the gap **forward**
to the next *distinct* instant.

Order and direction both matter. A backward difference gives the surviving
record at a shared instant a weight of zero, so half the observations vanish
from every time-weighted number with no error and no change in row count.
`tests/test_timestamp_collapse.py` asserts the failure mode, not just the fix.

## Spread

| metric | definition |
|---|---|
| `spread_cents_mean` / `_median` | `(ask − bid)` × 100 |
| `spread_bps_mean` / `_median` | `(ask − bid) / mid` × 10,000 |
| `spread_cents_tw` / `spread_bps_tw` | duration-weighted by how long each quote stood |
| `crossed_locked_share` | fraction with `ask ≤ bid` |
| `quote_observations` | usable quotes in the window |

Crossed and locked quotes are dropped from the spread statistics: a negative
spread is not a capturable one. The share is reported so it can't hide.

`spread_bps_tw` is the numerator of every ranking ratio.

## Noise — five variants × three horizons

Median absolute change between consecutive **fixed-clock** buckets, in bps of
the buckets' own level. Fixed clock, never trade-to-trade — otherwise busy
stocks look artificially calm. Median rather than mean so one auction-sized
jump doesn't define the number.

Horizons: **5s, 10s, 30s**. Naming: `noise_bps_<variant>_<h>s`.

| variant | bucket value | known flaw |
|---|---|---|
| `tw_mid` | duration-weighted midpoint | current best guess; flicker averages out |
| `last_mid` | last midpoint in bucket | a 40-share bid appearing and vanishing 10c away moves it with no economic content |
| `trade_price` | last trade price in bucket | contains the spread itself, so spread ÷ noise partly divides spread by spread |
| `bid_side` | duration-weighted bid | — |
| `ask_side` | duration-weighted ask | — |

**Bid-side and ask-side are first-class outputs, not diagnostics.** s5 measured
602 bid-only moves, 664 ask-only, and exactly **one** two-sided move across
10,278 records. Two-sided repricing essentially never happens, so a mid-based
noise number averages over a quantity that is one-sided almost every time it
changes — and an unstable bid against a still offer is the strategy's actual
case.

*Approximation, stated:* a duration straddling a bucket boundary is attributed
whole to the bucket it started in. With ~1 ms median gaps against 5–30 s
buckets this is a rounding effect.

## Flicker — both variants, because they may be different quantities

| metric | definition |
|---|---|
| `quote_records_per_min` | raw record count ÷ minutes |
| `nbbo_changes_per_min` | records where bid **or** ask price actually moved |
| `bid_changes_per_min` / `ask_changes_per_min` | one side moved |
| `two_sided_change_share` | both moved ÷ any moved |
| `same_instant_share` | fraction lost to timestamp collapsing |

s5 found 78.4% of records identical on price, size *and* venue — venue
turnover explained only 1.7 points of the 80.1% raw figure. The likely cause
is that the NBBO is recomputed on every participant's update rather than only
when the best changes, which this endpoint's columns cannot confirm.

If so, `quote_records_per_min` measures **total quote traffic across all
venues** and `nbbo_changes_per_min` measures **inside-market instability**.
Those may rank tickers very differently. Both are computed; calibration
decides. The sign is unknown either way — high flicker might be bad
(can't hold queue position) or good (a book that keeps re-forming is one to
keep inserting into).

⚠️ `quote_records_per_min` is **genuine at tick and meaningless at 1s**, where
it is at most one record per second by construction and measures the sampler
rather than the book. The pipeline runs at tick so it is kept;
`config.flicker_variants(interval)` drops it automatically if the interval
ever changes, so it cannot survive into the ranking as a constant.

### `quotes_per_trade` — a ranking candidate

Quote records ÷ trades, per symbol-day. Both inputs are already in the pull,
so it costs nothing.

| | quotes_per_trade | realised $/round trip |
|---|---|---|
| LLY | 2.66 | 4.23 |
| FDX | 3.57 | 4.78 |
| DLTR | 4.07 | negative |
| LITE | 4.81 | 0.99 |

Monotonic apart from the FDX/LLY swap at the top. The mechanism is plausible:
high churn per execution is a book moving without trading — the "I sat there
re-pricing and nothing filled" case.

⚠️ **Units differ.** Those are dollars per *round trip*, not the $/minute
figures used everywhere else in calibration. The two orderings are not the
same and must not be compared across.

n = 4, so it enters calibration as a candidate on the same footing as the
noise variants. Computed by `metrics.cross_source_metrics`, which returns
nothing at all when the quote pull is missing rather than substituting a
value.

## Flow

| metric | definition |
|---|---|
| `trades_per_min`, `shares_per_min` | arrival rates |
| `trade_size_mean`, `trade_size_median` | — |
| `odd_lot_share` | size < the **price-tiered** round lot |
| `sub_100_share` | size < 100, fixed — kept for comparability only |
| `round_lot_size`, `reference_price` | the tier in force and the price that picked it |
| `odd_lot_flag_disagree_share` | where the tiered calculation and vendor condition 115 disagree |
| `at_bid_share` / `at_ask_share` / `between_share` | `price ≤ bid` / `price ≥ ask` / else |
| `two_sided_balance` | min(at_bid, at_ask) ÷ max(…) — 1.0 balanced, 0.0 one-way |
| `off_exchange_share` | TRF/ADF codes `{2, 57, 58, 59}` |
| `unidentified_exchange_share` | code 78, absent from the vendor enum |
| `off_mid_bps` | median `\|price − mid\| / mid` × 10,000 |

`two_sided_balance` is the point of the classification — this strategy needs
buyers *and* sellers arriving, not one-way flow.

### Round lots are price-tiered, and it is not cosmetic

Since November 2025 the round lot is not 100 shares:

| price | round lot |
|---|---|
| < $250 | 100 |
| $250 – $1,000 | **40** |
| $1,000 – $10,000 | **10** |
| ≥ $10,000 | 1 |

FDX at ~$330 has a 40-share round lot — and the s2 output shows it quoting
**40 × 40 at the inside, exactly one round lot**. LLY at ~$1,172 has a round
lot of 10.

The round-lot constraint is a large part of why spreads stay structurally wide
in these names, so a fixed size-100 boundary misclassifies the tape in
precisely the names this strategy targets. `config.round_lot_size(price)`
applies the tiers.

One **reference price per symbol-day** (median trade price), not per trade:
the exchanges assign the tier periodically from a prior reference price, so a
name trading either side of $250 intraday keeps one lot size all day. Applying
tiers per-print would flip the boundary mid-session and misclassify both sides
of it.

**Free correctness check:** the vendor flags odd lots independently with
condition 115. `odd_lot_flag_disagree_share` reports where that flag and the
tiered calculation disagree — either the tier here is wrong or the vendor
flags on a different basis, and both are worth seeing rather than assuming.

### `off_mid_bps` is a ranking candidate, not a diagnostic

| | off_mid_bps | realised $/min |
|---|---|---|
| FDX | 1.21 | 3.13 |
| LLY | 1.45 | 4.02 |
| DLTR | 1.17 | −1.32 |
| LITE | 2.78 | 0.51 |

LITE is more than double the rest and was the worst tradeable name. But FDX
and DLTR sit 0.04 bps apart at opposite ends of the realised results — so it
separates the worst name, and not the best from the worst. It goes into
calibration beside the noise variants.

## Ranking ratios

`ratio_<variant>_<h>s` = `spread_bps_tw ÷ noise_bps_<variant>_<h>s`, computed
for **every** variant and horizon so they can be compared. None is privileged
until calibration says which separates the top of the realised results from
the bottom.

**If none of them separate, that is the finding and it gets reported as one.**
A tidy ranking that doesn't track reality is worse than no ranking.

### Filters are read-time only. The pipeline does not filter.

`compute.py` writes a metrics row for **every** symbol it can compute, whether
or not it passes anything. Nothing is dropped during compute.

`config.DEFAULT_FILTERS` is read only by `rank.py` and the dashboard, and
nothing in the metric layer imports it. Two consequences, both wanted:

- Changing a threshold is a **page refresh, not a recompute**.
- The rows for names that **failed** are kept — and they are the only data
  that can say whether a threshold was set correctly. A pipeline that filters
  can never answer "what did the excluded ones look like?"

Same reasoning as retaining non-qualifying names at the universe stage.

| filter | default | slider range | guards |
|---|---|---|---|
| `min_spread_cents` | 5 | 0 – 25 | Below ~5¢ there's nothing to capture |
| `min_trades_per_min` | 10 | 0 – 100 | Not enough arrivals to get filled |
| `max_noise_bps` | **4** | 0.5 – 15 | Moves further in 10s than the spread is wide |

`max_noise_bps` was 10, which would not have bound on anything — the realised
names measure FDX 1.8, LLY 0.85, DLTR 2.7, LITE 5.2 bps. 4 flags LITE and
nothing else. It is a **slider, not a constant**, and the default is only
where the slider starts.

Dollar volume is a **floor only and appears nowhere in the ranking** — across
the 15 traded tickers it has no relationship to outcome.

## Provenance, per symbol-day

Several decisions silently change the numbers — condition-code exclusions,
auction-edge trimming, crossed and locked quotes, same-timestamp collapsing.
Reading this document should not be the only way to find out that a row was
computed from 94% of the tape.

`compute.py` writes a **provenance row alongside each metrics row**, into its
own `provenance` table, surfaced in the dashboard:

| item | meaning |
|---|---|
| `dropped_condition_<code>` | trades dropped, broken out per excluded code |
| `dropped_condition_any` | union — a print carrying two excluded codes counts once |
| `dropped_condition_code_sum` | sum of the per-code counts; exceeds the union when codes overlap |
| `trades_retained`, `trade_retained_share` | what survived, and as a share of raw |
| `quotes_crossed_locked`, `quote_retained_share` | quotes dropped as crossed or locked |
| `quote_records_before_collapse` / `_after_collapse` / `records_lost_to_collapse` | same-instant collapsing |
| `auction_minutes_trimmed` | minutes removed from the window edges |

The union and the sum are both stored because they answer different questions:
the union is how much tape was lost, the sum is how much each rule
contributed, and they differ exactly when a print carries two excluded codes.

## Every metric links to its definition

`metric_docs.py` maps each metric name to a section anchor and a one-line
definition, so the dashboard makes every column header a link. Looking at
`noise_bps_tw_mid_10s` is one click from learning it is a median of
duration-weighted midpoints diffed across fixed 10-second buckets, with a
bucket-straddle approximation.

The fifteen generated `noise_bps_*` and `ratio_*` columns resolve by pattern
rather than needing an entry each. `metric_docs.undocumented(names)` returns
metrics with no definition — an unlinked column in the dashboard is the signal
that a metric was added without being documented.

## Storage

Long format: `daily_metrics(trade_date, symbol, metric, value)`. The metric set
is explicitly unsettled — five noise variants at three horizons, two flicker
variants, and a calibration whose purpose is to delete the ones that don't
separate. A wide table would need a migration every time one is added or
dropped. Cost is a pivot on read (`db.metrics_wide`).
