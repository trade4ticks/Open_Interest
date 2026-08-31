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

| script | question | status |
|---|---|---|
| `s0_availability.py` | Is `trade_quote` on the Standard plan? **GATE** | ✅ available |
| `s1_venue_check.py` | Which endpoints actually need `venue=utp_cta`? | ✅ settled |
| `s6_session_bounds.py` | Where are the missing 19% of shares? | ✅ resolved |
| `s2_one_day.py` | Row count, real schema, wall clock, parquet size | ✅ |
| `s3_multiday_timing.py` | Does a 544-symbol backfill need concurrency? | ✅ settled |
| `s4_conditions.py` | Which trade condition codes to exclude | ✅ list set |
| `s5_quote_emission.py` | Change-only emission, or periodic samples? | ⬅ **re-run** |

Timing and storage are settled: **5.2 min** initial backfill at concurrency 4,
**39 s** nightly, **3.1 GB**. No concurrency work needed beyond the existing
4-connection cap.

### Schema, as actually returned

Confirmed from s6 on `history/trade_quote`. Trade-side fields carry a `trade_`
prefix only on the timestamp:

```
trade_timestamp  size  price  exchange  condition  ext_condition1..4
bid  ask  bid_size  ask_size  bid_condition  ask_condition
```

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

### The venue question is settled

`venue=utp_cta` is **not** hardcoded anywhere. `config.VENUE_BY_ENDPOINT` is a
per-endpoint table, and s1 has resolved every entry:

- `snapshot/ohlc` → `utp_cta`. **Required.** The default `nqb` returned 44% of
  true volume and omitted ~10,000 symbols.
- `history/trade_quote`, `history/quote` → **send nothing.** s1 found the with-
  and without-venue responses byte-identical, with 20 distinct exchange codes
  in the tape. The endpoint accepts the parameter and ignores it; it already
  reads the consolidated tape. This matches Theta's documented feed
  arrangement — 15-minute delayed from all three SIP networks alongside
  real-time Nasdaq Basic.

The original belief that `utp_cta` was needed everywhere was a generalisation
from the snapshot result, made without evidence, and it was wrong for the
history endpoints. **Do not add the parameter back "to be safe."** Sending
nothing is the measured-correct behaviour, not an omission.

`VENUE_POLICY_VERIFIED = True` now. It gates bulk pulls, so a multi-hour
backfill cannot run against an unverified assumption.

### The 19% volume gap: resolved

```
RTH prints (default window, 09:30–16:00)      776,192
closing cross, counted ONCE                 + 186,344
                                            ---------
                                              962,536
EOD consolidated reference                    956,900   (+0.59%)
```

There was never any missing volume. Two things at once:

1. **The default query window is regular hours.** s6 found the no-parameter
   pull byte-identical to an explicit 09:30–16:00 pull. The closing cross
   prints at 16:00:03 — three seconds outside — so it was excluded by
   construction. That is the entire 19%.
2. **Widening the window does not simply add it back.** A 04:00–20:00 pull
   overshoots to 180% of EOD, because the official close is *re-reported on a
   schedule for hours afterwards*. The same 186,344 shares at $330.88 on NYSE
   appear five times: once at 16:00:03.158 as condition 98 (`CLOSING`), then
   four more as condition 51 (`MC_OFFICIAL_CLOSE`) at 16:00:03.212, 16:10:00,
   18:30:00 and 19:00:00. The opening auction does the same — 20,056 shares
   once as 62 (`OPEN_REPORT`), once as 66 (`MC_OFFICIAL_OPEN`).

**The RTH default window excludes the closing cross by construction, and that
is correct for these metrics.** An auction print carries no meaningful quote —
the FDX opening quote was bid 330.45 / ask 336.15, a $5.70 spread — so
including auction prints would corrupt spread and noise rather than improve
coverage. We pull RTH only; trade counts and spreads were never contaminated.
The numbers are now understood rather than merely close.

### Auction prints vs restatements

Two sets that look alike and behave completely differently:

- `config.AUCTION_PRINT_CONDITION_CODES` = `{7, 26, 45, 62, 98}` — genuine
  executions. Real shares, counted **once**. This is the set to sum if auction
  volume ever becomes a feature.
- `config.RESTATEMENT_CONDITION_CODES` = `{51, 66}` — **not executions.** They
  re-report an auction that already printed under 62 or 98. Summing them adds
  volume that never traded, and is what makes a wide-window pull read 180%.

`config.EXCLUDED_CONDITION_CODES` is currently the restatements only. Odd lots
(115) and derivatively-priced prints (96) are deliberately *not* excluded yet —
`s4_conditions.py` measures how far off the quote each code actually sits, and
that measurement decides, not the code's name.

Also: `0` and `255` are padding in `ext_condition1..4`, not codes
(`config.NO_CONDITION_SENTINELS`). Left in, they dominate every census with a
meaningless top entry.

### The restatement guard

`scalp/quality.py` is the backstop underneath the condition codes, for a
restatement arriving under a code the table doesn't carry. It flags repeated
large prints sharing `(size, price, exchange)` more than a second apart.

The size floor (`RESTATEMENT_MIN_SHARES`, default 5,000) is load-bearing: that
signature also describes perfectly ordinary trading, since a 100-share print at
one price on one venue recurs dozens of times a day in any liquid name. Without
the floor the guard would fire across a large fraction of a normal tape.

Neither layer is sufficient alone, which the self-test demonstrates: the
opening restatement (code 66) shares a timestamp with the genuine print so the
time-based guard cannot see it, while an uncoded duplicated block is invisible
to the codes. **The guard flags; it never drops.** Exclusion is the metric
layer's decision, where it is visible.

```
python -m scalp.quality --selftest      # synthetic tape, no network
```

### Exchange and condition lookups

`config.EXCHANGE_NAMES` is the vendor's full published enum, transcribed
verbatim. Code 57 is FINRA/NASDAQ TRF and carried half of FDX's tape.
`config.OFF_EXCHANGE_CODES` is `{2, 57, 58, 59}` — the ADF plus the three
reporting facilities — which makes `off_exchange_share` free from the
`exchange` column with no second data source.

`config.TRADE_CONDITIONS` is **partial by design**: only codes read verbatim
from the vendor's table. The full enum runs past 148 and has not been
transcribed. `condition_name()` returns `unlabelled(<code>)` for anything
absent, and `s4_conditions.py` reports every observed code. An invented label
on a code that drives an exclusion decision is worse than no label.

## Storage goes to block 3, not the VPS root

The brief said `/data/equities_scalp/`. That was wrong — that is the main VPS
disk. Parquet goes to **volume storage block 3**, the mount already holding
`equity_1min` and the other equity parquet stores.

`SCALP_DATA_DIR` has **no default**. It must be set in `.env` beside the other
`*_DIR` entries, and `config.data_dir()` validates it before anything is
written:

- unset → raises with the export line to run
- parent missing → raises, since that usually means the volume isn't mounted
- **on the same filesystem as `/`** → raises

That last check is the one that matters. A path can look like a mount and not
be one: an unmounted `/mnt/block3` is an ordinary empty directory on the root
filesystem, and writing 3.1 GB there fills the VPS exactly as `/data` would.
Comparing device ids catches it; comparing path strings does not.

Step 0 scratch (`STEP0_DIR`) is exempt and stays where it is — small and
disposable.

## Emission mode: gap structure outranks repeat rate

The first s5 run concluded "periodic sampling" from an 80.1% identical-record
rate. Its own gap distribution said otherwise — p50 1 ms, p90 448 ms, max
21,523 ms, mean 175 ms. A sampler produces near-constant gaps; four orders of
magnitude between median and maximum is not a clock.

The repeat rate was inflated because the comparison omitted `bid_exchange` and
`ask_exchange`. NBBO records fire when *any* participant updates, so a
different venue taking over at the same price and size is a genuinely new
record in which every compared field is unchanged. s5 now compares
progressively — price, then size, then venue — and the drop at the venue step
is the measurement.

This decides whether the flicker metric survives. **Event-driven means
quote-records-per-minute IS book activity** and the count is used directly;
rebuilding it on "observed changes" would discard exactly the venue-turnover
events that make a book re-form. The verdict now keys on gap structure, and
says so when the exchange columns are absent rather than reporting an upper
bound as a measurement.

## Bid-side and ask-side noise are first-class

s5 measured this on FDX over 10,278 consecutive quote records:

| | count |
|---|---|
| bid moved, ask still | 602 |
| ask moved, bid still | 664 |
| **both moved** | **1** |

One in 10,278. Two-sided repricing essentially never happens — the midpoint
moves because one side twitched. A mid-based noise number is therefore an
average over a quantity that is one-sided almost every time it changes, and
the asymmetry is the phenomenon rather than a detail. `config.NOISE_VARIANTS`
carries `bid_side` and `ask_side` as outputs in their own right, not
diagnostics.

## Condition exclusions, set from measurement

The criterion is where a code's prints actually sit relative to the quote,
never the code's name.

**Excluded** (`config.OFF_QUOTE_CONDITION_CODES`) — systematically off-quote
across all four census symbols:

| code | | evidence |
|---|---|---|
| 96 | `DERIVATIVE` | 45–70% outside NBBO, 4.3–9.4 bps off mid |
| 4 | unlabelled | 46–62% outside NBBO |
| 124 | unlabelled | 40–100% outside NBBO |

4 and 124 carry no vendor label and are excluded purely on measurement, which
is the right basis.

**Kept:**

- **115 `ODD_LOT`** — 75–86% of all trades, only 1.2–2.8 bps off mid, 6–11%
  outside NBBO. Clean executions near the mid. Excluding odd lots is a common
  reflex and here it would delete most of the tape.
- **95 unlabelled** — borderline (17–25% outside NBBO) but carries 15–18% of
  volume and sits close to the mid. Kept, and listed in
  `config.REVISIT_AFTER_CALIBRATION`.

Exchange **78** is absent from the vendor enum but appears in all four symbols
at trivial volume — a real venue the docs haven't caught up with, not corrupt
data. It is labelled explicitly and tracked in
`config.UNIDENTIFIED_EXCHANGE_CODES` so off-exchange share can report it rather
than absorbing it into the on-exchange bucket by default.

## `off_mid_bps` is a ranking candidate

Median absolute distance of a trade from the prevailing midpoint, in bps, over
ordinary prints. Free from every trade — no bucketing, no time-weighting, no
horizon choice.

| | off_mid_bps | realised $/min |
|---|---|---|
| FDX | 1.21 | 3.13 |
| LLY | 1.45 | 4.02 |
| DLTR | 1.17 | −1.32 |
| LITE | 2.78 | 0.51 |

LITE is more than double the rest and was the worst tradeable name. But FDX and
DLTR sit 0.04 bps apart at opposite ends of the realised results, so this
separates the worst name and not the best from the worst. It goes into
calibration beside the four noise variants as a candidate, not a conclusion.

## Decided, not yet built

Recorded here so it isn't lost between now and the metric layer.

- **Exclude auction-period quotes from all spread and noise metrics.** The
  opening quote on FDX was bid 330.45 / ask 336.15 — a $5.70 spread that is an
  auction artifact and would wreck a daily average. Exclude the first and last
  minute of the session; the exclusion window is a config value, not a
  constant. The calibration output reports every metric **both with and without
  the exclusion** so the sensitivity is visible rather than assumed.
- **`off_exchange_share` joins the flow metrics**, computed via
  `config.is_off_exchange`. The lookup exists; the metric lands with the metric
  code.

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
