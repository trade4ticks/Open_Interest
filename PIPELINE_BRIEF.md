# Equities Scalp — Ticker Ranking Pipeline

## Purpose

I trade a passive scalping strategy: post a limit order 1–2 cents inside the best bid, wait
for a fill, exit near the offer. Median hold ~8 seconds, 200–400 round trips a day, one
position at a time. Two live sessions so far: 616 round trips, $1,966 net.

**My constraint is finding tickers.** Performance varies 10x by name and I currently pick
them by eye from a ToS watchlist exported to Excel. FDX gave me $3.13/min of attention;
LII gave $0.33/min and consumed 96 minutes of the session.

Build a pipeline that ranks a candidate universe so I know what to trade tomorrow.

## How I work — read this first

**You write scripts. You do not run them.** Do not connect to the VPS, do not call the
ThetaData API, do not fetch data, do not touch the database. I run everything myself and I
want control over when data gets pulled. Deliver scripts with clear arguments and I will
execute them and report back what happened.

This applies to verification too. Where something needs checking against live data, write
the script that checks it and tell me what output would indicate a problem. Don't try to
check it yourself.

## Deployment target

- VPS at **100.76.94.99**. ThetaData Terminal is already running there.
- This is the **same project** as my existing ThetaData fetches — connection handling,
  auth, and terminal access already exist in the codebase. Reuse them; don't reinvent.
- Postgres runs in Docker on the same box, also already configured.

## Phase 1 is nightly batch. Phase 2 will be intraday. Design accordingly.

The first build runs after the close and produces a ranking I read the next morning.

**But I will want an intraday version.** Not a streaming architecture, but a re-rank I can
run during the session on recent data. Structure the metric computation so it takes an
arbitrary time window rather than assuming a full day — same functions, different bounds.
Don't build the intraday path now; just don't wall it off.

## What makes a ticker tradeable for me

Three conditions, all necessary:

1. **Spread wide enough to capture.** Below ~5 cents there's nothing to work with.
2. **Low short-horizon noise relative to price.** If the stock moves further in 10 seconds
   than the spread is wide, I get run over chasing my own exit. Must be in basis points —
   a 10¢ move on a $1,100 stock is calm, the same move on a $130 stock is not.
3. **Enough trade arrivals to actually get filled.**

Ranking is roughly **spread_bps ÷ noise_bps**, subject to floors on all three. Each metric
alone has an obvious failure case, so no single one drives the ranking.

Scoping note: earlier analysis of my fills found tape features predict *whether I get
filled*, not *whether the fill is profitable*. Nothing predicted profit per trade. This is
an **opportunity ranker**, not a profit predictor.

## Data source — ThetaData

Standard stocks subscription, v3 API.

### CRITICAL: always pass `venue=utp_cta`

Default venue is `nqb` (Nasdaq Basic) — Nasdaq exchange plus Nasdaq TRF only. We verified
it returns **44% of true volume** and omits ~10,000 symbols entirely. With `utp_cta`
(merged UTP & CTA SIP tapes) FDX returned 956,900 shares, matching its EOD figure exactly.

Must be explicit on **every request**. A silently Nasdaq-only spread measurement would look
completely plausible and be wrong, with no EOD figure available to catch it.

### Endpoint behaviour we've confirmed

- `/v3/stock/snapshot/ohlc?symbol=*&venue=utp_cta` — whole market in one call. Returns
  timestamp, symbol, OHLC, volume, **count** (trade count). Only meaningful after RTH.
- `/v3/stock/history/trade_quote` — every trade paired with the prevailing NBBO at that
  trade. Per-symbol only, no wildcard. Multi-day supported, capped at one month.
- `/v3/stock/history/quote` — NBBO quote records. Intervals `tick` through `1h`, but
  **sub-1m only on single-day requests**.
- `/v3/stock/list/symbols` — full roster.
- Unlimited requests on Standard, but wall-clock time still matters.
- Historical = true consolidated NBBO. Real-time = Nasdaq Basic (BBO within 1% of NBBO
  99.22% of the time — far too coarse for measuring a 20-cent spread). Use historical
  throughout; the 15-minute delay is irrelevant for a 6pm job.

## Step 0 — discovery scripts, before any pipeline code

Write these, I'll run them, we'll design from the results.

1. Confirm `trade_quote` is available on Standard (not Pro-only).
2. FDX, one day: row count, columns actually returned, wall-clock time, parquet size.
3. Time a 10-day multi-day `trade_quote` for one symbol. ×544 gives initial fetch runtime
   and tells us whether concurrency is needed.
4. Enumerate the trade condition codes present, so we know what to exclude.
5. Whether `quote` records are emitted on every update or only on change — affects whether
   time-weighting needs forward-fill.

## Universe

`snapshot/ohlc?symbol=*&venue=utp_cta`, run after the close.

Filters: **price $100–$2,000**, **dollar volume ≥ $100M** (close × volume). Yields ~544
symbols on 2026-08-28 data. Thresholds in config — I'll tune them and may test sub-$100
names later.

**Hysteresis:** enter at ≥$100 and ≥$100M; exit only below $85 or $70M. Prevents boundary
names flickering in and out and leaving ragged history.

**Stickiness:** once a symbol enters, keep it in the fetch list for 30 days even if it
drops out. Costs little, preserves continuity.

Dollar volume is a **floor only, not a ranking input.** Across my 15 traded tickers it has
no relationship to outcome — MRNA is highest at $3.6B and lost money; FDX is near the
bottom at $317M and was my third best.

## Metrics

From `trade_quote`, per symbol, **regular hours only** (09:30–16:00 ET). Write the
computation to accept arbitrary start/end times, not hardcoded to a full session.

**Spread**
- Quoted spread (ask − bid), cents and bps, mean and median
- Time-weighted spread — weight each quote by how long it persisted

**Noise — compute all of these; I'll decide empirically which one works**

This is the open design question. Two candidates, each with a known flaw:

- *Midpoint noise* — median absolute change in (bid+ask)/2 between fixed 10-second
  intervals, in bps. Flaw: in a thin book, a 40-share bid appearing and vanishing 10 cents
  away moves the mid 5 cents with no economic content. Sparse flickery books are exactly
  what I trade, so this may report highest noise on my best names.
- *Trade-price noise* — same calculation on trade prices. Flaw: consecutive trades
  alternate between bid and ask, so trade-price variation contains the spread itself. Since
  the ranking metric is spread ÷ noise, that's partly dividing spread by spread.

So compute:
1. **Time-weighted midpoint noise** — within each 10s bucket, weight the midpoint by quote
   duration, then diff consecutive buckets. Flicker averages out; genuine repricing
   survives. This is my current best guess.
2. Instantaneous (last-quote) midpoint noise, for comparison
3. Trade-price noise, for comparison
4. **Bid-side and ask-side noise separately** — my example case is an unstable bid against
   a still offer, and the midpoint destroys that asymmetry by construction

At 5s, 10s, and 30s horizons. Fixed clock, not trade-to-trade — otherwise busy stocks look
artificially calm.

**Flicker / quote stability** — measured separately, not folded into noise
- Quote updates per minute
- How often the BBO changes *without* a trade at that price (cancel vs consumption)
- Median lifetime of the best bid and best offer

I don't know the sign on this. High flicker might be bad (unstable, can't hold queue
position) or good (a book that keeps re-forming is one I can keep inserting into). Measure
it and let the data say.

**Flow**
- Trades per minute; median and mean trade size; odd-lot share
- **Trade classification** — share at bid, at ask, between. This is my two-sidedness
  measure: I need buyers *and* sellers arriving, not one-way flow.
- Off-exchange share, if the exchange field distinguishes TRF prints

**Ranking metric:** spread_bps ÷ noise_bps, computed for each noise variant so they can be
compared.

Compute at two granularities from the same pull: **per day** (drives the dashboard) and
**per 15-minute window** (stored for later — my mornings substantially outperform my
afternoons and I want to investigate).

## Calibration — build the comparison, I'll run it

I have 15 tickers with live results. Write a script that outputs every computed metric for
these alongside my realised $/min, sorted so the relationship (or absence of one) is
visible at a glance. I'll run it and interpret.

| ticker | my $/min |     | ticker | my $/min |
|--------|----------|-----|--------|----------|
| EXPE   | 4.87     |     | STX    | 0.89     |
| LLY    | 4.02     |     | LITE   | 0.51     |
| FDX    | 3.13     |     | LII    | 0.33     |
| PANW   | 2.80     |     | DELL   | 0.03     |
| A      | 1.64     |     | Q      | −0.23    |
| DG     | 1.53     |     | MRNA   | −1.32    |
| TER    | 1.52     |     | DLTR   | −1.32    |
|        |          |     | INTU   | −1.91    |

Rough noise values from my own fill prices (crude proxy, right ballpark): FDX ≈ 1.8 bps,
LLY ≈ 0.85, LITE ≈ 5.2, DLTR ≈ 2.7.

The purpose is choosing among the noise variants. Whichever one best separates the top of
that list from the bottom is the one to rank on. **If none of them separate, say so** —
I've already been burned by results that looked clean and turned out to be artifacts, and
a tidy ranking that doesn't track reality is worse than no ranking.

## Storage

- **Parquet is the record.** `/data/equities_scalp/` on the VPS, alongside existing
  `/data/chain_eod`, `/data/equity_1min`, `/data/oi_raw`, `/data/spx_options`. Partition by
  symbol and date. Raw pulls only; never delete without asking.
- **Postgres holds derived metrics only.** No tick data ever. Tables: `universe` (nightly
  candidate list with qualifying values), `daily_metrics`, `intraday_metrics`.
- Size estimate ~1–1.5 MB per symbol-day, so 544 × 10 days ≈ 5–8 GB. Unverified — step 0
  measures it.
- **Retain every nightly ranking rather than overwriting.** In a month I'll have a feature
  history to test against my actual fills, which is the dataset I currently lack.

## Scripts

```
update_universe.py                      # 1 API call. Writes candidate list.
fetch.py --start DATE --end DATE        # the loop. Reads universe, caches parquet.
compute.py --start DATE --end DATE      # reads parquet, writes metrics to Postgres.
rank.py                                 # reads Postgres, outputs the ranking.
calibrate.py                            # the 15-ticker comparison above.
```

Each independently runnable, idempotent, **resumable** — if `fetch.py` dies at symbol 300,
rerunning skips the 299 already on disk. **Incremental by default:** check what's in
parquet, request only the gap. First run long, nightly runs short.

Nothing runs on a schedule unless I add it to cron myself.

## Dashboard

Nightly batch, so no streaming. FastAPI reading Postgres, or a static page regenerated
after each run. One row per ticker, sortable: spread bps, each noise variant, the ratios,
trades/min, trade size, two-sidedness, flicker, 10-day stability. Thresholds adjustable in
the UI.

Include a flag column marking tickers I've traded, with my realised $/min, so the
calibration check stays permanently visible rather than being a one-time test.

## Style

Python. Config file for thresholds, lookback windows, paths, and connection details — I
change these constantly. Small scripts I can run and inspect individually, not a framework.
Simple and readable over clever or general.
