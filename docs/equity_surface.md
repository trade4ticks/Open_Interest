# Equity option surface — interpolation stage

Stage 3 of the chain pipeline:

```
fetch (exists) -> clean (lib/clean_chain.py) -> INTERPOLATE (this) -> metrics
```

Resamples irregular chains — expiries where the exchange lists them, strikes
where liquidity is — onto a fixed (tenor x delta) grid, so any
`(ticker, trade_date, snapshot, dte, put_delta)` is a direct lookup.

| File | Role |
|---|---|
| `lib/surface_config.py` | every threshold and the output grid |
| `lib/surface_fit.py` | stages 1-4, pure computation, no I/O |
| `lib/surface_store.py` | stage 5, Postgres |
| `sql/08_equity_surface.sql` | schema + partition functions |
| `build_equity_surface.py` | CLI |
| `test_equity_surface.py` | 82 assertions |

```bash
python build_equity_surface.py init-db
python build_equity_surface.py batch --start 20260601 --end 20260630
python build_equity_surface.py batch --start 20260601 --end 20260630 --tickers AAPL,MSFT
python build_equity_surface.py incremental
python build_equity_surface.py intraday --source intraday
```

Every frame read goes through `clean_chain()` **before anything else** — that
module supplies `mid_price`, `spread`, `moneyness`, `gamma`, `dte` and the
`flag_*` columns this stage filters on, and does no file I/O of its own.

## Three things that are easy to get silently wrong

**`extrapolated` is not optional.** `UnivariateSpline(..., ext=3)` returns its
*boundary value* outside the fitted domain — a flat extrapolation, not an
error. Delta keeps varying with `k` even where `w` is pinned flat, so the
solver finds a root out there and returns a node whose IV is just the last real
strike's IV, with nothing marking it as fabricated. Every surface row carries
`extrapolated`, computed against the smile's own `[k_min, k_max]`.

Those rows are **written, not dropped**: a gap breaks rolling percentiles
downstream, and a flagged value lets the metrics layer decide. This matters
because the strategy this feeds trades 10-15 delta puts — exactly the nodes
most likely to fall outside a listed strike ladder.

**`forward_method` measures a known approximation.** When the put-call parity
regression fails, the fallback prices the forward off spot with a default
carry, which ignores dividends. On an index that is nearly free; on a
dividend-paying single name the forward is overstated by roughly the dividend
inside the tenor, shifting where every strike sits on the smile. The column
records `'pcp'` or `'spot_fallback'` per expiry so the frequency can be
measured on real chains. There is deliberately no dividend model.

**`dte_actual` vs `dte`.** `dte` is the grid label; `dte_actual` is the smile's
true tenor in days. They agree on normally-interpolated rows. They diverge on
near-expiry fallback rows, where the nearest listed expiry serves a bucket
below it — `FALLBACK_MAX_T_GAP` caps how far apart they may get, and
`dte_actual` makes the remaining gap visible so the metrics layer can apply its
own tolerance.

## Degenerate expiries are excluded from bracketing

A newly-listed expiry fits fine but over a tiny log-moneyness range. Because
`InterpolatedSmile` takes the **intersection** of its two endpoints' domains,
one narrow expiry destroys the wing of a tenor whose other endpoint is
excellent. Observed simultaneously on QQQ, SPY, GLD and IWM at 2026-06-01 1545,
all failing at exactly DTE 14 and clean at 5/7/10/21/30 — the same newly-listed
06-15 expiry in each:

| expiry | dte_actual | n_strikes_clean | k_min | k_max |
|---|---|---|---|---|
| 2026-06-12 | 11.01 | 388 | −0.293 | 0.097 |
| 2026-06-15 | 14.01 | **52** | **−0.026** | 0.008 |
| 2026-06-18 | 17.01 | 468 | −0.399 | 0.126 |

The 14 DTE target bracketed 06-12/06-15, clipping a −0.293 domain to −0.026, so
a 10-delta put (~5% OTM) landed outside and was written `extrapolated=True`.
06-12 and 06-18 bracket 14 days perfectly well. Because new dailies list
continuously, whichever tenor sits beside the newest listing breaks — **a hole
that wanders through the grid** rather than a stable missing value.

`select_bracketing_fits` drops fits whose put-side domain is anomalously narrow
*relative to the other fits at the same snapshot*:

```
domain_reach = |k_min| / sqrt(w_atm)          # sqrt(w) = sigma*sqrt(T)
excluded     = reach < NARROW_DOMAIN_RATIO * median(reach)
```

`reach` is how many sigma below the forward the domain extends — directly
interpretable, since a 25-delta put sits near 0.67 sigma and a 10-delta near
1.28. Raw `k_min` is not comparable across expiries; a 928-day expiry spans far
more log-moneyness than an 8-day one at equal quality.

**The rule is relative, and that is load-bearing.** QQQ's degenerate fit reaches
~0.5 sigma, but T's perfectly legitimate 11 DTE fit reaches ~1.0 — it genuinely
has no 10-delta wing while its 25-delta node is real and useful. Any absolute
cutoff catching the first discards the second, trading a good 25-delta node to
save a 10-delta one that never existed. Relative separates them: QQQ's outlier
is a tenth of its neighbours' reach; T's fits are all similarly narrow so none
is an outlier.

Two guards abandon the filter entirely rather than break bracketing: if fewer
than 2 fits would survive, or if more than a third trip the rule (which means
the whole chain is thin, not one expiry degenerate).

**Exclusion is for bracketing only.** The fit is still computed, still written
to diagnostics, and still participates in the calendar-arbitrage check — which
deliberately runs over every usable fit, since a degenerate expiry can still
violate it. The near-expiry fallback uses the filtered list too, so a
degenerate nearest expiry cannot serve a 0DTE row.

`equity_surface_diagnostics.domain_reach` and `.excluded_from_bracketing` record
the metric and whether the rule fired, so `NARROW_DOMAIN_RATIO` can be tuned
from observed data and confirmed to be firing on newly-listed expiries rather
than legitimately thin chains.

## Conventions

- **T uses 16:00 ET**, not 16:15. Equity options settle at the close; 16:15 is
  an SPX convention that would overstate T on every row, badly at 0DTE.
- **Put delta is a positive integer 5-95.** `forward put delta = N(d1) - 1`
  with `d1 = (-k + w/2)/sqrt(w)`.
- **Time interpolation is linear in total variance**, not IV — that is what
  keeps the blend between two bracketing expiries arbitrage-free.
- **All greeks are forward greeks**, underlying = F, every node priced as a
  put. Theta can be positive for deep-ITM puts; that is correct, not a bug.
- At the forward, `d1 = +sqrt(w)/2 > 0`, so `atm_put_delta = N(d1) - 1` lands
  in `(-0.5, 0)` — slightly **less** negative than -0.5.

## Near-expiry fallback

Targets below the nearest listed expiry cannot bracket from below. At `dte = 0`
there is never an expiry with `T < 0`, so a 0DTE row can only come from using
the nearest expiry's smile directly — and when that expiry *is* today's, this
is not an approximation at all, it is the correct smile with T shrinking
through the day.

Only the **largest** un-bracketable bucket is eligible, and only if the nearest
fit's `T` is within `FALLBACK_MAX_T_GAP` (4/365) of that bucket's nominal `T`.
The cap allows a Monday 0DTE bucket to be served by Friday's expiry on a
weekly-only name, and blocks a 25-day expiry from populating a `dte=21` row
with a 25-day smile.

A tenor that cannot bracket and is not the fallback bucket is simply skipped,
so **the front of the grid is legitimately sparser than the rest**. A row count
below the full tenor set is normal, not an error.

0DTE behaves unlike every other tenor: `T` changes minute to minute, gamma is
very large, and the smile is unstable near the close. Expected.

## Implied-rate bounds

`R_MIN, R_MAX = 0.0, 0.10` (was `-0.05, 0.20`). Outside them
`solve_forward_rate` raises and the caller falls back to spot-plus-carry.

The old bounds were far too wide to catch a bad regression. Observed on real
data, all passing the old validation:

```
AAPL 2026-06-01 0945:   +12.5%,  -4.7%,  -2.5%,  +0.16%
T    2026-06-01 1545:   +14.9%,  +10.3%,  -2.8%
```

The true rate is near 4-5% and roughly flat across tenors; none of those
resembles it. The cause is **structural, not a coding error**: put-call parity
assumes European exercise and equity options are American, so early-exercise
premium turns the parity equality into an inequality and biases the regression.
This does not arise on European index options, which is why the inherited
bounds were adequate there.

Impact is bounded. `r` does not enter implied vol (vendor-supplied) or
`k = ln(K/F)` (which uses `F` only). It appears solely in the discount factor
for price, theta, vega and gamma — negligible at short tenors, ~10% at
multi-year ones. Tightening routes these fits to the spot fallback instead of
accepting an implausible rate, so **expect the `spot_fallback` share in
diagnostics to rise**. That is the intended effect and `forward_method` already
records it.

## Thresholds

All in `lib/surface_config.py`.

`MIN_STRIKES_FOR_FIT = 5` is inherited and barely above the cubic spline's
minimum of 4. It is likely too permissive for thin single-name chains, but is
left unchanged pending observed data — `equity_surface_diagnostics.n_strikes_clean`
records the distribution so it can be inspected before retuning.

## Tables

`equity_surface` and `equity_atm` are monthly RANGE partitions on `trade_date`;
`ensure_equity_surface_partition(date)` / `ensure_equity_atm_partition(date)`
create the child on demand, so a backfill into a new month needs no migration.
`ticker` is part of every primary key and unique constraint. All writes are
`ON CONFLICT DO UPDATE`, so reprocessing a date is idempotent.

`equity_surface_diagnostics` doubles as pipeline state: `incremental` resumes
from its max `trade_date`, and `intraday` compares its row count per snapshot
against what is on disk to decide what still needs work. Diagnostics rows are
written even when a snapshot produced no surface rows — that is precisely the
case they exist to explain, and it is what stops an incremental run retrying a
date forever.

NumPy sanitisation on write is not optional: psycopg2 has no adapter for
`numpy.float64` and falls back to `repr()`, emitting literals like
`np.float64(2.46)` that Postgres rejects. NaN/Inf become NULL for the same
reason.

## One correction to the spec

The spec's ATM section comments `atm_delta = N(d1_atm) - 1` as "slightly more
negative than -0.5". The formula is the standard one and is implemented as
written, but the parenthetical is inverted: `d1_atm = +sqrt(w)/2 > 0`, so
`N(d1) > 0.5` and the result is slightly **less** negative than -0.5
(e.g. -0.4856 at w = 0.005). The test asserts the formula.

## Tests

`python test_equity_surface.py` — 82 assertions. The Postgres idempotence test
skips with a stated reason when no database is reachable. Covers 16:00 vs
16:15, parity recovering a known F and r, `spot_fallback` on too few pairs, the
`extrapolated` flag on a deliberately narrow ladder, alpha=0/alpha=1 boundary
behaviour, no tenor without a bracketing pair, the capped fallback emitting and
skipping either side of the cap, `dte_actual` semantics, butterfly detection on
a spiked smile, and per-expiry calendar flagging.
