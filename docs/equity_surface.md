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
