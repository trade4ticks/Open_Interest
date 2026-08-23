"""
backfill_call_price.py — populate equity_surface.call_price on rows written
before that column existed, WITHOUT re-fitting.

call_price arrived as an ALTER, so every row from an earlier run has it NULL
and only a full surface rebuild would fill it. A rebuild means re-reading the
chain store and re-running the spline fit for every snapshot — hours of CPU to
recover a quantity that is already implied by what was stored.

It is implied exactly. bs_put_forward needs (F, K, T, sigma, r):

    F     = equity_surface.forward
    K     = equity_surface.strike
    sigma = equity_surface.iv
    T     = equity_surface.dte_actual / 365      (dte_actual IS smile.T * 365)
    r     = NOT STORED

r is the only gap, and it cannot be recovered by joining
equity_surface_diagnostics either: diagnostics is keyed by EXPIRY, while a
surface row is keyed by the grid tenor DTE, which is a blend of two bracketing
expiries. There is no 1:1 mapping.

But r does not have to be looked up. d1 and d2 depend only on F, K, T and
sigma, all of which are stored, and three stored greeks each carry the discount
factor as a plain multiplicative term:

    price = disc * (K*N(-d2) - F*N(-d1))    ->  disc = price / (...)
    vega  = disc * F*n(d1)*sqrt(T) / 100    ->  disc = 100*vega / (...)
    gamma = disc * n(d1) / (F*sigma*sqrt(T))->  disc = gamma*F*sigma*sqrt(T)/n(d1)

So disc is over-determined, and r = -ln(disc)/T.

WHY THE RATE IS RECOVERED PER SMILE, NOT PER ROW
------------------------------------------------
build_snapshot prices every delta node of one tenor from a single smile:

    for dte in sorted(smiles):
        for d in deltas:
            bs_put_forward(smile.F, node["strike"], smile.T, node["iv"], smile.r)

so F, T and r are constant across all nodes sharing
(ticker, trade_date, snapshot, dte). That matters, because a far-wing node can
carry NO recoverable rate at all. At F=100, K=55, 12 vol and 7 DTE, d1 is ~36:
n(d1) underflows to exactly 0.0 and both normal CDFs saturate, so price, vega
and gamma are all exactly zero and every candidate denominator is zero — while
the CALL at that node is worth ~45. Recovering row by row would abandon
precisely the rows whose call price is largest.

Pooling the smile fixes it. The near-the-money nodes are perfectly conditioned,
they pin r, and the wings inherit it. It also turns the redundancy into a
check: every node of a smile must agree on the rate, so `spread` (the widest
disagreement) is reported and a group exceeding MAX_RATE_SPREAD is refused
outright rather than averaged into a plausible-looking number.

SELF-VALIDATION
---------------
The script does not reimplement the call formula. It recovers r and invokes
bs_put_forward itself, so a backfilled row comes from exactly the code path a
rebuild would use. The recovered rate is then fed back through the pricer and
the resulting price, vega, gamma and theta are compared against the four values
already on the row. Theta is the sharpest of these because it depends on r
EXPLICITLY and not only through disc, so it pins the rate rather than just the
discount factor.

That gives two layers with different blast radii, and the split is not
accidental. price, vega and gamma FEED the rate recovery, so corrupting one
moves that node's estimate away from its siblings' and the intra-smile spread
check refuses the WHOLE smile — correct, because a smile whose nodes disagree
about r cannot be priced at all. theta is not a rate input, so the smile's rate
survives and the error is caught one layer down by per-node reproduction,
costing only that node.

A node whose four stored greeks are all ~0 cannot validate anything — the
comparison passes vacuously. In the far wing they come out around 1e-285, which
is worse than zero in one respect: it looks like a number. Those nodes are
counted separately and reported, so "validated" never quietly includes them.

Usage:
    python backfill_call_price.py --dry-run          # measure, touch nothing
    python backfill_call_price.py
    python backfill_call_price.py --start 20260601 --end 20260630
    python backfill_call_price.py --verify           # re-check populated rows
"""
from __future__ import annotations

import argparse
import logging
import math
import sys
from collections import defaultdict
from datetime import datetime

import numpy as np
import psycopg2.extras
from scipy.stats import norm

from lib.surface_fit import bs_put_forward

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backfill_call_price")

# Relative tolerance for reproducing the four stored greeks. Doubles survive a
# Postgres round trip exactly, so real agreement is at machine epsilon; 1e-9 is
# loose enough to absorb a scipy version's last-ulp differences and still tight
# enough that a genuine model mismatch cannot slip through.
RTOL = 1e-9
ATOL = 1e-12

# A greek below this contributes no information about disc — it has underflowed
# and its ratio to an equally-underflowed denominator is noise. Relative to the
# node's own scale, not absolute, because F ranges from a few dollars to
# several hundred across the universe.
COND_FLOOR = 1e-8

# Widest disagreement tolerated between the per-node rate estimates of one
# smile. They are algebraically identical, so anything above rounding means the
# rows did not come from a single bs_put_forward call.
MAX_RATE_SPREAD = 1e-6

# lib/surface_config.R_MIN..R_MAX is (0.0, 0.10) with R_DEFAULT 0.05. Outside a
# widened band, the recovery went wrong rather than the fit.
R_SANE_LO, R_SANE_HI = -0.02, 0.20

GROUP_KEYS = ("ticker", "trade_date", "snapshot", "dte")
FETCH_COLS = ["ticker", "trade_date", "snapshot", "dte", "put_delta",
              "iv", "strike", "forward", "price", "theta", "vega", "gamma",
              "dte_actual"]


def _ok(got, want) -> bool:
    """Reproduction check. A stored NULL is not evidence either way."""
    if want is None:
        return True
    if got is None:
        return False
    return abs(got - want) <= max(ATOL, RTOL * abs(want))


def _geometry(row):
    """(F, K, T, sigma, d1, n(d1)) or None if the row cannot be priced."""
    F, K = row.get("forward"), row.get("strike")
    sigma, dte_actual = row.get("iv"), row.get("dte_actual")
    if None in (F, K, sigma, dte_actual):
        return None
    if not all(np.isfinite([F, K, sigma, dte_actual])):
        return None
    T = dte_actual / 365.0
    if T <= 0 or sigma <= 0 or F <= 0 or K <= 0:
        return None
    sqrtT = math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * sqrtT)
    return F, K, T, sigma, d1, float(norm.pdf(d1)), sqrtT


def disc_candidates(row) -> list:
    """[(source, disc)] from this row alone. Empty when everything underflows.

    Each denominator is screened against the node's own scale: a candidate is
    only admitted when both the greek and its denominator carry real digits.
    """
    geo = _geometry(row)
    if geo is None:
        return []
    F, K, T, sigma, d1, npd1, sqrtT = geo
    d2 = d1 - sigma * sqrtT
    out = []

    price = row.get("price")
    if price is not None and math.isfinite(price):
        put_undisc = K * float(norm.cdf(-d2)) - F * float(norm.cdf(-d1))
        if put_undisc > COND_FLOOR * max(F, K):
            out.append(("price", price / put_undisc))

    if npd1 > COND_FLOOR:
        vega = row.get("vega")
        if vega is not None and math.isfinite(vega):
            out.append(("vega", vega / (F * npd1 * sqrtT / 100.0)))
        gamma = row.get("gamma")
        if gamma is not None and math.isfinite(gamma):
            out.append(("gamma", gamma / (npd1 / (F * sigma * sqrtT))))
    return out


def recover_rate(rows: list) -> tuple:
    """(r, info) for one smile's worth of rows. r is None on refusal.

    Pools candidates across every node of the smile, because F, T and r are
    constant within it and the well-conditioned nodes must carry the wings.
    """
    info = {"n_candidates": 0, "spread": None, "reason": None, "T": None}
    Ts = {round(r["dte_actual"], 9) for r in rows
          if r.get("dte_actual") is not None}
    Fs = {round(r["forward"], 9) for r in rows if r.get("forward") is not None}
    if len(Ts) > 1 or len(Fs) > 1:
        # One smile, one forward, one tenor. More than one means the grouping
        # key is wrong, and averaging across them would be meaningless.
        info["reason"] = (f"group is not a single smile: {len(Fs)} forward(s), "
                          f"{len(Ts)} tenor(s)")
        return None, info

    cands = [d for row in rows for _, d in disc_candidates(row)]
    info["n_candidates"] = len(cands)
    if not cands:
        info["reason"] = "no node in the smile carries a recoverable rate"
        return None, info

    disc = float(np.median(cands))
    info["spread"] = float(max(cands) - min(cands))
    if not math.isfinite(disc) or not 0.0 < disc <= 1.5:
        info["reason"] = f"implausible discount factor {disc:.6g}"
        return None, info
    if info["spread"] > MAX_RATE_SPREAD:
        info["reason"] = (f"nodes disagree on the rate by {info['spread']:.2e} "
                          f"— not one bs_put_forward call")
        return None, info

    T = next(iter(Ts)) / 365.0
    info["T"] = T
    r = -math.log(disc) / T
    if not R_SANE_LO <= r <= R_SANE_HI:
        info["reason"] = f"recovered rate {r:.4%} outside the sane band"
        return None, info
    return r, info


def call_prices_for_group(rows: list) -> tuple:
    """([(row, call_price, reason)], stats) for one smile.

    Pure: no database, no I/O. This is the whole algorithm and the unit tests
    drive it directly.
    """
    stats = {"validated": 0, "vacuous": 0, "refused": 0, "r": None,
             "spread": None}
    r, info = recover_rate(rows)
    stats["spread"] = info["spread"]
    if r is None:
        stats["refused"] = len(rows)
        return [(row, None, info["reason"]) for row in rows], stats
    stats["r"] = r

    out = []
    for row in rows:
        if row.get("price") is None:
            # bs_put_forward returned its `none` dict at fit time — T <= 0 or
            # sigma <= 0. The row never had a price and must not gain a call.
            out.append((row, None, "price is NULL (node was never priced)"))
            stats["refused"] += 1
            continue
        geo = _geometry(row)
        if geo is None:
            out.append((row, None, "missing or degenerate F, K, sigma or T"))
            stats["refused"] += 1
            continue
        F, K, T, sigma, *_ = geo
        g = bs_put_forward(F, K, T, sigma, r)
        bad = next((n for n in ("price", "vega", "gamma", "theta")
                    if not _ok(g[n], row.get(n))), None)
        if bad is not None:
            out.append((row, None, f"{bad} does not reproduce: stored "
                                   f"{row.get(bad)!r} vs {g[bad]!r}"))
            stats["refused"] += 1
            continue
        # All four stored greeks ~0 means the comparison above proved nothing.
        # Count it honestly rather than letting it inflate "validated".
        substantive = any(abs(row.get(n) or 0.0) > ATOL
                          for n in ("price", "vega", "gamma", "theta"))
        stats["validated" if substantive else "vacuous"] += 1
        out.append((row, g["call_price"], None))
    return out, stats


UPDATE_SQL = """
UPDATE equity_surface s SET call_price = v.cp
FROM (VALUES %s) AS v(ticker, trade_date, snapshot, dte, put_delta, cp)
WHERE s.ticker = v.ticker
  AND s.trade_date = v.trade_date::date
  AND s.snapshot = v.snapshot
  AND s.dte = v.dte::smallint
  AND s.put_delta = v.put_delta::smallint
"""


def _units(conn, start, end, verify: bool) -> list:
    """(trade_date, snapshot) units of work.

    Chunked this way because it is how the data was written, and because one
    snapshot contains WHOLE smiles — the grouping the rate recovery needs. A
    chunk is ~30k rows even on a fully populated date, where doing a whole
    date at once would pull millions into memory.
    """
    where = ["call_price IS NOT NULL" if verify else "call_price IS NULL"]
    params = []
    if start:
        where.append("trade_date >= %s")
        params.append(start)
    if end:
        where.append("trade_date <= %s")
        params.append(end)
    with conn.cursor() as cur:
        cur.execute(f"SELECT trade_date, snapshot, count(*) FROM equity_surface"
                    f" WHERE {' AND '.join(where)} "
                    f"GROUP BY 1, 2 ORDER BY 1, 2", params)
        return cur.fetchall()


def _load(conn, trade_date, snapshot, verify: bool) -> list:
    """Every row of the snapshot, not only the ones needing a value.

    Loading the whole snapshot is deliberate: the rate is recovered from the
    smile, so a partially-backfilled group must still see its well-conditioned
    siblings. Filtering to call_price IS NULL here would break exactly the
    wings this design exists to serve.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {', '.join(FETCH_COLS)}, call_price FROM equity_surface "
            f"WHERE trade_date = %s AND snapshot = %s", (trade_date, snapshot))
        rows = []
        for rec in cur.fetchall():
            row = dict(zip(FETCH_COLS, rec[:-1]))
            row["_had_call"] = rec[-1] is not None
            rows.append(row)
    return rows


def process(conn, trade_date, snapshot, verify: bool, dry_run: bool) -> dict:
    rows = _load(conn, trade_date, snapshot, verify)
    groups = defaultdict(list)
    for row in rows:
        groups[tuple(row[k] for k in GROUP_KEYS)].append(row)

    agg = {"read": 0, "recovered": 0, "written": 0, "validated": 0,
           "vacuous": 0, "mismatched": 0}
    failures, rates, spreads, out = {}, [], [], []
    for _, grp in groups.items():
        results, st = call_prices_for_group(grp)
        if st["r"] is not None:
            rates.append(st["r"])
        if st["spread"] is not None:
            spreads.append(st["spread"])
        agg["validated"] += st["validated"]
        agg["vacuous"] += st["vacuous"]
        for row, cp, reason in results:
            target = row["_had_call"] if verify else not row["_had_call"]
            if not target:
                continue
            agg["read"] += 1
            if reason is not None:
                key = reason.split(":")[0].split("—")[0].strip()
                failures[key] = failures.get(key, 0) + 1
                continue
            if verify:
                if not _ok(cp, row.get("call_price")):
                    agg["mismatched"] += 1
                agg["recovered"] += 1
                continue
            agg["recovered"] += 1
            out.append((row["ticker"], row["trade_date"], row["snapshot"],
                        int(row["dte"]), int(row["put_delta"]), float(cp)))

    if out and not dry_run and not verify:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, UPDATE_SQL, out, page_size=1000)
        conn.commit()
        agg["written"] = len(out)
    agg["failures"] = failures
    agg["rates"] = rates
    agg["spreads"] = spreads
    return agg


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Backfill equity_surface.call_price without re-fitting.")
    ap.add_argument("--start", help="YYYYMMDD")
    ap.add_argument("--end", help="YYYYMMDD")
    ap.add_argument("--dry-run", action="store_true",
                    help="recover and validate, write nothing")
    ap.add_argument("--verify", action="store_true",
                    help="re-derive rows that ALREADY have call_price and "
                         "compare, instead of filling NULLs. Never writes.")
    args = ap.parse_args()

    def day(s):
        return datetime.strptime(s, "%Y%m%d").date() if s else None

    start, end = day(args.start), day(args.end)
    from db import get_connection

    with get_connection() as conn:
        units = _units(conn, start, end, args.verify)
        total = sum(u[2] for u in units)
        mode = ("VERIFY" if args.verify
                else "DRY RUN" if args.dry_run else "BACKFILL")
        print(f"=== call_price {mode} ===")
        print(f"{total:,} target row(s) across {len(units)} "
              f"(date, snapshot) unit(s)")
        if not units:
            print("nothing to do.")
            return 0
        print(f"range: {units[0][0]} .. {units[-1][0]}\n")

        agg = {k: 0 for k in ("read", "recovered", "written", "validated",
                              "vacuous", "mismatched")}
        failures, rates, spreads = {}, [], []
        for i, (d, snap, _n) in enumerate(units, 1):
            st = process(conn, d, snap, args.verify, args.dry_run)
            for k in agg:
                agg[k] += st[k]
            for k, v in st["failures"].items():
                failures[k] = failures.get(k, 0) + v
            rates.extend(st["rates"])
            spreads.extend(st["spreads"])
            if i % 200 == 0 or i == len(units):
                log.info("%d/%d units  %s row(s) recovered", i, len(units),
                         f"{agg['recovered']:,}")

        print(f"\n  read       {agg['read']:>12,}")
        print(f"  recovered  {agg['recovered']:>12,}  "
              f"({agg['recovered'] / max(agg['read'], 1):.2%})")
        print(f"  written    {agg['written']:>12,}"
              f"{'   (none — dry run)' if args.dry_run else ''}"
              f"{'   (none — verify)' if args.verify else ''}")
        print(f"\n  reproduced all 4 stored greeks   {agg['validated']:>12,}")
        print(f"  greeks all ~0, check was vacuous {agg['vacuous']:>12,}"
              f"   <- rate came from the smile, not the node")
        if args.verify:
            print(f"  DISAGREED with stored value      "
                  f"{agg['mismatched']:>12,}")
        if rates:
            q = np.percentile(rates, [0, 50, 100])
            print(f"\n  recovered rate   min {q[0]:.3%}   median {q[1]:.3%}  "
                  f" max {q[2]:.3%}   ({len(rates):,} smiles)")
            print("  (surface_config pins r to [0%, 10%] with a 5% default, "
                  "so this should sit inside that band)")
        if spreads:
            print(f"  widest intra-smile rate disagreement: "
                  f"{max(spreads):.2e}  (tolerance {MAX_RATE_SPREAD:g})")
        if failures:
            print(f"\n  {sum(failures.values()):,} row(s) left NULL:")
            for reason, n in sorted(failures.items(), key=lambda x: -x[1]):
                print(f"    {n:>10,}  {reason}")
            print("\n  Reported rather than guessed at. A row whose stored "
                  "greeks cannot be\n  reproduced is a row this script does "
                  "not understand.")
        else:
            print("\n  no refusals.")
        return 1 if (failures or agg["mismatched"]) else 0


if __name__ == "__main__":
    sys.exit(main())
