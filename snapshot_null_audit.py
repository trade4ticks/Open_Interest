"""
snapshot_null_audit.py — every column, NULL rate intraday vs at the close.

Read-only. Audits the WHOLE table rather than a list anyone guessed at, which
is the point: the spotvol_beta bug was found by eye, and the only reason it was
found is that someone happened to look at that column. This finds the pattern
wherever it exists.

--- The signature ----------------------------------------------------------

A column populated at 1545 and NULL at every 5-minute bucket on the same date,
for the same tickers. It hides well: the close is what anyone eyeballs, and the
close is perfectly healthy.

--- Expectation comes from the CODE, observation from the DATA ---------------

The expectation is not a hardcoded list. At startup this calls each helper in
lib/metrics_compute with a synthetic full snapshot and records which columns it
returns, giving a column -> helper map derived from the code as it actually is.
Helpers that read the loaded snapshot are SNAPSHOT-kind; helpers that read a
daily history window are DAILY-kind. A hardcoded list would be exactly the
"list I guessed at" this is meant to replace, and it would rot on the next
column added.

The data then either agrees with that expectation or does not, and the
disagreements are the findings.

--- The four groups --------------------------------------------------------

  A  CARRIES CORRECTLY     daily or hybrid, NULL rate flat across buckets.
                           rv_{t}d, vrp_{t}d, days_to_earnings live here.

  B  NULL INTRADAY, SHOULD NOT BE
                           materially more NULL intraday than at the close.
                           Split by expectation, because the two need different
                           responses:
                             B1 daily/hybrid  — a carry-forward bug, fix it
                             B2 snapshot-kind — needs judgement: a thin 09:35
                                chain legitimately loses wing nodes, but a 100%
                                gap is not thinness

  C  SNAPSHOT-SPECIFIC     snapshot-kind, NULL rate flat. Correct as is.

  D  MORE NULL AT THE CLOSE than intraday — the opposite direction, reported
                           because it is equally a bucket-dependence and nobody
                           would think to look for it.

A second, VALUE-level pass follows the NULL one, because a column can be fully
populated at every bucket and still be wrong: if it is recomputed per snapshot
from something snapshot-specific it holds a different value in each. Pure-daily
columns must show exactly one value per ticker per day.

--- Comparing like with like ------------------------------------------------

Only tickers with a row at BOTH the baseline bucket and the bucket being
compared are counted. Otherwise a ticker that failed to capture at 10:15 would
read as "every column NULL at 10:15", which is a capture problem wearing a
metric problem's clothes.

Usage:
    python snapshot_null_audit.py                        # latest date
    python snapshot_null_audit.py --date 2026-08-24
    python snapshot_null_audit.py --date 2026-08-24 --by-bucket
    python snapshot_null_audit.py --threshold 0.05 --verbose
    python snapshot_null_audit.py --table equity_metrics_z
"""
from __future__ import annotations

import argparse
import sys
from datetime import date as _date
from datetime import datetime, timedelta

from db import get_connection
from lib.metrics_config import BASELINE_SNAPSHOT, TENORS, Z_COLUMNS

# A column is "materially" more NULL intraday once the gap exceeds this share
# of the compared tickers. 0.10 is loose enough to ignore one flaky ticker in
# 121 and tight enough that a real gap cannot hide.
DEFAULT_THRESHOLD = 0.10

# Postgres takes hundreds of aggregates in one SELECT, but the parse cost and
# the row width both grow; chunking keeps each query readable in EXPLAIN.
CHUNK = 100

CARRIES, BROKEN_DAILY, BROKEN_SNAP, SNAPSHOT_OK, INVERTED = (
    "A", "B1", "B2", "C", "D")

# Populated by owner_map(): the spot-vol columns whose as-of cutoff differs
# between the baseline bucket and the rest, so two distinct values a day is
# correct for them. Derived, not declared — see owner_map.
_ASOF_SPLIT: set = set()


# =============================================================================
# Expectation: which helper owns each column, derived by calling them
# =============================================================================
def owner_map() -> dict:
    """{column: (helper, kind)} for every base column, from the code itself.

    Calls each metric family with a synthetic snapshot that has every node at
    every tenor, so each helper returns its full key set. Raises if the union
    does not exactly cover the registry — a column produced by no helper, or a
    helper producing something the registry does not know, is precisely the
    drift this audit must not paper over.

    THREE KINDS, and the third is not a refinement — leaving it out produces
    false positives:

      snapshot  reads the loaded snapshot. Varies by bucket, by design.
      daily     reads a daily history window only. One value per session.
      hybrid    BOTH. vrp_{t}d is iv_{t}d_atm (this bucket) minus rv_{t}d
                (daily), so it must be POPULATED at every bucket like a daily
                column while its VALUE moves with the bucket like a snapshot
                one. Judging it by either rule alone flags it wrongly.

    Hybrid is detected, not declared: each daily-window helper is called twice
    with different snapshot-derived inputs and the outputs are diffed. A key
    that moves depends on the snapshot. That survives someone adding a metric
    which mixes the two, where a hardcoded family list would not.
    """
    import lib.metrics_compute as C
    from lib.metrics_config import BASE_NAMES

    cap = datetime(2026, 1, 2, 15, 47)
    snap = {"nodes": {}, "atm": {}, "diag": [
        {"forward_method": "pcp", "n_strikes_clean": 100.0,
         "domain_reach": 4.0, "calendar_arb": False,
         "butterfly_arb": False, "skipped": False}]}
    for t in TENORS:
        snap["nodes"][t] = {
            d: {"iv": 0.30, "strike": 100.0, "price": 5.0, "call_price": 5.0,
                "extrapolated": False, "captured_at": cap, "source": "live"}
            for d in (5, 10, 25, 50, 75, 90)}
        snap["atm"][t] = {"atm_iv": 0.30, "atm_strike": 100.0,
                          "atm_forward": 100.5, "underlying_price": 100.0,
                          "price": 5.0, "captured_at": cap, "source": "live"}

    level, iv, strike, extrap = C._level(snap)
    d0 = _date(2026, 1, 2)
    ohlc = [{"d": d0 + timedelta(days=i), "o": 100.0, "h": 101.0, "l": 99.0,
             "c": 100.0 + i} for i in range(80)]
    # The IV history must RUN UP TO trade_date, must vary, and must not be
    # PERIODIC, or the as-of probe below cannot see anything. A short repeating
    # pattern is the trap: with period 5 and a 21-day window, dropping the last
    # observation shifts the window onto an identical multiset, both cutoffs
    # return the same numbers, and the split reads as absent when it is merely
    # unexercised. A small LCG is deterministic without being periodic here.
    _s = 12345
    ivh: dict = {t: [] for t in TENORS}
    for i in range(80):
        _s = (1103515245 * _s + 12345) % 2147483648
        for _j, _t in enumerate(TENORS):
            # A different response per tenor, so a probe that collapsed the
            # grid onto one series would show up as identical columns.
            ivh[_t].append({
                "d": d0 + timedelta(days=i),
                "iv": 0.30 + 0.02 * (1 + _j) * (_s % 1000) / 1000.0,
                "s": 100.0 + 2.0 * ((_s >> 10) % 1000) / 1000.0})
    td = ivh[TENORS[0]][-1]["d"]

    # _realized takes the snapshot's IV dict as well as the daily bars. Run it
    # against a SECOND set of IVs and diff: whatever moves is hybrid, the rest
    # is purely daily. This is what tells vrp_* apart from rv_* without naming
    # either.
    realized_a = C._realized(ohlc, td, iv)
    iv_b = {k: (v + 0.05 if v is not None else None) for k, v in iv.items()}
    realized_b = C._realized(ohlc, td, iv_b)
    hybrid = {k for k in realized_a if realized_a[k] != realized_b[k]}

    # _spot_vol takes no snapshot data, only the bucket LABEL, which selects
    # the as-of cutoff. Columns that move between the two are the ones with the
    # documented baseline-vs-intraday split, and may hold two values a day.
    sv_close = C._spot_vol(ivh, td, BASELINE_SNAPSHOT)
    sv_noon = C._spot_vol(ivh, td, "1215")
    asof_split = {k for k in sv_close if sv_close[k] != sv_noon[k]}

    families = [
        ("_level", "snapshot", level),
        ("_skew", "snapshot", C._skew(iv, strike)),
        ("_convexity", "snapshot", C._convexity(iv)),
        ("_risk_reversal", "snapshot", C._risk_reversal(iv)),
        ("_term", "snapshot", C._term(iv)),
        ("_structure", "snapshot", C._structure(snap, iv, 100.0)),
        ("_quality", "snapshot", C._quality(snap, extrap)),
        ("_realized", "daily", realized_a),
        ("_spot_vol", "daily", sv_close),
        ("_calendar", "daily", C._calendar(td, [])),
    ]
    out, seen_twice = {}, []
    for helper, kind, produced in families:
        for name in produced:
            if name in out:
                seen_twice.append(name)
            out[name] = (helper, "hybrid" if name in hybrid else kind)
    global _ASOF_SPLIT
    _ASOF_SPLIT = asof_split

    want = set(BASE_NAMES)
    missing = sorted(want - set(out))
    extra = sorted(set(out) - want)
    if missing or extra or seen_twice:
        raise SystemExit(
            "the column -> helper map does not cover the registry:\n"
            f"  produced by no helper : {missing[:10]}\n"
            f"  not in the registry   : {extra[:10]}\n"
            f"  produced twice        : {sorted(set(seen_twice))[:10]}\n"
            "Fix lib/metrics_compute or this map before trusting an audit.")
    return out


def z_owner(base_map: dict) -> dict:
    """Z columns inherit their base column's kind.

    A z is a function of the base's history at the daily baseline, so after the
    z redefinition every z column should be computable at every bucket. A z
    that is materially more NULL intraday than at the close is a finding
    regardless of what its base does.
    """
    return {c.name: (base_map[c.base_column][0] + "->z",
                     base_map[c.base_column][1])
            for c in Z_COLUMNS if c.base_column in base_map}


# =============================================================================
# Observation
# =============================================================================
def _latest_date(conn, table: str):
    with conn.cursor() as cur:
        cur.execute(f"SELECT max(trade_date) FROM {table}")
        return cur.fetchone()[0]


def _buckets(conn, table: str, day) -> list:
    with conn.cursor() as cur:
        cur.execute(f"SELECT snapshot, count(*) FROM {table} "
                    f"WHERE trade_date = %s GROUP BY snapshot "
                    f"ORDER BY snapshot", (day,))
        return cur.fetchall()


def counts(conn, table: str, day, cols: list, snapshot: str) -> tuple:
    """(rows, {column: non_null}) for one bucket, over COMPARABLE tickers only.

    Comparable = the ticker also has a row at the baseline bucket that day.
    Without that restriction a ticker that failed to capture at 10:15 reads as
    every column being NULL at 10:15.
    """
    out, total = {}, 0
    for i in range(0, len(cols), CHUNK):
        part = cols[i:i + CHUNK]
        sel = ", ".join(f"count(m.{c}) AS c{j}" for j, c in enumerate(part))
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*), {sel} FROM {table} m "
                f"WHERE m.trade_date = %s AND m.snapshot = %s "
                f"  AND EXISTS (SELECT 1 FROM {table} b "
                f"              WHERE b.ticker = m.ticker "
                f"                AND b.trade_date = m.trade_date "
                f"                AND b.snapshot = %s)",
                (day, snapshot, BASELINE_SNAPSHOT))
            r = cur.fetchone()
        total = r[0]
        for j, c in enumerate(part):
            out[c] = r[1 + j]
    return total, out


def classify(base_rate, intra_rate, kind, threshold):
    """base_rate / intra_rate are NON-NULL shares.

    Hybrid counts as daily for COVERAGE: vrp_{t}d needs an ATM IV that is
    present at every bucket and an rv that is a daily constant, so a NULL gap
    is a bug rather than a thin-chain judgement call. Its VALUE is expected to
    move with the bucket, which the flatness check handles separately.
    """
    delta = base_rate - intra_rate          # >0 means worse intraday
    daily_like = kind in ("daily", "hybrid")
    if delta > threshold:
        return (BROKEN_DAILY if daily_like else BROKEN_SNAP), delta
    if delta < -threshold:
        return INVERTED, delta
    return (CARRIES if daily_like else SNAPSHOT_OK), delta


LABEL = {
    CARRIES:     "A  carries correctly (daily, flat across buckets)",
    BROKEN_DAILY: "B1 NULL INTRADAY AND SHOULD NOT BE (daily-derived — a bug)",
    BROKEN_SNAP: "B2 NULL intraday, snapshot-derived (judgement: thin chain, "
                 "or a bug)",
    SNAPSHOT_OK: "C  legitimately snapshot-specific (flat across buckets)",
    INVERTED:    "D  MORE null at the close than intraday (unexpected "
                 "direction)",
}


def audit(conn, table: str, day, colmap: dict, threshold: float,
          by_bucket: bool, verbose: bool) -> dict:
    buckets = _buckets(conn, table, day)
    have = {s for s, _ in buckets}
    if BASELINE_SNAPSHOT not in have:
        print(f"  {table}: no {BASELINE_SNAPSHOT} rows on {day} — nothing to "
              f"compare against")
        return {}
    intraday = sorted(s for s in have if s != BASELINE_SNAPSHOT)
    if not intraday:
        print(f"  {table}: only {BASELINE_SNAPSHOT} on {day} — pick a date "
              f"with intraday coverage")
        return {}

    cols = sorted(colmap)
    base_n, base_c = counts(conn, table, day, cols, BASELINE_SNAPSHOT)
    if not base_n:
        print(f"  {table}: no comparable rows at {BASELINE_SNAPSHOT}")
        return {}

    per_bucket = {}
    intra_tot, intra_cnt = 0, {c: 0 for c in cols}
    for s in intraday:
        n, c = counts(conn, table, day, cols, s)
        per_bucket[s] = (n, c)
        intra_tot += n
        for k in cols:
            intra_cnt[k] += c[k]

    print(f"\n=== {table} — {day} ===")
    print(f"  baseline {BASELINE_SNAPSHOT}: {base_n} comparable row(s)")
    print(f"  intraday: {len(intraday)} bucket(s), {intra_tot} row(s) "
          f"({intraday[0]} .. {intraday[-1]})")
    print(f"  materiality threshold: {threshold:.0%} of compared rows\n")

    groups: dict = {k: [] for k in LABEL}
    for c in cols:
        b = base_c[c] / base_n
        i = (intra_cnt[c] / intra_tot) if intra_tot else 0.0
        g, delta = classify(b, i, colmap[c][1], threshold)
        groups[g].append((c, b, i, delta, colmap[c][0]))

    for g in (BROKEN_DAILY, BROKEN_SNAP, INVERTED, CARRIES, SNAPSHOT_OK):
        rows = sorted(groups[g], key=lambda r: -abs(r[3]))
        print(f"  {LABEL[g]}   [{len(rows)}]")
        if not rows:
            print("      none")
            continue
        show = rows if (verbose or g in (BROKEN_DAILY, BROKEN_SNAP, INVERTED)) \
            else rows[:6]
        for c, b, i, d, helper in show:
            print(f"      {c:<28}{helper:<16}"
                  f"close {b:>6.1%}   intraday {i:>6.1%}   delta {d:>+7.1%}")
        if len(show) < len(rows):
            print(f"      ... and {len(rows) - len(show)} more "
                  f"(--verbose to list)")
        print()

    if by_bucket and (groups[BROKEN_DAILY] or groups[BROKEN_SNAP]):
        flagged = [r[0] for r in groups[BROKEN_DAILY] + groups[BROKEN_SNAP]]
        print(f"  per-bucket detail for the {len(flagged)} flagged column(s):")
        print(f"      {'column':<28}" + "".join(f"{s:>8}" for s in
                                                [BASELINE_SNAPSHOT] + intraday))
        for c in flagged[:40]:
            cells = f"{base_c[c] / base_n:>8.0%}"
            for s in intraday:
                n, cc = per_bucket[s]
                cells += f"{(cc[c] / n if n else 0):>8.0%}"
            print(f"      {c:<28}{cells}")
    return groups


def flatness(conn, table: str, day, colmap: dict) -> list:
    """The value-level check the NULL rate cannot make.

    A daily column can be non-NULL at every bucket and still be wrong: if it is
    recomputed per snapshot from something snapshot-specific, it will hold a
    DIFFERENT value in each bucket. NULL-rate auditing is blind to that, so
    this counts distinct values per (ticker, trade_date).

    Expected: one value per ticker per day. Two for the columns with an as-of
    split — the baseline row sits inside its own window while intraday buckets
    stop at T-1, so the close legitimately differs from the carried-forward
    value. Three or more is a real finding either way.

    Hybrids are excluded rather than given a higher limit: vrp_{t}d moves with
    every bucket's ATM IV, so it has as many values as there are buckets and no
    threshold on it would mean anything.
    """
    # PURE daily only. A hybrid like vrp_{t}d carries a snapshot IV term and is
    # SUPPOSED to differ between buckets; asserting flatness on it would flag
    # correct behaviour, which is worse than not checking.
    daily = sorted(c for c, (_h, k) in colmap.items() if k == "daily")
    if not daily:
        return []
    worst: dict = {}
    for i in range(0, len(daily), CHUNK):
        part = daily[i:i + CHUNK]
        inner = ", ".join(f"count(DISTINCT {c}) AS d{j}"
                          for j, c in enumerate(part))
        outer = ", ".join(f"max(d{j}) AS m{j}" for j in range(len(part)))
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {outer} FROM (SELECT ticker, {inner} FROM {table} "
                f"WHERE trade_date = %s GROUP BY ticker) s", (day,))
            r = cur.fetchone()
        for j, c in enumerate(part):
            worst[c] = r[j] or 0

    out = []
    for c, n in worst.items():
        # The as-of split set is derived in owner_map by running _spot_vol at
        # two buckets and diffing; anything in it legitimately holds the
        # carried-forward value and the close's.
        limit = 2 if c in _ASOF_SPLIT else 1
        if n > limit:
            out.append((c, n, limit, colmap[c][0]))
    return sorted(out, key=lambda r: -r[1])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", help="YYYY-MM-DD (default: latest)")
    ap.add_argument("--table", default="both",
                    choices=["equity_metrics", "equity_metrics_z", "both"])
    ap.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD,
                    help=f"materiality, share of rows "
                         f"(default {DEFAULT_THRESHOLD})")
    ap.add_argument("--by-bucket", action="store_true",
                    help="per-bucket breakdown for flagged columns")
    ap.add_argument("--verbose", action="store_true",
                    help="list every column, not just the flagged ones")
    args = ap.parse_args()

    base_map = owner_map()
    print("=== snapshot NULL audit ===")
    kinds = {}
    for _h, k in base_map.values():
        kinds[k] = kinds.get(k, 0) + 1
    print(f"  expectation derived from lib/metrics_compute: "
          f"{len(base_map)} base column(s) — "
          + ", ".join(f"{n} {k}" for k, n in sorted(kinds.items())))
    print(f"  as-of split (two values a day is correct): "
          f"{sorted(_ASOF_SPLIT) if _ASOF_SPLIT else 'none'}")
    # Both probes are synthetic, so both can go quiet without failing. Say so
    # rather than letting a silent "none" tighten the flatness limits and
    # manufacture findings.
    if not _ASOF_SPLIT:
        print("  WARNING: the as-of probe found no split. Either _spot_vol no "
              "longer has one\n           (fine) or the probe stopped "
              "exercising it (not fine) — in which case\n           the "
              "flatness limits below are one too strict and may report "
              "findings\n           that are correct behaviour.")
    if not kinds.get("hybrid"):
        print("  WARNING: no hybrid columns detected. If vrp_{t}d still mixes "
              "a snapshot IV\n           with a daily rv, the probe has "
              "stopped working and flatness will\n           flag the whole "
              "vrp family wrongly.")

    tables = ([args.table] if args.table != "both"
              else ["equity_metrics", "equity_metrics_z"])
    rc = 0
    with get_connection() as conn:
        for t in tables:
            colmap = base_map if t == "equity_metrics" else z_owner(base_map)
            day = (_date.fromisoformat(args.date) if args.date
                   else _latest_date(conn, t))
            if day is None:
                print(f"\n  {t} is empty.")
                continue
            g = audit(conn, t, day, colmap, args.threshold, args.by_bucket,
                      args.verbose)
            if g.get(BROKEN_DAILY) or g.get(BROKEN_SNAP):
                rc = 2
            if g and t == "equity_metrics":
                # Value-level, and only worth running on the base table: a z
                # column varies with its base by construction.
                flat = flatness(conn, t, day, colmap)
                print(f"\n  VALUE-LEVEL: daily columns holding more than one "
                      f"value per ticker/day   [{len(flat)}]")
                if not flat:
                    print("      none — every daily column is flat across the "
                          "session (spot_vol allowed two, its as-of split)")
                for c, n, limit, helper in flat:
                    print(f"      {c:<28}{helper:<16}up to {n} distinct "
                          f"value(s)   (allowed {limit})")
                    rc = 2

    print("\nRead-only — nothing was written.")
    if rc:
        print("Exit 2: at least one column is materially more NULL intraday.")
    return rc


if __name__ == "__main__":
    sys.exit(main())
