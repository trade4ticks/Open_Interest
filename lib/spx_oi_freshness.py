"""
lib/spx_oi_freshness.py — is a same-day SPX/SPXW OI snapshot today's
publication, or still yesterday's? The snapshot stage of run_spx_pipeline.py
writes a snapshot only when this says fresh.

THE TEST — front_5pct, the user's choice from the full-history measurement
--------------------------------------------------------------------------
Compare the snapshot with the stored T-1 chain on ONE expiration and the
strikes near the money:

    front  = the nearest expiration >= T in the STORED T-1 chain (never taken
             from the arriving snapshot: a stale chain's own front is
             yesterday's expiry, a different contract set)
    strikes within FRONT_MONEYNESS (5%) of spot, spot = the close of the
             session before T from index_ohlc (the last grid bar's close)

and take the share of those contracts whose open_interest is identical on both
sides. Near-the-money strikes in the front expiration trade every day, so on a
fresh chain their OI moves; a stale snapshot IS the T-1 chain and scores 100%.

Measured on the stored history, 2019-2026, worst fresh day (2019-10-10, a
genuine vendor duplicate, excluded): SPX 46.1% (contracts min 65, p05 116,
median 172); SPXW 35.3% (min 35, p05 107, median 162). One threshold for both
roots:

    front identical share > STALE_IDENTICAL_SHARE (70%)        -> stale, do not write
    front contracts < FRONT_MIN_CONTRACTS                       -> too few to score, do not write
    all-alive shared count outside SHARED_BAND x yesterday's    -> truncated / malformed, do not write

"Yesterday's" shared count is the same all-alive join between the stored T-1
and T-2 chains. Both sanity values were set by the user from the full-history
distribution --measure-freshness reports (neither refused a fresh day in seven
years at its earlier, tighter setting):

    SHARED_BAND floor 0.70: SPX's lowest ratios, 0.827-0.843, all fall on the
        first session after monthly opex, when an expiry rolls out of the shared
        set; a real truncation reads near 0.5. Ceiling 1.25.
    FRONT_MIN_CONTRACTS 20: SPXW's fewest was 35, on the stress days the data
        matters most (March 2020, 2022-06-14, 2025-04-07). At 20 contracts a
        stale verdict still needs 14 frozen, against a worst fresh score of 35%.

The spot dependency is deliberate and must be LOUD: with no index_ohlc close
for the session before T the pre-open job fails and writes nothing. There is
no fallback to a spot-free variant.

Only contracts alive on the day with OI > 0 are compared. No clock time
appears anywhere: content is the only gate.

Store schema: {SPX_OI_RAW_DIR}/{ROOT}/{YEAR}.parquet with columns trade_date,
expiration, strike, option_type, open_interest. Root is the directory.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

STALE_IDENTICAL_SHARE = 0.70      # one threshold, both roots (user, from the measurement)
FRONT_MONEYNESS = 0.05            # strikes within 5% of the T-1 close
FRONT_MIN_CONTRACTS = 20          # user, from the measurement (fewest fresh: SPXW 35, on stress days)
SHARED_BAND = (0.70, 1.25)        # user, from the measurement (lowest fresh: SPX 0.827, post-opex roll-off)
KEYS = ["expiration", "strike", "option_type"]
COLS = ["trade_date", "expiration", "strike", "option_type", "open_interest"]


def stored_chain(store: Path, root: str, day: date) -> pd.DataFrame:
    """The stored chain of one root on one trade_date (empty if absent)."""
    p = Path(store) / root.upper() / f"{day.year}.parquet"
    if not p.exists():
        return pd.DataFrame(columns=COLS)
    df = pd.read_parquet(p, columns=COLS, filters=[("trade_date", "==", day)])
    return normalise(df)


def normalise(df: pd.DataFrame) -> pd.DataFrame:
    """Same dtypes on both sides of a join, whatever produced the frame."""
    if df.empty:
        return pd.DataFrame(columns=COLS)
    out = df[COLS].copy()
    out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.date
    out["expiration"] = pd.to_datetime(out["expiration"]).dt.date
    out["strike"] = pd.to_numeric(out["strike"], errors="coerce").astype("float64")
    out["option_type"] = out["option_type"].astype(str)
    out["open_interest"] = pd.to_numeric(out["open_interest"], errors="coerce").astype("int64")
    return out


@dataclass
class Overlap:
    shared: int
    identical: int

    @property
    def share(self) -> float:
        return self.identical / self.shared if self.shared else math.nan


def _join(new: pd.DataFrame, old: pd.DataFrame, keep) -> Overlap:
    def live(df):
        df = normalise(df)
        df = df[(df["open_interest"] > 0) & keep(df)]
        return df[KEYS + ["open_interest"]]
    m = live(new).merge(live(old), on=KEYS, suffixes=("_new", "_old"))
    return Overlap(shared=len(m), identical=int((m["open_interest_new"] == m["open_interest_old"]).sum()))


def overlap(new: pd.DataFrame, old: pd.DataFrame, alive_on: date) -> Overlap:
    """Contracts alive on `alive_on` with OI > 0 in both chains."""
    return _join(new, old, lambda df: df["expiration"] >= alive_on)


def front_expiration(prev_chain: pd.DataFrame, today: date) -> date | None:
    """The nearest expiration >= today in the STORED T-1 chain (OI > 0)."""
    c = normalise(prev_chain)
    c = c[(c["expiration"] >= today) & (c["open_interest"] > 0)]
    return None if c.empty else min(c["expiration"])


def front_overlap(new: pd.DataFrame, old: pd.DataFrame, front: date, spot: float,
                  moneyness: float = FRONT_MONEYNESS) -> Overlap:
    """Contracts of expiration `front` with |strike / spot - 1| <= moneyness, OI > 0 on both sides."""
    return _join(new, old, lambda df: (df["expiration"] == front)
                 & ((df["strike"] / spot - 1.0).abs() <= moneyness))


@dataclass
class Verdict:
    root: str
    fresh: bool
    reason: str
    rows: int
    shared: int                 # all-alive shared contracts (the truncation check)
    ref_shared: int             # the same join for T-2 -> T-1
    front: date | None
    front_contracts: int        # what the score is computed from
    identical_share: float      # THE score: front, within 5% of spot


def assess(root: str, snap: pd.DataFrame, prev_chain: pd.DataFrame, prev2_chain: pd.DataFrame,
           today: date, prev_day: date, spot: float) -> Verdict:
    """snap = today's snapshot; prev_chain / prev2_chain = stored T-1 / T-2 chains;
    spot = the close of the session before today (index_ohlc). The caller must
    have failed already if spot is unknown — this never runs without it."""
    if spot is None or not (spot > 0):
        raise ValueError(f"assess() needs the T-1 close; got {spot!r}")
    front = front_expiration(prev_chain, today)
    v = dict(root=root, rows=len(snap), shared=0, ref_shared=0, front=front, front_contracts=0,
             identical_share=math.nan)
    if snap.empty:
        return Verdict(fresh=False, reason="empty snapshot", **v)
    now = overlap(snap, prev_chain, alive_on=today)
    ref = overlap(prev_chain, prev2_chain, alive_on=prev_day)
    v.update(shared=now.shared, ref_shared=ref.shared)
    if ref.shared == 0:
        return Verdict(fresh=False, reason="no reference: stored T-1 and T-2 chains share no contracts", **v)
    lo, hi = SHARED_BAND[0] * ref.shared, SHARED_BAND[1] * ref.shared
    if not lo <= now.shared <= hi:
        return Verdict(fresh=False, reason=f"shared count {now.shared:,} outside {SHARED_BAND[0]:.2f}-"
                                           f"{SHARED_BAND[1]:.2f} x yesterday's {ref.shared:,} "
                                           f"(truncated or malformed response?)", **v)
    if front is None:
        return Verdict(fresh=False, reason=f"the stored T-1 chain has no expiration on or after {today}", **v)
    fo = front_overlap(snap, prev_chain, front, spot)
    v.update(front_contracts=fo.shared, identical_share=fo.share)
    if fo.shared < FRONT_MIN_CONTRACTS:
        return Verdict(fresh=False, reason=f"only {fo.shared} front-expiration contracts ({front}) within "
                                           f"{FRONT_MONEYNESS:.0%} of {spot:,.2f} on both sides "
                                           f"(< {FRONT_MIN_CONTRACTS}); too few to score", **v)
    if fo.share > STALE_IDENTICAL_SHARE:
        return Verdict(fresh=False, reason=f"STALE: {fo.share:.1%} of {fo.shared} front contracts ({front}, "
                                           f"within {FRONT_MONEYNESS:.0%} of spot) carry T-1's exact OI "
                                           f"(> {STALE_IDENTICAL_SHARE:.0%})", **v)
    return Verdict(fresh=True, reason=f"fresh: {fo.share:.1%} of {fo.shared} front contracts ({front}) "
                                      f"identical to T-1 (<= {STALE_IDENTICAL_SHARE:.0%})", **v)


def measure(store: Path, root: str, days: list[date]) -> pd.DataFrame:
    """Identical share and shared count for each consecutive pair of stored
    days — the calibration the user ran for SPXW, for any root."""
    rows = []
    chains = {d: stored_chain(store, root, d) for d in days}
    for a, b in zip(days, days[1:]):
        if chains[a].empty or chains[b].empty:
            continue
        o = overlap(chains[b], chains[a], alive_on=b)
        rows.append({"day": b, "prev": a, "shared": o.shared, "identical_share": o.share})
    return pd.DataFrame(rows)


# --- Measurement over the full history, by contract subset (read-only) -----------------
#
# The FRONT EXPIRATION of a day pair (T-1 -> T) is the nearest expiration >= T in
# the STORED T-1 chain: the contracts still alive on T, chosen without looking at
# the newer chain. A stale chain's own front is yesterday's expiry, a different
# contract set, so selecting from the arriving chain would join on almost nothing
# exactly when it matters. Both sides are then compared on that one expiration.
# Moneyness is against the close of the session before T (index_ohlc), which no
# stage of the morning run produces. Nothing here reads the volume store.

VARIANTS = {
    "a_all":       "TRUE",                                   # baseline: the current test
    "front_1pct":  "is_front AND ABS(mny) <= 0.01",
    "front_2pct":  "is_front AND ABS(mny) <= 0.02",
    "front_5pct":  "is_front AND ABS(mny) <= 0.05",
    "front_all":   "is_front",                               # no spot needed
}


def measure_variants(store: Path, root: str, spot: pd.DataFrame, sessions: list[date],
                     memory_limit: str = "3GB") -> pd.DataFrame:
    """One row per (day T, variant): contracts scored and identical share, for every
    stored day T whose predecessor in the store is the previous NYSE session.
    Also returns the front expiration chosen for each pair (column `front`).
    spot = [trade_date T, spot] with spot = the close of the session before T."""
    import duckdb
    files = sorted(p.as_posix() for p in (Path(store) / root.upper()).glob("*.parquet") if p.stem.isdigit())
    if not files:
        return pd.DataFrame()
    prev_of = pd.DataFrame({"trade_date": sessions[1:], "prev_session": sessions[:-1]})
    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"SET memory_limit = '{memory_limit}'")
        con.register("prev_df", prev_of)
        con.register("spot_df", spot)
        flist = "[" + ", ".join("'" + f.replace("'", "''") + "'" for f in files) + "]"
        cols = ",\n".join(
            f"COUNT(*) FILTER (WHERE {cond}) AS n__{k}, "
            f"SUM(CASE WHEN same THEN 1 ELSE 0 END) FILTER (WHERE {cond}) AS same__{k}"
            for k, cond in VARIANTS.items())
        sql = f"""
        WITH c AS (
            SELECT trade_date, expiration, strike, option_type, open_interest
            FROM read_parquet({flist}) WHERE open_interest > 0
        ),
        days AS (
            SELECT trade_date, LAG(trade_date) OVER (ORDER BY trade_date) AS prev
            FROM (SELECT DISTINCT trade_date FROM c)
        ),
        fronts AS (
            SELECT d.trade_date, MIN(o.expiration) AS front
            FROM days d JOIN c o ON o.trade_date = d.prev AND o.expiration >= d.trade_date
            GROUP BY d.trade_date
        ),
        j AS (
            SELECT d.trade_date,
                   (n.expiration - d.trade_date)            AS dte,
                   n.strike / s.spot - 1.0                  AS mny,
                   n.expiration = f.front                   AS is_front,
                   n.open_interest = o.open_interest        AS same
            FROM days d
            JOIN prev_df p ON p.trade_date = CAST(d.trade_date AS DATE) AND p.prev_session = CAST(d.prev AS DATE)
            JOIN spot_df s ON s.trade_date = CAST(d.trade_date AS DATE)
            JOIN c n ON n.trade_date = d.trade_date AND n.expiration >= d.trade_date
            JOIN fronts f ON f.trade_date = d.trade_date
            JOIN c o ON o.trade_date = d.prev AND o.expiration = n.expiration
                    AND o.strike = n.strike AND o.option_type = n.option_type
        )
        SELECT j.trade_date, ANY_VALUE(f.front) AS front, {cols}
        FROM j JOIN fronts f USING (trade_date) GROUP BY j.trade_date ORDER BY j.trade_date
        """
        wide = con.execute(sql).df()
        n_days = con.execute(f"SELECT COUNT(DISTINCT trade_date) - 1 FROM read_parquet({flist}) "
                             f"WHERE open_interest > 0").fetchone()[0]
    finally:
        con.close()
    rows = []
    for k in VARIANTS:
        n = wide[f"n__{k}"].fillna(0).astype(int)
        same = wide[f"same__{k}"].fillna(0)
        rows.append(pd.DataFrame({"trade_date": pd.to_datetime(wide["trade_date"]).dt.date, "variant": k,
                                  "front": pd.to_datetime(wide["front"]).dt.date, "contracts": n, "identical_share": (same / n.where(n > 0)).astype(float)}))
    out = pd.concat(rows, ignore_index=True)
    out.attrs["stored_pairs"] = int(n_days)
    return out
