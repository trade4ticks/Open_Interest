"""
lib/spx_oi_freshness.py — is a same-day SPX/SPXW OI snapshot today's
publication, or still yesterday's? The pre-open tier of run_spx_pipeline.py
writes a snapshot only when this says fresh.

THE TEST (the user's rule, measured on the VPS)
-----------------------------------------------
Join the snapshot to the stored T-1 chain on (expiration, strike,
option_type) and take the share of shared contracts whose open_interest is
identical. Across 52 consecutive SPXW day pairs from 2026-09-01 that share
ran 40.6% - 60.4% (median 55.5%), on 12,900 - 14,500 shared contracts a day.
A stale snapshot IS the T-1 chain, so it scores ~100%.

    identical share > STALE_IDENTICAL_SHARE (0.85)      -> stale, do not write
    shared count outside SHARED_BAND x yesterday's      -> malformed / truncated, do not write

"Yesterday's" shared count is the same join between the stored T-1 and T-2
chains, so a truncated response cannot pass on a small sample.

Only contracts still alive on the day are compared (expiration >= that day):
the snapshot endpoint keeps yesterday's expiries for a while (vendor purge
lag, ~75% of Tue-Fri in the 120-ticker store) while the history files mostly
do not, and those contracts would sit in the join with frozen, identical OI.
Zero OI is excluded on both sides; neither endpoint parser keeps it anyway.

No clock time appears anywhere: content is the only gate.

Store schema: {SPX_OI_RAW_DIR}/{ROOT}/{YEAR}.parquet with columns trade_date,
expiration, strike, option_type, open_interest. Root is the directory.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

STALE_IDENTICAL_SHARE = 0.85
SHARED_BAND = (0.80, 1.25)
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


def overlap(new: pd.DataFrame, old: pd.DataFrame, alive_on: date) -> Overlap:
    """Contracts alive on `alive_on` with OI > 0 in both chains."""
    def live(df):
        df = normalise(df)
        return df[(df["expiration"] >= alive_on) & (df["open_interest"] > 0)][KEYS + ["open_interest"]]
    m = live(new).merge(live(old), on=KEYS, suffixes=("_new", "_old"))
    return Overlap(shared=len(m), identical=int((m["open_interest_new"] == m["open_interest_old"]).sum()))


@dataclass
class Verdict:
    root: str
    fresh: bool
    reason: str
    rows: int
    shared: int
    identical_share: float
    ref_shared: int


def assess(root: str, snap: pd.DataFrame, prev_chain: pd.DataFrame, prev2_chain: pd.DataFrame,
           today: date, prev_day: date) -> Verdict:
    """snap = today's snapshot; prev_chain / prev2_chain = stored T-1 / T-2 chains."""
    if snap.empty:
        return Verdict(root, False, "empty snapshot", 0, 0, math.nan, 0)
    now = overlap(snap, prev_chain, alive_on=today)
    ref = overlap(prev_chain, prev2_chain, alive_on=prev_day)
    lo, hi = SHARED_BAND[0] * ref.shared, SHARED_BAND[1] * ref.shared
    v = dict(root=root, rows=len(snap), shared=now.shared, identical_share=now.share, ref_shared=ref.shared)
    if ref.shared == 0:
        return Verdict(fresh=False, reason="no reference: stored T-1 and T-2 chains share no contracts", **v)
    if not lo <= now.shared <= hi:
        return Verdict(fresh=False, reason=f"shared count {now.shared:,} outside {SHARED_BAND[0]:.2f}-"
                                           f"{SHARED_BAND[1]:.2f} x yesterday's {ref.shared:,} "
                                           f"(truncated or malformed response?)", **v)
    if now.share > STALE_IDENTICAL_SHARE:
        return Verdict(fresh=False, reason=f"STALE: {now.share:.1%} of shared contracts carry T-1's exact OI "
                                           f"(> {STALE_IDENTICAL_SHARE:.0%})", **v)
    return Verdict(fresh=True, reason=f"fresh: {now.share:.1%} identical to T-1 "
                                      f"(<= {STALE_IDENTICAL_SHARE:.0%})", **v)


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
