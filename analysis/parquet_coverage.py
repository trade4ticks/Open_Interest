"""Step 1c: is the tape actually on disk for every ticker-day I traded, and
how big is it. Read-only; opens no parquet, only stats the files.

WORTH RUNNING BEFORE THE ANALYSIS, not after. A symbol-day with no parquet
contributes no metrics, and if that absence is discovered only when the output
CSV has blank rows it looks like a metric failure rather than a coverage gap.
The scalp universe is a ranked subset -- a name traded on a discretionary
whim need never have been in it -- so missing days are expected, and the
question is how many.

The byte total is the runtime estimate. Compute is dominated by decompressing
and preparing each symbol-day once, not by the per-episode windows.
"""
from __future__ import annotations

import argparse
import pathlib
import sys

import pandas as pd

# The analysis lives beside the package it reads, not inside it, so the repo
# root goes on the path here rather than being demanded as PYTHONPATH at every
# invocation. Placed before the `scalp` import deliberately.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from scalp import store


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("csv", nargs="?", default="fills_dump.csv")
    a = ap.parse_args()

    df = pd.read_csv(a.csv, parse_dates=["entry_ts"])
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    rows = []
    for (d, sym), grp in df.groupby(["trade_date", "symbol"], sort=True):
        p = store.day_path(sym, d)
        ok = p.exists() and p.stat().st_size > 0
        rows.append({"trade_date": d, "symbol": sym, "trips": len(grp),
                     "present": ok, "mb": p.stat().st_size / 1e6 if ok else 0.0})
    inv = pd.DataFrame(rows)

    have = inv[inv["present"]]
    miss = inv[~inv["present"]]
    print(f"ticker-days traded   {len(inv)}")
    print(f"  parquet present    {len(have)}  ({have['trips'].sum()} trips)")
    print(f"  parquet MISSING    {len(miss)}  ({miss['trips'].sum()} trips)")
    print(f"total parquet        {have['mb'].sum():.1f} MB  "
          f"(median {have['mb'].median():.2f} MB/symbol-day)")
    if len(miss):
        print("\nmissing -- these ticker-days can produce no metrics:")
        print(miss[["trade_date", "symbol", "trips"]].to_string(index=False))


if __name__ == "__main__":
    sys.exit(main())
