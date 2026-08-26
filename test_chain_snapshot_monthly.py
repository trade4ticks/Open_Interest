"""
test_chain_snapshot_monthly.py — the monthly-layout primitives.

The store moved from {ticker}/{YYYY}.parquet to {ticker}/{YYYYMM}.parquet to
bound the append's peak memory. Everything here is about the keying and the
file discovery that change introduced, because those are what silently do the
wrong thing rather than crashing:

  * a month key must never collide with or be mistaken for a year
  * list_months must not read a leftover year file as a month, and
    list_legacy_year_files must find it so callers can SAY it is being skipped
  * a write batch must map to exactly one file, or every month gets merged
    twice per run

Uses a temporary store directory; touches no real data and needs no vendor.

Run:  python test_chain_snapshot_monthly.py     (exit 1 on any failure)
"""
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

import lib.chain_snapshot_store as S
from lib.chain_fetch_common import chunk_range

PASS, FAIL = [], []


def check(name, got, want):
    ok = got == want
    (PASS if ok else FAIL).append(name)
    print(f"  [{'ok  ' if ok else 'FAIL'}] {name:<52} got={got!r}")


# Point the store at a scratch directory for the discovery tests.
ROOT = Path(tempfile.mkdtemp()) / "chain_snapshots"
ROOT.mkdir(parents=True)
S.CHAIN_SNAPSHOTS_DIR = ROOT


print("\n=== 1. month keys are YYYYMM, zero-padded, year-safe ===")
check("January", S.month_key(date(2024, 1, 15)), 202401)
check("October is 202410, not 20241", S.month_key(date(2024, 10, 1)), 202410)
check("December", S.month_key(date(2024, 12, 31)), 202412)
check("filename zero-pads", S.month_path("SPY", 202401).name, "202401.parquet")
check("filename for October", S.month_path("SPY", 202410).name,
      "202410.parquet")
check("ticker dir is upper", S.month_path("spy", 202401).parent.name, "SPY")
# The property that matters: a month key is always 6 digits, so it can never
# be confused with a 4-digit year stem by list_months / list_legacy_year_files.
check("every month of a year is 6 digits",
      all(len(str(S.month_key(date(2024, m, 1)))) == 6 for m in range(1, 13)),
      True)


print("\n=== 2. months_between covers the range, inclusive ===")
check("within one month",
      S.months_between(date(2024, 3, 4), date(2024, 3, 29)), {202403})
check("straddling a month end",
      S.months_between(date(2024, 1, 20), date(2024, 2, 18)),
      {202401, 202402})
check("straddling a year end",
      S.months_between(date(2023, 12, 20), date(2024, 2, 3)),
      {202312, 202401, 202402})
check("a full year is 12",
      len(S.months_between(date(2024, 1, 1), date(2024, 12, 31))), 12)
check("a single day is one month",
      S.months_between(date(2024, 7, 4), date(2024, 7, 4)), {202407})
check("seven years is 84",
      len(S.months_between(date(2019, 1, 1), date(2025, 12, 31))), 84)


print("\n=== 3. discovery ignores what is not a month file ===")
d = ROOT / "SPY"
d.mkdir()
for name in ("202401.parquet",       # a month
             "202410.parquet",       # a month, two-digit
             "202412.parquet",       # a month
             "2024.parquet",         # LEGACY year file
             "2025.parquet",         # LEGACY year file
             "202413.parquet",       # month 13 does not exist
             "202400.parquet",       # month 0 does not exist
             "20240101.parquet",     # a session file, wrong store
             "202402.parquet.tmp",   # in flight, not committed
             "notes.txt"):
    (d / name).write_text("x")

check("only real months listed", S.list_months("SPY"),
      [202401, 202410, 202412])
check("legacy year files surfaced",
      [p.name for p in S.list_legacy_year_files("SPY")],
      ["2024.parquet", "2025.parquet"])
check("unknown ticker lists nothing", S.list_months("NOPE"), [])
check("unknown ticker has no legacy", S.list_legacy_year_files("NOPE"), [])

d2 = ROOT / "QQQ"
d2.mkdir()
for name in ("202401.parquet", "202402.parquet"):
    (d2 / name).write_text("x")
check("a migrated ticker trips no legacy gate",
      S.list_legacy_year_files("QQQ"), [])
check("a migrated ticker lists its months", S.list_months("QQQ"),
      [202401, 202402])


print("\n=== 4. snapped batches map to exactly one file each ===")
b = chunk_range(date(2024, 1, 20), date(2024, 3, 10), 30, snap_month=True)
check("one file per batch",
      all(len(S.months_between(a, z)) == 1 for a, z in b), True)
check("starts at the range start", b[0][0], date(2024, 1, 20))
check("ends at the range end", b[-1][1], date(2024, 3, 10))
check("contiguous, no gap or overlap",
      all(b[i][1] + timedelta(days=1) == b[i + 1][0]
          for i in range(len(b) - 1)), True)
check("leap February is whole",
      chunk_range(date(2024, 2, 1), date(2024, 2, 29), 60, snap_month=True),
      [(date(2024, 2, 1), date(2024, 2, 29))])
check("December rolls into January",
      chunk_range(date(2024, 12, 15), date(2025, 1, 15), 60, snap_month=True),
      [(date(2024, 12, 15), date(2024, 12, 31)),
       (date(2025, 1, 1), date(2025, 1, 15))])

inside = chunk_range(date(2024, 1, 1), date(2024, 1, 31), 10, snap_month=True)
check("max_days still bounds inside a month", len(inside), 4)
check("no window exceeds max_days",
      max((z - a).days + 1 for a, z in inside), 10)

# The default must be untouched: chain_intraday still uses it unsnapped.
check("default still crosses months",
      any(a.month != z.month for a, z in
          chunk_range(date(2024, 1, 20), date(2024, 3, 10), 30)), True)

# A whole year, snapped, is exactly one batch per month with a generous cap.
year = chunk_range(date(2024, 1, 1), date(2024, 12, 31), 365, snap_month=True)
check("a snapped year is 12 batches", len(year), 12)
check("and covers every month",
      sorted(S.month_key(a) for a, _ in year),
      [202401, 202402, 202403, 202404, 202405, 202406,
       202407, 202408, 202409, 202410, 202411, 202412])


print("\n=== 5. schema normalisation decides safely ===")
import pyarrow as pa
from lib.chain_snapshot_store import _SCHEMA
from migrate_chain_snapshots_to_monthly import resolve_target_schema


def _swap(**by_name):
    """_SCHEMA with some fields' types replaced."""
    return pa.schema([(f.name, by_name.get(f.name, f.type)) for f in _SCHEMA])


t, norm, unsafe = resolve_target_schema(_SCHEMA)
check("identical schema needs nothing", (norm, unsafe), ([], []))

# The real divergence: 2026's files stored the three string columns wide.
wide = _swap(ticker=pa.large_string(), snapshot=pa.large_string(),
             option_type=pa.large_string())
t, norm, unsafe = resolve_target_schema(wide)
check("large_string is normalised", norm,
      ["ticker", "snapshot", "option_type"])
check("and nothing is unsafe", unsafe, [])
check("target uses the store's type", t.field("ticker").type, pa.string())
check("target keeps the other columns",
      t.field("strike").type, _SCHEMA.field("strike").type)

# Lossy differences must halt, not be guessed at.
check("float32 for float64 is unsafe",
      resolve_target_schema(_swap(strike=pa.float32()))[2], ["strike"])
check("date64 for date32 is unsafe",
      resolve_target_schema(_swap(trade_date=pa.date64()))[2], ["trade_date"])
check("a coarser timestamp is unsafe",
      resolve_target_schema(_swap(timestamp=pa.timestamp("s")))[2],
      ["timestamp"])
check("int for float is unsafe",
      resolve_target_schema(_swap(bid=pa.int64()))[2], ["bid"])

# A mix reports both, and the unsafe one is what stops the run.
mixed = _swap(ticker=pa.large_string(), strike=pa.float32())
t, norm, unsafe = resolve_target_schema(mixed)
check("mixed: safe listed", norm, ["ticker"])
check("mixed: unsafe listed", unsafe, ["strike"])

# Metadata must survive, or normalised files read back with different pandas
# dtypes than their already-correct neighbours.
check("source metadata is carried onto the target",
      resolve_target_schema(wide.with_metadata({b"pandas": b"{}"}))[0].metadata,
      {b"pandas": b"{}"})


print("\n" + "=" * 60)
print(f"PASSED {len(PASS)} / {len(PASS) + len(FAIL)}")
if FAIL:
    for f in FAIL:
        print("  -", f)
    sys.exit(1)
print("ALL GREEN")
