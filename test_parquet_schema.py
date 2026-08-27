"""
test_parquet_schema.py — the canonical-type guard, for every store.

Regression test for a real incident: a pyarrow 23 -> 24 upgrade on the VPS
left `pa.Table.from_pandas(df, schema=_SCHEMA)` no longer forcing the declared
type on the string columns, so chain_snapshots' 2026 files were written with
ticker / snapshot / option_type as large_string. Nothing objected at write
time and pandas cannot tell large_string from string on read, so it surfaced
months later when a migration tried to cast one of those files — and all three
are dedupe-key columns.

What is tested here:

  * normalize_to_schema returns the table untouched when types already agree
    (the hot path — it must not copy)
  * it corrects the exact divergence that happened, for EVERY store's declared
    schema, not just the one that was bitten
  * it refuses rather than truncates when a cast would actually lose data
  * it carries schema metadata across, since that is what decides whether
    pd.read_parquet returns StringDtype or object

Needs pyarrow. Touches no store and writes no files.

Run:  python test_parquet_schema.py     (exit 1 on any failure)
"""
import sys

import pyarrow as pa

from lib.parquet_schema import normalize_to_schema, schema_diff

PASS, FAIL = [], []


def check(name, got, want):
    ok = got == want
    (PASS if ok else FAIL).append(name)
    print(f"  [{'ok  ' if ok else 'FAIL'}] {name:<56} got={got!r}")


def widen(schema: pa.Schema) -> pa.Schema:
    """The same schema with every string column as large_string.

    This is precisely what the upgraded pyarrow produced.
    """
    return pa.schema([
        (f.name, pa.large_string() if f.type == pa.string() else f.type)
        for f in schema
    ])


def empty_table(schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pydict(
        {f.name: pa.array([], type=f.type) for f in schema}, schema=schema)


# Every store that declares a canonical schema. Imported lazily inside the
# loop so one store failing to import does not hide the others.
STORES = [
    ("oi_raw",          "lib.parquet_store",        "_SCHEMA"),
    ("chain_eod",       "lib.chain_store",          "_SCHEMA"),
    ("chain_snapshots", "lib.chain_snapshot_store", "_SCHEMA"),
    ("chain_intraday",  "lib.chain_intraday_store", "_SCHEMA"),
    ("chain_live",      "lib.chain_intraday_store", "SCHEMA"),
    ("equity_1min",     "lib.equity_1min_store",    "_SCHEMA"),
    ("equity_1min:man", "lib.equity_1min_store",    "_MANIFEST_SCHEMA"),
]


print("\n=== 1. every store's schema survives the round trip ===")
schemas = {}
for label, module, attr in STORES:
    mod = __import__(module, fromlist=[attr])
    schema = getattr(mod, attr)
    schemas[label] = schema

    # (a) already canonical -> returned as-is, no copy.
    t = empty_table(schema)
    out = normalize_to_schema(t, schema, where=label)
    check(f"{label}: canonical table is passed through", out is t, True)

    # (b) the real divergence -> corrected.
    wide = widen(schema)
    n_str = sum(1 for f in schema if f.type == pa.string())
    if n_str == 0:
        check(f"{label}: (no string columns to widen)", True, True)
        continue
    check(f"{label}: has string column(s) to protect", n_str > 0, True)
    check(f"{label}: schema_diff names them", len(schema_diff(schema, wide)),
          n_str)
    out = normalize_to_schema(empty_table(wide), schema, where=label)
    check(f"{label}: large_string is corrected to the schema",
          out.schema.equals(schema), True)


print("\n=== 2. values survive the correction ===")
snap = schemas["chain_snapshots"]
wide = widen(snap)
row = {}
for f in wide:
    if f.name == "ticker":
        row[f.name] = pa.array(["SPY", "SPY"], type=f.type)
    elif f.name == "snapshot":
        row[f.name] = pa.array(["0945", "1545"], type=f.type)
    elif f.name == "option_type":
        row[f.name] = pa.array(["C", "P"], type=f.type)
    else:
        row[f.name] = pa.array([None, None], type=f.type)
t = pa.Table.from_pydict(row, schema=wide)
out = normalize_to_schema(t, snap, where="values")
check("type is now the store's", out.schema.field("ticker").type, pa.string())
check("ticker values unchanged", out.column("ticker").to_pylist(),
      ["SPY", "SPY"])
check("snapshot values unchanged", out.column("snapshot").to_pylist(),
      ["0945", "1545"])
check("option_type values unchanged", out.column("option_type").to_pylist(),
      ["C", "P"])
check("row count unchanged", out.num_rows, 2)


print("\n=== 3. a lossy cast raises rather than truncating ===")
# float64 -> int64 on a fractional value is exactly the kind of silent
# narrowing that would be far worse than a failed write.
src = pa.schema([("strike", pa.float64())])
dst = pa.schema([("strike", pa.int64())])
t = pa.Table.from_pydict({"strike": pa.array([1.5, 2.5])}, schema=src)
raised = None
try:
    normalize_to_schema(t, dst, where="lossy")
except Exception as exc:
    raised = type(exc).__name__
check("lossy cast raises", raised is not None, True)
check("and does not silently truncate", raised != "AssertionError", True)


print("\n=== 4. metadata is carried onto the canonical schema ===")
# Without this, a corrected file reads back with different pandas dtypes than
# its already-correct neighbours - trading a type divergence for a dtype one.
meta = {b"pandas": b'{"index_columns": []}'}
t = empty_table(widen(snap).with_metadata(meta))
out = normalize_to_schema(t, snap, where="meta")
check("metadata survives the cast", out.schema.metadata, meta)
check("types are still canonical", out.schema.field("ticker").type, pa.string())

# And a table that needs no cast keeps whatever it already had.
t2 = empty_table(snap.with_metadata(meta))
check("metadata survives the pass-through",
      normalize_to_schema(t2, snap).schema.metadata, meta)


print("\n" + "=" * 60)
print(f"PASSED {len(PASS)} / {len(PASS) + len(FAIL)}")
if FAIL:
    for f in FAIL:
        print("  -", f)
    sys.exit(1)
print("ALL GREEN")
