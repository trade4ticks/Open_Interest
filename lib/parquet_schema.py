"""
Canonical-type enforcement for every parquet store in this project.

Deliberately tiny and dependency-light — pyarrow and logging, nothing else —
so that all five stores can import it without pulling in vendor clients,
config, or each other.

--- Why this exists --------------------------------------------------------

Every store declares an explicit `_SCHEMA` and writes through
`pa.Table.from_pandas(df, schema=_SCHEMA, preserve_index=False)`. That reads
like it forces the column types, and for a long time it did.

It stopped being sufficient across a pyarrow 23 -> 24 upgrade on the VPS: the
chain_snapshots store's 2026 files came out with `ticker`, `snapshot` and
`option_type` as `large_string` where `_SCHEMA` says `string` and every
earlier file has `string`. Same characters, wider offsets (int64 vs int32).

It went unnoticed because nothing at write time objected and pandas cannot
tell the two apart on read. It surfaced months later, when the monthly-layout
migration tried to `cast()` a file and halted — and all three columns are part
of that store's DEDUPE_KEYS, so a mixed pair breaks any Arrow-level operation
spanning them, on the type rather than on the data.

The lesson is not about that specific version. It is that the declared schema
was a REQUEST, honoured by the library version that happened to be installed,
and a request is not an invariant. This module makes it one: no table reaches
disk without its types having been checked against the schema the store
declares, whatever pandas and pyarrow decide to do next.

--- Behaviour --------------------------------------------------------------

`normalize_to_schema` is a no-op on the overwhelmingly common path where the
types already agree — one `Schema.equals`, no copy. When they do not, it casts
and logs at WARNING naming the columns, their incoming types, and the file, so
a divergent writer identifies itself in the run log instead of leaving a file
to be found by a migration a year later.

The cast raises on a genuinely lossy conversion rather than truncating, which
is the behaviour we want: a failed write is recoverable, a silently narrowed
value is not.

Incoming schema METADATA is carried onto the canonical schema. A bare
`cast(schema)` would drop it, and for these stores it is the pandas metadata
block that decides whether `pd.read_parquet` returns StringDtype or object —
so dropping it would trade a type divergence for a dtype divergence.
"""
from __future__ import annotations

import logging

import pyarrow as pa

log = logging.getLogger(__name__)


def schema_diff(schema: pa.Schema, table_schema: pa.Schema) -> list[str]:
    """Names of `schema`'s fields whose type differs in `table_schema`.

    Fields absent from `table_schema` are not reported here: a missing column
    is a different and much louder failure, and the cast below raises on it
    with a better message than this function could produce.
    """
    out = []
    for f in schema:
        if f.name not in table_schema.names:
            continue
        if table_schema.field(f.name).type != f.type:
            out.append(f.name)
    return out


def normalize_to_schema(table: pa.Table, schema: pa.Schema,
                        where: str = "") -> pa.Table:
    """Return `table` with exactly `schema`'s column types.

    Cheap and non-allocating when they already agree, which is the normal
    case. Any correction is logged at WARNING with enough detail to find the
    writer responsible.
    """
    if table.schema.equals(schema):
        return table

    changed = schema_diff(schema, table.schema)
    if changed:
        log.warning(
            "%snon-canonical column type(s) %s — casting to the store schema. "
            "Incoming: %s. The write is corrected, but a writer is producing "
            "types this store does not declare; check the pandas/pyarrow "
            "versions in use against the ones that wrote its neighbours.",
            f"{where}: " if where else "",
            ", ".join(changed),
            ", ".join(f"{n}={table.schema.field(n).type}"
                      for n in changed[:5]))
    # Carry the incoming metadata onto the canonical schema, but only when
    # there IS any: Schema.with_metadata(None) is not documented to be a
    # no-op and its behaviour has varied, which is the exact class of
    # version-dependent assumption that produced this module.
    target = schema
    md = table.schema.metadata
    if md:
        target = schema.with_metadata(md)
    return table.cast(target)
