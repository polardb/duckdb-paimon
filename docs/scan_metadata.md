# Bound scan metadata

`paimon_scan` exposes copied metadata through DuckDB's `get_bind_info` callback.
The path overload, warehouse/database/table overload and attached catalog scan
use the same contract. The callback performs no filesystem or catalog lookup.

Consumers recognize `ScanType::EXTERNAL` with `paimon.version = INTEGER 1`.
An unrecognized or unavailable payload must retain the original CPU scan/error
path. In particular, unavailable metadata does not mean an empty table.

| Option | DuckDB type | Meaning |
|---|---|---|
| `paimon.path` | VARCHAR | Bound table data path |
| `paimon.branch` | VARCHAR | Resolved branch |
| `paimon.schema_json` | VARCHAR | Retained read schema |
| `paimon.schema_id` | BIGINT | Retained read schema ID |
| `paimon.snapshot_id` | nullable BIGINT | Bound snapshot; NULL means readable empty at bind |
| `paimon.options` | LIST(STRUCT(key VARCHAR, value VARCHAR)) | Retained native options |
| `paimon.pruning` | LIST(STRUCT(op VARCHAR, field_index INTEGER, literal BIGINT)) | Optional integral conjuncts |

Validate the recognized payload before interpreting a NULL snapshot as empty.
All returned Values own their contents independently of the binding. Options can
contain credentials; consumers must not log them indiscriminately.

Pruning supports INTEGER/BIGINT comparisons `eq`, `ne`, `lt`, `le`, `gt`, `ge`.
The producer validates native field identity and literal type against the full
bound schema before exporting an atom. `field_index` is the full-table ordinal,
never the position in an optimized projection. The BIGINT literal carrier does
not widen the bound field's SQL type. AND can export supported conjuncts; an OR
subtree is omitted as a whole. An empty list means no optional pruning.

Native pruning is best effort. Consumers must retain the complete SQL residual,
derive names/types from the full bound schema, and remap reader predicates to the
final read projection. They own native plans/readers, Arrow roots and GPU import
lifetimes; no native object or reader service crosses this metadata interface.

The callback does not provide query-global allocation admission, reader quotas,
cancellation, module leases or GPU execution. Returning owned Values can allocate
before a consumer examines their size. These properties require separate consumer
and native integration checks; a post-copy size check does not bound producer
allocations. Producer metadata tests alone do not qualify those properties.

Before constructing payload Values, the producer limits the combined path, branch,
schema and option-key/value text to 1 MiB, options to 64, and predicate traversal
to 256 visits and depth 16 (the root has depth 0). Text must be valid UTF-8. AND
children are traversed; OR and other opaque operations are treated as leaves.
Exceeding this profile returns unversioned EXTERNAL metadata and preserves the
CPU path. These shape limits are not an allocation-byte quota or immutable
deployment guarantee; a consumer must separately qualify its allocation bound.
