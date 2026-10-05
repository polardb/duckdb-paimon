# Bound scan consistency

## What is retained

Binding resolves a data-table scan to one snapshot, or to an explicit
empty-at-bind state. The binding owns the table location, effective read options
and complete read schema. Statistics, filtered split planning and each fresh
reader use that retained description, not a new lookup of the latest snapshot.

```text
Bind table at S1 ----> retained { S1, schema, location, options }
                               |           |           |
Writer commits S2              |           |           |
                               v           v           v
                          statistics     splits       rows
                             at S1        at S1       at S1

New binding --------> S2
```

Both `paimon_scan` overloads and attached-table scans follow this rule. An
empty binding stays empty after the first commit. Readers do not share consumed
cursors: a fresh execution state can read the same binding again. A timestamp
selector is resolved to a concrete ID, not reevaluated during execution.

Historical data uses the latest schema observed at bind, matching the existing
schema policy; it does not reconstruct the snapshot's historical SQL schema.
Schema/location changes detected during binding fail with a retry diagnostic.
An attached table whose cached column definition differs from the scan schema
is rejected with a detach/reattach diagnostic. This compares field identities,
names, types (including nested fields and nullability), partition keys and
primary keys. Option/comment-only changes do not require reattachment. Already
bound scans keep their owned schema and options.

## SQL behavior and limits

- Each prepared SQL execution obtains a fresh binding. A latest read can see
  new commits on the next `EXECUTE`; an explicit snapshot ID still selects that
  ID. Retaining a callback binding is different from reexecuting SQL `PREPARE`.
- `EXPLAIN` shows `Bound Snapshot` and `Read Schema` from the stored description.
- Binding now constructs an unfiltered native scan plan and rereads schema and
  location. This adds metadata/manifest work before execution. `PREPARE`,
  `EXPLAIN` and `DESCRIBE` can therefore fail earlier for invalid selectors or
  unavailable metadata. A native `NotImplemented` status without an explicit
  version is instead retained as an unreadable binding: `DESCRIBE` and view
  creation work, but statistics and reader initialization rethrow that status.
  Such a binding is never treated as empty and never retried against latest.
  Errors for explicit versions, invalid inputs and I/O are not deferred.
  `PREPARE`/`EXPLAIN` can still fail if optimization requests statistics.
- Initial support is default-branch batch scanning. Non-main branches, fallback
  branches, tags and unsupported scan modes are explicitly rejected. Conflicting
  effective timestamp/ID selectors are rejected, including table options.
  An explicit scan mode conflicting with a selector is also rejected; only
  an absent/default mode is normalized. Persistent fallback-branch options
  now fail even where the native scanner used to ignore them. An explicit ID
  plus a persisted timestamp selector is an error rather than ID precedence.
- Missing/expired snapshots or inconsistent plan IDs fail rather than advancing
  to latest. Optional native count-reader failures can still yield unknown
  statistics, but snapshot-planning errors propagate.
- Global-index pruning is disabled only for data-evolution tables. At this native
  pin that nested planner reloads the latest schema despite `SetTableSchema`.
  That can prune using the wrong field identity after a rename. Scan predicates,
  partition/file pruning and DuckDB residual filters remain active, but scalar
  data-evolution global-index acceleration is unavailable until the native API
  can preserve the supplied schema throughout index planning. Primary-key sorted-index pruning remains
  enabled because that planner honors the supplied schema and selected snapshot.
- There are no retention leases, cross-table transactions, serialized-plan
  guarantees, GPU readers or new primary-key merge guarantees in this change.
  Concurrent deletion can still make a retained snapshot unreadable. Table
  drop/recreation with identical metadata is not a supported identity guarantee.
  Separate references to the same table within a self-join, UNION or subquery
  bind independently and can see different snapshots in one statement.

## Tests

Build using the repository's normal dependencies and submodule pins, then run:

```sh
make test_release
cmake --build build/release --target paimon_check_snapshot_binding
./build/release/test/unittest 'test/sql/paimon_snapshot_binding.test'
```

`make test_release`, `make test_debug` and `make test_reldebug` run the callback
target before SQLLogicTests. CI jobs that invoke these targets inherit it;
this wiring alone is not evidence that a public CI job has executed.

The callback executable retains the registered function's real binding while a
separate writer commits, then invokes statistics/init/read without rebinding.
It checks typed full rows, counts, empty tables, missing snapshots, fresh readers
after partial consumption, schema ownership and timestamp selection across
all three entry forms. Catalog `AT` selection and SQL prepared rebinding have
SQLLogicTests. Tests do not include production `.cpp` files or inspect private
bind structures. The schema tests stage metadata in private copies. The indexed
test adds a nullable integer field and then exchanges its name with an indexed
integer field using authored schemas under `test/cpp/fixtures`; data/index bytes
are unchanged. These are controlled visibility tests, not writer-protocol tests.
Retained primary-key upserts and partition predicates have separate cases.
The PK-index case uses the pinned native submodule's source-backed BTree fixture
and checks raw callback rows, without a residual filter masking lost pruning.
Its snapshot publication is controlled fixture staging, not a normal commit.
The SQL file opts out of alternative verification because plan serialization
may rebind without retaining this snapshot; serialization support is separate.

Each run has a unique warehouse. Failed inputs are kept for diagnosis; success
removes only that run's directory. Nonzero exit, timeout and process-launch
failures propagate through CMake and Make. Run SQLLogicTests from the repository
root, and do not run two suites concurrently against the shared `data` directory.
