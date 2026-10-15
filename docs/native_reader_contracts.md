# Native Reader Contract Tests

These CPU tests prepare the native read bridge tracked in issue #138. They use
the checked-in local append-only Parquet fixture at snapshot 3 and independently
specified rows, including variable-width strings. They do not enable a Sirius
source or execute GPU operators.

Run through the normal entry point:

```sh
GEN=ninja make
make test_release
```

For a focused run after building `unittest_paimon`:

```sh
build/release/test/unittest_paimon '[native_reader]' --warn NoTests
```

## Qualified Contracts

| Check | Scope |
| --- | --- |
| Retained roots | Recheck complete root arrays/schemas after subsequent batches and EOF; release on another thread in both schema/array orders. |
| Serialized handover | Create on the caller, complete each worker read before the next read or release, and compare every row against the same-thread case's independent expected answers. This is not concurrent reader access. |
| End of stream | A null array is EOF; a synthetic zero-row root is not. The fixture does not generate a zero-row non-EOF batch, so progression through such batches is not qualified. |
| Missing data | Remove copied Parquet files after planning; reader creation or its first read must return an error, not successful EOF. Do not retry a failed reader. |
| Resource release | Keep native owners until all roots are released, then close/destroy readers. The query-specific native pool must return to zero usage. This is not whole-process allocation accounting. |

The selected native revision is
`53f9c86d45aabb0a6f1a379271da07d7a9f27a3d`. At that revision, retaining the
underlying native memory pool alone does not keep every Arrow pool adaptor
alive. The tests retain the reader tree and table/scan owners until root release;
they do not claim roots may outlive readers. Upstream paimon-cpp #260 changes
that contract at `885b2c00e7438d87c8e80512f0743f2bb8e3ce93`, which is not the
current gitlink. No dependency upgrade or replacement lifetime API is included.

Tests supply an inline executor and explicitly disable prefetch, multi-thread
row conversion, late materialization, read-ahead cache, Parquet pre-buffering
and Parquet decode threads. This selected configuration does not establish a
general no-background-work guarantee for other readers or options.

Primary-key merge, other formats/types, branch/schema evolution, hard planning
limits, native allocation refusal, cancellation/draining, cross-extension bridge
behavior and GPU import remain separate integration requirements. Passing these
tests must not be presented as completion of those requirements.
