# Bound-read descriptor bridge

The V1 descriptor contract is the first step of the Sirius Paimon integration.
The experimental V2 extension below adds native planning and batch reads, with
whole-call cancellation checks. It supplies no bounded planning admission, GPU
scan or CPU replay. DuckDB's existing CPU scan behavior is unchanged.

## Discovery and Trust

Both `paimon_scan` overloads and attached-table scans carry one module-local,
immutable `PaimonReadBridgeInfo` singleton in `TableFunction::function_info`.
Repeated function registration in the same loaded module shares this object.
The wrapper uses the matching DuckDB C++ ABI; this is not a version-independent
DuckDB extension API. A consumer must use the same DuckDB revision and compatible
compiler ABI as the loaded producer.

Consumers must first verify the provider and scan callbacks against a trusted
reference definition. A function name or a successful `accepts_bind` call is not
provider authentication. Only after that check may they interpret the wrapper,
check the API magic/version/size and required callbacks, and pass a live
`FunctionData` pointer to the provider. The consumer never casts private Paimon
bind data or calls paimon-cpp directly. This PR does not register a Sirius
connector or implement its trusted-reference lookup.

`TryGetPaimonReadBridge(candidate, trusted)` compares the scan/bind/init callbacks
and singleton wrapper identity before accessing the wrapper, then validates the
API prefix and every required callback. `trusted` must already be an unmodified
definition from an authenticated provider, with its module kept loaded. Passing
an arbitrary catalog entry as both arguments does not establish trust. Hidden
RTTI can differ across modules, so discovery does not use `dynamic_cast`.
The fixture owns LOAD and captures the unmodified registration; on Linux/macOS
it authenticates scan/bind and all bridge callback addresses with `dladdr`
against the exact loaded file. A production connector still needs its own
trusted-reference acquisition.

## Descriptor Contract

The C-compatible header `src/include/paimon_read_bridge.h` defines version 1:

- `accepts_bind`: accepts only a live Paimon scan binding, or returns zero for
  NULL or another live DuckDB binding type. Arbitrary or dangling pointers are
  invalid inputs, not a supported validation mechanism.
- `retain`: copies the immutable bound identity without metadata I/O, rebinding
  SQL or planning splits. Success transfers one opaque descriptor to the caller.
  It remains valid after the original bind data is destroyed. Failure leaves the
  output NULL and returns a structured status; no C++ exception crosses the
  callback. A caller-provided error buffer receives a truncated, NUL-terminated
  diagnostic when its capacity is nonzero.
- `describe`: exposes immutable borrowed views of the warehouse, requested
  database/table, resolved location/branch, schema ID, full native schema JSON
  and effective native options. The schema includes stable field IDs and types.
  `has_snapshot == 0` means the table was empty at binding, never "latest".
  Options alone cannot reconstruct a read: consumers must handle empty identity
  before any native planning, even if options have no `scan.snapshot-id`.
  An undersized output buffer is rejected without writing it. A valid-sized
  output is cleared on NULL-descriptor failure.
- `release`: releases one live descriptor using the producer's allocator; NULL
  is allowed. Do not release twice, release concurrently with another callback,
  or use borrowed views afterwards. Concurrent read-only `describe` calls are
  permitted while the owner remains alive.

Keep the extension code loaded until all callbacks and descriptors are finished.
The handle does not itself pin the dynamic library. Options may contain resolved
credentials: do not log, serialize or expose them to untrusted users. A retained
identity is not a retention lease, serialized-plan support or proof that files
still exist. It also does not certify native reader support on every branch,
catalog, type or scan mode.

## Acceptance and Tests

`make test_release`, `make test_debug` and `make test_reldebug` include
`paimon_check_bound_read_bridge`. The default host has built-in Paimon, so this
lane checks descriptor and native contracts, NOT cross-DSO qualification. The same tests in
the separate dynamic-only configuration below LOAD the real dynamic extension;
that consumer links only DuckDB, without Paimon/Arrow C++ libraries or a built-in
producer. On Linux/macOS the dynamic lane also checks that callback addresses
belong to the loaded extension. A missing producer must fail. The public C header
is compiled as C. Both lanes are required for bridge qualification; a passing
built-in lane does not substitute for dynamic evidence.

The `Dynamic Bound Read Bridge` workflow runs the focused dynamic-only target with
optimized Clang on Linux and macOS for pushes and pull requests. It retains test
reports, build configuration and failing warehouses from the same runner root;
successful warehouses are removed. Cloud backends are disabled
in this bridge lane; the normal distribution workflow covers their build.
No-tests and failed-child controls exercise the same CMake runner's failure path.
The common runner prints retained JUnit diagnostics on failure, including in the
normal distribution workflow. Its report-presence check is not XML validation;
test success and no-tests rejection rely on the pinned DuckDB Catch runner's
exit status. Qualification separately checks structured case/section results.

The focused target can be run from an existing build:

```sh
cmake --build build/release --target paimon_check_bound_read_bridge
```

For a host with no statically linked Paimon at all, configure a separate build
with the tracked dynamic-only extension configuration (plus the platform's usual
native dependency/toolchain options):

```sh
cmake -S duckdb -B build/bridge -DOVERRIDE_GIT_DESCRIBE=v2.0.0 -DDUCKDB_EXTENSION_CONFIGS="$PWD/test/extension_config_bridge.cmake"
cmake --build build/bridge --target paimon_check_bound_read_bridge
```

The tests check all three scan entry points, retained snapshot/options after the
binding is destroyed and another connection commits, empty-at-bind identity,
descriptor access after table metadata disappears, module-local info identity,
foreign/NULL bindings, undersized outputs and bounded diagnostics, and refusal
of deferred unsupported reads. These are CPU descriptor tests, not GPU execution
or native-reader cancellation/ownership qualification.

## Experimental native-read extension (V2)

V2 is a partial implementation of [#139](https://github.com/polardb/duckdb-paimon/issues/139),
layered on the retained descriptor contract from PR #135. It does **not** close
#139 or qualify bounded production planning. No SDK or DuckDB pin changes.

`TryGetPaimonNativeReadBridge` authenticates the same callback/singleton identity
as descriptor discovery, then checks the V2 prefix and all native callbacks.
The V1 descriptor layout and retain/describe/release semantics stay unchanged;
the V2 prefix advertises version 2 and the full V2 size. Updated descriptor
consumers accept either exact V1 or exact V2. An older V1-only consumer rejects
V2 safely. Recompile consumers with the new header to use the experimental API.
Provider authentication and matching DuckDB/compiler ABI remain the caller's
responsibility. Keep the producer module loaded until every handle and borrowed
Arrow view is released; this API cannot acquire or authenticate a module lease.

Call sequence:

1. Retain a bound descriptor through either `paimon_scan` overload or attached
   scan. Retain still does no metadata I/O or planning.
2. Set `PaimonPlanOptionsV1.struct_size` and request `create_plan`. **Every finite
   `planning_byte_limit`, including zero, is refused before native planning.**
   The pinned native API cannot bound its ordinary/transitive planning
   allocations. `UINT64_MAX` explicitly allows unbounded planning for controlled
   qualification. A completed plan's split count is not admission control.
3. Obtain opaque split handles using the returned count, then open readers.
   Each split retains the immutable plan; each reader retains its split,
   `TableRead`, pool and executor. Descriptors/plans/splits may be released once
   their child handles have been obtained. No latest snapshot lookup or replay
   is performed. Empty at bind yields an empty plan even after later writes.
4. `next_batch` returns `OK` with an opaque batch and borrowed **complete** Arrow
   array/schema roots, `EOF` with no output, or a distinct error status. Column
   zero is native `_VALUE_KIND`; subsequent fields follow retained schema order.
   Zero-row non-EOF batches remain `OK`, not EOF. This ABI has no projection or
   pushed predicate interface; consumers must apply their residual expressions.
5. Release the batch through `release_batch` before advancing that reader. A
   live batch refuses advancement, preserving the current roots. Never release,
   move or mutate a borrowed root or child. A batch retains the reader and pool
   after other handles release. The producer releases both complete roots before
   their C shells and before native reader/table/pool destruction.

Each reader serializes native advancement and root release. The caller must
serialize operations and release on the same handle; independent readers may
run concurrently. There is no thread-affine API promise or shared concurrent
reader support. Handles are type-specific, live, provider-owned capabilities,
not validated arbitrary addresses; release once, with NULL allowed.

Cancellation callbacks are borrowed only during each call. They must be
thread-safe, non-throwing, and must not re-enter the bridge. Checks occur before
and after whole native operations. Already-running native calls, filesystem I/O
and internal decoding cannot be interrupted. A cancellation after a successful
native batch releases its roots and publishes no output. A read error or
cancellation makes that reader terminal; later calls return its terminal status.
Resource refusal for an outstanding batch permits retry after its release.
All supplied output slots are cleared on entry. Native exception text is not
exported, to avoid exposing credential-bearing options or paths.

The experimental path accepts only append-only Parquet/ORC tables without data
evolution. It explicitly disables prefetch and multithreaded row conversion and
uses a synchronous executor. A local copy of the retained read options forces
`parquet.read.executor.thread-count=0` and
`parquet.read.enable-pre-buffer=false`, disabling the pinned Parquet global-CPU
pool resizing/decoding and asynchronous prebuffering paths. Descriptor views
keep their original options; these deliberate scheduling overrides do not
change snapshot/schema/query semantics. There is no performance-parity claim.
Accepted bridge-executor work completes inline,
including nested submission, so no accepted jobs remain to drain on return.
Pinned `ScanContextBuilder` construction/reset can still create transient
four-thread default executors before/after applying that custom executor; this
is not a claim of zero native threads. Other native formats, primary-key merge
readers and data-evolution topology are outside the API's accepted scope.
This bridge does not resize global pools, replace global executors or change
Arrow global thread settings. It does not certify a bound on all process
threads or remote-filesystem internals.

Run the tests through `paimon_check_bound_read_bridge`, including on the
`test/extension_config_bridge.cmake` dynamic-only host from the dynamic bridge
workflow. The consumer includes DuckDB and the C bridge headers, never native
Paimon/Arrow C++ headers or libraries. Tests compare 5000 integer payloads and
unique complete IDs for both formats and all three entry points, exercise old
and empty bound snapshots, missing retained snapshots/data, refusal before I/O,
whole-call cancellation, terminal statuses, worker handover and batch-held owner
lifetimes. This bounded fixture coverage does not establish general nested/null
or primary-key semantics, arbitrary allocation-failure cleanup, native
zero-row-batch progression, whole-program sanitization, Paimon GPU execution,
or exact Sirius-host compatibility. Those require separate qualification.
