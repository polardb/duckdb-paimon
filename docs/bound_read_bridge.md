# Bound-read descriptor bridge

This is the descriptor-only first step of the Sirius Paimon integration. It does
not implement a GPU scan, native split planning, batch reads, cancellation,
memory admission or CPU replay. Those require separately reviewed interfaces and
qualification. DuckDB's existing CPU scan behavior is unchanged.

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
lane checks descriptor contracts, NOT cross-DSO qualification. The same tests in
the separate `DONT_LINK` configuration below LOAD the real dynamic extension;
that consumer links only DuckDB, without Paimon/Arrow C++ libraries or a built-in
producer. On Linux/macOS the dynamic lane also checks that callback addresses
belong to the loaded extension. A missing producer must fail. The public C header
is compiled as C. Both lanes are required for bridge qualification; a passing
built-in lane does not substitute for dynamic evidence.

The focused target can be run from an existing build:

```sh
cmake --build build/release --target paimon_check_bound_read_bridge
```

For a host with no statically linked Paimon at all, configure a separate build
with the tracked dynamic-only extension configuration (plus the platform's usual
native dependency/toolchain options):

```sh
cmake -S duckdb -B build/bridge -DDUCKDB_EXTENSION_CONFIGS="$PWD/test/extension_config_bridge.cmake"
cmake --build build/bridge --target paimon_check_bound_read_bridge
```

The tests check all three scan entry points, retained snapshot/options after the
binding is destroyed and another connection commits, empty-at-bind identity,
descriptor access after table metadata disappears, module-local info identity,
foreign/NULL bindings, undersized outputs and bounded diagnostics, and refusal
of deferred unsupported reads. These are CPU descriptor tests, not GPU execution
or native-reader cancellation/ownership qualification.
