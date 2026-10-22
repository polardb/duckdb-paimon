# Bind-Time Arrow Schema Ownership

The native schema export is owned immediately by a release-aware unique pointer.
DuckDB schema population, names/types conversion and bind-state assignment all
run while that owner is alive. Both normal return and exceptions invoke the root
Arrow release callback before deleting its C shell. No reader or snapshot
selection behavior changes.

On Linux, the normal `make test_release` callback prerequisite also runs a
dedicated schema-ownership executable. It uses GNU linker wrapping of the pinned
DuckDB conversion symbol to exercise the real Paimon bind callback, checks the
normal query, injects a conversion error after export, counts complete root
release and shell deletion, and checks subsequent host liveness. Sized and
unsized delete wrappers observe only the exported shell's exact address and
forward every call to the real allocator. The wrappers are linked only into
this test executable, never either production extension library.
Observation state is thread-local: the exported root is released and its shell
deleted synchronously in the binder frame; unrelated worker deletes are not
observed.

The probe covers both the direct scan binder and catalog table lookup. It tests
an injected error before conversion, a real converter error on the second child
after one column has been initialized, and an injected error after conversion.
The temporary invalid format is restored before the producer releases its schema.
Catalog lookup is repeated after detach/attach so the failure cannot use a cached
table entry.

After building, the focused test is:

```sh
build/release/test/unittest_paimon_schema_release '[schema_release]' --warn NoTests
```

The fault-injection executable is Linux-specific; other platforms retain the
existing binding and SQL regression suites. This does not qualify retained
row-array lifetime, GPU execution, or every possible allocator failure.
