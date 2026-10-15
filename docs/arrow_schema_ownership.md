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

After building, the focused test is:

```sh
build/release/test/unittest_paimon_schema_release '[schema_release]' --warn NoTests
```

The fault-injection executable is Linux-specific; other platforms retain the
existing binding and SQL regression suites. This does not qualify retained
row-array lifetime, GPU execution, or every possible allocator failure.
