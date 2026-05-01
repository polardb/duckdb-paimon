# duckdb-paimon

DuckDB extension for querying Apache Paimon tables directly via SQL.

## Commands

```bash
GEN=ninja make debug # build (debug)
make test_debug # run tests (debug)
./build/debug/duckdb # launch DuckDB shell with extension pre-loaded

GEN=ninja make # build (release)
make test # run tests (release)
./build/release/duckdb
```

## Code conventions

- C++17, `duckdb` namespace throughout
- Use DuckDB utility types: `unique_ptr`, `make_uniq`, `optional_ptr`
- Apache License 2.0 header required on all new source files
- Code style is inherited from DuckDB (`.clang-format` delegates to `duckdb/.clang-format`). Run `make format-fix` to auto-format before committing

## Branches

- `main` — targets DuckDB 1.5
- `v1.4-andium` — targets DuckDB 1.4

## paimon-cpp submodule

`third_party/paimon-cpp` is built as a CMake `ExternalProject` — do not edit its CMake configuration directly. Apply changes via patches in `patches/` instead. Patches are applied idempotently at build time.
