# Contributing

We welcome bug reports, feature proposals, documentation improvements, and code contributions.

Please follow our [Code of Conduct](CODE_OF_CONDUCT.md) when participating in the project.

For development setup, building, and running the extension, see the [README development guide](README.md#development-guide). Test commands are documented in [Running the Tests](README.md#running-the-tests).

## Reporting Issues and Proposing Features

For suspected security vulnerabilities, follow the [security policy](SECURITY.md).

Search existing [issues](https://github.com/polardb/duckdb-paimon/issues) and [pull requests](https://github.com/polardb/duckdb-paimon/pulls) before opening a new issue.

For a bug report, include:

- DuckDB and extension versions, operating system, and CPU architecture.
- The catalog type and storage backend involved, when relevant.
- A minimal SQL example and any table setup or sample data needed to reproduce the issue.
- Expected behavior, actual behavior, and relevant error messages or logs.

Remove credentials and sensitive data from examples and logs before posting them.

For a feature proposal, describe the use case and expected behavior. Discuss substantial changes in an issue before starting implementation so contributors and maintainers can agree on the approach.

## Choosing a Target Branch

Open pull requests against the branch matching the DuckDB version you are targeting. See the [README compatibility matrix](README.md#compatibility) for the version-to-branch mapping.

Mention other affected versions in the issue or pull request if a fix may need to be applied to multiple branches.

## Code Style and Scope

- Use C++17 and the `duckdb` namespace.
- Prefer DuckDB utility types such as `unique_ptr`, `make_uniq`, and `optional_ptr`.
- Include an Apache License 2.0 header in new source files, following existing files.
- Follow DuckDB's code style and run `make format-fix` before submitting C++ changes. Review the resulting diff for unrelated formatting changes.
- Keep each pull request focused on one change and preserve unrelated code and formatting.

Add or update regression coverage for behavior changes. SQLLogicTests live in [test/sql](test/sql); follow the existing tests for examples. Update user-facing documentation when behavior or usage changes.

## Dependency Changes

`third_party/paimon-cpp` is built as a CMake `ExternalProject`. For changes to its CMake configuration, add a patch under [patches](patches) and register it in `PAIMON_CPP_PATCHES` in the root [CMakeLists.txt](CMakeLists.txt), rather than editing the submodule's CMake files directly. The build applies these patches idempotently.

Keep submodule updates intentional and explain why they are needed in the pull request.

## Submitting a Pull Request

1. Create a branch in your fork based on the appropriate target branch.
2. Make the change and review the diff for unrelated edits.
3. For code changes, run the relevant tests using the [README instructions](README.md#running-the-tests).
4. Open a pull request with a concise title. Explain the problem, the resulting behavior, and how you verified the change. Link related issues and mention any compatibility implications.

Address review feedback and resolve relevant CI failures before the pull request is merged.
