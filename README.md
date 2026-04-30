# DuckDB Paimon Extension 🦆

This extension enables [DuckDB](https://duckdb.org/) to read and query [Apache Paimon](https://paimon.apache.org/) format data directly — no ETL pipelines, no Flink/Spark clusters required. Just open a DuckDB shell and run SQL against your Paimon tables.

Similar to other extensions, duckdb-paimon brings DuckDB's powerful local analytics to the Paimon data lake ecosystem.

## About Apache Paimon

[Apache Paimon](https://paimon.apache.org/) is a lake format that enables building a Realtime Lakehouse Architecture with Flink and Spark for both streaming and batch operations. It innovatively combines lake format and LSM structure, bringing realtime streaming updates into the lake architecture.

## Implementation

This extension is built on top of [paimon-cpp](https://github.com/alibaba/paimon-cpp), an open-source C++ library that provides native access to Paimon format data. It is the first library that brings native Paimon read/write capabilities to the C++ ecosystem.

- **Zero JVM dependency** — No Java runtime required. Pure C++ implementation means minimal memory footprint and instant startup.
- **Apache Arrow data exchange** — Data flows between paimon-cpp and DuckDB via Apache Arrow, the industry standard for columnar in-memory data, enabling zero-copy transfers with no serialization overhead.
- **Parallel scan architecture** — Paimon tables are split into independent Splits, and DuckDB's multi-threaded execution engine reads them in parallel to fully utilize multi-core CPUs.
- **Secure credential management** — OSS credentials are managed through DuckDB's native Secret Manager with scope isolation and automatic key redaction.

## Features

- Read Paimon table data (local and remote OSS)
- Projection pushdown optimization
- Predicate pushdown optimization
- Multiple file format support (manifest / data)
- Catalog ATTACH support
- DuckDB Secret-based OSS credential management
- Snapshot history inspection
- Snapshot-based time travel queries

## Use Cases

### Lightweight Ad-hoc Queries on Realtime Lakehouses

Data is written into Paimon by Flink in real time. Analysts can query it directly on OSS using DuckDB + duckdb-paimon — **no compute cluster needed**, reducing query latency from minutes to seconds.

### Data Validation & Quality Checks

Use DuckDB in CI/CD pipelines to run data quality assertions on Paimon tables, verifying that Flink job outputs meet expectations. Lightweight, fast, and dependency-free.

### Data Exploration & Debugging

Data engineers developing Flink jobs can instantly inspect the current state of Paimon tables using DuckDB Shell, quickly locating data issues — far more efficient than launching a Flink SQL Client.

### Cross-format Federated Queries

DuckDB natively supports Parquet, CSV, JSON, Iceberg, and more. Combined with duckdb-paimon, you can JOIN Paimon tables with other data sources without any data movement:

```sql
-- Join a Paimon orders table with a local CSV dimension table
SELECT o.order_id, o.amount, c.customer_name
FROM paimon_scan('oss://...', 'db', 'orders') o
JOIN read_csv('customers.csv') c ON o.customer_id = c.id;
```

## Development Guide

### Building

Clone the repository with submodules:

```shell
git clone --recurse-submodules https://github.com/polardb/duckdb-paimon.git
cd duckdb-paimon
```

`--recurse-submodules` pulls DuckDB and paimon-cpp, which are required to build the extension.

Build in release mode:

```shell
GEN=ninja make
```

Or build in debug mode:

```shell
GEN=ninja make debug
```

### Running the Tests

```shell
# Release
make test

# Debug
make test_debug
```

## Usage

The examples below use sample data bundled in the `data/` directory of this repository. Start the DuckDB shell with the extension pre-loaded:

```shell
./build/release/duckdb
```

### Query Local Paimon Tables

Pass the table path directly to `paimon_scan`, or use separate warehouse / database / table arguments:

```sql
SELECT * FROM paimon_scan('./data/testdb.db/testtbl');
┌─────────┬───────┬───────┬────────┐
│   f0    │  f1   │  f2   │   f3   │
│ varchar │ int32 │ int32 │ double │
├─────────┼───────┼───────┼────────┤
│ Alice   │     1 │     0 │   11.0 │
│ Bob     │     1 │     1 │   12.1 │
│ Cathy   │     1 │     2 │   13.2 │
│ David   │     2 │     0 │   21.0 │
│ Eve     │     2 │     1 │   22.1 │
│ Frank   │     2 │     2 │   23.2 │
│ Grace   │     3 │     0 │   31.0 │
│ Henry   │     3 │     1 │   32.1 │
│ Iris    │     3 │     2 │   33.2 │
└─────────┴───────┴───────┴────────┘

-- SELECT * FROM paimon_scan('./data', 'testdb', 'testtbl');
```

### Query Remote OSS Paimon Tables

First create a secret to supply OSS credentials, then query using either a full table path or separate warehouse / database / table arguments:

```sql
-- Configure OSS credentials
CREATE SECRET my_oss (
    TYPE paimon,
    key_id 'your-access-key-id',
    secret 'your-access-key-secret',
    endpoint 'oss-cn-hangzhou.aliyuncs.com'
);

SELECT * FROM paimon_scan('oss://your-bucket/warehouse/your_db.db/your_table');
SELECT * FROM paimon_scan('oss://your-bucket/warehouse', 'your_db', 'your_table');
```

### Attach as Catalog

ATTACH a Paimon warehouse as a catalog to browse and query all databases and tables inside it with standard DuckDB SQL:

```sql
ATTACH './data' AS my_catalog (TYPE paimon);

SHOW ALL TABLES;
SELECT * FROM my_catalog.testdb.testtbl;

-- For an OSS warehouse:
-- ATTACH 'oss://my-bucket/warehouse' AS my_catalog (TYPE paimon);
```

### Inspect Snapshot History

Use `paimon_snapshots` to list all snapshots of a Paimon table — useful for auditing commit history, diagnosing data issues, or identifying a snapshot ID for time-travel queries:

```sql
SELECT snapshot_id, commit_kind, commit_time, total_record_count
FROM paimon_snapshots('./data/testdb.db/testtbl')
ORDER BY snapshot_id;
┌─────────────┬─────────────┬─────────────────────────┬────────────────────┐
│ snapshot_id │ commit_kind │      commit_time        │ total_record_count │
│    int64    │   varchar   │       timestamp         │       int64        │
├─────────────┼─────────────┼─────────────────────────┼────────────────────┤
│           1 │ APPEND      │ 2026-01-15 10:48:23.486 │                  3 │
│           2 │ APPEND      │ 2026-01-15 10:48:23.509 │                  6 │
│           3 │ APPEND      │ 2026-01-15 10:48:23.528 │                  9 │
└─────────────┴─────────────┴─────────────────────────┴────────────────────┘

-- SELECT snapshot_id, commit_kind, commit_time, total_record_count
-- FROM paimon_snapshots('oss://your-bucket/warehouse', 'your_db', 'your_table')
-- ORDER BY snapshot_id;
```

### Time Travel Queries

Query a historical version of a table by snapshot ID or by timestamp. Use `paimon_snapshots` first to identify the snapshot you want.

```sql
-- Read from a specific snapshot (6 rows — state after the second append)
SELECT * FROM paimon_scan('./data/testdb.db/testtbl', snapshot_from_id=2);

-- Read from a point in time (returns the snapshot active at that moment)
SELECT * FROM paimon_scan('./data/testdb.db/testtbl', snapshot_from_timestamp=TIMESTAMP '2026-01-15 10:48:23.5');
```

When using an ATTACHed catalog, the same functionality is available via DuckDB's native `AT` clause:

```sql
ATTACH './data' AS my_catalog (TYPE paimon);

-- AT (VERSION => snapshot_id)
SELECT count(*) FROM my_catalog.testdb.testtbl AT (VERSION => 2);

-- AT (TIMESTAMP => point_in_time)
SELECT count(*) FROM my_catalog.testdb.testtbl AT (TIMESTAMP => TIMESTAMP '2026-01-15 10:48:23.5');
```

## Related Projects

- **[Apache Paimon](https://paimon.apache.org/)** — Realtime lakehouse format
- **[paimon-cpp](https://github.com/alibaba/paimon-cpp)** — Native C++ library for Paimon (underlying dependency)
- **[DuckDB](https://duckdb.org/)** — Embeddable OLAP database
- **[duckdb-iceberg](https://github.com/duckdb/duckdb_iceberg)** — DuckDB's official Iceberg extension

## Join the Community

We welcome contributions and discussions! If you have questions, ideas, or want to connect with other users and developers, join our community by clicking [here](https://qr.dingtalk.com/action/joingroup?code=v1,k1,xL7wNtAi3J83o8gW/R+2vl0twZAzSwohxbXwCwQG6v8=&_dt_no_comment=1&origin=11) or scan the QR code below:

<img src="./docs/group-qrcode.png" alt="DingTalk Group QR Code" width="240">
