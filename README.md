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
- Write Paimon tables (CREATE TABLE AS, INSERT INTO; append-only tables only)
- DDL support (CREATE/DROP SCHEMA, CREATE/DROP TABLE)
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

## Usage

The examples below use sample data bundled in the `data/` directory of this repository. Start the DuckDB shell with the extension pre-loaded:

```shell
./build/release/duckdb
```

### Query Paimon Tables

Attach a Paimon warehouse as a catalog, then query its tables using standard DuckDB SQL. Use `paimon_scan` instead when attaching a whole warehouse is unnecessary. The local path below is only an example:

```sql
-- Attach a warehouse and query its tables.
ATTACH './data' AS local_paimon (TYPE paimon);

SELECT count(*) FROM local_paimon.testdb.testtbl;

-- Alternatively, scan a single table with paimon_scan.
SELECT count(*) FROM paimon_scan('./data/testdb.db/testtbl');
SELECT count(*) FROM paimon_scan('./data', 'testdb', 'testtbl');
```

### Write Data

With an ATTACHed catalog, you can create schemas, create tables, and insert data using standard DuckDB SQL:

```sql
ATTACH './data' AS my_catalog (TYPE paimon);

CREATE SCHEMA my_catalog.my_db;

CREATE TABLE my_catalog.my_db.orders AS
    SELECT 1 AS order_id, 99.9::DECIMAL(18,2) AS amount, 'Alice' AS customer;

INSERT INTO my_catalog.my_db.orders
    SELECT 2, 49.5, 'Bob'
    UNION ALL
    SELECT 3, 150.0, 'Charlie';

SELECT * FROM my_catalog.my_db.orders ORDER BY order_id;

DROP TABLE my_catalog.my_db.orders;
DROP SCHEMA my_catalog.my_db;
```

To prevent accidental writes, attach the catalog in read-only mode:

```sql
ATTACH './data' AS my_catalog (TYPE paimon, READ_ONLY);
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
```

### Time Travel Queries

Query a historical version of a table by snapshot ID or by timestamp. Use `paimon_snapshots` first to identify the snapshot you want.

When using an ATTACHed catalog, use DuckDB's native `AT` clause. For a single table scan, pass the same snapshot option to `paimon_scan`:

```sql
-- Query an attached catalog with DuckDB's native AT clause.
ATTACH './data' AS my_catalog (TYPE paimon);

-- AT (VERSION => snapshot_id)
SELECT count(*) FROM my_catalog.testdb.testtbl AT (VERSION => 2);

-- AT (TIMESTAMP => point_in_time)
SELECT count(*) FROM my_catalog.testdb.testtbl AT (TIMESTAMP => TIMESTAMP '2026-01-15 10:48:23.5');

-- Alternatively, scan a single table with paimon_scan.
-- Read from a specific snapshot (6 rows — state after the second append)
SELECT count(*) FROM paimon_scan('./data/testdb.db/testtbl', snapshot_from_id=2);

-- Read from a point in time (returns the snapshot active at that moment)
SELECT count(*) FROM paimon_scan('./data/testdb.db/testtbl', snapshot_from_timestamp=TIMESTAMP '2026-01-15 10:48:23.5');
```

### Query Remote Paimon Tables

Remote object storage catalogs are read-only (currently). Create a scoped Paimon Secret before attaching or scanning a remote table.

#### Alibaba Cloud OSS

```sql
CREATE SECRET my_oss (
    TYPE paimon,
    PROVIDER config,
    key_id 'your-access-key-id',
    secret 'your-access-key-secret',
    endpoint 'oss-cn-hangzhou.aliyuncs.com',
    scope 'oss://your-bucket/warehouse'
);

ATTACH 'oss://your-bucket/warehouse' AS oss_paimon (TYPE paimon);
SELECT count(*) FROM oss_paimon.your_db.your_table;

SELECT count(*) FROM paimon_scan('oss://your-bucket/warehouse/your_db.db/your_table');
```

#### Amazon S3

Choose one S3 credential provider for a scope. `credential_chain` uses the AWS credential chain, including the selected AWS CLI profile and refreshed SSO credentials when available:

```sql
CREATE SECRET my_s3 (
    TYPE paimon,
    PROVIDER credential_chain,
    profile 'default',
    region 'ap-northeast-2',
    scope 's3://your-bucket/warehouse'
);
```

Use `config` to provide static credentials instead:

```sql
CREATE SECRET my_s3_static (
    TYPE paimon,
    PROVIDER config,
    key_id 'your-access-key-id',
    secret 'your-secret-access-key',
    session_token 'optional-session-token',
    region 'ap-northeast-2',
    scope 's3://your-bucket/warehouse'
);
```

After creating either Secret, attach or scan the warehouse:

```sql
ATTACH 's3://your-bucket/warehouse' AS s3_paimon (TYPE paimon);
SELECT count(*) FROM s3_paimon.your_db.your_table;

SELECT count(*) FROM paimon_scan('s3://your-bucket/warehouse/your_db.db/your_table');
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

## Related Projects

- **[Apache Paimon](https://paimon.apache.org/)** — Realtime lakehouse format
- **[paimon-cpp](https://github.com/alibaba/paimon-cpp)** — Native C++ library for Paimon (underlying dependency)
- **[DuckDB](https://duckdb.org/)** — Embeddable OLAP database

## Join the Community

We welcome contributions and discussions! If you have questions, ideas, or want to connect with other users and developers, join our community by clicking [here](https://qr.dingtalk.com/action/joingroup?code=v1,k1,xL7wNtAi3J83o8gW/R+2vl0twZAzSwohxbXwCwQG6v8=&_dt_no_comment=1&origin=11) or scan the QR code below:

<img src="./docs/group-qrcode.png" alt="DingTalk Group QR Code" width="240">
