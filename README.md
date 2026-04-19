# DuckDB Paimon Extension 🦆

This extension enables [DuckDB](https://duckdb.org/) to read and query [Apache Paimon](https://paimon.apache.org/) format data directly — no ETL pipelines, no Flink/Spark clusters required. Just open a DuckDB shell and run SQL against your Paimon tables.

Similar to other extension, duckdb-paimon brings DuckDB's powerful local analytics to the Paimon data lake ecosystem.

## About Apache Paimon

[Apache Paimon](https://paimon.apache.org/) is a lake format that enables building a Realtime Lakehouse Architecture with Flink and Spark for both streaming and batch operations. It innovatively combines lake format and LSM structure, bringing realtime streaming updates into the lake architecture.

## Implementation

This extension is built on top of [paimon-cpp](https://github.com/alibaba/paimon-cpp), an open-source C++ library that provides native access to Paimon format data. It is the first library that brings native Paimon read/write capabilities to the C++ ecosystem.

### Technical Highlights

- **Zero JVM dependency** — No Java runtime required. Pure C++ implementation means minimal memory footprint and instant startup.
- **Apache Arrow data exchange** — Data flows between paimon-cpp and DuckDB via Apache Arrow, the industry standard for columnar in-memory data, enabling zero-copy transfers with no serialization overhead.
- **Parallel scan architecture** — Paimon tables are split into independent Splits, and DuckDB's multi-threaded execution engine reads them in parallel to fully utilize multi-core CPUs.
- **Secure credential management** — OSS credentials are managed through DuckDB's native Secret Manager with scope isolation and automatic key redaction.

## Features

- Read Paimon table data (local and remote OSS)
- Projection pushdown optimization
- Predicate pushdown optimization
- Multiple file format support (Parquet data files, ORC manifest files)
- Catalog ATTACH support
- DuckDB Secret-based OSS credential management

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

## Getting Started

Clone the repository:

```shell
git clone --recurse-submodules https://github.com/polardb/duckdb-paimon.git
cd duckdb-paimon
```

Note that `--recurse-submodules` will ensure DuckDB and paimon-cpp are pulled which are required to build the extension.

### Building

```shell
GEN=ninja make
```

### Running the Extension

To run the extension code, simply start the shell with `./build/release/duckdb`. This shell will have the extension pre-loaded.

Now we can use the features from the extension directly in DuckDB:

#### Query Local Paimon Tables

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
```

#### Query Remote OSS Paimon Tables

```sql
-- Configure OSS credentials
CREATE SECRET my_oss (
    TYPE paimon,
    key_id 'your-access-key-id',
    secret 'your-access-key-secret',
    endpoint 'oss-cn-hangzhou.aliyuncs.com'
);

-- Query Paimon tables on OSS
SELECT * FROM paimon_scan('oss://your-bucket/warehouse', 'your_db', 'your_table');
```

#### Attach as Catalog

```sql
ATTACH 'oss://my-bucket/warehouse' AS paimon_lake (TYPE paimon);

SHOW ALL TABLES;
DESCRIBE paimon_lake.sales_db.orders;
```

### Running the Tests

```shell
make test
```

## Related Projects

- **[Apache Paimon](https://paimon.apache.org/)** — Realtime lakehouse format
- **[paimon-cpp](https://github.com/alibaba/paimon-cpp)** — Native C++ library for Paimon (underlying dependency)
- **[DuckDB](https://duckdb.org/)** — Embeddable OLAP database
- **[duckdb-iceberg](https://github.com/duckdb/duckdb_iceberg)** — DuckDB's official Iceberg extension

## Join the Community

We welcome contributions and discussions! If you have questions, ideas, or want to connect with other users and developers, join our community by clicking [here](https://qr.dingtalk.com/action/joingroup?code=v1,k1,xL7wNtAi3J83o8gW/R+2vl0twZAzSwohxbXwCwQG6v8=&_dt_no_comment=1&origin=11) or scan the QR code below:

<img src="./docs/group-qrcode.png" alt="DingTalk Group QR Code" width="240">
