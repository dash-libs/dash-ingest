# DashIngest — Databricks Library

[![CI](https://github.com/dash-libs/dash-ingest/actions/workflows/ci.yml/badge.svg)](https://github.com/dash-libs/dash-ingest/actions)
[![PyPI](https://img.shields.io/pypi/v/dash-ingest)](https://pypi.org/project/dash-ingest/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

Part of the **[Dashlibs](https://github.com/dash-libs)** suite — Databricks libraries built for business users.

ADF-style data ingestion: pick a source kind, fill a few plain fields — no
hand-written `abfss://` URIs or JDBC connection strings — and run.

## Installation

```bash
%pip install dash-ingest
```

## Quick Start

```python
import dashingest
dashingest.launch()   # Opens interactive UI in your Databricks notebook
```

Or drive it directly from code:

```python
from dashingest import ADLSSource, IngestTarget, run_ingestion

source = ADLSSource(storage_account="myacct", container="raw", path="sales/2024.csv")
target = IngestTarget(table="main.bronze.sales", write_mode="merge", merge_keys=["order_id"])
result = run_ingestion(source, target)
result.display()
```

## Sources

| Kind | What you provide |
|---|---|
| Databricks Volume | catalog, schema, volume, path |
| ADLS Gen2 | storage account, container, path |
| Amazon S3 | bucket, path |
| DBFS | path |
| Database (JDBC) | engine (postgres/mysql/sqlserver/oracle/snowflake), host, database, table or query |
| REST API | URL, optional JSON path to the records |

File format (csv/json/parquet/excel/avro/orc/text) is inferred from the
path's extension if not set explicitly — most ingestions need zero format
options.

## Write modes

`append` · `overwrite` · `merge` (upsert into Delta by `merge_keys`, with
schema evolution where the runtime supports it).

## Part of Dashlibs

| Library | Purpose |
|---|---|
| dash-dq | Data Quality |
| dash-synthetic | Synthetic Data Generation |
| dash-ml | ML Lifecycle Management |
| dash-ingest | Data Ingestion |
| dash-gov | Data Governance |
| dash-ontology | Ontology & Lineage for AI |

## License

Apache 2.0
