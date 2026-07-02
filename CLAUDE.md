# CLAUDE.md — dash-ingest

Part of the **Dashlibs** suite. See ~/dashlibs for the full context.

## Purpose
ADF-style ingestion from Databricks Volumes, ADLS Gen2, S3, DBFS, JDBC
databases, and REST APIs into Delta tables — pick a source kind, fill a
few plain fields (no hand-written URIs/JDBC strings), run.

## Structure
- `/connectors.py` — pure logic: source/target dataclasses (including
  advanced JDBC options — SSL, fetch size, partitioned parallel reads,
  raw `connection_properties`; and REST API auth/pagination), path building
  (`resolve_path`), format inference (`infer_format_from_path`), JDBC URL/
  driver construction (`build_jdbc_url`/`jdbc_driver`) — no Spark, fully
  unit-tested
- `/readers.py`    — pure logic: per-file-format reader option dataclasses
  (`CsvReaderOptions`, `ExcelReaderOptions`, ...) and `build_reader_options()`
  translating them into real Spark/spark-excel option keys — no Spark
- `/ingestor.py`   — Spark/JDBC/REST-touching glue: `run_ingestion()`,
  `test_connection()` (lightweight reachability check per source kind, no
  data read — uses Spark's Hadoop filesystem API directly for path-based
  sources, not `dbutils`), `preview()` (first N rows without writing
  anywhere). Only its handful of pure helpers (`_extract_json_path`,
  `_rest_auth_headers`, `_rest_basic_auth`) are unit-tested; the rest needs
  a live SparkSession.
- `/ui.py`         — ipywidgets UI (built on `dashui`, including
  `dashui.editable_table()` for connection properties/headers/params),
  `launch()` entrypoint
- `tests/`         — pytest, no Spark dependency for unit tests

## Key Design Rules
- Never import Spark at module level — always inside functions
- UI calls core classes; never contains business logic
- `launch()` is always the public entrypoint for business users

## CI
- `ci.yml`    — PR gate: lint → test → build
- `daily.yml` — 06:00 UTC: tests + .health/log.txt commit
- `release.yml`— Monday 09:00 UTC: patch bump + GitHub release
