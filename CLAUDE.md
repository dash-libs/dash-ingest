# CLAUDE.md — dash-ingest

Part of the **Dashlibs** suite. See ~/dashlibs for the full context.

## Purpose
ADF-style ingestion from Databricks Volumes, ADLS Gen2, S3, DBFS, JDBC
databases, and REST APIs into Delta tables — pick a source kind, fill a
few plain fields (no hand-written URIs/JDBC strings), run.

## Structure
- `/connectors.py` — pure logic: source/target dataclasses, path building
  (`resolve_path`), format inference (`infer_format_from_path`), JDBC URL/
  driver construction (`build_jdbc_url`/`jdbc_driver`) — no Spark, fully
  unit-tested
- `/ingestor.py`   — Spark/JDBC/REST-touching glue: `run_ingestion()` loads
  via the right reader for the source type and writes append/overwrite/merge
- `/ui.py`         — ipywidgets UI (built on `dashui`), `launch()` entrypoint
- `tests/`         — pytest, no Spark dependency for unit tests (covers
  connectors.py only — ingestor.py needs a live SparkSession)

## Key Design Rules
- Never import Spark at module level — always inside functions
- UI calls core classes; never contains business logic
- `launch()` is always the public entrypoint for business users

## CI
- `ci.yml`    — PR gate: lint → test → build
- `daily.yml` — 06:00 UTC: tests + .health/log.txt commit
- `release.yml`— Monday 09:00 UTC: patch bump + GitHub release
