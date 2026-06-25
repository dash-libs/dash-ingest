# CLAUDE.md — dash-ingest

Part of the **Dashlibs** suite. See ~/dashlibs for the full context.

## Purpose
Data ingestion from CSV/JSON/Parquet/JDBC into Delta. ingestor.py=Ingestor API

## Structure
- `/ui.py`       — ipywidgets UI, `launch()` entrypoint
- `/*.py`        — core logic
- `tests/`           — pytest, no Spark dependency for unit tests

## Key Design Rules
- Never import Spark at module level — always inside functions
- UI calls core classes; never contains business logic
- `launch()` is always the public entrypoint for business users

## CI
- `ci.yml`    — PR gate: lint → test → build
- `daily.yml` — 06:00 UTC: tests + .health/log.txt commit
- `release.yml`— Monday 09:00 UTC: patch bump + GitHub release
