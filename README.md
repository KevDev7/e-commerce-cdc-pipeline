# Synthea CDC

A healthcare billing data engineering project using synthetic Synthea data to simulate an operational PostgreSQL system and capture real transaction-log changes.

**Scope:** patients, encounters, claims, and claim transactions.

**Planned stack:** PostgreSQL WAL-based CDC → AWS S3 → Amazon Redshift, with dbt transformations and Airflow orchestration; Docker for local development.

**Focus:** initial loads, inserts/updates/deletes, reliable recovery and replay, and historical analytics.

**Deliverable:** populated, tested Redshift marts, using raw → staging → intermediate → marts layers. Dashboards and data visualizations are out of scope.

**Status:** local source and simulator implemented; change-file parsing and 15 dbt models validated locally. The five marts are populated in a local test warehouse. AWS DMS/S3/Redshift integration and Airflow orchestration are not deployed yet.

## Local source

Requires Docker Desktop, Python 3.12 and [uv](https://docs.astral.sh/uv/).

1. Copy `.env.example` to `.env` and set a local development password.
2. Run:

```bash
uv sync --locked
docker compose up -d --wait postgres
uv run synthea-cdc init
uv run synthea-cdc seed
uv run synthea-cdc simulate --scenario demo-001
uv run pytest --integration -q
```

Run a single business phase with `--phase open`, `--phase bill`, etc. Repeating a seed or completed scenario does not duplicate data. Use a new scenario name for new activity.

`docker compose stop` stops the local service while keeping its data. The `.env` file and downloaded sample are ignored by Git. AWS commands must explicitly use the `synthea-cdc` profile.

## Warehouse models

```bash
uv run python scripts/build_local_warehouse.py
```

This loads a snapshot fixture into a separate local PostgreSQL warehouse and runs dbt. It is a development check, not a replacement for the planned DMS capture or a claim of Redshift deployment. The project includes Redshift and local PostgreSQL dbt profiles; credentials come from environment variables.

See [source design and simulation](docs/source.md), [warehouse design](docs/warehouse.md), and [validation results](docs/validation.md). Cloud resource provisioning awaits an agreed spending limit.
