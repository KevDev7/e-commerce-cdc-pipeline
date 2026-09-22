# Synthea CDC

A healthcare billing data engineering project using synthetic Synthea data to simulate an operational PostgreSQL system and capture real transaction-log changes.

**Scope:** patients, encounters, claims, and claim transactions.

**Planned stack:** PostgreSQL WAL-based CDC → AWS S3 → Amazon Redshift, with dbt transformations and Airflow orchestration; Docker for local development.

**Focus:** initial loads, inserts/updates/deletes, reliable recovery and replay, and historical analytics.

**Deliverable:** populated, tested Redshift marts, using raw → staging → intermediate → marts layers. Dashboards and data visualizations are out of scope.

**Status:** local PostgreSQL source, seed loader and business simulator implemented. AWS capture, warehouse models and orchestration are next; no cloud pipeline is deployed yet.

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

See [source design and simulation](docs/source.md). Cloud resource provisioning awaits an agreed spending limit.
