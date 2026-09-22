# Synthea CDC

A healthcare billing data engineering project using synthetic Synthea data to simulate an operational PostgreSQL system and capture real transaction-log changes.

**Scope:** patients, encounters, claims, and claim transactions.

**Stack:** RDS PostgreSQL → AWS DMS (WAL-based CDC) → S3 → Redshift → dbt staging/intermediate/marts. Airflow and Docker run locally.

**Focus:** initial loads, inserts/updates/deletes, reliable recovery and replay, and historical analytics.

**Deliverable:** populated, tested Redshift marts, using raw → staging → intermediate → marts layers. Dashboards and data visualizations are out of scope.

**Status:** validated end to end on AWS on 2026-09-22. All 15 dbt models, 42 data tests and four Airflow tasks passed. Every current source field reconciled with Redshift. Temporary cloud resources were deleted after validation to avoid ongoing charges.

## What the demonstration proves

- A real 100,147-row initial load followed by 26 WAL-derived change events.
- Capture resumes after downtime and recovers transactions committed while DMS was stopped.
- Change files can arrive before snapshot files; latest source sequence determines current state.
- Redelivering a CDC file and retrying the batch preserve all 100,173 distinct raw events.
- Patient city history, hard deletes, rollback exclusion and claim/payment totals behave as expected.

The business workload is simulated using synthetic data. PostgreSQL generates the actual WAL and AWS DMS captures it. This is a small portfolio demonstration, not a production workload benchmark. See [measured results and limits](docs/validation.md).

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

This loads a snapshot fixture into a separate local PostgreSQL warehouse and runs dbt. It is an inexpensive development check; the actual AWS demonstration uses DMS change files. The project includes Redshift and local PostgreSQL dbt profiles; credentials come from environment variables.

See [source design](docs/source.md), [warehouse design](docs/warehouse.md), [AWS runbook](docs/run-cloud.md), and [validation evidence](docs/evidence/cloud-validation.json). The initial AWS test allowance was $5; deployment is manual, with a small Redshift usage limit and an explicit teardown procedure.
