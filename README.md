# Synthea CDC

A healthcare billing data engineering project using synthetic Synthea data to simulate an operational PostgreSQL system and capture real transaction-log changes.

**Scope:** patients, encounters, claims, and claim transactions.

**Planned stack:** PostgreSQL WAL-based CDC → AWS S3 → Amazon Redshift, with dbt transformations and Airflow orchestration; Docker for local development.

**Focus:** initial loads, inserts/updates/deletes, reliable recovery and replay, and historical analytics.

**Deliverable:** populated, tested Redshift marts, using raw → staging → intermediate → marts layers. Dashboards and data visualizations are out of scope.

**Status:** initial repository setup; the pipeline is not yet implemented.
