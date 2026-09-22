# Olist CDC

A student data engineering portfolio project that loads **real, anonymized historical Olist orders** into PostgreSQL, simulates clearly labeled new business activity, and captures the resulting database changes through transaction logs.

**Architecture:** PostgreSQL → AWS DMS (WAL-based CDC) → S3 → Redshift → dbt raw/staging/intermediate/marts. Airflow and Docker run locally. Five-minute downstream batches run during demos, with one active run and idle-file skipping.

**What is real and what is simulated:** the starting records come from [Olist's Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Subsequent new orders, payments, updates and disposable deletes are simulated by our Python application through SQL. PostgreSQL generates real WAL. This is not a connection to Olist's live production systems. Dataset attribution/license: Olist, CC BY-NC-SA 4.0; downloaded data is excluded from Git.

## Reliability demonstrated

| Scenario | Expected behavior | Evidence |
|---|---|---|
| SQL inserts, updates, deletes and rollback | Committed changes appear in PostgreSQL WAL; rolled-back changes do not | Local logical-decoding tests |
| Source writes during an initial snapshot | Snapshot stays consistent; changes remain available in WAL | Local exported-snapshot test |
| Load fails after writing one table | Raw rows and file checkpoint roll back together; retry succeeds | Multi-table failure test |
| Same file retried or events redelivered under a new filename | Event identities prevent duplicate raw records | Replay tests |
| Snapshot arrives after a newer update/delete | Source sequence wins; deleted rows stay deleted | dbt integration fixtures |
| Two items and two payments for an order | Separate aggregation prevents multiplied totals | dbt integration fixtures and real Olist reconciliation |
| Raw load succeeds but dbt fails | Batch is not acknowledged; next run still builds marts | Batch checkpoint and Airflow tests |
| No new capture files | Skip warehouse work without waking Redshift | S3 metadata fixtures and Airflow tests |

These are local results. The Olist AWS demonstration is still pending; the tests do not substitute for real DMS → S3 → Redshift validation.

## Current validation

The Olist migration is **locally validated; its AWS end-to-end run is still pending**. All 415,418 selected source rows loaded successfully. All 16 dbt models and 39 data tests passed against the real seed using a local snapshot fixture; every current source field reconciled. The 27-test suite covers real local WAL, retries, rollback, deletes, history, late files and batch checkpoints. Separate Airflow container checks verify idle skips and failure propagation.

Previous AWS results belong to the [archived Synthea implementation](docs/archive/synthea/README.md). They do not validate the new Olist DMS layout or cloud marts. No AWS infrastructure was started for this migration.

## Source and model scope

| Source table | Historical rows | Mart |
|---|---:|---|
| customers | 99,441 | dim_customers; dim_customer_history |
| orders | 99,441 | fct_orders |
| order_items | 112,650 | fct_order_items |
| order_payments | 103,886 | fct_order_payments |

`customer_id` is the order-associated customer record; `customer_unique_id` links repeat customers. Item and payment composite keys are preserved and given deterministic row keys. Item/payment amounts aggregate independently before joining orders, avoiding multiplied totals. Orders missing items or payments remain visible. Customer history tracks observed changes after capture starts, not addresses from before the historical export.

The deliverable ends at populated, tested marts. No dashboards, Spark, Kafka cluster or always-on cloud infrastructure is required for this scope.

## Run locally

Requires Docker Desktop, Python 3.12 and uv. Copy `.env.example` to `.env` and set a local development password. Existing Synthea users should use `POSTGRES_DB=olist`; the Olist Compose project has a separate data volume.

```sh
uv sync --locked
docker compose up -d --wait postgres
uv run olist-cdc init
uv run olist-cdc seed
uv run python scripts/build_local_warehouse.py
uv run python scripts/validate_local_olist.py
uv run pytest --integration -q
```

The local warehouse builder is explicitly a snapshot fixture, not a CDC extractor. It preserves an existing fixture; it does not follow later source mutations. Actual capture in AWS is DMS. The tests independently verify actual PostgreSQL WAL and exercise downstream DMS-format event fixtures.

To simulate new source activity:

```sh
uv run olist-cdc simulate --scenario demo-001
# Or execute phases individually:
uv run olist-cdc simulate --scenario demo-002 --phase open
```

Phases are open → approve → ship → deliver → correct → create-delete-test → delete-test → rollback-test. Retrying a phase is idempotent. Simulated IDs are deterministic and their scenario is recorded in project metadata. Hard deletes target only the disposable test records. `docker compose stop` stops the local database without deleting its data.

## Cloud demonstrations

Use the [AWS runbook](docs/run-cloud.md) after agreeing a budget for a new paid session. Airflow starts paused and runs every five minutes when enabled. Quiet runs check capture/S3 without waking Redshift; completed-batch checkpoints advance only after dbt and reporting succeed. Stop scheduling and delete paid infrastructure after each demo.

The GitHub repository and existing AWS profile/resource ownership names still use `synthea-cdc` for continuity. The active application/package, DAG, databases, source schema and capture prefix are Olist-specific. Keep using only that project's configured AWS profile, never another project's credentials.

See [source semantics](docs/source.md), [warehouse schema](docs/warehouse.md), [Olist validation](docs/validation.md) and [migration inspection](docs/olist-migration.md).

## Next improvements

Order-status history and a small local batch audit table are the next implementation steps. After they pass tests, their results will be documented here. A separately budgeted Olist AWS run remains necessary to measure scheduled latency, recovery and cost. Parquet and incremental mart optimization are deferred.
