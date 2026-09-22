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
| Multiple status changes between warehouse batches | Retain observed transitions; suppress same-status updates without losing deletes/reinserts | Order-history integration fixtures |
| Late file changes a previously built history interval | Recompute affected entity history, including removing obsolete versions | Successive incremental-build tests |
| Item/payment changes without an order update | Recalculate that order, including when its last detail is deleted | Successive incremental-build tests |
| Incremental mart fails after writing rows and its checkpoint | Roll back both; retry matches a full rebuild | SQL failure-injection test |
| Quiet build or duplicate event delivery | Leave existing mart rows untouched | PostgreSQL row-identity checks |
| Failed task followed by a successful retry | Preserve both attempts and report final batch outcome correctly | SQLite audit tests and real Airflow task-state checks |

Local fixtures cover adversarial ordering and row-identity checks. The live AWS run additionally verified bootstrap overlap, raw/mart rollback, capture recovery, replay, scheduled processing and source-to-Redshift reconciliation; see [cloud evidence](docs/evidence/olist-aws-validation.json).

## Current validation

The Olist pipeline is **validated locally and end to end on AWS**. All 415,418 selected historical rows passed through RDS PostgreSQL → DMS → S3 → Redshift. The current 19 dbt models and 49 data tests pass on Redshift. Every current source field reconciled after simulated inserts, updates and deletes.

The live demonstration verified 21 changes committed during the initial customer snapshot, recovery of 14 changes committed while DMS was stopped, atomic raw-load and incremental-mart retries, and duplicate-file replay. Two real scheduled Airflow batches passed; the next quiet run skipped loading/building/reporting. Bootstrap timing used 100,002 additional synthetic customer rows, reported separately from Olist's records. The local suite includes 37 tests, including the measured workload generator.

[Reproducible checks and measured evidence](docs/validation.md) distinguish actual cloud results from local fixtures. Earlier Synthea results remain [archived](docs/archive/synthea/README.md).

## Source and model scope

| Source table | Historical rows | Mart |
|---|---:|---|
| customers | 99,441 | dim_customers; dim_customer_history |
| orders | 99,441 | fct_orders; fct_order_status_history |
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

## Incremental dbt processing

All six marts use dbt incremental models with `unique_key` and the `delete+insert` strategy. Each mart tracks the raw files it has processed and selects affected customer, order, item or payment keys. Order totals also refresh when item or payment records change. Staging and intermediate models remain views.

Affected entities are replaced inside a transaction, including removing hard-deleted records and obsolete history versions. Window calculations filter to affected entities before ranking events; history is recalculated from each entity's retained events, so a late file can correct earlier intervals. The mart's file checkpoint commits with its changes; a failed model retries those files. Unchanged entities retain their existing rows. This adds incremental warehouse processing to the existing log-based capture.

Local tests compare successive incremental results with `--full-refresh`, inject a SQL failure after the checkpoint write, and verify quiet/replayed batches do not rewrite mart rows. This reduces mart writes; it is not a claim that every upstream scan or data-quality test is incremental. See [processing and recovery details](docs/incremental-dbt.md).

## Customer versions at order creation

New captured orders carry a customer_version_id pointing to the observed address version when their INSERT occurred. An order created before a customer correction keeps the earlier version; a later order uses the new version. Customer-only late files also revisit affected orders. Original historical snapshot orders have NULL version IDs with `creation_not_captured`; missing captured history is labeled separately. We do not invent customer history for 2016–2018 purchases.

## Order-status history

`fct_orders` answers “what is the order's current state?” `fct_order_status_history` answers “which states did we observe, and when did they change?” It retains transitions such as created → approved → shipped → delivered, even when multiple changes arrive in one batch. Repeated updates with the same status do not create extra versions; deletes and reinserts remain visible.

Each version includes observation timestamps, source-sequence bounds, an initial-snapshot flag, deletion/current flags and observed_duration_seconds for closed, non-deleted intervals. Historical Olist orders begin with their observed snapshot state. We do not infer missing earlier statuses or measure capture-observation time as the original purchase-to-delivery duration.

## Inspect batch operations

```sh
uv run python scripts/report_batches.py
uv run python scripts/report_batches.py --run-id 'your-airflow-run-id'
```

The local audit records task attempts, run outcomes, elapsed time, committed-file input counts by operation, snapshot rows, dbt outcome and the last successful completion. Quiet batches remain distinguishable from failures. SQLite runs locally alongside Airflow, so reporting does not wake Redshift. Input counts are not claims about new warehouse rows after deduplication. See [audit schema and retry semantics](docs/batch-audit.md).

## Measured workload

On an active 4-RPU Redshift warehouse, a simulated 250-order workload produced **1,600 real captured changes** (1,000 inserts, 500 updates, 100 hard deletes). The final S3 object arrived 60 seconds after source writes finished. Loading took 32 seconds; incremental dbt plus all 49 tests took 102 seconds. All source fields reconciled, 225 orders remained, and totals/customer versions were correct. This was one manually invoked batch over existing Olist history, not sustained throughput or a five-minute latency guarantee. [Conditions and reproduction](docs/workload.md).

## Demo cleanup

The AWS stack, its database/warehouse/capture resources, bucket and snapshots have
been removed. Downloaded datasets, local capture files and the project database
volumes were also removed from the Mac at the user's request. Code and small
validation reports remain. Teardown no longer downloads captures by default.
[Cleanup and reported compute usage](docs/evidence/olist-session-cleanup.json).

## Remaining validation

The live demo verifies correctness and scheduled execution, not sustained production throughput or a latency SLA. A [measured Parquet comparison](docs/parquet-evaluation.md) supports retaining gzip CSV for the current small batches. dbt test failure blocks batch acknowledgement but does not provide atomic publication of all marts.
