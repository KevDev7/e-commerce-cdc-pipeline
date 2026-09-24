# Olist CDC

A student portfolio project demonstrating real PostgreSQL log-based change data
capture, reliable incremental processing, and tested warehouse marts.

```text
Olist ZIP → PostgreSQL on RDS → DMS reads WAL → S3 CSV captures
                                                   ↓
                                            Typed Parquet
                                                   ↓
                                            Redshift raw
                                                   ↓
                                  dbt staging → intermediate → marts
```

Airflow runs locally in Docker and processes available files every five minutes
during demonstrations. Quiet batches skip warehouse work. RDS, DMS and Redshift
are temporary; the source archive and captured data stay in S3 between demos.

## What is real and what is simulated

The starting data is the real, anonymized [Brazilian E-Commerce Public Dataset
by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), Kaggle
version 2, licensed CC BY-NC-SA 4.0. We load four of its nine CSV files:

| Source table | Historical rows | Warehouse marts |
|---|---:|---|
| customers | 99,441 | dim_customers; dim_customer_history |
| orders | 99,441 | fct_orders; fct_order_status_history |
| order_items | 112,650 | fct_order_items |
| order_payments | 103,886 | fct_order_payments |

Python simulates new orders, status/address updates and disposable deletes by
committing SQL transactions. PostgreSQL generates the real WAL that DMS captures;
this project is not connected to Olist's production systems. The complete original
ZIP is retained, while the selected tables contain 415,418 historical rows.

## What the pipeline demonstrates

- Initial loading followed by log-based inserts, updates and hard deletes.
- Source ordering, duplicate-event handling, atomic raw loads and safe retries.
- Six incremental dbt marts with per-model checkpoints and affected-entity updates.
- Customer SCD Type 2 behavior and order-status history derived from captured events.
- Customer-version joins for new captured orders; pre-capture history stays unknown.
- Separate item/payment aggregation, source reconciliation and data-quality tests.
- Scheduled batches, idle skipping and completion only after successful builds/tests.

The deliverable ends at populated, tested marts. Product/seller dimensions,
dashboards, Spark and Kafka are outside this scope. See the [source contract](docs/source.md),
[warehouse models](docs/warehouse.md) and [incremental recovery rules](docs/incremental-dbt.md).

## Run a demonstration

Follow the **[AWS runbook](docs/run-cloud.md)** for setup, seeding, capture,
scheduling, validation and teardown. It is the main operating guide. Agree a
spending allowance before provisioning a new session, use only the project's
`synthea-cdc` AWS profile, and remove paid compute after the demonstration.
That legacy resource/profile name identifies this project's resources; the data is Olist.

No full database is needed on your Mac. The cloud seed download is temporary.
GitHub CI uses small disposable PostgreSQL fixtures and checks actual Airflow task
states. [Optional development instructions](docs/development.md) are separate
from the cloud demonstration.

## S3 layout

```text
source/original-olist-brazilian-ecommerce.zip
raw/dms/olist/ecommerce/<table>/LOAD*.csv
raw/dms/olist/cdc/*.csv
copy-ready/<source-hash>/<content-hash>/<table>.parquet
validation/
```

Original CSV captures are replayable evidence. Derived Parquet files use explicit
types and Zstandard compression for Redshift COPY. dbt's staging, intermediate
and marts are warehouse layers, not S3 folders. [Path details](docs/s3-layout.md).

## Verification and limits

The September 22, 2026 AWS Parquet demonstration loaded all 415,418 historical
rows and verified a 15-change simulated batch, raw rollback/retry, replay,
hard deletes, customer-version joins and source-to-target reconciliation.
[Saved results](docs/evidence/olist-parquet-validation.json) describe that tested
revision, not a new cloud run after every repository change.

Current changes are checked in [GitHub CI](https://github.com/KevDev7/synthea-cdc/actions).
The [validation history](docs/validation.md) distinguishes cloud runs, local fixtures
and earlier implementations. The five-minute schedule is a trigger interval,
not a latency guarantee. dbt tests block batch acknowledgement but do not publish
all marts atomically. Historical customer attributes before capture are unknown.
