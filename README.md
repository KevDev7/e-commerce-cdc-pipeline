# Olist CDC

A student portfolio project demonstrating real PostgreSQL log-based change data
capture, incremental ingestion, and fully rebuilt warehouse marts.

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
compute is temporary. Between demos, data stays in S3, an RDS snapshot and the
Redshift namespace; restoring source access or recreating a warehouse workgroup
is required before querying again.

## Vendor architecture

![Olist CDC vendor architecture: RDS PostgreSQL, AWS DMS, S3, Redshift, dbt, and Airflow running in Docker](docs/images/vendor-architecture.png)

The Redshift icons represent layers within one Redshift Serverless warehouse.
dbt builds customer SCD Type 2 history during transformation and runs data-quality
tests across the models.

## Simplified marts schema

![Simplified Olist marts: current customers linked to orders, order items and payments, with separate SCD Type 2 customer history](docs/images/simplified-marts-schema.png)

This scoped dimensional model draws on **star-schema** fact/dimension separation
and the multiple fact grains found in **galaxy (fact constellation)** designs:
orders, order items and payments. It is not a textbook implementation of either
pattern: item and payment facts connect through orders, rather than each directly
sharing a set of dimensions. Selected columns and logical relationships are shown;
keys are not enforced warehouse constraints.

`dim_customer_history` stores SCD Type 2 versions separately, identified by
`customer_id`, and retains history after deletion from the current customer table.
Orders join to current customer details, not historical versions. See the
[warehouse model definitions](docs/warehouse.md) for the full columns and grains.

## What is real and what is simulated

The starting data is the real, anonymized [Brazilian E-Commerce Public Dataset
by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), Kaggle
version 2, licensed CC BY-NC-SA 4.0. We load four of its nine CSV files:

| Source table | Historical rows | Warehouse marts |
|---|---:|---|
| customers | 99,441 | dim_customers; dim_customer_history |
| orders | 99,441 | fct_orders |
| order_items | 112,650 | fct_order_items |
| order_payments | 103,886 | fct_order_payments |

Python simulates new orders, status/address updates and disposable deletes by
committing SQL transactions. PostgreSQL generates the real WAL that DMS captures;
this project is not connected to Olist's production systems. The complete original
ZIP is retained, while the selected tables contain 415,418 historical rows.

## What the pipeline demonstrates

- Initial loading followed by log-based inserts, updates and hard deletes.
- Source ordering, readable event IDs, duplicate-event handling, atomic raw loads and safe retries.
- Five dbt marts rebuilt from retained raw events on each active batch.
- Customer SCD Type 2 history derived from captured events; order facts show current status.
- Orders linked to current customers, with customer attribute history stored separately.
- Separate item/payment aggregation, source reconciliation and data-quality tests.
- Scheduled batches, idle skipping and completion only after successful builds/tests.

The deliverable ends at populated, tested marts. Product/seller dimensions,
dashboards, Spark and Kafka are outside this scope. See the [source contract](docs/source.md),
[warehouse models](docs/warehouse.md) and [dbt processing and retries](docs/dbt-processing.md).

Source capture and raw loading process new changes only. dbt uses ordinary table
builds for the marts; the pipeline is not incremental at every layer. Customer
history survives each rebuild because its input events remain in raw.

## Run a demonstration

Follow the **[AWS runbook](docs/run-cloud.md)** for setup, seeding, capture,
scheduling, validation and teardown. It is the main operating guide. Agree a
spending allowance before provisioning a new session, use only the project's
`synthea-cdc` AWS profile, and remove paid compute after the demonstration.
That legacy resource/profile name identifies this project's resources; the data is Olist.
[Optional verification scenarios](docs/verification.md) are separate from the normal run.

No full database is needed on your Mac. The cloud seed download is temporary.
GitHub CI uses small disposable PostgreSQL fixtures and checks actual Airflow task
states. [Fixture-based development checks](docs/development.md) are separate
from the cloud demonstration.

## S3 layout

```text
dataset/original-olist-brazilian-ecommerce.zip
raw/initial-load/<table>/LOAD*.csv
raw/cdc/*.csv
copy-ready/initial-load/<table>/LOAD*.parquet
copy-ready/cdc/<original-cdc-filename-without-.csv>/<table>.parquet
```

Original CSV captures are replayable evidence. Derived Parquet files use explicit
types and Zstandard compression for Redshift COPY. dbt's staging, intermediate
and marts are warehouse layers, not S3 folders. [Path details](docs/s3-layout.md).

## Verification and limits

The September 24, 2026 test of the earlier model graph completed **five consecutive
five-minute Airflow cycles**, processing 90 simulated changes without a new
warehouse backfill or manual warehouse loads. Every cycle passed 17 models,
49 tests and comparison of all four current tables against RDS. The test also
followed one order across batches and verified customer-version joins, hard
deletes and rollback exclusion. [Results and the initial idle-cycle finding](docs/five-cycle-validation.md)
include actual run IDs, load counts and startup limitations. Historical customer-to-order
links and the separate order-status history mart have since been removed; those results do
not validate the simplified graph on Redshift.

The simplified graph has **14 models and 39 dbt data tests**: four staging views,
five intermediate views and five fully rebuilt marts. Local fixture checks passed;
the retained AWS warehouse still has the earlier graph until the documented
[upgrade](docs/run-cloud.md#upgrade-after-the-modeling-simplification) is run.

Current changes are checked in [GitHub CI](https://github.com/KevDev7/synthea-cdc/actions).
The [validation history](docs/validation.md) distinguishes cloud runs, local fixtures
and earlier implementations. The five-minute schedule is a trigger interval,
not a latency guarantee. dbt tests block batch acknowledgement but do not publish
all marts atomically. Historical customer attributes before capture are unknown.

## Future stretch goal: streaming with Debezium and Kafka

Explore a separate streaming version to learn skills beyond the current
five-minute microbatch pipeline:

- Replace AWS DMS with Debezium to capture PostgreSQL WAL changes continuously.
- Send change events through Kafka to learn topics, partitions, consumer offsets
  and replay.
- Add a streaming consumer to practice continuous change processing, ordered
  updates, delete handling and recovery after failures.

This is a future learning goal, not an implemented feature. The current DMS
pipeline already demonstrates real log-based CDC. Debezium and Kafka can also
feed microbatches; adding them alone would not make our downstream processing
continuous.
