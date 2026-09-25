# E-commerce CDC Pipeline

An AWS data pipeline that captures PostgreSQL changes and builds tested e-commerce
analytics marts in Redshift. It demonstrates log-based CDC, orchestration,
dimensional modeling, and recovery using the [Olist dataset](docs/source.md).

The source contains **415,418 historical rows** across customers, orders, order
items, and payments. Python simulates new business transactions; PostgreSQL WAL
and AWS DMS provide real change capture.

## Technical highlights

- **Log-based CDC:** initial loads and ongoing inserts, updates, and deletes from
  RDS PostgreSQL through AWS DMS.
- **Python and warehouse ingestion:** retain original CSV captures in S3, convert
  them to typed Parquet, and load Redshift with `COPY`.
- **Reliable processing:** preserve source ordering, deduplicate replayed events,
  apply hard deletes, and commit raw rows with their file ledger for safe retries.
- **SQL and dimensional modeling:** dbt builds staging, intermediate, and five
  marts, including customer SCD Type 2 history. Separate item/payment aggregation
  prevents double-counting order totals.
- **Orchestration:** Airflow runs locally in Docker on a five-minute schedule,
  skips idle warehouse work, and acknowledges batches only after builds and tests
  succeed. Raw ingestion is incremental; active batches fully rebuild the marts.
- **Data quality:** source-to-target reconciliation, dbt data tests, and GitHub CI
  with disposable PostgreSQL fixtures and Airflow task-state checks.

## Vendor architecture

![E-commerce CDC Pipeline vendor architecture: RDS PostgreSQL, AWS DMS, S3, Redshift, dbt, and Airflow running in Docker](docs/images/vendor-architecture.png)

The Redshift icons represent layers within one Redshift Serverless warehouse.
dbt builds customer history during transformation and tests the resulting models.
Downstream processing uses five-minute microbatches.

## Simplified marts schema

![Simplified Olist marts: current customers linked to orders, order items and payments, with separate SCD Type 2 customer history](docs/images/simplified-marts-schema.png)

| Mart | Grain — one row per |
|---|---|
| `dim_customers` | Current customer record |
| `dim_customer_history` | Observed customer version or deletion marker |
| `fct_orders` | Current order, with item and payment totals |
| `fct_order_items` | Current order item |
| `fct_order_payments` | Current payment entry |

Orders join to current customer details; SCD Type 2 history is retained separately.
The diagram shows selected columns and logical relationships.
[Full model definitions](docs/warehouse.md).

## Validation

| Evidence | Result |
|---|---|
| Earlier AWS demonstrations | Loaded the historical dataset; verified replay, deletes, recovery, and source-to-target reconciliation. |
| September 24 scheduled AWS run | Five consecutive Airflow cycles processed 90 simulated changes; all four current tables reconciled with RDS after every cycle. |
| Current simplified implementation | 56 local tests passed. Redshift adapter parsing confirmed 14 models and 39 dbt data tests; a new end-to-end Redshift run is pending. |

Cloud results apply to the revisions tested, which preceded the latest model
simplification. See the [validation evidence](docs/validation.md),
[five-cycle report](docs/five-cycle-validation.md), and
[GitHub CI](https://github.com/KevDev7/e-commerce-cdc-pipeline/actions).

## Run it and explore the implementation

Start with the **[AWS runbook](docs/run-cloud.md)** for setup, simulation, scheduled
processing, verification, and cleanup. It covers the spending allowance and cloud
resource lifecycle. For checks without AWS, use the
[development guide](docs/development.md).

- [Source data and simulated workload](docs/source.md)
- [Warehouse design](docs/warehouse.md) and [dbt processing](docs/dbt-processing.md)
- [S3 file layout](docs/s3-layout.md)
- [Reliability checks](docs/reliability.md) and [verification scenarios](docs/verification.md)
- [Future learning roadmap](docs/roadmap.md)
