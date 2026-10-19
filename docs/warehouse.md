# Pipeline and warehouse design

AWS DMS loads the initial RDS PostgreSQL data and continuously captures committed
changes from WAL. Original CSV captures stay in S3. Python prepares per-table CSV
inputs and sends Redshift COPY commands. Raw loading is incremental; dbt rebuilds
the five marts from retained events whenever a batch has new data.

The warehouse schemas are `raw`, `staging`, `intermediate`, and `marts`.
Staging and intermediate are views; marts are tables. Development fixtures use
PostgreSQL, while cloud demonstrations use Redshift.

## Layer responsibilities

| Layer | Responsibility |
|---|---|
| Ingestion | Validate CSV structure and CDC metadata; prepare per-table COPY inputs |
| Raw | Append delivered events, keeping source values as strings and repeated event copies |
| Staging | Deduplicate event IDs, convert source data types, and check source keys |
| Intermediate | Apply source ordering and deletes to derive current records and customer history |
| Marts | Define analytical grains, join tables, and calculate business totals |

## S3 files

```text
dataset/original-olist-brazilian-ecommerce.zip
raw/initial-load/<table>/LOAD*.csv
raw/cdc/*.csv
copy-ready/initial-load/<table>/LOAD00000001.csv
copy-ready/cdc/<original-cdc-filename-without-.csv>/<table>.csv
```

The ZIP is the complete Olist version 2 download. Original DMS files under `raw/`
remain unchanged. DMS maps the source schema `ecommerce` to `initial-load` for
snapshot paths. CDC records can carry either schema label; their file location
identifies whether they are snapshots or changes.

A CDC file can contain several tables, so Python prepares a separate CSV for each
table. Derived paths mirror the original capture path and are reused on retries.
Source values remain strings; only CDC metadata is normalized and typed before
COPY. The current pipeline uses CSV throughout ingestion.

Capture files are immutable. Each new DMS capture lineage needs fresh raw storage
and checkpoints; independent task sequences must not be mixed.

## Raw: delivered events

Four tables retain the [source columns](source.md), including `updated_at`, plus:

| Column | Meaning |
|---|---|
| `_event_id` | `cdc:<table>:<sequence>` or `snapshot:<table>:<row-key>` identity |
| `_op` | Insert, update, or delete: I, U, or D |
| `_source_order` | DMS change sequence; snapshots use zero |
| `_commit_at` | CDC commit time or snapshot transfer time |
| `_is_snapshot` | Whether the event came from the initial load |
| `_source_file` | Original S3 object URI |

Python validates column counts, supported tables/schema labels and operations,
the required CDC change sequence, and commit metadata. New DMS captures append
`_source_order` and `_commit_at` after the source columns. For replay, the parser
also accepts retained files with the old `_source_lsn` column before those two
columns; that unused value is ignored. Both layouts produce the same event IDs
and COPY-ready rows. The shared contract is in `src/olist_cdc/schema.py`.
Business values remain strings in raw.

Removing `_source_lsn` requires no raw schema change or full backfill. Existing
captures and prepared files can stay in S3. The updated table mappings take effect
when deployed to DMS; the parser supports both layouts before and after deployment.

COPY writes directly into the raw tables. All table COPYs from one original file
and its `raw.loaded_files` entry share one transaction. A failure rolls back that
file's raw rows and ledger entry together. Prepared S3 files remain available.
The ledger stores `source_file`, `loaded_at`, and `row_count`; a completed file
key is skipped on retry without downloading it again. A new filename delivers
another raw copy, even if its event IDs were seen before.

## Staging: typed, distinct events

Each of four staging views returns one row per `_event_id`, using `row_number()`
and choosing file provenance by ascending `_source_file`. Separate changes to the
same business record remain separate events. A dbt test rejects conflicting
contents for the same event ID, excluding file provenance from the comparison.

SQL converts amounts to exact `numeric(14,2)`, counters to integers, and dates to
timestamps. Invalid values fail instead of becoming null. Excess fractional
precision in money is rejected rather than rounded. Business timestamps remain
without a timezone; timezone-bearing inputs are rejected. Naive `updated_at`
values are interpreted as UTC. Nulls, empty text, and postal-code leading zeros
are preserved.

Source-key tests reject null or empty keys. Python retains these rows so they can
be inspected in raw. Snapshot rows missing a key receive a stable file/line hash
identity so distinct invalid rows do not collapse into one event. Copies under
another filename cannot be safely matched without a source key. Failed staging
tests block dependent models and batch completion; no repair is silently applied.

## Intermediate: current state and customer history

Four `int_*_current` views select the newest event for each source key, then
exclude deleted records. Filtering deletes before selecting the latest event
would bring back older rows. Source sequence determines order, so late snapshots
and out-of-order file arrivals cannot overwrite newer changes. These views expose
source columns, including `updated_at` for reconciliation.

`int_customer_history` derives observed customer versions, including deletion
markers. It calculates version IDs, observation timestamps, source-order bounds,
and current/deleted flags. A snapshot overlapping earlier captured CDC cannot
establish an earlier history version; it remains available for current-state
processing, but ambiguous history starts with CDC. Sequence bounds distinguish
changes that share a timestamp.

## Marts: five rebuilt tables

| Model | Grain / key |
|---|---|
| `dim_customers` | Current customer record / `customer_id` |
| `dim_customer_history` | Observed customer version or deletion marker / `customer_version_id` |
| `fct_orders` | Current order / `order_id` |
| `fct_order_items` | Current order item / `order_item_key` |
| `fct_order_payments` | Current payment entry / `payment_key` |

The current customer dimension publishes ID, unique ID, postal code, city, and
state. The history dimension publishes those attributes with `customer_version_id`,
`observed_from/to`, `source_order_from/to`, `is_deleted`, `is_initial_snapshot`,
and `is_current`. History is reconstructed from events in dbt SQL, rather than
managed through dbt snapshots.

The order fact adds `item_total`, `freight_total`, `order_total`, `item_count`,
`payment_total`, `payment_count`, `has_items`, and `has_payments`. Items and payments
are aggregated separately before joining orders, preventing two items and two
payments from multiplying into four combinations. Payment amounts are not
multiplied by installment count. Orders without details remain, with zero totals
and explicit presence flags. Item and payment facts retain their source detail
columns except `updated_at`.

Orders join to current customer details. After a customer moves, old and new
orders both join to the current city; separate SCD Type 2 history retains earlier
cities. This does not reconstruct customer addresses at historical purchase time.
History is partitioned by `customer_id`, not inferred across records sharing
`customer_unique_id`; see the source guide for that distinction.

The model uses fact/dimension separation and multiple fact grains. It is not a
textbook star or galaxy schema: item and payment facts connect through orders.
Diagram relationships are logical, not enforced warehouse constraints. Customer
history survives deletion from the current customer dimension.

## Batch processing and retries

1. Airflow checks capture health and whether the S3 manifest changed since the
   last successful batch. Unchanged input skips warehouse work.
2. The loader commits new files to raw, using the file ledger for safe retries.
3. `dbt build` queries the views, rebuilds all five mart tables, and runs tests.
4. Only successful loading, builds, and tests advance the batch completion marker.

If raw loading succeeds but dbt fails, the next run skips completed raw files
and rebuilds the marts again. Rebuilding from the same events reproduces the
same business rows and history. There are no per-model mart checkpoints.

Each mart uses dbt's standard table materialization. The five marts are not
published together in one transaction: one model can succeed before another
fails. Treat the batch as complete only after the whole build and tests pass.
One active Airflow run prevents overlapping scheduled writers; manual commands
must also wait for active work to finish.

After editing transformation SQL, pause Airflow, finish active work, and run the
manual build in the [runbook](run-cloud.md), even if S3 is unchanged. A normal
build recomputes all rows; no checkpoint reset or full-refresh flag is needed.
Full rebuilds become more expensive as data grows. Five minutes is the scheduling
interval, not a guaranteed end-to-end latency. [Testing](testing.md) documents
checks and completed results.
