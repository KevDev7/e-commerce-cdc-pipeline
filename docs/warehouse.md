# Warehouse layers

The same raw/staging/intermediate/marts structure targets local PostgreSQL for development and Redshift for cloud demonstrations. DMS writes original transaction-preserving CSVs to S3 under `raw/`. They are retained unchanged; explicitly typed Zstandard Parquet inputs for Redshift COPY are derived under `copy-ready/`.

The database schemas are `raw`, `staging`, `intermediate`, and `marts`.
The dbt schema-naming macro uses each configured layer name directly. Historical
validation reports may retain the previous `analytics_` names.

## Raw: four append-only event tables

`raw.customers`, `raw.orders`, `raw.order_items`, `raw.order_payments` retain [all source columns](source.md), including `updated_at`, plus:

| Metadata | Meaning |
|---|---|
| _event_id | Readable `cdc:<table>:<source-sequence>` or `snapshot:<table>:<row-key>` identity for deduplication |
| _op | I, U, or D |
| _source_order | DMS change sequence; snapshots use zero |
| _commit_at | CDC commit time; snapshot transfer time |
| _is_snapshot | Initial-load record flag |
| _source_file | Original S3 object URI |

`raw.loaded_files` stores source_file, loaded_at, row_count. Raw rows and their ledger entry commit atomically. Redelivery is deduplicated by event identity; already loaded keys are skipped without downloading again. Capture files are immutable: never overwrite a loaded key. A new capture lineage requires fresh raw storage, not reuse of old sequence numbers.

## Staging: four views

`stg_customers` and `stg_orders` expose typed source fields and event ID, operation, sequence, timestamp and snapshot flag. Item/payment staging keeps source fields and only event ID, operation and sequence; those tables do not build temporal history. Tests check event uniqueness, required identifiers and supported operations. This is the only staging layer.

## Intermediate: five views

- Four `int_*_current` views rank each source row key by source sequence, select its latest state and exclude tombstones. Late snapshots never overwrite newer changes. These four views expose only source columns (including `updated_at` for reconciliation); event metadata and the internal ranking helper stay inside the calculation.
- `int_customer_history` records observed attribute changes with version IDs, observed_from/to, source_order_from/to, is_deleted and is_initial_snapshot. A snapshot that overlaps already captured CDC cannot establish an earlier history version; it stays in raw/current processing, but ambiguous history begins with CDC.

## Marts: five rebuilt tables

The grain defines what one row represents and which key identifies it:

| Model | One row represents | Primary key |
|---|---|---|
| dim_customers | A currently present source customer record | customer_id |
| dim_customer_history | An observed customer-record version or deletion marker | customer_version_id |
| fct_orders | A currently present order | order_id |
| fct_order_items | A currently present order-item record | order_item_key, derived from order_id + order_item_id |
| fct_order_payments | A currently present order-payment entry | payment_key, derived from order_id + payment_sequential |

In the historical export, `customer_id` identifies an order-associated customer
record. `customer_unique_id` links the same customer across those records. Counting
`dim_customers` rows therefore counts source customer records; distinct
`customer_unique_id` values count customer identities within that population.
History is partitioned by `customer_id`, so separate records for a repeat customer
are not merged into an inferred person-level address history. The simulated source
also allows another order to reference an existing customer record.

- **dim_customers:** one current customer record, with customer_id, customer_unique_id, postal_code, city, state.
- **dim_customer_history:** those attributes plus customer_version_id, observed_from/to, source_order_from/to, is_deleted, is_initial_snapshot and is_current.
- **fct_orders:** source order fields excluding updated_at, plus item_total, freight_total, order_total, item_count, payment_total, payment_count, has_items, has_payments.
- **fct_order_items:** source item fields excluding updated_at; one row per order and item sequence.
- **fct_order_payments:** source payment fields excluding updated_at; one row per order and payment sequence.

The order fact reads the intermediate views and aggregates current items and payments separately before joining orders; no separate aggregate views are built. Payment value is not multiplied by installment count. Two items and two payments therefore remain two of each, not four duplicated combinations. Orders lacking details survive left joins, with explicit presence flags and zero aggregate counts.

Orders reference current customer attributes through
`fct_orders.customer_id = dim_customers.customer_id`. For example, after a customer
moves from sao paulo to campinas, both their old and new orders join to campinas.
`dim_customer_history` separately retains both observed cities. Orders are not
assigned to historical customer versions, and we do not claim to reconstruct
addresses at historical purchase times.

### Modeling scope and history implementation

These are scoped dimensional marts. Customer versions implement SCD Type 2
behavior using retained CDC events and dbt SQL; there are no `dbt snapshot`
models. Sequence bounds preserve separate changes even when they share a commit
timestamp. Observation timestamps are deliberately named `observed_from` and
`observed_to` because the original export cannot supply earlier business-effective
dates. Retained events also let late files correct previously built intervals.

Customer history is the single SCD Type 2 example. Order facts show current
status; original order changes remain in raw, without a separate status-history mart.
Product/seller dimensions
would broaden dimensional-modeling coverage but require additional source tables;
they remain optional extensions. Customer tier has no field or business rule in
the current source contract. Additional tiers would require an explicit business
definition and clearly labeled derived or simulated values.

The nine staging/intermediate models remain views. Each active batch rebuilds
the five marts with dbt's standard `table` materialization. `raw.loaded_files`
still tracks incremental ingestion; there is no mart checkpoint table. Repeated
builds reproduce the same business rows and history from the retained events.
See [dbt processing](dbt-processing.md) for retries and publication limits.
Five-minute scheduling is a trigger interval, not a latency guarantee.

The raw event contract is unchanged by the modeling simplification. Six raw
metadata fields still support ordering, deduplication and history; original DMS
captures retain their source format. Retained raw data can rebuild the simpler
marts without a source backfill. Follow the [upgrade steps](run-cloud.md#upgrade-after-the-modeling-simplification)
when the retained warehouse is next brought online.
