# Warehouse layers

The same raw/staging/intermediate/marts structure targets local PostgreSQL for development and Redshift for cloud demonstrations. DMS writes original transaction-preserving CSVs to S3 under `raw/dms/olist/`. They are retained unchanged; explicitly typed Zstandard Parquet inputs for Redshift COPY are derived under `copy-ready/parquet-v1/`.

## Raw: four append-only event tables

`raw.customers`, `raw.orders`, `raw.order_items`, `raw.order_payments` retain [all source columns](source.md), including `updated_at`, plus:

| Metadata | Meaning |
|---|---|
| _event_id | Deterministic identity for deduplication |
| _op | I, U, or D |
| _source_lsn | DMS source log position |
| _source_order | DMS change sequence; snapshots use zero |
| _commit_at | CDC commit time; snapshot transfer time |
| _is_snapshot | Initial-load record flag |
| _source_file | Original S3 object URI |

`raw.loaded_files` stores source_file, content_sha256, loaded_at, row_count. Raw rows and their ledger entry commit atomically. Redelivery is deduplicated by event identity; changed contents at an already loaded key are rejected. A new capture lineage requires fresh raw storage, not reuse of old sequence numbers.

## Staging: four views

`stg_customers`, `stg_orders`, `stg_order_items`, `stg_order_payments` expose typed source fields and event ID, operation, sequence, timestamp and snapshot flag. Tests check event uniqueness, required identifiers and supported operations. This is the only staging layer.

## Intermediate: nine views

- Four `int_*_current` views rank each source row key by source sequence, select its latest state and exclude tombstones. Late snapshots never overwrite newer changes.
- `int_order_item_totals` aggregates item prices, freight and item count by order.
- `int_order_payment_totals` aggregates payment value and payment-entry count by order. An installment count is not a multiplier for payment_value.
- `int_customer_history` records observed attribute changes with version IDs, observed_from/to, source_order_from/to, is_deleted and is_initial_snapshot. A snapshot that overlaps already captured CDC cannot establish an earlier history version; it stays in raw/current processing, but ambiguous history begins with CDC. This preserves the preceding project's overlap fix.

`int_order_status_history` applies the same overlapping-snapshot safeguard to orders, retains status transitions and deletion markers, and suppresses repeated same-status updates. It uses source sequence to order versions even when commit timestamps match.

`int_order_creations` identifies the latest captured INSERT for each order key. It records customer_id_at_creation, creation_source_order and captured_created_at. Snapshot-only orders have no captured creation; a reinsert begins a new lifecycle.

## Marts: six incremental tables

The grain defines what one row represents and which key identifies it:

| Model | One row represents | Primary key |
|---|---|---|
| dim_customers | A currently present source customer record | customer_id |
| dim_customer_history | An observed customer-record version or deletion marker | customer_version_id |
| fct_orders | A currently present order | order_id |
| fct_order_items | A currently present order-item record | order_item_key, derived from order_id + order_item_id |
| fct_order_payments | A currently present order-payment entry | payment_key, derived from order_id + payment_sequential |
| fct_order_status_history | An observed order-status version or deletion/reinsertion transition | order_status_version_id |

In the historical export, `customer_id` identifies an order-associated customer
record. `customer_unique_id` links the same customer across those records. Counting
`dim_customers` rows therefore counts source customer records; distinct
`customer_unique_id` values count customer identities within that population.
History is partitioned by `customer_id`, so separate records for a repeat customer
are not merged into an inferred person-level address history. The simulated source
also allows another order to reference an existing customer record.

- **dim_customers:** one current customer record, with customer_id, customer_unique_id, postal_code, city, state.
- **dim_customer_history:** those attributes plus customer_version_id, observed_from/to, source_order_from/to, is_deleted, is_initial_snapshot and is_current.
- **fct_orders:** source order fields excluding updated_at, plus item_total, freight_total, order_total, item_count, payment_total, payment_count, has_items, has_payments, captured_created_at, customer_version_id and customer_history_status.
- **fct_order_status_history:** order_status_version_id, order_id, status, observed_from/to, source_order_from/to, is_deleted, is_initial_snapshot, is_current and observed_duration_seconds. Closed non-deleted intervals measure observed time; open versions and deletion markers have NULL duration. Historical snapshots do not reconstruct earlier lifecycle transitions.
- **fct_order_items:** source item fields excluding updated_at; one row per order and item sequence.
- **fct_order_payments:** source payment fields excluding updated_at; one row per order and payment sequence.

Items and payments are aggregated separately before joining orders. Two items and two payments therefore remain two of each, not four duplicated combinations. Orders lacking details survive left joins, with explicit presence flags and zero aggregate counts.

Customer history begins at capture observation, years after most historical purchases. For captured new orders, fct_orders links to the customer version covering the captured INSERT's source sequence and whose observation time is no later than that commit. Source sequence resolves changes sharing a commit timestamp. The assignment uses the customer on that INSERT; later reassignment of the current order does not rewrite who created it. `customer_id` remains the current source value.

`customer_history_status` is `matched`, `creation_not_captured`, or `customer_history_unavailable`. The latter two have NULL customer_version_id. Original historical snapshot orders therefore remain explicitly unknown, rather than borrowing a present-day address. The anchor is captured creation time, not reconstructed purchase-time attributes. `customer_unique_id` remains available for repeat-customer analysis.

The fact reads the canonical intermediate history view so selected builds can see all loaded events. The materialized dimension uses exactly the same version IDs and precedes the fact in a full build. Build the complete graph for tested, consistent published marts; an isolated model run does not synchronize its dependencies.

### Current attributes versus attributes at order creation

The simulated repeat-order scenario demonstrates the two join meanings:

1. A customer record is created with city `sao paulo`, then places the first order.
2. Its city changes to `campinas`, then it places a follow-up order.

| Order | Join through customer_id to dim_customers | Join through customer_version_id to dim_customer_history |
|---|---|---|
| First order | campinas: current city | sao paulo: observed city at captured creation |
| Follow-up order | campinas: current city | campinas: observed city at captured creation |

The [AWS check](evidence/olist-parquet-validation.json) verified the historical
joins and that all 99,441 original snapshot orders have unknown pre-capture
customer versions. Use the historical join to attribute captured orders to their
creation-time city; use the current join to describe the order's current customer
record. A later reassignment can make these references point to different records.

### Modeling scope and history implementation

These are scoped dimensional marts. Customer versions implement SCD Type 2
behavior using retained CDC events and dbt SQL; there are no `dbt snapshot`
models. Sequence bounds preserve separate changes even when they share a commit
timestamp. Observation timestamps are deliberately named `observed_from` and
`observed_to` because the original export cannot supply earlier business-effective
dates. Retained events also let late files correct previously built intervals.

Customer history, historical order joins and order-status history demonstrate
how captured changes become useful warehouse state. Product/seller dimensions
would broaden dimensional-modeling coverage but require additional source tables;
they remain optional extensions. Customer tier has no field or business rule in
the current source contract. Additional tiers would require an explicit business
definition and clearly labeled derived or simulated values.

The 13 staging/intermediate models remain views. The six marts incrementally replace affected entities, using newly loaded files to identify work. Six `<mart>__files` metadata tables in `analytics_marts` each store `source_file varchar(2048)`; they are processing checkpoints, not additional business models. Temporary pending-file and affected-key tables exist only during dbt connections. See [incremental processing](incremental-dbt.md) for deletion, history and recovery semantics. Raw ingestion is also incremental. Five-minute scheduling does not imply streaming joins, exactly-once transport, historical address reconstruction or a five-minute latency guarantee.
