# Warehouse layers

The same raw/staging/intermediate/marts structure targets local PostgreSQL for development and Redshift for cloud demonstrations. DMS writes original transaction-preserving CSVs to S3 under `olist-v1/`. They are retained unchanged; compressed CSV inputs for Redshift COPY are derived under `copy-ready/`.

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

## Intermediate: eight views

- Four `int_*_current` views rank each source row key by source sequence, select its latest state and exclude tombstones. Late snapshots never overwrite newer changes.
- `int_order_item_totals` aggregates item prices, freight and item count by order.
- `int_order_payment_totals` aggregates payment value and payment-entry count by order. An installment count is not a multiplier for payment_value.
- `int_customer_history` records observed attribute changes with version IDs, observed_from/to, source_order_from/to, is_deleted and is_initial_snapshot. A snapshot that overlaps already captured CDC cannot establish an earlier history version; it stays in raw/current processing, but ambiguous history begins with CDC. This preserves the preceding project's overlap fix.

`int_order_status_history` applies the same overlapping-snapshot safeguard to orders, retains status transitions and deletion markers, and suppresses repeated same-status updates. It uses source sequence to order versions even when commit timestamps match.

## Marts: six tables

- **dim_customers:** one current customer record, with customer_id, customer_unique_id, postal_code, city, state.
- **dim_customer_history:** those attributes plus customer_version_id, observed_from/to, source_order_from/to, is_deleted, is_initial_snapshot and is_current.
- **fct_orders:** source order fields excluding updated_at, plus item_total, freight_total, order_total, item_count, payment_total, payment_count, has_items and has_payments.
- **fct_order_status_history:** order_status_version_id, order_id, status, observed_from/to, source_order_from/to, is_deleted, is_initial_snapshot, is_current and observed_duration_seconds. Closed non-deleted intervals measure observed time; open versions and deletion markers have NULL duration. Historical snapshots do not reconstruct earlier lifecycle transitions.
- **fct_order_items:** source item fields excluding updated_at; one row per order and item sequence.
- **fct_order_payments:** source payment fields excluding updated_at; one row per order and payment sequence.

Items and payments are aggregated separately before joining orders. Two items and two payments therefore remain two of each, not four duplicated combinations. Orders lacking details survive left joins, with explicit presence flags and zero aggregate counts.

Customer history begins at capture observation, years after most historical purchases. Facts reference customer_id; we do not invent a transaction-time version key for historical orders that predate capture. `customer_unique_id` remains available for repeat-customer analysis.

All 18 models rebuild the small current views/marts from retained raw events. Raw ingestion is incremental. Five-minute scheduling does not imply streaming joins, exactly-once transport, historical address reconstruction or a five-minute latency guarantee.
