# Warehouse layers

The same raw/staging/intermediate/marts structure targets local PostgreSQL for development and Redshift for cloud demonstrations. DMS writes original transaction-preserving CSVs to S3 under `raw/`. They are retained unchanged; explicitly typed Zstandard Parquet inputs for Redshift COPY are derived under `copy-ready/`.

The database schemas are `raw`, `staging`, `intermediate`, and `marts`.
The dbt schema-naming macro uses each configured layer name directly. This
configuration applies to the next warehouse build; historical validation reports
retain the previous `analytics_` names.

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
- `int_customer_history` records observed attribute changes with version IDs, observed_from/to, source_order_from/to, is_deleted and is_initial_snapshot. A snapshot that overlaps already captured CDC cannot establish an earlier history version; it stays in raw/current processing, but ambiguous history begins with CDC. This preserves the preceding project's overlap fix.

## Marts: five incremental tables

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

The order fact reads the intermediate views and aggregates affected current items and payments separately before joining orders; no separate aggregate views are built. Payment value is not multiplied by installment count. Two items and two payments therefore remain two of each, not four duplicated combinations. Orders lacking details survive left joins, with explicit presence flags and zero aggregate counts.

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
status; original order changes remain in raw, without a separate status-history mart. Product/seller dimensions
would broaden dimensional-modeling coverage but require additional source tables;
they remain optional extensions. Customer tier has no field or business rule in
the current source contract. Additional tiers would require an explicit business
definition and clearly labeled derived or simulated values.

The nine staging/intermediate models remain views. The five marts incrementally replace affected entities, using newly loaded files to identify work. One `marts.processed_files` tracking table stores `model_name varchar(128)` and `source_file varchar(2048)`; each pair records one model's completed input file. It is not an additional business model. Temporary pending-file and affected-key tables exist only during dbt connections. See [incremental processing](incremental-dbt.md) for deletion, history and recovery semantics. Raw ingestion is also incremental. Five-minute scheduling does not imply streaming joins, exactly-once transport, historical address reconstruction or a five-minute latency guarantee.

Event IDs are readable strings rather than SHA-256 digests. The raw column is
`varchar(128)` to fit composite snapshot keys. Source business IDs are unchanged.
Replay into a fresh raw warehouse when upgrading from hashed IDs; do not mix the
two representations in an existing warehouse. Rebuild marts from that raw baseline.
The retained CSV format is unchanged, including its source `updated_at` fields.
Removing those fields would require a new capture format, so they remain.

Metadata cleanup: prepared Parquet and raw tables no longer persist `_source_lsn`.
Original DMS CSVs retain their captured format; the parser checks the incoming log
position but does not carry it into the warehouse. Six raw metadata fields remain.
History bounds, deletion/snapshot flags and mart file ledgers remain because they
support ordered history, honest history interpretation and safe incremental builds.
Use a fresh warehouse for this raw-column change; existing raw tables require
rebuilding from the retained CSVs. No paid cloud run was performed for this change.
