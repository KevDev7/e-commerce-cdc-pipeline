# Warehouse layers

The same raw/staging/intermediate/marts structure targets local PostgreSQL for development and Redshift for cloud demonstrations. DMS writes original transaction-preserving CSVs to S3 under `raw/`. They are retained unchanged; per-table CSV inputs for Redshift COPY are derived under `copy-ready/`. Source values remain strings in raw; staging assigns their data types.

The database schemas are `raw`, `staging`, `intermediate`, and `marts`.
The dbt schema-naming macro uses each configured layer name directly. Historical
validation reports may retain the previous `analytics_` names.

## Responsibility boundaries

- **Ingestion:** parse the capture format, validate event structure and prepare
  per-table CSV files before loading RAW. Source column values remain strings.
  Only CDC metadata (sequence, commit time and snapshot flag) is normalized and typed. It does not calculate business
  measures or reconstruct current records. The shared source/event schema lives in
  `src/olist_cdc/schema.py`, independent of source seeding and warehouse drivers.
- **RAW:** retain every delivered row from each new file, including repeated event
  IDs, store source columns as VARCHAR, and commit the file's rows atomically with its load ledger entry.
- **STAGING:** deduplicate by event ID, cast source values to their declared types,
  and expose a consistent source-event
  interface, including the complete raw metadata envelope. Do not reconstruct
  current records or calculate business measures.
- **INTERMEDIATE:** reconstruct source records and observed history, including
  ordering, deletes, version intervals and current-version flags.
- **MARTS:** define analytical grains, joins and measures, and publish facts and
  dimensions. Do not repeat CDC reconstruction or recalculate history flags.

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

`raw.loaded_files` stores source_file, loaded_at, row_count. Raw rows and their ledger entry commit atomically. Redelivery under a different file key adds another raw copy of each event; repeated IDs within a file are also preserved. Already loaded file keys are skipped without downloading again, so retrying a completed file does not add copies. Capture files are immutable: never overwrite a loaded key. A new capture lineage requires fresh raw storage, not reuse of old sequence numbers.

Python sends `COPY` directly to each raw event table. No temporary tables or
intermediate INSERT statements are needed because raw retains every delivered
row. All table COPYs from one original capture file and its file-ledger entry
share one transaction. A failure in any COPY or the ledger write rolls back the
file's raw rows together. Derived CSV files already written to S3 remain available.

## Staging: four views

All four staging views expose typed source fields and the same six raw metadata
fields: event ID, operation, sequence, commit timestamp, snapshot flag and source
file. Each view returns one row per `_event_id`, using `row_number()` and choosing
provenance by ascending `_source_file`. Ties between identical copies do not change
business values. Different events for the same source primary key remain separate.
Source amounts become exact `numeric(14,2)` values, counters become integers,
and dates become timestamps. Invalid casts fail instead of becoming null. Money
with excess fractional precision is rejected rather than rounded. Business
timestamps remain timezone-less; timezone-bearing values are rejected. Naive
`updated_at` values are interpreted as UTC. Nulls stay null, and text columns
retain empty strings and postal-code leading zeros. These conversions run in SQL
views when queried; raw loading can succeed before a dbt conversion fails.

The `conflicting_event_copies` dbt test compares event contents excluding
`_source_file`. If the same ID carries different values, it fails the build rather
than accepting an arbitrary version as valid. Raw retains both copies for inspection.
The `required_source_key` tests reject null or empty source keys in all four
staging models. Python retains those rows instead of rejecting the file. CDC
rows still use the DMS sequence as their event identity. A snapshot row without
a key uses `snapshot-missing-key:<table>:<SHA-256 of file URI and line number>`;
this preserves each bad row separately and stays stable on a retry of the same
immutable file. A copy under another filename gets a separate fallback identity;
without a source key, it cannot be safely matched across files. Valid snapshot
identities remain unchanged. Failed staging tests block dependent dbt models and
prevent Airflow from marking the batch complete; the rows remain available for
inspection in raw and staging. No rows are silently dropped or repaired.

Other tests check staged event uniqueness and supported operations. This is the only staging layer.

## Intermediate: five views

- Four `int_*_current` views rank each source row key by source sequence, select its latest state and exclude tombstones. Late snapshots never overwrite newer changes. These four views expose only source columns (including `updated_at` for reconciliation); event metadata and the internal ranking helper stay inside the calculation.
- `int_customer_history` records observed attribute changes with version IDs,
  observed_from/to, source_order_from/to, is_deleted, is_initial_snapshot and
  is_current. The current flag is true only for the latest non-deleted version.
  A snapshot that overlaps already captured CDC cannot establish an earlier history
  version; it stays in raw/current processing, but ambiguous history begins with CDC.

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
- **dim_customer_history:** publishes the reconstructed history from INTERMEDIATE,
  with those attributes plus customer_version_id, observed_from/to,
  source_order_from/to, is_deleted, is_initial_snapshot and is_current.
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

The simplified diagram draws on star-schema fact/dimension separation and the
multiple fact grains found in galaxy (fact constellation) designs. It is not a
textbook implementation of either pattern: item and payment facts connect through
orders, rather than each directly sharing a set of dimensions. Diagram keys and
relationships are logical, not enforced warehouse constraints. Customer history
retains versions after deletion from the current customer table, so it has no
mandatory foreign key to that table.

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

## Upgrade to staging deduplication

This was the earlier staging-only change. For the current CSV/string-based raw
design, follow [the newer upgrade below](#upgrade-to-string-based-raw).

Pause scheduling and finish any active run before deploying this change. Run a
normal `dbt build` to replace staging views and rebuild marts from retained raw
rows. No raw-table schema change, source backfill or capture restart is needed.
Raw copies suppressed by the older loader cannot be recovered from Redshift;
original files remain in S3. New deliveries under new keys are preserved going
forward. Do not clear the existing file ledger to force a rebuild.

Event ID still identifies one source change, but is no longer unique at raw's
physical grain: raw stores deliveries. Staging returns distinct source events.
The Python parser continues to validate column counts, supported operations,
and ordering metadata; source-key validation now belongs to staging. Duplicate
event IDs are no longer rejected
before preparation of COPY-ready CSV files. This changes deduplication ownership, not the source
schema or supported business operations.

## Upgrade to string-based raw

This change replaces the earlier typed raw columns and Parquet COPY inputs. A
normal dbt build alone cannot migrate those raw tables. The cloud loader checks
raw source-column types and refuses an earlier typed schema before loading files.

For the next authorized AWS demonstration:

1. Pause Airflow and finish active runs. Preserve the existing warehouse and its
   ledger until the replacement has passed reconciliation. Keep original S3
   captures unchanged.
2. Prepare a fresh warehouse/raw baseline with the current DDL. Do not point the
   current loader at the retained typed namespace, clear its ledger, or mix
   independent DMS capture lineages. The normal `CREATE IF NOT EXISTS` statements
   do not migrate existing tables.
3. Replay the original initial-load and CDC CSV captures from the same lineage.
   The loader creates CSV COPY inputs, loads raw strings, and records each file.
   Old derived Parquet files are ignored because loading lists only original
   `.csv` captures under `raw/`; leave them intact as historical artifacts.
4. Run `dbt build` and source-to-target reconciliation while the source is quiet.
   Compare marts and customer history with the prior baseline before resuming
   scheduling or retiring the old warehouse.

No AWS migration has been performed. Provisioning the replacement warehouse and
retiring retained resources remain part of a separately budgeted cloud session.
The earlier staging-only upgrade above describes the prior milestone and is not
sufficient for this raw schema change.
