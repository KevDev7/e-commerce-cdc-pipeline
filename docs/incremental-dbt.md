# Incremental dbt processing

Capture still reads PostgreSQL WAL through DMS. This change affects the warehouse transformation stage: all six marts use dbt `incremental` materialization, a `unique_key`, and `delete+insert`. Staging and intermediate remain views. There is no new scheduler or service.

## How one model processes a batch

1. A transactional pre-hook creates the model's file ledger if missing and freezes the list of unprocessed files from `raw.loaded_files` into a temporary table. A first build or full refresh resets that ledger within the transaction.
2. On incremental runs, raw events from those files identify affected entity keys. Customer marts use customer_id, order history uses order_id, and detail facts use their row keys. `fct_orders` also includes orders referenced by changed items/payments, including deleted details and any previously observed parent.
3. The hook removes the affected entities from the target. This explicit removal handles hard deletes that produce no replacement row and history versions made obsolete by a late event; ordinary `delete+insert` only removes keys present in its incoming result.
4. Shared SQL macros place the affected-key filter before current-state/history window functions, rather than depending on the optimizer to push a predicate through a view. The intermediate views use those same definitions without an incremental filter. All retained events for those entities remain available to resolve source order, history intervals and aggregate totals. dbt applies its standard `delete+insert` strategy with the model's unique key.
5. A transactional post-hook acknowledges only the frozen file list. The mart writes and checkpoint commit together. Failure rolls both back.

Each mart owns an `analytics_marts.<mart>__files` ledger. A successful selected model cannot consume another model's work. First deployment over existing tables processes all retained files because these ledgers are initially empty. A full refresh rebuilds every row, including after transformation logic changes.

An arrival checkpoint answers “which files have I processed?” Source sequence answers “which event is newer?” Keeping those separate prevents a late, lower-sequence file from being skipped. A file redelivering only known event IDs has no new raw rows, so it advances the model checkpoints without rewriting mart rows.

## Recovery and limits

Run one warehouse writer at a time: the DAG already allows one active run. Pause it and wait for completion before manual loads/builds. This implementation does not coordinate simultaneous dbt invocations or publish all marts atomically.

Model checkpoints commit after successful model SQL, before the graph's data tests finish. A failing test still prevents Airflow batch acknowledgement. On retry, already successful models retain their checkpoints and the tests run again. If fixing a failure requires changing transformation logic for previously processed rows, use a full refresh; rerunning an incremental model without new files does not apply changed SQL to its old rows.

For a local rebuild from the existing raw fixture:

```sh
uv run python scripts/build_local_warehouse.py --full-refresh
```

For Redshift during an authorized cloud session, with the scheduler paused:

```sh
uv run python scripts/run_cloud.py build --full-refresh
```

These commands run `dbt build --full-refresh`, including data tests. A schema change deliberately fails normal incremental builds (`on_schema_change: fail`); update the model and rebuild intentionally. Keep raw events for replay and history reconstruction. Do not reset raw data while retaining mart checkpoints; a new capture lineage requires fresh warehouse state.

Incremental here means window calculations use the affected entities' histories and mart writes are limited to those entities. Views, key discovery and tests can still scan retained raw data; this is not a guarantee of constant query cost as history grows. No scale or Redshift performance benchmark is claimed. Per-model file ledgers also grow with retained files; partitioning and retention tuning are outside this small demonstration.

## Evidence

`tests/test_incremental_marts.py` compares successive builds against a full refresh across all six marts, checks that unrelated rows retain their PostgreSQL row identities, and injects a failure after checkpoint insertion to verify transaction rollback. It covers detail-only changes, deleting the last item/payment, reinsertion, late changes that remove old history boundaries, duplicate delivery, no-input runs and independent model checkpoints. `tests/test_marts.py` additionally loads stable or overlapping snapshots after the first mart build.

The real 415,418-row Olist fixture also upgrades and reconciles locally. The live Olist AWS demonstration also verifies Redshift execution, rollback, replay and scheduled batches; see [validation evidence](validation.md). Neither demonstration establishes sustained production throughput.

## Customer version dependencies

Order facts also revisit orders linked to customers with newly loaded events.
This handles late customer history even when the order itself did not change.
Creation anchors come only from non-snapshot INSERT events; key reuse selects
the latest INSERT. Canonical observed history provides stable version IDs shared
with dim_customer_history. A full build orders that dimension before the fact.
The fact reads the intermediate history view so an isolated fact run does not
silently use an older materialized dimension. Run the complete graph before
claiming all mart relationships have passed.

The customer join adds columns to fct_orders. Existing warehouses must run
`python scripts/run_cloud.py build --full-refresh` once (or the local equivalent)
after this upgrade. Retained raw events rebuild the assignments; no source reload
or capture reset is needed. Normal runs afterward remain incremental.

The customer-version lookup reads the canonical history view; unlike the fact's order/detail calculations, its customer window scan is not guaranteed to be limited to affected customers. Incremental materialization reduces fact writes, not necessarily every upstream scan.

The current graph has 17 models: four staging views, seven intermediate views
and six incremental marts. Two unused intermediate aggregate views were removed;
`fct_orders` continues to calculate its existing affected-order totals internally.
Earlier cloud reports retain the model counts from those tested revisions.
