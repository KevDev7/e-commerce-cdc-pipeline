# Incremental dbt processing

Capture still reads PostgreSQL WAL through DMS. This change affects the warehouse transformation stage: all six marts use dbt `incremental` materialization, a `unique_key`, and `delete+insert`. Staging and intermediate remain views. There is no new scheduler or service.

## How one model processes a batch

1. A run-start hook creates `marts.processed_files` before dbt workers start. Each model's transactional pre-hook locks this table and freezes its unprocessed files from `raw.loaded_files` into a temporary table. A first build or full refresh resets only that model's entries within the transaction.
2. On incremental runs, raw events from those files identify affected entity keys. Customer marts use customer_id, order history uses order_id, and detail facts use their row keys. `fct_orders` also includes orders referenced by changed items/payments, including deleted details and any previously observed parent.
3. The hook removes the affected entities from the target. This explicit removal handles hard deletes that produce no replacement row and history versions made obsolete by a late event; ordinary `delete+insert` only removes keys present in its incoming result.
4. Marts read the intermediate views directly, then select the affected entities. Those views reconstruct current state and history from all retained events. dbt applies its standard `delete+insert` strategy with the model's unique key. This keeps the SQL aligned with the layer diagram; view calculations may scan more history than the rows written to the marts.
5. A transactional post-hook acknowledges only the frozen file list. The mart writes and checkpoint commit together. Failure rolls both back.

All six marts share `marts.processed_files`, with `model_name varchar(128)` and `source_file varchar(2048)`. Every checkpoint lookup, reset and insert is scoped to the model name. A successful selected model cannot consume another model's work. First deployment over existing tables processes all retained files because these ledgers are initially empty. A full refresh rebuilds every row, including after transformation logic changes.

An arrival checkpoint answers “which files have I processed?” Source sequence answers “which event is newer?” Keeping those separate prevents a late, lower-sequence file from being skipped. A file redelivering only known event IDs has no new raw rows, so it advances the model checkpoints without rewriting mart rows.

## Recovery and limits

Run one warehouse writer at a time: the DAG already allows one active run. Pause it and wait for completion before manual loads/builds. This implementation does not coordinate simultaneous dbt invocations or publish all marts atomically.

Model checkpoints commit after successful model SQL, before the graph's data tests finish. A failing test still prevents Airflow batch acknowledgement. On retry, already successful models retain their checkpoints and the tests run again. If fixing a failure requires changing transformation logic for previously processed rows, use a full refresh; rerunning an incremental model without new files does not apply changed SQL to its old rows.

For Redshift during an authorized cloud session, with the scheduler paused:

```sh
uv run python scripts/run_cloud.py build --full-refresh
```

This command runs `dbt build --full-refresh`, including data tests. A schema change deliberately fails normal incremental builds (`on_schema_change: fail`); update the model and rebuild intentionally. Keep raw events for replay and history reconstruction. Do not reset raw data while retaining mart checkpoints; a new capture lineage requires fresh warehouse state.

Incremental here means mart writes are limited to affected entities. Intermediate views, key discovery and tests can scan retained raw data; this is not a guarantee of constant query cost as history grows. No scale or Redshift performance benchmark is claimed. The shared ledger also grows with retained files; partitioning and retention tuning are outside this small demonstration.

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
`python scripts/run_cloud.py build --full-refresh` once
after this upgrade. Retained raw events rebuild the assignments; no source reload
or capture reset is needed. Normal runs afterward remain incremental.

All mart inputs now use intermediate views directly. Incremental materialization reduces writes, not necessarily upstream scans. The simplification needs a new Redshift timing check before claiming the earlier cloud run durations apply.

The current graph has 17 models: four staging views, seven intermediate views
and six incremental marts. Two unused intermediate aggregate views were removed;
`fct_orders` continues to calculate its existing affected-order totals internally.
Earlier cloud reports retain the model counts from those tested revisions.

## Shared checkpoint table

Mart transactions lock `processed_files` before checkpoint reads and release it
at commit or rollback. This intentionally serializes mart updates within a dbt
run, while views and tests can still use multiple threads. It favors simple,
safe retries over parallel mart throughput for this small portfolio workload.
No extra uniqueness constraint is assumed: model-scoped pending-file selection
and the transaction lock prevent duplicate checkpoint pairs in a supported run.

There is no active warehouse to migrate. The next fresh build creates only the
shared table; old evidence retains the former `<mart>__files` names. Existing
older warehouses would require a planned full rebuild and retirement of the old
checkpoint tables. No paid Redshift validation was performed for this change;
CI checks first builds with two dbt threads, selected-model rollback/retry,
selected full refresh isolation, no-input runs, and full rebuild equivalence.
