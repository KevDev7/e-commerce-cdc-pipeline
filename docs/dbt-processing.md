# dbt processing and retries

Capture and raw ingestion are incremental. The five business marts use dbt's
standard `table` materialization: every active batch rebuilds them from the
intermediate views. Staging and intermediate remain views.

## One batch

1. Airflow checks whether S3 captures changed since the last successful batch.
   If unchanged, it skips warehouse work without connecting to Redshift.
2. The raw loader loads only new files. `raw.loaded_files` and event identities
   prevent duplicate ingestion; raw rows and the file entry commit together.
3. `dbt build` reconstructs current state and customer history through the views,
   rebuilds all five marts, and runs data tests.
4. Only a successful build and tests advance Airflow's batch completion marker.

There is no `marts.processed_files` table, per-model checkpoint, affected-key
filter or custom incremental hook in the current design.

## Why history and deletes still work

Raw retains the initial snapshot and captured change events. Current-state views
choose the newest event for each source key and exclude deleted rows. Customer
history derives versions from the retained events each time; rebuilding does not
forget earlier customer attributes. A late file is considered on the next build.

The order fact aggregates items and payments separately before joining orders,
so multiple items and payments do not multiply totals. Orders reference the
current customer dimension. Historical customer-to-order matching is out of scope.

## Retry a failure

If raw loading succeeds but dbt fails, the batch completion marker does not move.
The next run skips already loaded raw files and rebuilds the marts again.
With the same raw data and model SQL, repeated builds produce the same business
rows and history, without accumulating duplicates.

A model build can succeed before a later model or data test fails. The five marts
are not published as one atomic warehouse transaction. Wait for the complete
`dbt build` and tests to succeed before considering a batch complete. Run one
warehouse writer at a time, as enforced by the DAG's single active run setting.

After editing transformation SQL, pause Airflow and wait for active work to finish,
then run `python scripts/run_cloud.py build`. An ordinary build recomputes all
existing rows; no special full-refresh flag or checkpoint reset is needed.
See the [runbook](run-cloud.md#upgrade-after-the-modeling-simplification) for
removing retired relations from an older retained warehouse.

## Cost and verification

Rebuilding writes all mart rows on every active batch, so warehouse work can grow
with retained data. Idle batches still skip warehouse work. Local fixture timing
is not a Redshift cost or latency guarantee; check cloud runtime during the next
authorized session before claiming the earlier five-minute results still apply.

`tests/test_mart_rebuilds.py` exercises successive changes, hard deletes,
reinsertion, late events, replay, repeated builds and SQL failure/retry. Customer
history and current joins have separate checks. Raw atomic loading and Airflow
failure/skip behavior retain their existing tests. Dated [validation results](validation.md)
distinguish earlier incremental marts from this simpler rebuild approach.
