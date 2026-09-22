# Local batch audit

The six Airflow tasks record their attempts in `data/olist-microbatch/audit.sqlite`, using Python's built-in SQLite support. The project bind mount keeps this file across container restarts. This control data stays local; idle batches do not connect to Redshift just to write monitoring records.

```sh
uv run python scripts/report_batches.py
uv run python scripts/report_batches.py --run-id 'your-airflow-run-id'
```

The first command shows the latest 20 runs and the last successful completion. The second shows every task attempt, including load metrics, dbt outcomes, durations and errors. Airflow supplies `BATCH_RUN_ID` and `BATCH_ATTEMPT` to every task. Ungrouped manual commands retain their existing logging behavior and do not invent a shared batch identity.

## Tables and views

`task_attempts` contains run_id, step, attempt, started_at, finished_at, duration_seconds, status and JSON details. Its key is run_id + step + attempt. Status is incomplete until the wrapper finishes, then success, failed or skipped. Retrying a failed task retains earlier attempts.

`batch_runs` is a view using the latest attempt of each task to derive the outcome. A batch succeeds only when the latest completion acknowledgement succeeds after the other recorded work. A quiet pending check yields skipped. Failures stay visible; unfinished or interrupted work is incomplete. Clearing and rerunning an earlier task does not reuse an old completion as evidence of success.

Run summaries expose dbt_status, elapsed_seconds, files_committed, input_inserts, input_updates, input_deletes and snapshot_rows. Elapsed time spans the first attempt through the last recorded finish, including retry delays; it is not source-to-mart latency. The last successful batch excludes quiet runs.

## Metric semantics

Load details include files_listed, files_already_loaded, files_committed, snapshot_rows and input_I/U/D. File/event counts advance only after the raw transaction commits. If a later file fails, the attempt retains counts for its earlier committed files. A retry skips those already loaded files; run summaries sum committed-input metrics across attempts.

These are **incoming records from newly committed file-ledger entries**, not counts of new physical warehouse rows. Replayed events under a different filename still count as input even when event deduplication prevents new rows. Snapshot rows are counted separately from CDC inserts. Absent operation keys mean zero, not an unobserved activity estimate.

Build success means `dbt build` returned successfully; failed dbt tests fail that task. It does not mean all marts were published atomically: some dbt tables may have rebuilt before a later test fails. The batch checkpoint and completion audit are withheld in that case.

Exception classes and subprocess exit codes are recorded, rather than potentially sensitive exception messages. Existing Airflow logs contain execution details. Hard termination, host loss or audit-write failure can leave incomplete records: check Airflow before interpreting them as active work. This is a small demo audit, not a replacement for Airflow state or a distributed monitoring service.

`completed.json` and per-run JSON manifests are replay-control files, separate from the audit database. Deleting the audit loses monitoring history but does not reset the batch checkpoint. Deleting the checkpoint forces processing on the next batch. Pause the DAG and finish active work before changing either.

## Verification

Five SQLite tests cover failed attempts, retry recovery, quiet runs, partial metrics and clearing tasks after completion. SQL/S3 fixtures verify load counters do not advance on an injected commit failure. The real Airflow task-state checks execute the actual audit wrapper around shell fixtures and compare audit status with DAG outcomes. None of these invoke AWS; the Olist live scheduled demo remains pending.
