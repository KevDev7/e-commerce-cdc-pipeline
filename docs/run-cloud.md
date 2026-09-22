# Short AWS demonstration

Use only the `synthea-cdc` AWS profile. Agree a new session allowance before provisioning Olist; the historical $5 Synthea sessions have finished. Finish local preparation first. This stack creates billable resources. A Redshift usage limit covers compute only, not the total AWS bill.

The existing AWS profile, stack and ownership labels retain `synthea-cdc`; active database names are `olist`, the source schema is `ecommerce`, and capture prefix is `olist-v1`. Use fresh raw storage and checkpoints for Olist. Preserve the old deleted-stack record and run outputs in an ignored dated archive before an approved new session. Do not update a live Synthea capture task in place.

## Provision and capture

From the repository root, with `.venv` installed:

```sh
.venv/bin/python scripts/cloud_stack.py validate
.venv/bin/python scripts/cloud_stack.py create
.venv/bin/python scripts/cloud_stack.py status
```

`create` makes one CloudFormation stack. It uses three existing default public subnets, new project security groups, no NAT gateway, one RDS PostgreSQL micro instance, one DMS small instance, and one private S3 bucket. Database access is restricted to your current public IPv4 address and the DMS security group. RDS/DMS use encrypted connections. A changed home IP requires updating the stack's ClientCidr parameter. Do not open database ports to everyone.

The stack creates DMS's standard service roles only when absent; existing roles are left alone. All other resources belong to this stack. The source is a reproducible copy of historical Olist data plus disposable simulated activity, so deletion intentionally does not retain a paid database snapshot.

After CREATE_COMPLETE:

```sh
.venv/bin/python scripts/cloud_stack.py env
.venv/bin/python scripts/prepare_cloud_source.py
.venv/bin/python scripts/cloud_stack.py connections
```

Repeat `connections` until both endpoints report `successful`, then start the task. Starting is deliberate: the source must be seeded first. Wait for all four tables to complete their full load; inspect real files before accepting the CSV contract.

```sh
.venv/bin/python scripts/capture.py start
.venv/bin/python scripts/capture.py status
.venv/bin/olist-cdc --cloud simulate --scenario aws-001
```

For recovery testing, stop capture, wait until stopped, commit a second scenario, then resume from the saved checkpoint:

```sh
.venv/bin/python scripts/capture.py stop
.venv/bin/python scripts/capture.py status
.venv/bin/olist-cdc --cloud simulate --scenario aws-002
.venv/bin/python scripts/capture.py resume
```

## Warehouse and orchestration

```sh
.venv/bin/python scripts/cloud_stack.py warehouse
.venv/bin/python scripts/cloud_stack.py status
.venv/bin/python scripts/cloud_stack.py env
.venv/bin/python scripts/cloud_stack.py usage-limit
cp dbt/profiles.yml.example dbt/profiles.yml
.venv/bin/python scripts/airflow_credentials.py
docker compose -f compose.airflow.yaml up -d --build
```

Run `usage-limit` before queries. It checks base/max capacity are both 4 RPUs and creates a monthly 6-RPU-hour limit with the deactivate action. At the checked regional rate, that is $2.25 in compute; enforcement/billing delay and other services mean this is not a guaranteed $5 account cap.

Airflow runs locally at http://localhost:8085 using its standalone development setup. Its generated login is stored in the container's `/opt/airflow/standalone_admin_password.txt`. The six tasks are **check capture → check pending files → load raw → dbt build (including tests) → report marts → acknowledge batch**.

The DAG schedules every five minutes (`*/5 * * * *`), with `catchup=False` and one active run at a time. It starts paused on first installation. Airflow preserves pause state on existing installations, so explicitly pause it before preparing another demo. Once capture and the warehouse are ready, enable the schedule for the demonstration:

```sh
docker compose -f compose.airflow.yaml exec airflow airflow dags unpause olist_cdc
```

DMS captures continuously while the stack exists. The Airflow schedule processes available S3 files; it does not restrict ingestion to events from a particular five-minute window. A newly committed change can wait for DMS delivery, the next scheduled run, and warehouse processing. Five minutes is the trigger interval, not a guaranteed end-to-end latency. If a run takes longer, subsequent runs wait rather than overlap; keep the demo small and inspect Airflow run durations.

The pending-file task compares S3 CSV keys, ETags and sizes with the last completed batch. When unchanged, it exits with Airflow's skip code and **load, build, report and acknowledgement are skipped without connecting to Redshift**. Capture health is still checked. Empty or inaccessible capture fails visibly rather than being treated as a quiet batch.

The local checkpoint is under ignored `data/olist-microbatch/` in the bind-mounted project directory. It advances only after loading, dbt tests and reporting succeed. If loading succeeds but dbt fails, the next run still builds the marts even though the raw file ledger already contains those files. Files arriving after the pending check are picked up again next run if necessary. Missing checkpoint state causes a safe extra load/build. Keep one scheduler for this checkout, do not run manual warehouse commands concurrently, and clear this checkpoint directory while paused if resetting the warehouse or restoring a baseline. Transformation changes also require the explicit full-refresh procedure below; clearing only the local checkpoint does not reset dbt model checkpoints. Per-run manifests are small local demo artifacts and may also be cleared while paused.

Inspect batches without querying Redshift using `.venv/bin/python scripts/report_batches.py`; add `--run-id` to see task attempts and load counts. [The local audit](batch-audit.md) lives in `data/olist-microbatch/audit.sqlite`. When resetting only replay-control state, remove `completed.json` and per-run JSON manifests while paused; keep the SQLite file if retaining monitoring history.

Before cleanup, pause future runs, let the active run finish, and stop local Airflow. Pausing alone does not cancel a running batch:

```sh
docker compose -f compose.airflow.yaml exec airflow airflow dags pause olist_cdc
# After the active run finishes:
docker compose -f compose.airflow.yaml stop
```

Skipping idle warehouse work reduces query activity, but RDS and DMS still cost money while provisioned. Delete the stack after each demo using the cleanup steps below.

`scripts/run_cloud.py check|load|build|report` runs the exact same commands manually. The project virtual environment is separate from Airflow's dependencies inside the image. Only a temporary session for the project AWS profile is made available to the container; other AWS profiles are not mounted. It inherits the developer user's permissions for this portfolio test, not a production runtime role. Refresh that session after one hour if a later authorized demo needs it.

After changing transformation logic that must apply to existing rows, pause scheduling and wait for any active run to finish, then run:

```sh
.venv/bin/python scripts/run_cloud.py build --full-refresh
```

This rebuilds marts from retained raw events and resets each model's file checkpoint in its transaction. It runs tests and uses Redshift compute within the agreed session budget. Normal scheduled builds remain incremental. See [incremental recovery](incremental-dbt.md).

## Replay and cleanup

The loader stores original DMS files unchanged, derives compressed COPY inputs under `copy-ready/`, and commits the raw records with the file ledger in one transaction. A replay skips identical files; events redelivered under another file name are deduplicated by source identity. Build marts only after the batch finishes. Each capture lineage requires a fresh raw baseline, not a reset of an existing task's sequence.

```sh
.venv/bin/python scripts/cloud_stack.py delete
# The first call stops a running capture task. After it stops, repeat:
.venv/bin/python scripts/cloud_stack.py delete
.venv/bin/python scripts/cloud_stack.py status
docker compose -f compose.airflow.yaml stop
```

Deletion downloads captured files to ignored `data/aws-capture`, empties the project bucket, then deletes the stack. After `status` confirms DELETE_COMPLETE, run `.venv/bin/python scripts/cloud_stack.py cleanup-logs` to remove the DMS-generated log group. Verify no project RDS/DMS/Redshift resources remain. The local state file records the stack ID and test start time; creation refuses another session while that record exists. Keep it as evidence until reviewing costs and approving any additional session. Remove `.aws/credentials` after the test. Do not delete or modify resources belonging to other projects.

With the source quiescent and the latest DMS batch loaded, `.venv/bin/python scripts/reconcile_cloud.py` compares every current field with Redshift. `.venv/bin/python scripts/verify_replay.py` redelivers a real CDC file and retries the batch, asserting unchanged raw counts and unique event identities. These tools were adapted for Olist; their previous AWS executions used Synthea and do not validate this new schema. Reconciliation against an actively changing source would require coordinating a common checkpoint, which this small demo does not automate.
