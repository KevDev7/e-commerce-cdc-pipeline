# Short AWS demonstration

Use only the `synthea-cdc` AWS profile. Agree a new session allowance before provisioning Olist; the historical $5 Synthea sessions have finished. Finish local preparation first. This stack creates billable resources. A Redshift usage limit covers compute only, not the total AWS bill.

The existing AWS profile, stack and ownership labels retain `synthea-cdc`; active database names are `olist`, the source schema is `ecommerce`, and capture prefix is `raw/dms/olist`. Use fresh raw storage and checkpoints for Olist. Preserve the old deleted-stack record and run outputs in an ignored dated archive before an approved new session. Do not update a live Synthea capture task in place.

## Provision and capture

From the repository root, with `.venv` installed:

```sh
.venv/bin/python scripts/cloud_stack.py validate
.venv/bin/python scripts/cloud_stack.py create
.venv/bin/python scripts/cloud_stack.py status
```

`create` makes one CloudFormation stack. It uses three existing default public subnets, new project security groups, no NAT gateway, one RDS PostgreSQL micro instance, one DMS small instance, and one private S3 bucket. Database access is restricted to your current public IPv4 address and the DMS security group. RDS/DMS use encrypted connections. A changed home IP requires updating the stack's ClientCidr parameter. Do not open database ports to everyone.

The stack creates DMS's standard service roles only when absent; existing roles are left alone. All other resources belong to this stack. The source is a reproducible copy of historical Olist data plus disposable simulated activity, so deletion intentionally does not retain a paid database snapshot.

The S3 bucket has CloudFormation `Retain` policies. It survives stack deletion, while RDS and Redshift do not retain database snapshots. Each new demo creates a fresh capture lineage in a new bucket; do not merge unrelated source sequences.

After CREATE_COMPLETE:

```sh
.venv/bin/python scripts/cloud_stack.py env
.venv/bin/python scripts/prepare_cloud_source.py
.venv/bin/python scripts/cloud_stack.py connections
```

`prepare_cloud_source.py` downloads the pinned ZIP into a temporary directory, uploads a copy as `source/original-olist-brazilian-ecommerce.zip`, loads RDS and removes the temporary directory on exit. It never creates a local source database.

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

A newly created workgroup can report available before its SQL endpoint is reachable. If the first connection times out, check workgroup status and the existing `/32` security rule, allow initialization to finish, then retry the same command. A connection failure before loading writes no raw data or file checkpoint.

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

## Incremental mart rollback check

With Airflow paused and the initial marts already built, simulate another order
scenario and load its capture files without building marts. Then run:

```sh
.venv/bin/python scripts/verify_mart_failure.py
.venv/bin/python scripts/run_cloud.py build
```

The check injects a SQL error after the `fct_orders` checkpoint write using a
temporary copy of the dbt project. It compares actual Redshift rows and checkpoint
state before and after failure, retries the model, and records the result under
ignored `data/mart-failure.json`. The normal build afterward updates the remaining
marts and runs all data tests. Original project SQL is not modified by the check.

## Replay and cleanup

The loader stores original DMS files unchanged, derives explicitly typed Zstandard Parquet COPY inputs under `copy-ready/`, and commits the raw records with the file ledger in one transaction. A replay skips identical files; events redelivered under another file name are deduplicated by source identity. Build marts only after the batch finishes. Each capture lineage requires a fresh raw baseline, not a reset of an existing task's sequence.

```sh
.venv/bin/python scripts/cloud_stack.py delete
# The first call stops a running capture task. After it stops, repeat:
.venv/bin/python scripts/cloud_stack.py delete
.venv/bin/python scripts/cloud_stack.py status
docker compose -f compose.airflow.yaml stop
```

Before deletion, stop simulated writes. Deletion retains the bucket and its data
and removes the compute stack. It does not export sample rows or download data
to the workstation. It refuses to delete a legacy stack whose deployed bucket lacks `DeletionPolicy=Retain`; update that policy first. The small `data/cloud-state.json` records `retained_bucket`, region and capture prefix before deletion. After `status` confirms DELETE_COMPLETE, run `.venv/bin/python scripts/cloud_stack.py cleanup-logs` to remove the DMS log group. Verify no project RDS/DMS/Redshift compute or database snapshots remain, and verify the retained bucket is private and readable with the project profile. Remove `.env.cloud` and `.aws/credentials` afterward. Keep deployment metadata before approving another session. Never modify other projects' resources.

The retained bucket incurs S3 storage/request charges until deliberately removed. It remains accessible through the project AWS profile after its stack-managed DMS/COPY roles are removed. Rebuilding a warehouse later requires a new COPY role scoped to that retained bucket, the matching capture prefix and a fresh raw database/ledger; then load the original CSV captures to regenerate Parquet and rebuild dbt. Retention is not an automated cross-session restore service.

With the source quiescent and the latest DMS batch loaded, `.venv/bin/python scripts/reconcile_cloud.py` compares every current field with Redshift. `.venv/bin/python scripts/verify_replay.py` redelivers a real CDC file and retries the batch, asserting unchanged raw counts and unique event identities. See the Olist validation evidence for completed executions. Reconciliation against an actively changing source would require coordinating a common checkpoint, which this small demo does not automate.

## Verify a completed simulated lifecycle

After dbt succeeds, check named scenarios against real raw events and marts:

```sh
uv run python scripts/verify_cloud_scenario.py demo-001
uv run python scripts/reconcile_cloud.py
```

The scenario check requires all eight simulation phases. It verifies 8 inserts,
4 updates and 2 deletes, unique event identities, the four observed order statuses,
independent item/payment totals, customer address history, hard-delete application
and exclusion of the rolled-back update. Reconciliation compares every current
source field after the source is quiet. Saved reports remain in ignored `data/`.

## Verify customer versions for new orders

With Airflow paused, after a full simulated lifecycle corrected the customer's city:

```sh
uv run python scripts/verify_customer_join.py write --scenario demo-001
# Wait for DMS to deliver the new INSERT, then load and build.
uv run python scripts/run_cloud.py load
uv run python scripts/run_cloud.py build
uv run python scripts/verify_customer_join.py verify --scenario demo-001
```

The added order intentionally has no items/payments. The check proves the original
order retains sao paulo, the follow-up uses campinas, and all 99,441 original orders
retain unknown pre-capture customer history. This is a simulated repeat order on
the reconstructed source, not an extra record from Olist. Existing warehouses need
one full refresh to add the new fact columns before returning to incremental runs.

## Local data retention

Use cloud databases for normal demonstrations. Keep code and small sanitized validation reports, not local datasets after demos. No local PostgreSQL container is required for the cloud commands.
Remove the downloaded `data/olist` seed and any explicitly requested
`data/aws-capture` archive when finished. The local PostgreSQL volume contains both
source and warehouse rows; remove the stopped project containers and their
`olist-cdc_source-data` volume to remove those rows too. The
`olist-cdc_airflow-data` volume holds local Airflow metadata/logs and can also be
removed after saving the small validation summary. Do not remove other projects'
containers or volumes. Future local tests recreate disposable databases.
