# Short AWS demonstration

Use only the `synthea-cdc` AWS profile. The initial authorized allowance is $5; finish local preparation before provisioning. This stack creates billable resources. A Redshift usage limit covers compute only, not the total AWS bill.

## Provision and capture

From the repository root, with `.venv` installed:

```sh
.venv/bin/python scripts/cloud_stack.py validate
.venv/bin/python scripts/cloud_stack.py create
.venv/bin/python scripts/cloud_stack.py status
```

`create` makes one CloudFormation stack. It uses three existing default public subnets, new project security groups, no NAT gateway, one RDS PostgreSQL micro instance, one DMS small instance, and one private S3 bucket. Database access is restricted to your current public IPv4 address and the DMS security group. RDS/DMS use encrypted connections. A changed home IP requires updating the stack's ClientCidr parameter. Do not open database ports to everyone.

The stack creates DMS's standard service roles only when absent; existing roles are left alone. All other resources belong to this stack. The source is a disposable synthetic dataset, so deletion intentionally does not retain a paid database snapshot.

After CREATE_COMPLETE:

```sh
.venv/bin/python scripts/cloud_stack.py env
.venv/bin/python scripts/prepare_cloud_source.py
.venv/bin/python scripts/cloud_stack.py connections
```

Verify both DMS endpoint tests succeed, then start the task with `start-replication`. Starting is deliberate: the source must be seeded first. Wait for all four tables to complete their full load; inspect real files before accepting the CSV contract. Run the existing simulator with `.env.cloud` loaded to commit changes to RDS.

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

Airflow runs locally at http://localhost:8085 using its standalone development setup. Its generated login is stored in the container's `/opt/airflow/standalone_admin_password.txt`. The four tasks are **check capture → load raw → dbt build (including tests) → report marts**. `schedule=None` means each demonstration is manually triggered, avoiding unattended cloud queries. DMS itself stays running during the demonstration. For regular operation, a downstream batch schedule can be set without restarting DMS.

`scripts/run_cloud.py check|load|build|report` runs the exact same commands manually. The project virtual environment is separate from Airflow's dependencies inside the image. Only a temporary session for the project AWS profile is made available to the container; other AWS profiles are not mounted. It inherits the developer user's permissions for this portfolio test, not a production runtime role. Refresh that session after one hour if a later authorized demo needs it.

## Replay and cleanup

The loader stores original DMS files unchanged, derives compressed COPY inputs under `copy-ready/`, and commits the raw records with the file ledger in one transaction. A replay skips identical files; events redelivered under another file name are deduplicated by source identity. Build marts only after the batch finishes. Each capture lineage requires a fresh raw baseline, not a reset of an existing task's sequence.

```sh
.venv/bin/python scripts/cloud_stack.py delete
# The first call stops a running capture task. After it stops, repeat:
.venv/bin/python scripts/cloud_stack.py delete
.venv/bin/python scripts/cloud_stack.py status
docker compose -f compose.airflow.yaml stop
```

Deletion downloads captured files to ignored `data/aws-capture`, empties the project bucket, then deletes the stack. Confirm DELETE_COMPLETE and no project RDS/DMS/Redshift resources remain. The local state file records the stack ID and test start time; creation refuses another session while that record exists. Keep it as evidence until reviewing costs and approving any additional session. Remove expired `.aws/credentials` after the test. Do not delete or modify resources belonging to other projects.
