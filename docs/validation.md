# Validation results

Executed on 2026-09-22. [Machine-readable evidence](evidence/cloud-validation.json) contains the counts, reconciliation hashes, replay checks, business checks and actual Airflow task states.

## Real AWS end-to-end test

The pipeline used RDS PostgreSQL 17.11, DMS 3.6.1, S3, Redshift Serverless at 4 RPUs, dbt Core 1.12.5 with dbt-redshift 1.11.1, and local Airflow 2.11.2 in Docker.

1. Loaded 108 patients, 5,571 encounters, 9,421 claims and 85,047 financial entries into RDS: **100,147 initial rows**.
2. DMS completed all four initial table loads. Actual transformed CSV files passed the parser, including source-position, change-sequence, timestamps and NULL values.
3. Committed scenario `aws-001`: **7 inserts, 4 updates and 2 deletes** reached S3. No rolled-back status appeared.
4. Stopped DMS, committed `aws-002` while it was stopped, then resumed from the saved checkpoint. All **13 additional events** arrived. The raw total became **100,173 distinct events**.
5. Loaded actual CDC files before snapshot files. dbt reconstructed current state using source sequence, rather than arrival order.
6. Built **15 models and passed all 42 dbt tests on Redshift**.
7. Compared every current source column, including `updated_at`, against the four Redshift current-state models using ordered canonical row hashes. Every table matched.
8. Copied a real CDC file to another S3 key and reloaded, then retried the whole batch. All raw row counts and distinct event counts remained unchanged.
9. Executed Airflow run `aws-validation-20260922`: **check → load → build → report**, all successful. Its dbt build passed again. This was a manual run with `schedule=None`, not a continuously scheduled production service.

## Populated Redshift marts

| Mart | Rows |
|---|---:|
| dim_patients | 110 |
| dim_patient_history | 116 |
| fct_encounters | 5,573 |
| fct_claims | 9,423 |
| fct_claim_transactions | 85,051 |

Both simulated claims retained OPEN → BILLED → CLOSED in raw history. Their marts contained one claim each, $100 in charges, $100 in payments, zero primary outstanding balance and two financial entries. Patient history preserved Boston → Cambridge; disposable patients and encounters were absent from current state.

## Local regression checks

**13 tests passed**, covering seed atomicity and retry, real PostgreSQL WAL behavior, rollback exclusion, CSV quoting/NULL handling, unexpected capture paths, failed-file rollback, duplicate delivery, changed-file detection, out-of-order events, patient history and financial aggregation. Local warehouse integration also runs the 42 dbt tests. GitHub Actions runs this suite with a disposable PostgreSQL container and no AWS credentials.

The local full-data fixture is explicitly a snapshot fixture. Cloud validation above uses actual DMS output and Redshift, not mocked AWS services.

## Limits of this demonstration

- The business workload is simulated; the source data is synthetic. This is not a benchmark of hypothetical production event volumes.
- The initial seed was stable during the full load. Concurrent writes during the first snapshot, source failover, schema evolution and task replacement were not tested. A replacement capture lineage requires a fresh prefix and raw baseline.
- The four-table schema is fixed. DMS DDL auditing is disabled; the reader has SELECT and replication privileges. Unexpected CSV layouts fail parsing, but this is not an automated schema-evolution system.
- Patient history records observed changes from capture onward; it cannot recover addresses for historical visits before capture.
- Raw loads are incremental. Small marts are fully rebuilt for simplicity; there is no Spark or always-on cloud orchestration.
- Teardown removes the populated cloud tables to avoid ongoing costs. Code, captured local files, test results and compact evidence allow another authorized demonstration.

## Cost and cleanup

The approved initial allowance was $5. Redshift was constrained to 4 RPUs and a 6-RPU-hour monthly deactivate limit. Its usage view reported **1,680 charged RPU-seconds**, approximately **$0.175 in compute**, at the pre-teardown check. This is a usage-based estimate, not the final bill; it excludes later usage, RDS, DMS, storage, networking, tax and credits.

Cleanup was verified at **08:52 UTC on 2026-09-22**: CloudFormation DELETE_COMPLETE, with the project RDS instance, DMS instance/task, Redshift workgroup/namespace, S3 bucket and generated DMS log group all absent. Local project containers were stopped. Captured files and task logs are preserved in ignored local data folders; temporary container credentials and deleted-resource database passwords were removed. The evidence file records the checks.
