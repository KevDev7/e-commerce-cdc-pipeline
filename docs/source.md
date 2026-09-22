# Operational source

The project uses four Synthea CSVs to seed a simplified PostgreSQL application database. The simulation then commits new business activity to that database. The workload is simulated; the resulting PostgreSQL WAL is real.

| Table | Grain / key | Relationship |
|---|---|---|
| patients | One patient / patient_id | Parent of encounters and claims |
| encounters | One encounter / encounter_id | Belongs to a patient |
| claims | One claim / claim_id | References patient and encounter |
| claim_transactions | One financial entry / transaction_id | References claim and patient |

Native Synthea UUIDs are retained. The field mapping is explicit in `src/synthea_cdc/seed.py`. We copy a focused subset of columns, not an entire EHR schema. Foreign keys verify identity links, but they do not prove every clinical or financial interpretation.

Blank values stay NULL. In particular, AMOUNT is not a universal payment measure: a PAYMENT row can have AMOUNT blank and PAYMENTS populated. Keep AMOUNT, PAYMENTS, ADJUSTMENTS, TRANSFERS and OUTSTANDING separate when modeling billing. Claim STATUS1 and its three outstanding fields are retained; STATUS1 is not a summary of all payer statuses.

Dates and timestamps are parsed by PostgreSQL with UTC set for loading. A date-only value becomes midnight UTC if loaded into a timestamp column. Added `updated_at` timestamps describe writes to our database, not original Synthea change times, and are not used to extract CDC. Updates refresh them through a trigger.

`REPLICA IDENTITY FULL` allows old values to be available for update/delete decoding. It increases WAL volume; this is an intentional learning choice for a small source.

## Data provenance and repeatability

Source: [official Synthea sample downloads](https://synthetichealth.github.io/downloads.html). The latest CSV sample URL is pinned by SHA-256 in the loader. If its contents change, loading stops for inspection instead of silently changing the dataset. The archive remains in ignored `data/`; it is never committed.

The seed load is one database transaction in parent-before-child order. A completed archive hash is recorded in `project_meta.seed_runs`. Repeating the load leaves subsequent application changes intact. A different seed requires an intentional fresh database rather than an implicit truncate.

Docker Compose uses an isolated `synthea-cdc` project and volume, binds PostgreSQL only to localhost port 55432, and enables logical WAL. The local owner is a development superuser; the cloud demonstration uses the source owner for setup/simulation and a separate DMS reader with SELECT and replication grants. The Docker image digest and Python lockfile pin the tested environment.

## Business simulation

Each scenario uses deterministic UUIDs and records completed phases in the same transaction as its business writes. Retrying a completed phase has no effect. Different scenario names create independent activity.

1. `open`: insert a fictional patient, encounter, OPEN claim, and $100 charge in one transaction.
2. `bill`: close the encounter and mark the claim BILLED.
3. `pay`: insert a $100 payment and close the claim with zero primary outstanding.
4. `correct`: correct the fictional patient's city/postal code.
5. `create-delete-test`: insert dedicated disposable patient/encounter records.
6. `delete-test`: hard-delete only those disposable records, child before parent.
7. `rollback-test`: roll back a sentinel claim status update; CDC must never emit it.

The first four phases are a simplified learning workflow, not a recreation of an insurer's adjudication rules. Hard deletion is a technical test, not a recommended clinical-record workflow. Historical seed records are not modified by the simulation.

To observe each transition separately, execute one phase at a time. The default `all` option still commits separate transactions, but does not simulate realistic elapsed time.

The local integration test uses PostgreSQL's `test_decoding` output plugin to inspect actual WAL. This is a verification tool; the cloud demonstration uses AWS DMS with the same PostgreSQL logical decoding plugin.
