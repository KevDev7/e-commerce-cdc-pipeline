# Warehouse layers

The warehouse contains an append-only raw event history, staging views, intermediate views, and five materialized marts. At this project's volume, rebuilding these small mart tables is easier to understand and verify than adding incremental state to every model. Raw ingestion is incremental and records processed files; mart computation is currently a full rebuild from retained events.

| Layer | Responsibility |
|---|---|
| S3 raw landing | Retain original DMS snapshot and change files for replay |
| Redshift raw | Typed source values plus operation, sequence, position, commit timestamp and file provenance |
| staging | Consistent source/event columns |
| intermediate | Latest state, delete handling, patient history and claim-level payment aggregates |
| marts | Current patients, observed patient history, encounters, claims and financial entries |

dbt's default schema naming yields `analytics_staging`, `analytics_intermediate`, and `analytics_marts`. Raw data uses `raw`. There is one staging layer. The same SQL is exercised on PostgreSQL for inexpensive local validation and was also executed successfully with the Redshift adapter against real Redshift.

## Event ordering and replay

DMS uses a single task for the four `healthcare` tables. The validated S3 contract is transaction-ordered CSV with `PreserveTransactions=true`, all I/U/D operations, an explicit null marker, and added source-position, change-sequence and commit-timestamp fields. The parser follows the [documented DMS CSV layout](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.S3.html): operation, table, schema, original columns, then configured metadata. The transformed column order and metadata were confirmed against real DMS 3.6.1 full-load and CDC files.

The [DMS change sequence](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TableMapping.SelectionTransformation.Expressions.html) provides a task-level ordering value. An event identity combines table and sequence. Snapshot rows have sequence zero and identity based on table and primary key. Current state selects the greatest sequence, then excludes tombstones; filtering deletes before ranking would resurrect deleted records. Re-delivery is deduplicated and arrival order does not determine current state.

One warehouse corresponds to one baseline and one DMS task lineage. An intentional full reload or replacement task must use a fresh landing prefix and fresh raw tables; do not mix independent sequence histories. This simple project does not automate task-lineage migration or source failover.

The processed-file ledger and each file's raw writes commit together. File retries are skipped only after a successful transaction, and changing already-loaded file content is an error. Raw values remain available for rebuilding models. An unexpected CSV layout or missing sequence stops loading rather than silently guessing. The source schema is fixed; automatic DDL propagation and schema evolution are not implemented.

## Marts and grain

- `dim_patients`: one row per currently present patient.
- `dim_patient_history`: one row per observed patient attribute change or deletion. It exposes observation-time and source-order bounds plus a current flag.
- `fct_encounters`: one row per currently present encounter.
- `fct_claims`: one row per currently present claim, joined to financial totals aggregated to claim grain first.
- `fct_claim_transactions`: one row per currently present financial entry. Charges, payments, adjustments and transfers retain separate source fields.

Patient history begins when capture starts. It cannot reconstruct a patient's address at a historical 2016 encounter from a later static export. Facts therefore retain patient IDs; they do not claim an unknowable historical address match. Source-order bounds distinguish changes with identical timestamps. No-change patient updates do not create a new attribute version; deletions close the previous version and add a tombstone.

Payment totals sum the PAYMENTS field, not generic AMOUNT. Charge totals sum AMOUNT only on CHARGE records. NULL contributions count as zero for those aggregate measures, while raw NULLs and the three original claim outstanding fields are preserved. TRANSFERIN/TRANSFEROUT are not added to revenue. These definitions describe the sample fields, not a complete insurance accounting system.

## Local verification

```bash
uv run python scripts/build_local_warehouse.py
uv run pytest --integration -q
```

The first command creates a separate local `synthea_warehouse` database, loads an explicitly labeled snapshot fixture from the source and runs dbt. It is **not** a CDC extractor. Repeating it retains the existing fixture and rebuilds models. Integration tests separately exercise DMS-format change fixtures in temporary databases, including out-of-order arrival, replay, hard deletes, multiple payments per claim, and patient history. The source WAL test independently confirms actual PostgreSQL log behavior. The real end-to-end DMS → S3 → Redshift and Airflow results are recorded in [validation results](validation.md).
