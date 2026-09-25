# Olist reliability checks

The pipeline preserves source ordering, atomic loading and retry-safe recovery.
Dated cloud results and their limits are in [Olist validation](validation.md).

The local suite verifies committed I/U/D in PostgreSQL WAL and excludes a rolled-back status update. An exported snapshot remains consistent while a second connection commits changes; logical decoding captures those changes. Source simulation steps and seed loads are retry-safe.

Warehouse fixtures test late snapshots, out-of-order CDC files, hard deletes, replay, separate item/payment aggregation and observed customer history. An invalid second table rolls back the first table's writes and the file ledger. A corrected retry succeeds once. Snapshot transfer times cannot establish an earlier history version when they overlap captured CDC.

Five-minute Airflow batches acknowledge S3 metadata only after load/build/test success. Failed builds stay eligible on the next run even if raw loading already committed. Quiet batches skip all Redshift work. Airflow retains task states and attempts; structured task logs include partial committed-input metrics and errors. Current order marts exclude deleted orders; their captured changes remain in raw. Customer history is the one modeled history example.

Mart tests inject a SQL failure during a table rebuild and verify that the prior
order table remains intact, then retry successfully. Repeated builds preserve
business rows and history. The whole mart layer is not published atomically;
Airflow marks completion only after the complete build and tests succeed.

## Verification

The [optional verification guide](verification.md) lists bootstrap overlap,
raw failure injection, replay and source reconciliation.
Use the [development guide](development.md) for small disposable fixture tests.
Completed AWS results remain in the dated [validation history](validation.md).
