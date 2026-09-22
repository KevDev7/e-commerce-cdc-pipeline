# Olist reliability checks

The migration retains the preceding pipeline's ordering, atomic loading and recovery mechanisms. Current results and their limits are in [Olist validation](validation.md).

The local suite verifies committed I/U/D in PostgreSQL WAL and excludes a rolled-back status update. An exported snapshot remains consistent while a second connection commits changes; logical decoding captures those changes. Source simulation steps and seed loads are retry-safe.

Warehouse fixtures test late snapshots, out-of-order CDC files, hard deletes, replay, separate item/payment aggregation and observed customer history. An invalid second table rolls back the first table's writes and the file ledger. A corrected retry succeeds once. Snapshot transfer times cannot establish an earlier history version when they overlap captured CDC.

Five-minute Airflow batches acknowledge S3 metadata only after load/build/report success. Failed builds stay eligible on the next run even if raw loading already committed. Quiet batches skip all Redshift work. Local audit records retain task attempts, partial committed-input metrics, and final batch outcomes; interrupted work stays incomplete. Order-status history retains observed transitions and tombstones without inventing historical states. The active freshness script stops nested dbt processes on timeout.

## Future paid AWS checks

Prepare a fresh Olist stack according to [the runbook](run-cloud.md). Before enabling the scheduler, use:

- `scripts/verify_bootstrap.py write --seconds 120 --padding-customers 0`, followed by `verify` after capture/warehouse catch-up, to prove writes strictly within the real customers full-load interval. If no overlap is observed, report inconclusive rather than claiming success; additional explicitly synthetic padding is an optional test fixture.
- `scripts/verify_load_failure.py <unloaded-CDC-S3-key>` to fail the actual loader after raw writes but before the ledger and then retry.
- `scripts/reconcile_cloud.py` with the source quiescent, and `scripts/verify_replay.py` to verify actual file redelivery.
- `scripts/check_freshness.py --timeout 300` for an active simulated customer probe. Pause the DAG and finish active runs first; this script invokes load/build itself and does not measure the scheduler's waiting interval.

Completed Olist AWS checks and their results are recorded in the dated [validation evidence](validation.md). Re-run relevant checks when changing the capture or loading path. Preserve prior ignored run artifacts in a dated archive before reusing output paths. Do not mix old source files or evidence with the new capture lineage.
