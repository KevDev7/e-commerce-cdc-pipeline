# Olist validation

Executed locally on 2026-09-22. [Machine-readable results](evidence/olist-local-validation.json) record source/target fingerprints, counts and aggregate totals.

- Downloaded and inspected Kaggle dataset version 2; SHA-256 pinned.
- Seeded 99,441 customers, 99,441 orders, 112,650 items and 103,886 payments: **415,418 source rows**. Repeating the seed returned already_loaded.
- Built a labeled local snapshot fixture, followed by **18 dbt models and 45 data tests**, all passing.
- Reconciled every current source column with the intermediate current-state views using ordered row fingerprints.
- Verified 775 orders without items and one without payment remain in the marts.
- Order-grain totals match independent source totals: items 13,591,643.70; freight 2,251,909.54; payments 16,008,872.12. Payments are not forced to equal item plus freight totals.
- **34 local tests pass**, including real PostgreSQL logical decoding, exported snapshot plus concurrent writes, rollback exclusion, atomic multi-table raw loads, replay, late files, observed address history and microbatch retries.
- Order-status history tests cover repeated statuses, multiple changes within a batch, overlapping snapshots, delete/reinsert cycles and snapshot-only historical orders. The full real seed produces 99,441 initial order-status observations, not reconstructed lifecycle histories.
- Batch audit tests preserve failed/retried attempts, partial committed-input metrics, quiet skips and incomplete work. Load metric fixtures distinguish committed from rolled-back files.
- Real Airflow task-state checks with shell fixtures pass for new files, quiet skips, failed dbt builds and recovery. The checks run the actual SQLite audit wrapper around shell fixtures and verify its outcome agrees with Airflow. Schedule is five minutes, no catch-up, one active run, paused on creation.

## What remains unverified

This migration did not provision AWS. New Olist DMS CSVs, Redshift COPY, live scheduled batches, source-to-Redshift reconciliation, bootstrap overlap and cloud freshness still require an explicitly budgeted AWS demonstration. Local snapshot files are test fixtures; they are not evidence of DMS capture. Local WAL tests exercise real logs independently of those fixtures.

Earlier AWS runs validated the **Synthea** implementation. Their evidence is preserved under [archive/synthea](archive/synthea/README.md) and must not be presented as Olist results.
