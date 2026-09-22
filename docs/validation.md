# Olist validation

## Current Parquet loading path

The separate Parquet session loaded all 415,418 historical source rows through
DMS CSV → explicitly typed Zstandard Parquet → Redshift COPY. Initial and
incremental builds each passed all 19 models and 49 data tests. The simulated
lifecycle plus repeat order produced 15 captured changes (9 inserts, 4 updates,
2 deletes), without including the rolled-back update.

An injected failure after writes to all four raw tables rolled back both rows
and the file ledger; retry succeeded once. Every current source field reconciled
after dbt finished. Redelivery under another S3 key and a whole-batch retry
preserved raw counts and event identities. Historical customer joins preserved
the earlier city on the first order and the corrected city on the repeat order.
All 99,441 historical orders retained unknown pre-capture customer history.

[Sanitized results](evidence/olist-parquet-validation.json) include exact counts,
fingerprints and build outcomes. The 42-test suite and Airflow smoke checks
passed in GitHub CI. This AWS session invoked shared pipeline commands manually;
it did not repeat the unchanged scheduler or establish a new latency benchmark.


Executed locally on 2026-09-22. [Machine-readable results](evidence/olist-local-validation.json) record source/target fingerprints, counts and aggregate totals.

- Downloaded and inspected Kaggle dataset version 2; SHA-256 pinned.
- Seeded 99,441 customers, 99,441 orders, 112,650 items and 103,886 payments: **415,418 source rows**. Repeating the seed returned already_loaded.
- Built a labeled local snapshot fixture, followed by **18 dbt models and 45 data tests**, all passing.
- Reconciled every current source column with the intermediate current-state views using ordered row fingerprints.
- Verified 775 orders without items and one without payment remain in the marts.
- Order-grain totals match independent source totals: items 13,591,643.70; freight 2,251,909.54; payments 16,008,872.12. Payments are not forced to equal item plus freight totals.
- **35 local tests pass**, including real PostgreSQL logical decoding, exported snapshot plus concurrent writes, rollback exclusion, atomic multi-table raw loads, replay, late files, observed address history and microbatch retries.
- Order-status history tests cover repeated statuses, multiple changes within a batch, overlapping snapshots, delete/reinsert cycles and snapshot-only historical orders. The full real seed produces 99,441 initial order-status observations, not reconstructed lifecycle histories.
- Batch audit tests preserve failed/retried attempts, partial committed-input metrics, quiet skips and incomplete work. Load metric fixtures distinguish committed from rolled-back files.
- Real Airflow task-state checks with shell fixtures pass for new files, quiet skips, failed dbt builds and recovery. The checks run the actual SQLite audit wrapper around shell fixtures and verify its outcome agrees with Airflow. Schedule is five minutes, no catch-up, one active run, paused on creation.

## Incremental dbt validation

The six marts now use dbt incremental materialization. The existing real Olist warehouse upgraded without dropping raw data or requesting a full refresh: missing per-model file checkpoints caused the first build to process all retained files. All 18 models and 45 data tests passed and the real-seed reconciliation still matched.

Successive DMS-format fixture batches verify updates, hard deletes, reinserts, detail-only changes, late lower-sequence files, changed history boundaries, and snapshots arriving after an earlier build. A duplicate delivery and a no-input build preserve PostgreSQL row identities. A forced SQL failure after the mart and checkpoint writes rolls back both. A selected model advances only its own checkpoint. Final results in all six marts equal a full rebuild, and the following incremental run leaves those rows untouched.

The local fixtures test edge cases and physical row identities. The live Redshift checks below independently validate cloud execution and rollback.

## Live Olist AWS validation — 2026-09-22

[Machine-readable cloud evidence](evidence/olist-aws-validation.json) records this new Olist run. It does not reuse Synthea results.

- RDS PostgreSQL 17.11 with logical replication, DMS 3.6.1 full-load-and-cdc, original transaction-preserving CSV in S3, Redshift Serverless capped at 4 RPUs.
- All 415,418 real selected records were seeded. To make bootstrap overlap observable, the test added 100,000 padding customers and two disposable tracking customers. These are explicitly synthetic, not additional Olist records.
- Seven source transactions committed fully inside the customers' DMS full-load interval. All 21 corresponding insert/update/delete events were verified exactly once in Redshift. The full writer produced 399 CDC events.
- A 14-event lifecycle passed through live WAL and all warehouse layers: 8 inserts, 4 updates, 2 hard deletes; the rolled-back update was absent. Created/approved/shipped/delivered history, observed city changes, and independent item/payment totals were verified.
- DMS was stopped before a second lifecycle was committed, then resumed using its saved checkpoint. Airflow loaded all 14 events and passed the dbt build. All current source columns reconciled afterward.
- `verify_load_failure.py` injected an exception after actual raw INSERTs across all four tables but before the file ledger. Counts and ledger rolled back; retry inserted each new event once; another retry changed nothing.
- `verify_mart_failure.py` injected actual SQL division-by-zero after the fct_orders checkpoint insert in a temporary dbt project. Rows and checkpoint remained unchanged; retry applied the pending changes.
- `verify_replay.py` redelivered a captured CDC file under a different S3 key and retried loading; raw counts and unique event identities stayed equal and unchanged.
- Two scheduler-created Airflow runs completed with all six tasks successful. The following quiet run executed check/pending and skipped load/build/report/complete. The task audit agrees with actual Airflow states.
- The first Redshift build exposed a TIMESTAMPTZ/DATEDIFF incompatibility. UTC normalization fixed it without weakening types or history semantics; a local DST regression confirms one elapsed hour across a clock change. The corrected live build passed all 18 models and 45 tests.

Commands and prerequisites are in the [cloud runbook](run-cloud.md). Original captures, full logs and credentials are excluded from Git; only sanitized summaries are committed.

## Limits

The measured demonstration does not establish sustained throughput or a latency SLA. Airflow's interval is five minutes; capture delivery and build time add latency. Row-identity checks and exhaustive late-event fixtures run on local PostgreSQL. dbt tests gate batch acknowledgement, not atomic publication of the entire warehouse. Historical customer attributes before capture remain unknown.

## Customer-version join upgrade

The next verified model version contains **19 models and 49 data tests**; all pass
on Redshift after rebuilding retained raw events. The local suite now has **36 passing tests**.
A live follow-up order for an existing simulated customer resolves to campinas,
while that customer's original order retains sao paulo. All 99,441 original
snapshot orders keep NULL version IDs with creation_not_captured.

Local multi-batch tests additionally cover equal commit timestamps, customer-only
late files correcting assignments, missing history filled later, key reuse and
subsequent customer reassignment. Incremental results equal a full refresh.
[Join evidence](evidence/olist-customer-join.json) records the actual cloud check.
