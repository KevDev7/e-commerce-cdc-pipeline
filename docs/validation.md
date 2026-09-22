# Validation status

Recorded on 2026-09-22. This distinguishes working local components from the cloud pipeline that still needs deployment and testing.

## Executed locally

- Loaded the pinned Synthea sample: 108 patients, 5,571 encounters, 9,421 claims and 85,047 financial entries. All enforced keys and references passed.
- Repeated the seed without overwriting subsequent data or duplicating records.
- Executed the seven-phase simulator and repeated it without duplicate activity.
- Observed 13 committed healthcare row changes through actual PostgreSQL logical decoding: 7 inserts, 4 updates and 2 deletes. Rolled-back changes were absent.
- Passed 11 Python/integration tests covering seed atomicity, retry, WAL, event parsing, failed-load rollback, duplicate delivery, changed-file detection and warehouse behavior.
- Built all 15 dbt models and passed all 42 dbt data tests on the real seed plus one simulation scenario.
- Populated five local marts: 109 current patients, 109 observed patient versions in the initial snapshot fixture, 5,572 encounters, 9,422 claims and 85,049 financial entries.
- Independently tested change-file fixtures with late arrival, replay, a patient tombstone, a city change and two payments for one claim. The claim retained one row with $100 in charges and $100 in payments; patient history retained the city transition; the deleted patient was absent from current state.

## Not yet established

- Actual AWS DMS transformed CSV column order and metadata values.
- Initial-load handoff and restart behavior of the deployed DMS task.
- S3-to-Redshift loading and Redshift execution of the models.
- Airflow DAG execution and the complete scheduled pipeline.
- Cloud cost and teardown behavior.

The full-data local warehouse is a snapshot fixture, not a WAL-driven replica. WAL capture is tested separately; DMS-format change fixtures test downstream semantics. The next integration milestone must connect those components using real AWS output before calling the pipeline end-to-end complete.
