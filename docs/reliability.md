# CDC reliability branch

Baseline: `main` at `23de727` retains the completed September 22 AWS demonstration. This branch adds three focused checks without changing the four-table architecture.

1. **Active initial load:** commit inserts, updates and deletes while DMS is performing the initial load. Record evidence that the writes overlap a table's load, then reconcile all current source fields after capture catches up. A run that misses the overlap is inconclusive, not a pass.
2. **Interrupted warehouse load:** fail after raw writes but before the file ledger commits. Verify rollback, retry the same file, then verify redelivery does not duplicate events.
3. **Freshness:** follow a known committed source change through S3, raw and marts during a bounded run. Record observations and fail on timeout; an idle table's old event timestamp is not proof of pipeline lag.

Local tests establish behavior for the local database or explicit fixtures only. AWS claims require actual DMS files and Redshift execution. New cloud runs require a separate spending allowance, preserve the previous run's evidence, and end with verified teardown.

Implementation and validation results will be added as each check is completed.
