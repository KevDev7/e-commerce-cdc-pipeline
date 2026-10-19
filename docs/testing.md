# Testing and results

Use the [development guide](development.md) for local tests and CI, and the
[AWS runbook](run-cloud.md) for a normal demonstration. Additional cloud checks
below require an active, separately budgeted AWS session. Pause Airflow and finish
active runs before manual warehouse loading or builds.

## What the checks cover

| Area | Checks |
|---|---|
| Source | Retry-safe seeding/simulation, committed inserts/updates/deletes, and rollback exclusion |
| Ingestion | CSV structure and metadata, replay compatibility with captures containing the retired `_source_lsn` column, source string preservation, atomic file loads, and completed-file retries |
| Staging | Event deduplication, conflicting copies, required keys, and strict type conversion |
| Intermediate | Out-of-order events, late snapshots, deletes/reinserts, and customer history intervals |
| Marts | Current customer joins, independent item/payment totals, repeated builds, and model failure/retry |
| Orchestration | Quiet skips, failed-build propagation, and completion only after successful builds/tests |

The rollback check observes PostgreSQL and the CDC path excluding uncommitted
writes; PostgreSQL provides transaction rollback. Our code implements file-load
transactions, staging checks, model logic, and batch completion safeguards.
Local fixtures test negative cases without downloading Olist or using AWS.
Offline tests compare initial-load and insert/update/delete records in both CSV
layouts for all four tables. They verify identical event identities and COPY-ready
output, and exercise the cloud loader with fake S3/SQL clients for both layouts.
The two-column metadata capture configuration has not been verified in a new AWS
run; the dated results below describe the earlier three-column capture format.

## Verify a normal demonstration

After running all simulation phases and processing the latest files, keep the
source quiet and run:

```sh
uv run python scripts/verify_cloud_scenario.py demo-001
uv run python scripts/reconcile_cloud.py
```

Replace `demo-001` with your scenario name. The scenario verifier checks 9 inserts,
4 updates, and 2 deletes; staged event uniqueness; captured order statuses;
customer history and current joins; deleted records; rollback exclusion; and
independent order totals. Reconciliation compares every current source column
with its warehouse counterpart, reporting the first mismatch or count difference.
It does not save datasets locally. Results are small ignored reports under `data/`.

## Additional cloud checks

- **Capture interruption:** stop DMS, wait until stopped, commit another scenario,
  then resume capture from its saved checkpoint and process the new files.

  ```sh
  uv run python scripts/capture.py stop
  uv run python scripts/capture.py status
  uv run olist-cdc --scenario demo-002
  uv run python scripts/capture.py resume
  ```

- **Replay:** with all captures loaded, marts built, and the source quiet, run
  `uv run python scripts/verify_replay.py`. It copies a loaded CDC file to a new
  S3 key, checks that raw gains delivered copies while staging stays unique,
  builds/tests marts, and checks that retrying completed files adds nothing.
  It leaves the replay copy in S3 and records its key in `data/cloud-replay.json`.

- **Raw load failure:** run `uv run python scripts/verify_load_failure.py raw/cdc/ACTUAL_FILE.csv`
  using an actual, unloaded CDC key containing new events. The script deliberately
  fails before the ledger commit, checks rollback, retries successfully, and checks
  that another retry changes nothing. A multi-table file exercises rollback across
  tables. Mart build failure/retry is covered separately by local integration tests.

- **Changes during initial load:** before a fresh DMS task has ever started, run
  `uv run python scripts/verify_bootstrap.py write --seconds 120 --padding-customers 0`,
  then `verify` after capture and warehouse processing catch up. The writer starts
  capture and checks for commits strictly inside the initial-load interval. No
  observed overlap is inconclusive, not a pass. Optional padding is synthetic
  timing data, not additional Olist records.

Use each script's `--help` where supported for its arguments. A replacement DMS
capture lineage needs fresh raw state; never combine independent task sequences.

## Latest completed AWS run

On **October 14, 2026 (UTC)**, a fresh source, DMS task, S3 bucket, and Redshift
baseline verified the current CSV loader, string-based raw, staging checks,
and rebuilt marts.

| Check | Result |
|---|---|
| Initial load | All 415,418 historical rows; four DMS table loads completed without reported errors |
| dbt | All 14 models and 40 data tests passed after backfill, CDC processing, and replay |
| Business simulation | 9 inserts, 4 updates, and 2 deletes; totals, current joins, history, and delete checks passed |
| Reconciliation | Every current source field matched before simulation and after final replay |
| Raw failure/retry | Failure after three real COPYs left rows and ledger unchanged; retry succeeded once |
| Replay | Four delivered copies added to raw; unique staged events and all five mart counts/content fingerprints stayed unchanged |
| Airflow | Backfill and scheduled CDC batch succeeded; quiet runs skipped warehouse work |

Initial SQL connections timed out while the Redshift endpoint initialized; retry
succeeded without changing the security rules. Invalid-key/type cases were checked
with local fixtures, not injected into the live source. The October 13 local suite
passed 93 tests; this is the recorded result, not a new test run.

[Validation evidence](evidence/olist-20261014-validation.json) records the measured
results. [Cleanup evidence](evidence/olist-20261014-cleanup.json) confirms compute
was removed and one verified bucket, namespace, and source snapshot were retained.
Earlier JSON evidence describes the implementation tested on its recorded date;
removed guides remain available in Git history.

## Limits

These checks do not establish sustained throughput or a latency SLA. Current-state
reconciliation requires a quiet source; a common checkpoint for an actively
changing source is not automated. Batch completion is gated by tests, but the
whole mart layer is not published atomically. Customer history records observed
changes from capture onward, not business history before capture.
