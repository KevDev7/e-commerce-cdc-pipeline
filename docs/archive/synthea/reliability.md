# CDC reliability branch

Baseline: `main` at `23de727` retains the completed September 22 AWS demonstration. This branch adds three focused checks without changing the four-table architecture.

1. **Active initial load:** commit inserts, updates and deletes while DMS is performing the initial load. Record evidence that the writes overlap a table's load, then reconcile all current source fields after capture catches up. A run that misses the overlap is inconclusive, not a pass.
2. **Interrupted warehouse load:** fail after raw writes but before the file ledger commits. Verify rollback, retry the same file, then verify redelivery does not duplicate events.
3. **Freshness:** follow a known committed source change through S3, raw and marts during a bounded run. Record observations and fail on timeout; an idle table's old event timestamp is not proof of pipeline lag.

Local tests establish behavior for the local database or explicit fixtures only. AWS claims require actual DMS files and Redshift execution. New cloud runs require a separate spending allowance, preserve the previous run's evidence, and end with verified teardown.

## Local results

27 tests pass locally, including the 42 dbt data tests run by each model integration case. The AWS evidence below records the 19-test suite at the time of that run. Two subsequent local regressions verify timeout cleanup and command failure propagation, and six verify microbatch checkpoints and retries. Separate Airflow container checks verify scheduling configuration, idle skips, failed-build propagation and recovery using shell fixtures; they do not invoke AWS. Coverage demonstrates:

- PostgreSQL exports a consistent snapshot while a separate connection commits an insert, update and delete; real logical decoding contains all three operations.
- A late snapshot does not overwrite a newer update or resurrect a deleted patient in the marts (explicit DMS-format fixture).
- A snapshot transferred after CDC begins does not introduce an invented earlier patient-history version. This regression test initially failed with two backwards history intervals; the model now retains the snapshot in raw while deriving that patient's history from captured CDC.
- A failure loading the second table rolls back the first table and the file ledger; retry succeeds without duplication.
- Bootstrap validation rejects missing/partial overlap, and freshness requires the specific probe event and fails when its deadline expires.
- A freshness load/build timeout terminates the wrapper and its nested processes together, preventing an orphaned dbt process from continuing work. A real nested-process regression verifies that its heartbeat stops; normal command failures still propagate.

The local snapshot test uses PostgreSQL's native snapshot export, not DMS. The separate AWS run below establishes end-to-end behavior with actual DMS output and Redshift.

## AWS results — 2026-09-22

The second run used the existing architecture with an additional $5 allowance. [Machine-readable evidence](evidence/cloud-reliability.json) preserves the measurements separately from the original baseline.

| Check | Observed result |
|---|---|
| Active initial load | 197 writer transactions; four committed strictly inside the patients full-load interval, 09:39:37.379–09:39:40.033 UTC |
| Overlapping changes | All 12 inserts, updates and deletes from those four transactions found exactly once in Redshift raw |
| Raw history | 200,149 snapshot rows plus 527 CDC events: 179 inserts, 175 updates, 173 deletes; 200,676 distinct event identities |
| Mid-load failure | Injected exception after actual INSERTs into all four Redshift raw tables; all rows and the ledger rolled back |
| Retry | The same 124-event file loaded once; another retry changed no counts |
| Freshness | Known committed probe observed in S3 at 61.806s, raw at 72.445s, and the tested mart at 148.877s; passed the configured 300s threshold |
| Models | All 15 models and 42 dbt tests passed, including the fresh probe rebuild |
| Reconciliation | Every current field matched between PostgreSQL and Redshift across all four tables |

Final marts contained 100,112 patients, 100,626 patient-history versions, 5,572 encounters, 9,422 claims and 85,049 financial entries. The 100,000 extra synthetic patients were padding for the initial-load experiment. This is not a throughput benchmark.

The history regression was first reproduced locally: snapshot transfer timestamps could introduce backwards history intervals. The corrected model excludes ambiguous snapshot baselines from historical versioning while preserving them in raw/current-state processing. Two such snapshot rows occurred in the AWS data; all captured CDC remains retained. This avoids fabricating a pre-CDC history state.

An initial Redshift connection timed out before SQL work began. Subsequent network checks showed the endpoint reachable and retry succeeded without changing network permissions. The injected warehouse test covers a handled exception before commit, not a killed process or lost commit acknowledgment. Concurrent initial-load writes were tested on patients, not every possible cross-table transaction or schema change. The writer was stopped before final reconciliation.

The pre-teardown usage snapshot showed 1,920 charged RPU-seconds, approximately **$0.20 in Redshift compute** at the checked rate. This excludes later/billing-lag usage and other AWS services, taxes and credits. Cleanup was verified at **10:03 UTC**: CloudFormation DELETE_COMPLETE, with the project RDS instance, DMS instance/task, Redshift workgroup/namespace, S3 bucket and generated DMS log group all absent. Local project containers are stopped and temporary credentials removed. Captured files and detailed logs remain in ignored local data folders; the original run's files are preserved separately under `data/runs/baseline-20260922`.

## Bounded cloud experiment

After a new run is authorized, archive the previous ignored `data/cloud-state.json` and `data/aws-capture` together under a run-specific folder, after verifying the prior stack is deleted. Preserve the committed baseline evidence. Provision with the existing [runbook](run-cloud.md), seed the source and test DMS connections, but **do not start capture yet**.

```sh
.venv/bin/python scripts/verify_bootstrap.py write --seconds 120 --padding-patients 100000
```

This starts the fresh `full-load-and-cdc` task and repeatedly commits real patient inserts, updates and deletes. The extra synthetic patients make the small table's load observable; they are a test fixture, not a production-volume claim. `data/bootstrap.json` records source-clock commit bounds and DMS's actual patients-table load start/end times. A pass requires commits strictly inside that table's load interval. A missed interval is inconclusive and requires a reviewed repeat, not a claim of successful concurrency testing.

Enable Redshift and its existing usage limit. Before loading all files, choose an actual CDC file containing unseen events:

```sh
.venv/bin/python scripts/verify_load_failure.py capture-v1/cdc/ACTUAL_FILE.csv
.venv/bin/python scripts/run_cloud.py check
.venv/bin/python scripts/run_cloud.py load
.venv/bin/python scripts/run_cloud.py build
.venv/bin/python scripts/verify_bootstrap.py verify
.venv/bin/python scripts/check_freshness.py --timeout 300
.venv/bin/python scripts/reconcile_cloud.py
```

The failure tool runs the real Redshift COPY/INSERTs, injects an exception before the ledger write, verifies rollback, retries, and verifies a second retry is unchanged. Select a multi-table file to exercise cross-table rollback. It rejects already-loaded files or files containing no new events. This validates handled mid-transaction failure; it does not claim to test process termination or an ambiguous network failure at commit acknowledgment.

The freshness tool inserts an explicitly synthetic probe patient after initial load completes, waits for that exact CDC insert in S3, runs the existing raw load and dbt build, then checks raw and mart values. It records commit-acknowledgment-to-observation times on one monotonic clock in `data/freshness.json`. Polling adds measurement delay. The five-minute threshold is an experiment setting, not a measured guarantee or production SLA. A timeout or build failure records failure and exits nonzero. Old timestamps on idle business tables do not trigger false lag alarms. The probe remains in the disposable source for reconciliation.

If CDC files have not all landed yet, wait for capture and repeat the existing load/build before verifying bootstrap/reconciliation. Reconcile with the test writer stopped, so the comparison has a stable endpoint; writes were active **during the initial load**, which is the new scenario under test. Finish with the existing teardown and absence checks. Record cloud results separately from the successful baseline run.
