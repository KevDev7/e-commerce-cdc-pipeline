# Optional verification scenarios

These tools demonstrate specific CDC guarantees or measure a workload. They are
not required setup steps. Use the [normal runbook](run-cloud.md) first and run only
the checks relevant to your change within an agreed AWS session budget.
Pause Airflow and finish active runs before manually loading or building marts.
Bootstrap verification is the exception in timing: it needs a fresh, never-started
DMS task and starts capture itself, so choose that check before normal capture starts.

## Capture recovery

For recovery testing, stop capture, wait until stopped, commit a second scenario, then resume from the saved checkpoint:

```sh
.venv/bin/python scripts/capture.py stop
.venv/bin/python scripts/capture.py status
.venv/bin/olist-cdc --scenario aws-002
.venv/bin/python scripts/capture.py resume
```

## Incremental mart rollback check

With Airflow paused and the initial marts already built, simulate another order
scenario and load its capture files without building marts. Then run:

```sh
.venv/bin/python scripts/verify_mart_failure.py
.venv/bin/python scripts/run_cloud.py build
```

The check injects a SQL error after the `fct_orders` checkpoint write using a
temporary copy of the dbt project. It compares actual Redshift rows and checkpoint
state before and after failure, retries the model, and records the result under
ignored `data/mart-failure.json`. The normal build afterward updates the remaining
marts and runs all data tests. Original project SQL is not modified by the check.

With the source quiescent and the latest DMS batch loaded, `.venv/bin/python scripts/reconcile_cloud.py` compares ordered rows field by field with Redshift, reporting the first mismatch or a row-count difference. It does not hash rows or save datasets locally. `.venv/bin/python scripts/verify_replay.py` redelivers a real CDC file and retries the batch, asserting unchanged raw counts and unique event identities. See the Olist validation evidence for completed executions. Reconciliation against an actively changing source would require coordinating a common checkpoint, which this small demo does not automate.

## Verify a completed simulated lifecycle

After dbt succeeds, check named scenarios against real raw events and marts:

```sh
uv run python scripts/verify_cloud_scenario.py demo-001
uv run python scripts/reconcile_cloud.py
```

The scenario check requires all nine simulation phases. It verifies 9 inserts,
4 updates and 2 deletes, unique event identities, the four captured order statuses in raw,
independent item/payment totals, customer address history, hard-delete application
exclusion of the rolled-back update, and current-customer joins for the original
and follow-up orders, alongside separate customer history. Reconciliation compares every current
source field after the source is quiet. Saved reports remain in ignored `data/`.


## Bootstrap, raw rollback and freshness

- `scripts/verify_bootstrap.py write --seconds 120 --padding-customers 0`, then
  `verify` after capture and warehouse catch-up, checks commits strictly inside
  the initial-load interval. No observed overlap means the experiment is inconclusive.
  Synthetic padding is an optional timing fixture, not real Olist volume.
- `scripts/verify_load_failure.py <unloaded-CDC-S3-key>` injects failure after raw
  writes and verifies rollback and a successful retry. Use an unseen file with new events.
- `scripts/check_freshness.py --timeout 300` writes a simulated customer and measures
  its arrival through S3 and tested marts. It runs load/build itself and excludes
  the scheduler's waiting interval.

The scripts' `--help` output documents their arguments. A new capture lineage
needs fresh raw state; never combine source sequences from independent runs.

## Measured workload and completed experiments

The [small measured workload](workload.md) is optional. It verifies a larger
simulated batch and records timings; it is not a throughput or latency guarantee.
The [completed Parquet comparison](archive/parquet-evaluation/README.md) and
[earlier Synthea implementation](archive/synthea/README.md) are historical archives.
Keep dated results as evidence of the revision tested, not claims about a new run.
