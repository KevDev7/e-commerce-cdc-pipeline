# Five consecutive scheduled cycles

Status: verified on AWS on September 24, 2026. [Saved evidence](evidence/olist-five-cycle-validation.json).

Five consecutive scheduled runs started at approximately 22:25, 22:30, 22:35,
22:40 and 22:45 UTC. Every task succeeded on its first attempt. Each cycle passed
17 dbt models, 49 data tests and a full comparison of the four current source
tables against Redshift. The loads committed one new CDC file per cycle and
skipped all previously loaded files; no warehouse backfill or full refresh ran.

| Cycle | Changes | Continuing order after processing |
|---|---:|---|
| 1 | 19 | created |
| 2 | 18 | approved |
| 3 | 16 | shipped |
| 4 | 17 | delivered |
| 5 | 20 | delivered; follow-up order uses the corrected customer version |

The clean sequence captured 54 inserts, 24 updates and 12 deletes (90 events).
The initial timing attempt added 37 events separately: its first batch passed,
the next scheduled run correctly skipped idle input, and the following normal
scheduled run loaded the pending changes. Those runs are not counted among the
five clean cycles. The generator was aligned with a schedule boundary before
restarting the measurement; no pipeline processing logic was changed to hide
the idle result.

This demonstrates repeated operation for small, controlled workloads, not a
throughput benchmark or a latency guarantee. The first pre-test reconciliation
query on the restored warehouse timed out; retry passed before writes began.

Reuse the retained RDS snapshot, Redshift namespace and S3 bucket. The
`infra.restore.restore_template` helper defines temporary compute referencing
those existing resources and a CDC-only DMS task. Use `scripts/restore_cloud.py create --allowance-usd 5`, with a newly approved
allowance, instead of the fresh-demo command. Its retained resources are outside
the new stack; the shared deletion helper verifies these external references and
still requires a final RDS snapshot. After creation, `scripts/restore_cloud.py
reset-password` resets the restored source owner password privately. Update the
DMS reader password to the new protected environment value before testing the
endpoint; old source passwords were removed during cleanup.

Before generating business writes, reconcile the restored source against the
retained warehouse, start a fresh logical capture position, and confirm DMS is
running. Do not reuse an old LSN blindly. DMS change sequences are task-scoped:
check that new event IDs are distinct and new source-order values follow the
retained maximum before allowing the first new batch into the warehouse. This
controlled, quiescent cutover does not demonstrate arbitrary disaster recovery.

For each of five consecutive five-minute Airflow cycles, generate one new
15-event scenario. Also spread one additional scenario across the cycles:

| Cycle | Additional scenario phases | Events |
|---|---|---:|
| 1 | open | 4 |
| 2 | approve | 3 |
| 3 | ship | 1 |
| 4 | deliver, correct | 2 |
| 5 | repeat-order, create-delete-test, delete-test, rollback-test | 5 |

Total expected: 90 committed CDC events; rolled-back writes are excluded.
Use the normal scheduler and normal load/build tasks, without manual warehouse
loads, full refreshes, task clearing or manual DAG triggering in the measured
five-cycle sequence. Verify each scheduled run and the order's expected stage;
verify the complete scenarios and reconcile the final source state. Save actual
run IDs, task results, event counts and any failures, rather than inferring success
from the configuration. If a run fails, report it and distinguish any subsequent
clean five-cycle sequence.

After the test, preserve updated cloud data, create a final source snapshot,
remove all temporary compute, and save the cleanup result. The original snapshot
remains available unless its removal is separately authorized.

The measurement command is `scripts/verify_five_cycles.py --prefix <unique-name>`.
It expects available SQL endpoints, a running CDC-only task, a paused
Airflow instance with no active run, and temporary project AWS credentials valid for the run. It
pauses the DAG on success or failure and saves a small report in
`data/five-cycles.json`. It does not provision or clean up cloud resources.

The driver waits for a five-minute boundary before enabling the first measured
run. This leaves time for verification and the next DMS file before the following
trigger. Existing Airflow history is preserved and excluded from the new sequence.
An initial mid-interval attempt produced a correct idle skip while the generator
was still checking the prior batch; this is retained separately from the clean run.

[Final cleanup verification](evidence/olist-five-cycle-cleanup.json) confirms the
updated encrypted RDS snapshot is available, all 54 S3 objects and the Redshift
namespace remain, and project compute plus local Airflow data are removed. The
original source snapshot is also preserved. Reported Redshift usage at validation
was 6,572 charged RPU-seconds (about $0.685 at the checked regional rate), with
other AWS services and billing lag additional; this is not a final invoice.
