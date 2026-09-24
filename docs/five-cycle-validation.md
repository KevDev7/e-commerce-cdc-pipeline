# Five consecutive scheduled cycles

Status: preparation only; this is not evidence of an executed run.

Reuse the retained RDS snapshot, Redshift namespace and S3 bucket. The
`infra.restore.restore_template` helper defines temporary compute referencing
those existing resources and a CDC-only DMS task. It must not be submitted through
the fresh-demo `create` command. Its retained resources are outside the new stack;
the fresh-demo deletion helper also needs the restored-stack layout accounted for
before provisioning. Restored source credentials must be reset privately because
the old source passwords were removed during cleanup.

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
