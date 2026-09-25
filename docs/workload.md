# Historical measurement experiments

The standalone workload benchmark and active freshness probe were retired on
September 25, 2026 to keep the student project focused. Their code remains in Git
history; they are not part of the current demonstration workflow.

The September 22 workload report remains unchanged in
[the original evidence](evidence/olist-workload.json). It measured a simulated
batch on the earlier model graph; its historical customer-order join checks no
longer describe the current marts. It was not a sustained-throughput benchmark.

The former freshness probe manually invoked warehouse loading and dbt, excluding
the scheduler's waiting time. Its measurements were not end-to-end scheduled
latency guarantees.

For current use, follow the [normal demonstration](run-cloud.md), verify the
simulated scenario and reconcile source rows. The [five-cycle report](five-cycle-validation.md)
records the completed September 24 scheduled experiment, with its own limits.
