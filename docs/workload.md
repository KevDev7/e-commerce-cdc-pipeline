# Small measured CDC workload

This is a reproducible demonstration on an already initialized warehouse, not a
capacity test or a claim about 500K daily events. Airflow is paused while these
manual commands own the warehouse. Use an authorized, temporary AWS session from
the [runbook](run-cloud.md), with its existing spending and usage limits.

```sh
uv run python scripts/benchmark_cloud.py write --scenario measured-250 --orders 250
uv run python scripts/benchmark_cloud.py capture --scenario measured-250
```

`capture` returns exit code 2 until all expected event identities are present in
S3. Wait and repeat it before proceeding. The writer commits three transactions:
insert 250 customers/orders/items/payments; update order status and customer city;
delete all four records for 25 disposable orders. That produces 1,000 inserts,
500 updates and 100 hard deletes. These are synthetic SQL transactions on the
reconstructed database; DMS captures real PostgreSQL WAL. Use a fresh scenario
for each measurement. Previously committed phases are rejected, not replayed as
new changes. If a write is interrupted between phases, use a new scenario and do
not present the incomplete run as a successful measurement.

Run the same steps as the DAG, with a distinct run ID:

```sh
export BATCH_RUN_ID=benchmark__measured-250
export BATCH_ATTEMPT=1
uv run python scripts/run_cloud.py check
uv run python scripts/run_microbatch.py pending
uv run python scripts/run_cloud.py load
uv run python scripts/run_cloud.py build
uv run python scripts/run_cloud.py report
uv run python scripts/run_microbatch.py complete
uv run python scripts/benchmark_cloud.py verify --scenario measured-250
uv run python scripts/reconcile_cloud.py
```

Stop if any step fails. Completion is only appropriate after build/tests/report
succeed. `verify` checks operation counts, unique event IDs, 225 current rows per
table, order/payment totals and customer versions at order creation. The 25
deleted orders disappear from current marts. All surviving orders keep their
original sao paulo version, while current customers have moved to campinas.

The capture report records S3 object delivery time separately from when a manual
poll observed completion. Task summaries log loader and dbt durations. This
manual batch does not include waiting for a five-minute schedule. Results depend
on source transaction size, file layout, existing raw history, warehouse warmth
and the full test graph; one run must not be extrapolated to sustained throughput.

Live results for the 2026-09-22 run are recorded in [the workload evidence](evidence/olist-workload.json).
