# Documentation

[Project overview](../README.md)

## Source and workload

- [Olist dataset and simulated workload](source.md) — dataset origin, source tables, and simulated business activity.

## Pipeline design

- [Warehouse design](warehouse.md) — layer responsibilities, model columns, and relationships.
- [dbt processing](dbt-processing.md) — how captured changes become current records, customer history, and marts.
- [S3 file layout](s3-layout.md) — original captures and prepared CSV inputs.

## Running the project

- [AWS runbook](run-cloud.md) — setup, source loading, simulated transactions, scheduled batches, and cleanup.
- [Local development](development.md) — installation and tests without AWS.
- [AWS resource plan](cloud-plan.md) — infrastructure scope, spending controls, and retained data.

## Testing and reliability

- [Test results](validation.md) — completed checks, dated AWS results, and saved evidence.
- [Reliability safeguards](reliability.md) — ordering, deletes, deduplication, and safe retries.
- [Verification scenarios](verification.md) — instructions for reconciliation, replay, and failure checks.
- [GitHub CI](https://github.com/KevDev7/e-commerce-cdc-pipeline/actions) — automated workflow runs.
- [Earlier scheduled-run report](five-cycle-validation.md) — September 24 results from an earlier implementation.
- [Historical measurement experiments](workload.md) — retired benchmarks and their limits.

## Further learning

- [Roadmap](roadmap.md) — future learning goals; these are not implemented features.
