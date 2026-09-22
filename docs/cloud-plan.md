# Next milestone: AWS integration

Cloud deployment has not started. An initial spending limit is required before creating billable resources.

The intended small demonstration uses RDS PostgreSQL, one AWS DMS full-load-and-CDC task, a project S3 bucket, and Redshift Serverless. Airflow and dbt can run locally in Docker; a managed Airflow environment is not needed for this portfolio. Resources use the `synthea-cdc` prefix and Project tag, and commands use the `synthea-cdc` profile explicitly.

## Preparation already in the repository

- Four source tables with logical replication enabled locally.
- DMS table-selection and added-metadata rules in `infra/dms-table-mappings.json`.
- S3 endpoint settings template in `infra/dms-s3-settings.example.json`.
- CSV parser, raw event schema, file ledger, and tested dbt models.
- GitHub Actions for the local integration tests; no AWS secrets or AWS access are required by CI.

These are preparation artifacts, not proof of an AWS deployment. In particular, the actual DMS column order and header values must be checked against the parser using captured output.

## Remaining integration work

1. Provision small project resources with narrowly scoped runtime roles; choose network access and cleanup details for the approved test duration.
2. Seed the cloud source, then configure DMS with initial load plus ongoing capture. Select only the four healthcare tables, excluding project metadata.
3. Inspect actual full-load and CDC files. Verify ordering fields, null handling and schema layout before enabling the loader.
4. Implement and exercise S3 loading through Redshift COPY with an atomic processed-file ledger. Handle repeats and source-file provenance.
5. Run the dbt models on Redshift, reconcile with the source, and test changes during initial load, deletes, restart and replay.
6. Wire the verified commands into one straightforward Airflow DAG. Keep DMS running continuously during a demo; Airflow schedules downstream batches.
7. Record the end-to-end evidence, test teardown, and stop or remove billable resources according to the agreed demonstration plan.

## Cost reference, not a bill estimate

Read from the AWS Pricing API on 2026-09-22 for us-east-1:

| Candidate | Published compute price |
|---|---:|
| RDS PostgreSQL db.t4g.micro, Single-AZ | $0.016 per instance-hour |
| DMS t3.micro, Single-AZ | $0.0186 per instance-hour |
| Redshift Serverless, on-demand | $0.375 per RPU-hour |

At 4 RPUs, Redshift's base active-compute rate is $1.50/hour. AWS documents [4-RPU availability](https://aws.amazon.com/about-aws/whats-new/2025/06/amazon-redshift-serverless-4-rpu-capacity-option/). Availability and capacity limits must be confirmed during provisioning. These figures exclude storage, requests, network/public IPv4, any additional compute scaling, taxes and credits. Idle RDS/DMS instances can still incur charges; stopping a DMS task alone does not remove its replication-instance cost. The initial test should be short and followed by verified cleanup. This is not authorization to spend or a guaranteed dollar cap.
