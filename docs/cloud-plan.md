# AWS deployment choices

The user approved a **$5 initial AWS test allowance** on 2026-09-22. The completed demonstration used local preparation, short-lived cloud resources, and verified teardown. This is an operational allowance, not an AWS-enforced total billing cap. Do not start additional paid work beyond this allowance without agreement.

## Small demonstration architecture

- RDS PostgreSQL `db.t4g.micro`, version 17.11, Single-AZ, 20 GB storage.
- DMS `dms.t3.small`, version 3.6.1, 5 GB allocated storage. The pricing catalog still listed micro, but the actual orderable-instance API did not offer it; small was the smallest available option.
- One private S3 bucket with original DMS CSV files and separate compressed COPY inputs.
- Redshift Serverless with base and maximum capacity both set to 4 RPUs. A monthly 6-RPU-hour deactivate limit reserves headroom for other services within the test allowance.
- Local Docker/Airflow/dbt. No managed Airflow, Spark, NAT gateway or continuously scheduled warehouse queries.

One CloudFormation stack owns the temporary resources. It uses existing default public subnets but creates its own security groups, database, IAM access roles, bucket and warehouse. Access to database ports is restricted to the developer's current IPv4 address and the project DMS group where needed. The dedicated AWS CLI profile is always `synthea-cdc`. DMS standard service roles are created only if missing; existing roles are left alone.

DMS uses a reader with SELECT and logical replication grants. `CaptureDdls=false` avoids source DDL audit objects because this project has four fixed tables. Redshift's COPY role trusts the documented Redshift and Redshift Serverless service principals and can read only this bucket's derived COPY prefix.

## Executed validation

Actual DMS full-load and CDC files were parsed, copied to Redshift, modeled and tested. Recovery from stopped capture, real-file redelivery, source-to-target reconciliation and the actual Airflow run all passed. See [validation evidence](validation.md) and the [reproduction/cleanup runbook](run-cloud.md).

## Cost reference

Read from the AWS Pricing API on 2026-09-22 for us-east-1:

| Service | Published compute price |
|---|---:|
| RDS PostgreSQL db.t4g.micro, Single-AZ | $0.016 per instance-hour |
| DMS t3.small, Single-AZ | $0.0372 per instance-hour |
| Redshift Serverless, on-demand | $0.375 per RPU-hour |

At 4 RPUs, Redshift's active-compute rate is $1.50/hour. The observed pre-teardown usage was 1,680 charged RPU-seconds, approximately $0.175 in Redshift compute. This is not a final total AWS bill: billing can lag, and RDS, DMS, storage, networking, taxes and credits are separate. Stopping a DMS task alone does not stop replication-instance charges. The short test is followed by deletion of the stack and its generated DMS log group.
