# Short-lived AWS demo

Keep the existing small RDS PostgreSQL → DMS → S3 → Redshift topology. Run Airflow/dbt in local Docker. No Spark, Kafka service, managed Airflow or NAT gateway is needed for this portfolio scope. Five-minute batches skip warehouse work when files are unchanged.

The Olist pipeline has been validated locally and in a temporary AWS session; see [evidence](validation.md). A new AWS session needs its own agreed spending allowance; the previous Synthea session allowance is not a new deployment authorization. Compute remains temporary, with one active DAG run, a warehouse usage limit and explicit stack teardown. The private S3 bucket retains the pinned source archive, original DMS captures and derived Parquet between demonstrations; no full local database is required. Skipping Redshift queries does not stop RDS/DMS billing.

AWS profile, stack and ownership tags retain the existing `synthea-cdc` names to keep resource management in the same project. The source/warehouse database is now `olist`, source schema `ecommerce`, capture prefix `raw`, local Compose project `olist-cdc`, and DAG `olist_cdc`.

Use [the runbook](run-cloud.md) and report Olist cloud results separately from [historical Synthea results](archive/synthea/README.md).
