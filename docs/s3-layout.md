# S3 layout

The paths identify what the files do, without mixing dataset versions with
pipeline layout versions.

| Purpose | Current prefix | Previous prefix |
| --- | --- | --- |
| Original Kaggle dataset, release 2 | `source/olist/kaggle-v2/` | `source/olist-v2/` |
| Original DMS snapshots and changes | `raw/dms/olist/` | `olist-v1/` |
| Derived, typed Redshift COPY files | `copy-ready/parquet-v1/` | Unchanged |

The old `olist-v1` name described a capture layout, not Kaggle version 1.
`parquet-v1` still names the normalized file-format version, independently of
the dataset version. Snapshot rows and subsequent CDC changes share one capture
lineage; these paths do not imply separate datasets.

## Retained demo migration

The September 2026 retained bucket was reorganized while its compute stack was
deleted. The ZIP and original CSV bytes are preserved. Parquet files are
regenerated because their source-path hash and `_source_file` column must point
to the renamed CSV files. All other fields, including event IDs, source order,
operations and timestamps, must match the previous Parquet files exactly.
Old objects are removed only after all replacements are verified. The migration
[report](evidence/olist-s3-layout-migration.json) records the object mapping and checks.

Historical validation reports and reference exports retain their original paths
as evidence of earlier runs. Translate their prefixes using the table above
when looking for the files today. This rename is not a new DMS capture or a new
warehouse validation run.

Do not apply this move to a running deployment: existing file ledgers and
Airflow batch checkpoints refer to original paths. This retained demonstration
has no running warehouse to migrate. Future deployments use the updated DMS
configuration, IAM path and loader defaults together, with their own fresh raw
baseline.

The bucket, AWS profile and resource ownership labels retain `synthea-cdc` to
identify the existing project resources. They do not describe the active data.
