# S3 layout

Current retained bucket: `olist-cdc-ezajhtylhk8v` (14 files).

The duplicate replay CSV and its four derived Parquet files have been removed.
The original CDC batch remains. Retry and event-deduplication logic, automated
tests, and historical validation evidence are unchanged. Migration counts below
describe the bucket before this cleanup.

```text
dataset/original-olist-brazilian-ecommerce.zip
raw/initial-load/<table>/LOAD*.csv
raw/cdc/*.csv
copy-ready/initial-load/<table>/LOAD00000001.parquet
copy-ready/cdc/<original-cdc-filename-without-.csv>/<table>.parquet
```

Validation reports are kept in Git under `docs/evidence/`; duplicate S3 copies
have been removed. The bucket contains only the source archive, original captures
and derived Parquet.

`initial-load/` holds the baseline full-load snapshots; `cdc/` holds subsequent
changes. The PostgreSQL schema is still `ecommerce`. A DMS target-schema rename
is configured to map it to `initial-load` in S3 using the native
[schema transformation](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TableMapping.SelectionTransformation.Transformations.html).
CDC files keep their `cdc/` path. The parser accepts both the original and target
schema labels; file location determines whether a record is a snapshot.

The ZIP is the complete Kaggle version 2 download. DMS snapshots and CDC files
remain unchanged in `raw/`. Derived Parquet paths mirror each CSV's path relative
to `raw/`. The full relative path distinguishes snapshots with identical
filenames in different tables. Snapshot Parquet files keep the original `LOAD`
filename with a `.parquet` extension, without an extra folder. A CDC file may contain several tables, so each
gets its own Parquet file. The extension identifies the format; no format-version
folder or hash folder is needed.

Retries write the same prepared paths. Capture files must remain immutable:
the loader tracks original S3 keys and skips previously committed files without
re-downloading them. Events redelivered under another filename are still deduplicated.
A new capture lineage requires a fresh bucket and warehouse state.

On September 24, 2026, all 12 retained Parquet objects were copied to readable paths
inside S3. Replacement sizes and ETags matched before old derived keys were removed.
The ZIP, original CSVs and Parquet contents were unchanged; no dataset was downloaded
and no warehouse compute was started. Earlier reports retain their historical paths.

A subsequent change regenerated those 12 Parquet objects with readable event IDs.
All 415,418 snapshot rows and both 15-event CDC files were compared field by field
against the prior Parquet; only `_event_id` changed. Every uploaded replacement was
read back and verified. Original CSVs remain unchanged, and no local files were
created. Replaying the extra CDC copy still deduplicates its events in raw.

The simplified raw file ledger omits content checksums. Deploy against a fresh raw
warehouse, then replay retained CSV files and rebuild marts. Existing raw ledgers
from the older implementation must not be reused unchanged. No source reload is
needed when rebuilding the warehouse from the same retained capture.

Earlier path changes are documented in the [original layout report](evidence/olist-s3-layout-migration.json)
and [format-folder removal report](evidence/olist-copy-ready-layout-migration.json).
The AWS profile and compute/resource ownership labels retain `synthea-cdc` for
project isolation. The S3 bucket name uses `olist-cdc`; the dataset is Olist.

The capture prefix is now `raw/`: the optional `dms/olist/` nesting was removed
while project compute was inactive. All six CSV copies were verified before old
keys were removed. The 12 Parquet files kept their paths and every value except
`_source_file`, which now points to the shorter CSV path. The bucket still has
19 files. Original ZIP and CSV contents are unchanged.

Older reports retain the paths used during those runs. Rebuild a fresh warehouse
from the moved captures; do not reuse file ledgers or batch checkpoints from an
older path layout against a running warehouse.

The retained data was moved into the Olist-named bucket, and `source/` became
`dataset/`. The original ZIP and six CSVs were copied unchanged. All 12 Parquet
files were verified with only their `_source_file` bucket references updated.
The new bucket preserves encryption, blocked public access and bucket-owner
control. The old bucket was removed after all 19 destination files passed checks.
Future demo stacks also create buckets with the `olist-cdc-` prefix.

The retained snapshots have been moved to `initial-load/` in both layers. All
19 objects were verified: CSV bytes and Parquet data values are unchanged; only
the snapshot Parquet `_source_file` paths were updated. Dataset and CDC files
were untouched. Offline checks passed; the new DMS target-schema mapping still
needs confirmation during the next fresh cloud run.
