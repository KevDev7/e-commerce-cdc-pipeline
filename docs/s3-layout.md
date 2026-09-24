# S3 layout

```text
source/original-olist-brazilian-ecommerce.zip
raw/ecommerce/<table>/LOAD*.csv
raw/cdc/*.csv
copy-ready/ecommerce/<table>/LOAD00000001/<table>.parquet
copy-ready/cdc/<original-cdc-filename-without-.csv>/<table>.parquet
```

Validation reports are kept in Git under `docs/evidence/`; duplicate S3 copies
have been removed. The bucket contains only the source archive, original captures
and derived Parquet.

The ZIP is the complete Kaggle version 2 download. DMS snapshots and CDC files
remain unchanged in `raw/`. Derived Parquet paths mirror each CSV's path relative
to `raw/`. The full relative path distinguishes snapshots with identical
filenames in different tables. A CDC file may contain several tables, so each
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
The legacy `synthea-cdc` bucket/profile/resource labels still identify this project's
resources; the current dataset is Olist.

The capture prefix is now `raw/`: the optional `dms/olist/` nesting was removed
while project compute was inactive. All six CSV copies were verified before old
keys were removed. The 12 Parquet files kept their paths and every value except
`_source_file`, which now points to the shorter CSV path. The bucket still has
19 files. Original ZIP and CSV contents are unchanged.

Older reports retain the paths used during those runs. Rebuild a fresh warehouse
from the moved captures; do not reuse file ledgers or batch checkpoints from an
older path layout against a running warehouse.
