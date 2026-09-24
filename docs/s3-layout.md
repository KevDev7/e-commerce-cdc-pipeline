# S3 layout

```text
source/original-olist-brazilian-ecommerce.zip
raw/dms/olist/ecommerce/<table>/LOAD*.csv
raw/dms/olist/cdc/*.csv
copy-ready/ecommerce/<table>/LOAD00000001/<table>.parquet
copy-ready/cdc/<original-cdc-filename-without-.csv>/<table>.parquet
validation/
```

The ZIP is the complete Kaggle version 2 download. DMS snapshots and CDC files
remain unchanged in `raw/`. Derived Parquet paths mirror each CSV's path relative
to `raw/dms/olist/`. The full relative path distinguishes snapshots with identical
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

The simplified raw file ledger omits content checksums. Deploy against a fresh raw
warehouse, then replay retained CSV files and rebuild marts. Existing raw ledgers
from the older implementation must not be reused unchanged. No source reload is
needed when rebuilding the warehouse from the same retained capture.

Earlier path changes are documented in the [original layout report](evidence/olist-s3-layout-migration.json)
and [format-folder removal report](evidence/olist-copy-ready-layout-migration.json).
The legacy `synthea-cdc` bucket/profile/resource labels still identify this project's
resources; the current dataset is Olist.
