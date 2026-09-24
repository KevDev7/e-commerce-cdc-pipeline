# Archived Parquet evaluation — September 22, 2026

This completed experiment is retained for inspection, alongside its
[original comparison script](evaluate_parquet.py) and [results](results.json).
It depends on the original session reports and capture keys, which may no longer
exist. It is not a setup step or a check required for a normal demonstration.
See the [current runbook](../../run-cloud.md) for operating instructions.

**Decision: use explicitly typed Zstandard Parquet for derived Redshift COPY inputs;
retain the original Olist archive and DMS CSV captures.** The main benefits are an
explicit portable file schema and compression for larger snapshots. Small CDC
files can be larger than gzip CSV, as the measurements below demonstrate.

The loader shares its type definitions with the raw warehouse schema, preserves
column order for Redshift's positional COPY, and rejects decimal overflow or
fractional loss. It keeps original capture identity, source sequence, delete
markers and the atomic raw/file-ledger transaction. Data conversion happens in
memory; no derived files are written to the workstation.

The actual DMS records were converted in memory using PyArrow 25.0.1. The grouping
matches the current loader: one derived file per table per original DMS file.
Both Parquet codecs use the same explicit schema and microsecond timestamps.
The CSV baseline is the former `normalized_csv` gzip output, not uncompressed CSV.

| Actual input | Rows | Derived files | gzip CSV bytes | Snappy Parquet bytes | Zstandard Parquet bytes |
|---|---:|---:|---:|---:|---:|
| Simulated lifecycle captured by DMS | 14 | 4 | 1,980 | 23,531 | 23,864 |
| Measured workload captured by DMS | 1,600 | 10 | 136,570 | 304,110 | 185,626 |
| Real Olist order-items snapshot | 112,650 | 1 | 12,863,840 | 17,672,320 | 9,973,835 |

Zstandard Parquet reduced the large snapshot by about 22%, while increasing the
1,600-event batch by about 36%. The tiny lifecycle illustrates file-format
metadata overhead. Different batching, codecs and data distributions can change
the result. This comparison did **not** benchmark Parquet COPY, query speed,
column pruning or warehouse cost; it measures file size and typed round-trip correctness only.

Every Arrow → Parquet → Arrow round trip matched the complete typed table,
including decimal(35,0) change sequences, decimal(14,2) money, UTC audit/commit
timestamps, timezone-less business timestamps, NULLs, string keys/postal codes
and all seven CDC metadata fields. The large snapshot contains real Olist items;
the CDC samples contain explicitly simulated business transactions.

Original transaction-preserving capture remains unchanged. AWS documents that
DMS writes transaction-ordered S3 CDC as CSV regardless of DataFormat; our
Parquet path is a **derived** representation after capture. See
[AWS DMS S3 settings](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.S3.html#CHAP_Target.S3.Configuring)
and [Arrow's Parquet documentation](https://arrow.apache.org/docs/python/parquet.html).

## Live Redshift validation

Actual Parquet COPY loaded all 415,418 historical rows. Both initial and
incremental dbt builds passed 19 models and 49 tests. Exact source-to-warehouse
reconciliation, four-table rollback/retry, replay, deletes and customer-version
joins passed after 15 simulated changes. [Evidence](../../evidence/olist-parquet-validation.json).

The four new snapshot Parquet files total 36,244,702 bytes. Their original
**uncompressed DMS CSV** captures total 83,870,962 bytes; this is a different
baseline from the gzip comparison above. Retention keeps both representations,
so total S3 storage includes both, plus the pinned source ZIP. The in-memory
conversion does not persist derived datasets on the workstation.

## Historical reproduction prerequisites

During an authorized active AWS session, after the lifecycle/load-failure and
`aws-measured-250` checks have produced their ignored reports:

```sh
uv run --frozen docs/archive/parquet-evaluation/evaluate_parquet.py
```

This reads S3 and writes only an ignored metrics report; data buffers stay in
memory. It does not upload Parquet, change DMS, or execute warehouse writes.
PyArrow is now a locked production dependency.
[Detailed results and schemas](results.json) are committed
without source records or credentials. The user requested removal of local datasets after evaluation. Original captures
and the seed archive were removed from the Mac; default teardown no longer
downloads captures. Current teardown retains S3 data, so future read-only comparisons need no running databases. The script still expects the matching workload and lifecycle report keys from the measured session.

Redshift requires same-region S3 files and matching column counts/order; CSV-only COPY options are removed. See [columnar COPY requirements](https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-columnar.html).
