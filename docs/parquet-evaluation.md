# Derived Parquet evaluation

**Decision: keep gzip CSV for this project's current COPY inputs.** Parquet is
viable, but changing formats did not earn its additional dependency and loader
path for the small batches measured here. This is a workload-specific choice,
not a claim that CSV is generally better for analytics.

The actual DMS records were converted in memory using PyArrow 25.0.1. The grouping
matches the current loader: one derived file per table per original DMS file.
Both Parquet codecs use the same explicit schema and microsecond timestamps.
The CSV baseline is the existing `normalized_csv` gzip output, not uncompressed CSV.

| Actual input | Rows | Derived files | gzip CSV bytes | Snappy Parquet bytes | Zstandard Parquet bytes |
|---|---:|---:|---:|---:|---:|
| Simulated lifecycle captured by DMS | 14 | 4 | 1,980 | 23,531 | 23,864 |
| Measured workload captured by DMS | 1,600 | 10 | 136,570 | 304,110 | 185,626 |
| Real Olist order-items snapshot | 112,650 | 1 | 12,863,840 | 17,672,320 | 9,973,835 |

Zstandard Parquet reduced the large snapshot by about 22%, while increasing the
1,600-event batch by about 36%. The tiny lifecycle illustrates file-format
metadata overhead. Different batching, codecs and data distributions can change
the result. This comparison did **not** benchmark Parquet COPY, query speed,
column pruning or warehouse cost; it supports a size/complexity decision only.

Every Arrow → Parquet → Arrow round trip matched the complete typed table,
including decimal(35,0) change sequences, decimal(14,2) money, UTC audit/commit
timestamps, timezone-less business timestamps, NULLs, string keys/postal codes
and all seven CDC metadata fields. The large snapshot contains real Olist items;
the CDC samples contain explicitly simulated business transactions.

Original transaction-preserving capture remains unchanged. AWS documents that
DMS writes transaction-ordered S3 CDC as CSV regardless of DataFormat; a future
Parquet path would be a **derived** representation after capture. See
[AWS DMS S3 settings](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.S3.html#CHAP_Target.S3.Configuring)
and [Arrow's Parquet documentation](https://arrow.apache.org/docs/python/parquet.html).

## Reproduce

During an authorized active AWS session, after the lifecycle/load-failure and
`aws-measured-250` checks have produced their ignored reports:

```sh
uv run --frozen --with pyarrow==25.0.1 scripts/evaluate_parquet.py
```

This reads S3 and writes only an ignored metrics report; data buffers stay in
memory. It does not upload Parquet, change DMS, or execute warehouse writes.
PyArrow is an optional evaluation dependency, not part of the production runtime.
[Detailed results and schemas](evidence/olist-parquet-evaluation.json) are committed
without source records or credentials. Original captures are retained locally in
the ignored archive during stack teardown.
