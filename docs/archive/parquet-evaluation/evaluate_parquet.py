"""Read-only size/round-trip comparison; does not change capture or COPY behavior.

Archived comparison from September 22, 2026; not part of the demo pipeline.
Requires the completed workload report and active cloud environment. Data stays
in memory; the saved report contains sizes/types, not source rows or credentials.
"""
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import time

from dotenv import load_dotenv
import pyarrow as pa
import pyarrow.parquet as pq
from olist_cdc.cloud_load import aws_session, snapshot_table
from olist_cdc.parquet import field_type, value_for
import csv
import gzip
import io
from olist_cdc.events import NULL
from olist_cdc.events import RAW_METADATA, parse_csv, source_columns
from olist_cdc.seed import TABLES
from olist_cdc.warehouse import TYPES

ROOT=Path(__file__).resolve().parents[3]


def normalized_csv(events):
    """Historical gzip CSV baseline, used only for format comparisons."""
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    for event in events:
        writer.writerow([NULL if value is None else
                         ("true" if value else "false") if isinstance(value, bool)
                         else value for value in event.values])
    return gzip.compress(buffer.getvalue().encode(), mtime=0)


def measure(events, table):
    columns=source_columns(table)+list(RAW_METADATA)
    schema=pa.schema([(name,field_type(name)) for name in columns])
    arrays=[pa.array([value_for(event.values[i],field.type) for event in events],type=field.type)
            for i,field in enumerate(schema)]
    typed=pa.Table.from_arrays(arrays,schema=schema)
    result={'table':table,'rows':len(events),'gzip_csv_bytes':len(normalized_csv(events)),
            'schema':{f.name:str(f.type) for f in schema}}
    for codec in ('snappy','zstd'):
        start=time.monotonic();sink=pa.BufferOutputStream()
        pq.write_table(typed,sink,compression=codec,version='2.6',coerce_timestamps='us',allow_truncated_timestamps=False)
        body=sink.getvalue();restored=pq.read_table(pa.BufferReader(body))
        assert restored.equals(typed), f'{table}/{codec} changed typed values'
        result[codec]={'bytes':body.size,'write_and_roundtrip_seconds':round(time.monotonic()-start,4),
                       'exact_typed_roundtrip':True}
    return result


def main():
    load_dotenv(ROOT/'.env.cloud',override=True)
    workload=json.loads((ROOT/'data/workloads/aws-measured-250.json').read_text())
    lifecycle=json.loads((ROOT/'data/load-failure.json').read_text())['source_key']
    s3=aws_session().client('s3');bucket=os.environ['S3_BUCKET'];prefix=os.environ['CAPTURE_PREFIX']
    snapshot_keys=[o['Key'] for page in s3.get_paginator('list_objects_v2').paginate(
        Bucket=bucket,Prefix=prefix+'/ecommerce/order_items/') for o in page.get('Contents',[]) if o['Key'].endswith('.csv')]
    samples=[('14_event_lifecycle',lifecycle), *[('1600_event_workload',key) for key in workload['capture_files']],
             *[('real_order_items_snapshot',key) for key in snapshot_keys]]
    measurements=[]
    for sample,key in samples:
        body=s3.get_object(Bucket=bucket,Key=key)['Body'].read()
        source=f's3://{bucket}/{key}'
        events=parse_csv(body.decode(),source,snapshot_table=snapshot_table(key,prefix))
        for table in TABLES:
            batch=[event for event in events if event.table==table]
            if batch:
                item=measure(batch,table);item.update(sample=sample,source_key=key);measurements.append(item)
    summary={}
    for item in measurements:
        target=summary.setdefault(item['sample'],dict(rows=0,derived_files=0,gzip_csv_bytes=0,snappy_bytes=0,zstd_bytes=0))
        target['rows']+=item['rows'];target['derived_files']+=1
        target['gzip_csv_bytes']+=item['gzip_csv_bytes']
        for codec in ('snappy','zstd'): target[codec+'_bytes']+=item[codec]['bytes']
    result=dict(verified_at=datetime.now(timezone.utc).isoformat(),pyarrow_version=pa.__version__,
                scope='Actual DMS records; derived file size and typed round-trip only; no Redshift Parquet COPY timing',
                grouping='One derived file per table per original DMS file, matching the current loader',
                summary=summary,measurements=measurements)
    (ROOT/'data/parquet-evaluation.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(dict(pyarrow_version=pa.__version__,summary=summary),indent=2))


if __name__=='__main__': main()
