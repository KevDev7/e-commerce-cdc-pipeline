"""Redeliver a real CDC file: raw retains copies, staging/marts remain deduplicated."""
import json
import os
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
from olist_cdc.cloud_load import aws_session, load_pending, warehouse_connection
from olist_cdc.events import parse_csv
from olist_cdc.schema import SOURCE_COLUMNS


def counts():
    with warehouse_connection() as connection:
        cursor = connection.cursor()
        result = {}
        for table in SOURCE_COLUMNS:
            cursor.execute(f'SELECT count(*),count(DISTINCT _event_id) FROM raw.{table}')
            raw_rows, unique_events = cursor.fetchone()
            cursor.execute(f'SELECT count(*),count(DISTINCT _event_id) FROM staging.stg_{table}')
            staged_rows, staged_unique = cursor.fetchone()
            assert staged_rows == staged_unique == unique_events
            result[table] = dict(raw_rows=raw_rows, unique_events=unique_events, staged_rows=staged_rows)
        connection.commit()
        return result


def main():
    load_dotenv(ROOT / '.env.cloud', override=True)
    before = counts()
    s3 = aws_session().client('s3')
    bucket = os.environ['S3_BUCKET']
    prefix = os.environ.get('CAPTURE_PREFIX', 'raw') + '/cdc/'
    originals = sorted(x['Key'] for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix)
                       for x in page.get('Contents', []) if x['Key'].split('/')[-1].startswith('CDC_TXN-'))
    if not originals:
        raise RuntimeError('Capture and load a real CDC file before replay testing')
    original = originals[0]
    with warehouse_connection() as connection:
        cursor = connection.cursor()
        cursor.execute('SELECT 1 FROM raw.loaded_files WHERE source_file=%s', (f's3://{bucket}/{original}',))
        if not cursor.fetchone():
            raise RuntimeError('Load all capture files and build marts before replay testing')
        connection.commit()
    body = s3.get_object(Bucket=bucket, Key=original)['Body'].read().decode()
    events = parse_csv(body, f's3://{bucket}/{original}')
    delivered = {table: sum(e.table == table for e in events) for table in SOURCE_COLUMNS}
    replayed = prefix + f'replay-{uuid4().hex}-' + original.split('/')[-1]
    s3.copy_object(Bucket=bucket, Key=replayed, CopySource={'Bucket': bucket, 'Key': original}, ServerSideEncryption='AES256')
    load_pending()
    # Staging is a view, so it deduplicates immediately. Build/test marts as well.
    subprocess.run([sys.executable, str(ROOT/'scripts/run_cloud.py'), 'build'], check=True)
    after_redelivery = counts()
    for table in SOURCE_COLUMNS:
        assert after_redelivery[table] == {
            **before[table], 'raw_rows': before[table]['raw_rows'] + delivered[table]}
    load_pending()
    after_retry = counts()
    assert after_retry == after_redelivery
    result = dict(before=before, after_redelivery=after_redelivery, after_retry=after_retry,
                  replayed_key=replayed, staging_unique=True, dbt_build_and_tests_passed=True)
    path = ROOT/'data/cloud-replay.json'
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(result, indent=2))
    print('Raw retained the redelivery; staging stayed unique; retry skipped completed files:', json.dumps(result))


if __name__ == '__main__':
    main()
