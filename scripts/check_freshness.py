"""A bounded active probe: commit one synthetic customer and observe S3, raw and marts."""
import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from uuid import uuid4

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
from olist_cdc.cloud_load import aws_session, check_capture, warehouse_connection
from olist_cdc.db import connect
from olist_cdc.events import parse_csv


def within_budget(started, timeout):
    remaining = timeout - (time.monotonic() - started)
    if remaining <= 0:
        raise TimeoutError(f'Freshness probe exceeded {timeout}s; pipeline did not meet this run\'s threshold')
    return remaining


def run_step(command, timeout):
    """Stop the wrapper and its dbt descendants together on timeout/interruption.

    This project runs on macOS or Linux (including the Airflow container).
    """
    with subprocess.Popen(command, start_new_session=True) as process:
        try:
            returncode = process.wait(timeout=timeout)
        except BaseException:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass  # The process group already exited.
            process.wait()
            raise
        if returncode:
            raise subprocess.CalledProcessError(returncode, command)


def find_probe(s3, bucket, prefix, customer_id, seen):
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get('Contents', []):
            key = item['Key']
            if key in seen or not key.endswith('.csv'):
                continue
            source = f's3://{bucket}/{key}'
            events = parse_csv(s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode(), source)
            seen.add(key)
            for event in events:
                if event.table == 'customers' and event.values[0] == customer_id and event.values[-5] == 'I':
                    return key
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--timeout', type=int, default=300, help='Seconds for commit through tested marts; default 300')
    parser.add_argument('--poll', type=float, default=5)
    args = parser.parse_args()
    if args.timeout <= 0 or args.poll <= 0:
        parser.error('timeout and poll must be positive')
    if not (ROOT / '.env.cloud').exists():
        raise RuntimeError('No active cloud environment; provision an authorized demo first')
    load_dotenv(ROOT / '.env.cloud', override=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    check_capture()
    customer_id = uuid4().hex
    city = 'Freshness probe ' + customer_id
    started = time.monotonic()
    result = dict(customer_id=customer_id, threshold_seconds=args.timeout, poll_seconds=args.poll,
                  started_at=datetime.now(timezone.utc).isoformat(), status='running', observations={})
    output = ROOT / 'data/freshness.json'
    output.parent.mkdir(exist_ok=True)
    try:
        # This is an explicit synthetic workload record, not a timestamp watermark.
        with connect() as source:
            source.execute("""INSERT INTO ecommerce.customers (customer_id,customer_unique_id,city,state)
                VALUES (%s,%s,%s,'SP')""", (customer_id, customer_id, city))
            result['source_write_at'] = source.execute('SELECT clock_timestamp()').fetchone()[0].isoformat()
        # Measure on one monotonic clock, avoiding database/client clock skew.
        started = time.monotonic()
        result['commit_acknowledged_at'] = datetime.now(timezone.utc).isoformat()
        s3 = aws_session().client('s3')
        seen = set()
        bucket = os.environ['S3_BUCKET']
        prefix = os.environ.get('CAPTURE_PREFIX', 'raw') + '/cdc/'
        while True:
            within_budget(started, args.timeout)
            key = find_probe(s3, bucket, prefix, customer_id, seen)
            if key:
                result['source_key'] = key
                result['observations']['s3_seconds'] = round(time.monotonic() - started, 3)
                logging.info('Probe reached S3 after %.3fs', result['observations']['s3_seconds'])
                break
            time.sleep(min(args.poll, within_budget(started, args.timeout)))
        for step in ('load', 'build'):
            run_step([sys.executable, str(ROOT / 'scripts/run_cloud.py'), step],
                     timeout=within_budget(started, args.timeout))
            with warehouse_connection() as target:
                cursor = target.cursor()
                if step == 'load':
                    cursor.execute('SELECT count(*) FROM "raw".customers WHERE customer_id=%s AND city=%s AND _op=\'I\' AND NOT _is_snapshot', (customer_id, city))
                else:
                    cursor.execute('SELECT count(*) FROM marts.dim_customers WHERE customer_id=%s AND city=%s', (customer_id, city))
                if cursor.fetchone()[0] != 1:
                    raise AssertionError(f'Probe missing or duplicated after {step}')
                target.commit()
            within_budget(started, args.timeout)
            result['observations']['raw_seconds' if step == 'load' else 'marts_seconds'] = round(time.monotonic() - started, 3)
        result['status'] = 'passed'
    except Exception as error:
        result['status'] = 'failed'
        result['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        result['finished_at'] = datetime.now(timezone.utc).isoformat()
        output.write_text(json.dumps(result, indent=2))
        print(json.dumps(result, indent=2), flush=True)


if __name__ == '__main__':
    main()
