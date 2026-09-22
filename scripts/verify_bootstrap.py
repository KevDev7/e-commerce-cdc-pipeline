"""Write during a fresh DMS full load, then verify overlapping commits in Redshift raw."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
from uuid import uuid5, NAMESPACE_URL

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
from olist_cdc.cloud_load import aws_session, warehouse_connection
from olist_cdc.db import connect

OUTPUT = ROOT / 'data/bootstrap.json'


def overlaps(commit, table):
    """Require the whole commit interval to be inside the actual table load interval."""
    start, end = table.get('FullLoadStartTime'), table.get('FullLoadEndTime')
    return bool(start and end and start < datetime.fromisoformat(commit['before_commit'])
                <= datetime.fromisoformat(commit['after_commit']) < end)


def write_during_load(seconds, padding):
    if OUTPUT.exists():
        raise RuntimeError('Preserve and review data/bootstrap.json before another bootstrap experiment')
    dms = aws_session().client('dms')
    arn = os.environ['DMS_TASK_ARN']
    task = dms.describe_replication_tasks(Filters=[{'Name': 'replication-task-arn', 'Values': [arn]}])['ReplicationTasks'][0]
    if task['Status'] != 'ready' or task['MigrationType'] != 'full-load-and-cdc':
        raise RuntimeError('This check requires a fresh, never-started full-load-and-cdc task')
    run = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')
    identity = lambda label: uuid5(NAMESPACE_URL, 'olist-bootstrap:' + run + ':' + label).hex
    customer = identity('updated-customer')
    previous = identity('deleted-customer')
    result = dict(run=run, padding_customers=padding, updated_customer=customer, commits=[], status='running')
    OUTPUT.parent.mkdir(exist_ok=True)
    try:
        with connect(autocommit=True) as source:
            with source.transaction():
                # Extra synthetic rows keep the tiny customers table's full load observable.
                # This is a test fixture size, not a production-volume claim.
                source.execute("""INSERT INTO ecommerce.customers (customer_id,customer_unique_id,city,state)
                    SELECT md5(%s || g::text),md5(g::text),'Bootstrap padding','SP'
                    FROM generate_series(1,%s) g""", ('olist-bootstrap:' + run, padding))
                source.execute("""INSERT INTO ecommerce.customers (customer_id,customer_unique_id,city,state)
                    VALUES (%s,%s,'Before load','SP'),
                           (%s,%s,'Delete during load','SP')""", (customer, customer, previous, previous))
            dms.start_replication_task(ReplicationTaskArn=arn, StartReplicationTaskType='start-replication')
            deadline = time.monotonic() + seconds
            index = 0
            while time.monotonic() < deadline:
                inserted, city = identity(f'insert-{index}'), f'Bootstrap change {index}'
                with source.transaction():
                    source.execute("""INSERT INTO ecommerce.customers (customer_id,customer_unique_id,city,state)
                        VALUES (%s,%s,%s,'SP')""", (inserted, inserted, city))
                    source.execute('UPDATE ecommerce.customers SET city=%s WHERE customer_id=%s', (city, customer))
                    source.execute('DELETE FROM ecommerce.customers WHERE customer_id=%s', (previous,))
                    before = source.execute('SELECT clock_timestamp()').fetchone()[0]
                after = source.execute('SELECT clock_timestamp()').fetchone()[0]
                result['commits'].append(dict(before_commit=before.isoformat(), after_commit=after.isoformat(),
                    inserted=inserted, updated=customer, deleted=previous, city=city))
                previous = inserted
                index += 1
                time.sleep(0.25)
        tables = dms.describe_table_statistics(ReplicationTaskArn=arn)['TableStatistics']
        table = next(t for t in tables if t['SchemaName'] == 'ecommerce' and t['TableName'] == 'customers')
        result['customers_load'] = table
        result['overlapping_commits'] = [c for c in result['commits'] if overlaps(c, table)]
        if not result['overlapping_commits']:
            raise RuntimeError('No proven commits inside the customers full-load interval; experiment is inconclusive')
        result['status'] = 'overlap_proven_capture_verification_pending'
        print(f"Proved {len(result['overlapping_commits'])} commits during customers full load; verify after capture/warehouse catch up")
    except Exception as error:
        result['status'] = 'failed_or_inconclusive'
        result['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        OUTPUT.write_text(json.dumps(result, default=str, indent=2))


def verify():
    result = json.loads(OUTPUT.read_text())
    commits = result.get('overlapping_commits', [])
    if not commits:
        raise RuntimeError('No proven initial-load overlap to verify')
    with warehouse_connection() as target:
        cursor = target.cursor()
        for commit in commits:
            for operation, field in [('I', 'inserted'), ('U', 'updated'), ('D', 'deleted')]:
                query = 'SELECT count(*) FROM "raw".customers WHERE NOT _is_snapshot AND _op=%s AND customer_id=%s'
                params = [operation, commit[field]]
                if operation == 'U':
                    query += ' AND city=%s'
                    params.append(commit['city'])
                cursor.execute(query, tuple(params))
                if cursor.fetchone()[0] != 1:
                    raise AssertionError(f'Missing or duplicated overlapping {operation}: {commit[field]} {commit["city"]}')
        target.commit()
    result['status'] = 'overlapping_events_verified'
    result['verified_operations'] = len(commits) * 3
    result['verified_at'] = datetime.now(timezone.utc).isoformat()
    OUTPUT.write_text(json.dumps(result, indent=2))
    print(f'Verified all {len(commits) * 3} overlapping INSERT/UPDATE/DELETE events; run full source reconciliation as well')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['write', 'verify'])
    parser.add_argument('--seconds', type=int, default=120)
    parser.add_argument('--padding-customers', type=int, default=100000)
    args = parser.parse_args()
    if not 10 <= args.seconds <= 600 or not 0 <= args.padding_customers <= 500000:
        parser.error('Use 10–600 seconds and 0–500000 synthetic padding customers')
    if not (ROOT / '.env.cloud').exists():
        raise RuntimeError('No active cloud environment; provision an authorized demo first')
    load_dotenv(ROOT / '.env.cloud', override=True)
    if args.command == 'write':
        write_during_load(args.seconds, args.padding_customers)
    else:
        verify()
