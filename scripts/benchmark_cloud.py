"""A small, labeled SQL workload and measured CDC check; never a throughput SLA.

With scheduling paused: write, wait until capture succeeds, run one normal
load/build/report batch, then verify. Use a fresh scenario per measurement; previously committed phases are rejected.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import time

from dotenv import load_dotenv
from olist_cdc.cloud_load import aws_session, warehouse_connection
from olist_cdc.db import connect
from olist_cdc.events import parse_csv
from olist_cdc.simulate import ids

ROOT = Path(__file__).resolve().parents[1]


def identities(scenario, count):
    prefix = 'benchmark:'+scenario
    customers = [hashlib.md5(f'{prefix}:customer:{i}'.encode()).hexdigest() for i in range(1,count+1)]
    orders = [hashlib.md5(f'{prefix}:order:{i}'.encode()).hexdigest() for i in range(1,count+1)]
    return dict(customers=customers, orders=orders,
                order_items=[o+':1' for o in orders], order_payments=[o+':1' for o in orders])


def write(scenario, count):
    start = time.monotonic()
    started = datetime.now(timezone.utc).isoformat()
    deleted = count//10
    with connect() as connection:
        connection.execute('''CREATE TEMP TABLE workload_keys AS SELECT n,
            md5(%s || ':customer:' || n::text) AS cid,
            md5(%s || ':order:' || n::text) AS oid
            FROM generate_series(1,%s) n''', ('benchmark:'+scenario,'benchmark:'+scenario,count))
        connection.commit()
        for phase in ('insert','update','delete'):
            with connection.transaction():
                connection.execute('SELECT pg_advisory_xact_lock(8174202)')
                marker = 'benchmark-'+phase
                if connection.execute('SELECT 1 FROM project_meta.simulation_steps WHERE scenario=%s AND phase=%s',
                                      (scenario,marker)).fetchone():
                    raise ValueError('Use a new scenario for a timed workload; this phase already exists')
                if phase == 'insert':
                    connection.execute("""INSERT INTO ecommerce.customers
                        (customer_id,customer_unique_id,postal_code,city,state)
                        SELECT cid,cid,'01000','sao paulo','SP' FROM workload_keys""")
                    connection.execute("""INSERT INTO ecommerce.orders (order_id,customer_id,status,purchased_at)
                        SELECT oid,cid,'created',CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo' FROM workload_keys""")
                    connection.execute("""INSERT INTO ecommerce.order_items
                        (order_item_key,order_id,order_item_id,product_id,seller_id,shipping_limit_at,price,freight_value)
                        SELECT oid||':1',oid,1,md5('benchmark-product'),md5('benchmark-seller'),(CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')+interval '2 days',50,5 FROM workload_keys""")
                    connection.execute("""INSERT INTO ecommerce.order_payments
                        (payment_key,order_id,payment_sequential,payment_type,payment_installments,payment_value)
                        SELECT oid||':1',oid,1,'credit_card',1,55 FROM workload_keys""")
                elif phase == 'update':
                    connection.execute("""UPDATE ecommerce.orders SET status='delivered',
                        customer_delivered_at=CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo'
                        WHERE order_id IN (SELECT oid FROM workload_keys)""")
                    connection.execute("""UPDATE ecommerce.customers SET city='campinas',postal_code='13000'
                        WHERE customer_id IN (SELECT cid FROM workload_keys)""")
                else:
                    for table in ('order_items','order_payments','orders'):
                        connection.execute(f'DELETE FROM ecommerce.{table} WHERE order_id IN (SELECT oid FROM workload_keys WHERE n<=%s)',(deleted,))
                    connection.execute('DELETE FROM ecommerce.customers WHERE customer_id IN (SELECT cid FROM workload_keys WHERE n<=%s)',(deleted,))
                connection.execute('INSERT INTO project_meta.simulation_steps (scenario,phase) VALUES (%s,%s)',(scenario,marker))
    return dict(scenario=scenario, simulated=True, orders_created=count, orders_deleted=deleted,
                source_transactions=3, expected_operations={'I':4*count,'U':2*count,'D':4*deleted},
                write_started_at=started, write_finished_at=datetime.now(timezone.utc).isoformat(),
                write_seconds=round(time.monotonic()-start,3))


def capture(result):
    keys = {t:set(v) for t,v in identities(result['scenario'],result['orders_created']).items()}
    s3=aws_session().client('s3'); bucket=os.environ['S3_BUCKET']
    found={}; operations={'I':0,'U':0,'D':0}; files=[]; size=0; delivered=[]
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket,Prefix=os.environ['CAPTURE_PREFIX']+'/cdc/'):
        for obj in page.get('Contents',[]):
            body=s3.get_object(Bucket=bucket,Key=obj['Key'])['Body'].read()
            relevant=False
            for event in parse_csv(body.decode(),obj['Key']):
                if event.values[0] in keys[event.table]:
                    relevant=True
                    found[event.values[-7]]=event.values[-6]
            if relevant:
                files.append(obj['Key']);size+=len(body);delivered.append(obj['LastModified'])
    for op in found.values(): operations[op]=operations.get(op,0)+1
    if operations != result['expected_operations']:
        print('Captured so far:',operations,'expected:',result['expected_operations'])
        return False
    result.update(capture_complete_observed_at=datetime.now(timezone.utc).isoformat(),
                  captured_operations=operations,capture_files=files,capture_bytes=size,
                  capture_last_object_at=max(delivered).isoformat())
    return True


def verify(result):
    keys=identities(result['scenario'],result['orders_created'])
    remaining=result['orders_created']-result['orders_deleted']
    ops={'I':0,'U':0,'D':0}
    with warehouse_connection() as connection:
        cursor=connection.cursor()
        for table,column,mart in [('customers','customer_id','dim_customers'),('orders','order_id','fct_orders'),
            ('order_items','order_item_key','fct_order_items'),('order_payments','payment_key','fct_order_payments')]:
            placeholders=','.join(['%s']*len(keys[table]));params=tuple(keys[table])
            cursor.execute(f'SELECT _op,count(*),count(DISTINCT _event_id) FROM "raw".{table} WHERE {column} IN ({placeholders}) GROUP BY _op',params)
            for op,total,unique in cursor.fetchall():
                assert total==unique
                ops[op]=ops.get(op,0)+total
            cursor.execute(f'SELECT count(*) FROM analytics_marts.{mart} WHERE {column} IN ({placeholders})',params)
            assert cursor.fetchone()[0]==remaining
        placeholders=','.join(['%s']*len(keys['orders']))
        cursor.execute(f'''SELECT count(*),sum(o.order_total),sum(o.payment_total)
            FROM analytics_marts.fct_orders o JOIN analytics_marts.dim_customer_history h
            ON o.customer_version_id=h.customer_version_id
            WHERE o.order_id IN ({placeholders}) AND o.status='delivered' AND h.city='sao paulo' ''',tuple(keys['orders']))
        assert tuple(cursor.fetchone())==(remaining,55*remaining,55*remaining)
        connection.commit()
    assert ops==result['expected_operations']
    result.update(verified_at=datetime.now(timezone.utc).isoformat(),actual_operations=ops,
                  current_rows_per_table=remaining,totals_and_creation_versions_verified=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=['write','capture','verify'])
    parser.add_argument('--scenario',required=True)
    parser.add_argument('--orders',type=int,default=250)
    args=parser.parse_args();ids(args.scenario)
    if args.orders<1: parser.error('--orders must be positive')
    if not (ROOT/'.env.cloud').exists():
        raise RuntimeError('No active cloud environment; provision an authorized demo first')
    load_dotenv(ROOT/'.env.cloud',override=True)
    directory=ROOT/'data/workloads';directory.mkdir(exist_ok=True)
    path=directory/(args.scenario+'.json')
    if args.command=='write':
        if path.exists(): raise ValueError('This measured scenario already has a report; use a new name')
        result=write(args.scenario,args.orders)
    else:
        result=json.loads(path.read_text())
        if args.command=='capture' and not capture(result): raise SystemExit(2)
        if args.command=='verify': verify(result)
    path.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))


if __name__=='__main__': main()
