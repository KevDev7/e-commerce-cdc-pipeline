"""Observed creation-time joins stay correct across CDC batches and missing history."""
import os
import shutil
import subprocess

import pytest
from olist_cdc.db import ROOT
from olist_cdc.events import parse_csv
from olist_cdc.warehouse import initialize_raw, load_local
from test_events import csv_text
from test_incremental_marts import event


@pytest.mark.integration
def test_customer_version_at_captured_order_creation(database, tmp_path):
    initialize_raw(database)
    profiles = tmp_path/'profiles'
    profiles.mkdir()
    shutil.copyfile(ROOT/'dbt/profiles.yml.example', profiles/'profiles.yml')

    def load(name, rows, snapshot=None):
        text = csv_text(rows)
        load_local(database, parse_csv(text, name, snapshot_table=snapshot), name)

    def build(*extra):
        database.commit()
        result = subprocess.run([str(ROOT/'.venv/bin/dbt'), 'build', *extra,
            '--project-dir', str(ROOT/'dbt'), '--profiles-dir', str(profiles), '--target', 'local'],
            capture_output=True, text=True, timeout=120,
            env={**os.environ, 'WAREHOUSE_DATABASE': database.info.dbname,
                 'DBT_SEND_ANONYMOUS_USAGE_STATS': 'false',
                 'DBT_TARGET_PATH': str(tmp_path/'target'), 'DBT_LOG_PATH': str(tmp_path/'logs')})
        assert result.returncode == 0, result.stdout[-12000:]+result.stderr[-2000:]

    def assignments():
        return {row[0]: row[1:] for row in database.execute('''
            SELECT o.order_id,o.customer_history_status,h.city,h.source_order_from
            FROM marts.fct_orders o LEFT JOIN marts.dim_customer_history h
            ON o.customer_version_id=h.customer_version_id ORDER BY o.order_id''')}

    customer = dict(customer_id='c1', customer_unique_id='p1', city='sao paulo', state='SP')
    order = dict(order_id='o1', customer_id='c1', status='created', purchased_at='2017-01-01')
    # Same commit timestamp: the change sequence still places the order before the correction.
    same_time_update = event('customers', {**customer, 'city':'campinas'}, 30, 'U')
    same_time_update[-1] = event('orders', order, 20)[-1]
    load('first.csv', [event('customers', customer, 10), event('orders', order, 20),
                      same_time_update, event('orders', {**order, 'order_id':'o2'}, 40),
                      event('customers', {**customer, 'customer_id':'late'}, 70),
                      event('orders', {**order, 'order_id':'missing', 'customer_id':'late'}, 60)])
    snapshot = event('orders', {**order, 'order_id':'historical'}, 0)
    load('orders/LOAD.csv', [[snapshot[0], *snapshot[3:]]], 'orders')
    build()
    assert assignments() == {'historical':('creation_not_captured',None,None),
        'missing':('customer_history_unavailable',None,None),
        'o1':('matched','sao paulo',10), 'o2':('matched','campinas',30)}

    # A customer-only late file must revise an already acknowledged fact assignment.
    # It also fills missing history without inventing history for snapshot-only orders.
    load('late-customers.csv', [event('customers', {**customer,'city':'santos'}, 35, 'U'),
        event('customers', {**customer, 'customer_id':'late','city':'recife'}, 50)])
    build()
    assert assignments()['o1'] == ('matched','sao paulo',10)
    assert assignments()['o2'] == ('matched','santos',35)
    assert assignments()['missing'] == ('matched','recife',50)
    assert assignments()['historical'] == ('creation_not_captured',None,None)

    # Reusing an order key begins a new lifecycle. A later customer reassignment
    # does not change which customer originally created it.
    load('reinsert.csv', [event('orders', order, 80, 'D'),
                         event('orders', order, 90),
                         event('orders', {**order,'customer_id':'late'}, 95, 'U')])
    build()
    assert assignments()['o1'] == ('matched','santos',35)
    before = database.execute('SELECT * FROM marts.fct_orders ORDER BY order_id').fetchall()
    build('--full-refresh')
    assert database.execute('SELECT * FROM marts.fct_orders ORDER BY order_id').fetchall() == before
