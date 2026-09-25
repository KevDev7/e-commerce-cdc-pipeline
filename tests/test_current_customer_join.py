"""Orders reference current customers; SCD history remains an independent example."""
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
def test_current_customer_join_updates_without_rewriting_orders(database, tmp_path):
    initialize_raw(database)
    shutil.copyfile(ROOT/'dbt/profiles.yml.example', tmp_path/'profiles.yml')

    def load(name, rows, snapshot=None):
        load_local(database, parse_csv(csv_text(rows), name, snapshot_table=snapshot), name)

    def build(*extra):
        database.commit()
        result = subprocess.run([str(ROOT/'.venv/bin/dbt'), 'build', *extra,
            '--project-dir', str(ROOT/'dbt'), '--profiles-dir', str(tmp_path), '--target', 'local'],
            capture_output=True, text=True, timeout=120,
            env={**os.environ, 'WAREHOUSE_DATABASE': database.info.dbname,
                 'DBT_SEND_ANONYMOUS_USAGE_STATS': 'false',
                 'DBT_TARGET_PATH': str(tmp_path/'target'), 'DBT_LOG_PATH': str(tmp_path/'logs')})
        assert result.returncode == 0, result.stdout[-8000:]+result.stderr[-2000:]

    def joined_cities():
        return database.execute('''SELECT o.order_id,c.city FROM marts.fct_orders o
            JOIN marts.dim_customers c ON o.customer_id=c.customer_id ORDER BY 1''').fetchall()

    def stored_orders():
        return database.execute('SELECT *,xmin::text,ctid::text FROM marts.fct_orders ORDER BY order_id').fetchall()

    customer = dict(customer_id='c1', customer_unique_id='p1', city='sao paulo', state='SP')
    order = dict(order_id='o1', customer_id='c1', status='created', purchased_at='2017-01-01')
    load('first.csv', [event('customers', customer, 10), event('orders', order, 20),
                      event('customers', {**customer,'customer_id':'c2','city':'recife'}, 30)])
    snapshot = event('orders', {**order, 'order_id':'historical'}, 0)
    load('orders/LOAD.csv', [[snapshot[0], *snapshot[3:]]], 'orders')
    build()
    assert joined_cities() == [('historical','sao paulo'),('o1','sao paulo')]
    before = stored_orders()

    # Changing only customer attributes changes both joins without rewriting facts.
    load('customer-change.csv', [event('customers', {**customer,'city':'campinas'}, 40, 'U')])
    build()
    assert joined_cities() == [('historical','campinas'),('o1','campinas')]
    assert stored_orders() == before
    assert database.execute("SELECT city,is_current FROM marts.dim_customer_history WHERE customer_id='c1' ORDER BY source_order_from").fetchall() == [('sao paulo',False),('campinas',True)]

    # A later order reassignment follows the current source reference.
    load('reassign.csv', [event('orders', {**order,'customer_id':'c2'}, 50, 'U')])
    build()
    assert joined_cities() == [('historical','campinas'),('o1','recife')]
    expected = database.execute('SELECT * FROM marts.fct_orders ORDER BY order_id').fetchall()

    # The documented upgrade rebuild removes the three former history-link columns.
    for column, kind in [('captured_created_at','timestamp'),('customer_version_id','varchar(32)'),('customer_history_status','varchar(40)')]:
        database.execute(f'ALTER TABLE marts.fct_orders ADD COLUMN {column} {kind}')
    build('--full-refresh')
    cursor = database.execute('SELECT * FROM marts.fct_orders ORDER BY order_id')
    assert not {'captured_created_at','customer_version_id','customer_history_status'} & {c.name for c in cursor.description}
    assert cursor.fetchall() == expected
