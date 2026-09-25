"""Rebuild marts from retained events across changes, replay and SQL failure."""
from datetime import datetime, timedelta, timezone
import os
import shutil
import subprocess

import pytest

from olist_cdc.db import ROOT
from olist_cdc.events import NULL, parse_csv, source_columns
from olist_cdc.warehouse import initialize_raw, load_local
from test_events import csv_text

MARTS = ('dim_customers', 'dim_customer_history', 'fct_orders',
         'fct_order_items', 'fct_order_payments')


def event(table, data, sequence, op='I'):
    timestamp = (datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=sequence)).isoformat()
    values = [str(data.get(c, timestamp if c == 'updated_at' else NULL)) for c in source_columns(table)]
    return [op, table, 'ecommerce', *values, '0/ABC', str(sequence), timestamp]


@pytest.mark.integration
def test_mart_rebuilds_preserve_history_and_recover_after_failure(database, tmp_path):
    initialize_raw(database)
    profiles = tmp_path / 'profiles'
    profiles.mkdir()
    shutil.copyfile(ROOT / 'dbt/profiles.yml.example', profiles / 'profiles.yml')

    def build(*args, project=ROOT / 'dbt', success=True):
        database.commit()  # Release read locks before dbt replaces table relations.
        result = subprocess.run(
            [str(ROOT / '.venv/bin/dbt'), *args, '--project-dir', str(project),
             '--profiles-dir', str(profiles), '--target', 'local'], capture_output=True, text=True,
            env={**os.environ, 'WAREHOUSE_DATABASE': database.info.dbname,
                 'DBT_SEND_ANONYMOUS_USAGE_STATS': 'false',
                 'DBT_TARGET_PATH': str(tmp_path / 'target'), 'DBT_LOG_PATH': str(tmp_path / 'logs')},
            timeout=120)
        if success:
            assert result.returncode == 0, result.stdout[-12000:] + result.stderr[-2000:]
        else:
            assert result.returncode != 0 and 'division by zero' in result.stdout, result.stdout

    def load(name, rows):
        text = csv_text(rows)
        load_local(database, parse_csv(text, name), name)

    def contents():
        return {m: sorted(database.execute(f'SELECT * FROM marts.{m}').fetchall(), key=repr)
                for m in MARTS}

    customer = dict(customer_id='c1', customer_unique_id='person1', city='sao paulo', state='SP', postal_code='00123')
    spare_customer = {**customer, 'customer_id': 'c2'}
    untouched_customer = {**customer, 'customer_id': 'c3'}
    order = dict(order_id='o1', customer_id='c1', status='created', purchased_at='2017-01-01 12:00:00')
    item = dict(order_item_key='o1:1', order_id='o1', order_item_id=1, product_id='p', seller_id='s', price=50, freight_value=5)
    second_item = {**item, 'order_item_key': 'o1:2', 'order_item_id': 2}
    payment = dict(payment_key='o1:1', order_id='o1', payment_sequential=1, payment_type='credit_card', payment_installments=1, payment_value=60)
    second_payment = {**payment, 'payment_key': 'o1:2', 'payment_sequential': 2, 'payment_value': 50}
    baseline = [event('customers', customer, 1), event('customers', spare_customer, 2),
                event('customers', untouched_customer, 3), event('orders', order, 10),
                event('orders', {**order, 'order_id': 'o2', 'customer_id': 'c3'}, 11),
                event('orders', {**order, 'order_id': 'o3'}, 12),
                event('order_items', item, 13), event('order_items', second_item, 14),
                event('order_payments', payment, 15), event('order_payments', second_payment, 16),
                event('orders', {**order, 'status': 'approved'}, 20)]
    load('baseline.csv', baseline)
    build('build')
    initial = contents()
    assert database.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='marts' AND table_type='BASE TABLE' ORDER BY 1").fetchall() == [(m,) for m in sorted(MARTS)]

    # A manual repeat rebuild must produce the same business rows and history.
    build('build')
    assert contents() == initial

    changed_customer = {**customer, 'city': 'campinas'}
    changes = [event('orders', {**order, 'status': 'delivered'}, 40, 'U'),
               event('customers', changed_customer, 41, 'U'),
               event('order_items', {**item, 'price': 80}, 42, 'U'),
               event('order_payments', {**payment, 'payment_value': 85}, 43, 'U'),
               event('order_items', second_item, 44, 'D'), event('order_payments', second_payment, 45, 'D'),
               event('customers', spare_customer, 46, 'D'),
               event('orders', {**order, 'order_id': 'o3'}, 47, 'D'),
               event('orders', {**order, 'status': 'delivered'}, 60, 'U'),
               event('customers', changed_customer, 61, 'U')]
    load('updates.csv', changes)
    build('build')
    assert database.execute("SELECT item_total,freight_total,payment_total,item_count,payment_count FROM marts.fct_orders WHERE order_id='o1'").fetchone() == (80, 5, 85, 1, 1)
    assert database.execute("SELECT count(*) FROM marts.dim_customers WHERE customer_id='c2'").fetchone() == (0,)
    assert database.execute("SELECT count(*) FROM marts.fct_orders WHERE order_id='o3'").fetchone() == (0,)
    # Unchanged customers/orders and their histories retain their business values.
    now = contents()
    for mart, key, index in [('dim_customers', 'c3', 0), ('dim_customer_history', 'c3', 1),
                             ('fct_orders', 'o2', 0)]:
        assert [r for r in now[mart] if r[index] == key] == [r for r in initial[mart] if r[index] == key]

    # Lower-sequence files arriving later can remove an old history boundary and add others.
    load('late.csv', [event('orders', {**order, 'status': 'delivered'}, 30, 'U'),
                      event('customers', changed_customer, 31, 'U'),
                      event('customers', {**customer, 'city': 'santos'}, 51, 'U')])
    build('build')
    assert database.execute("SELECT status FROM marts.fct_orders WHERE order_id='o1'").fetchone() == ('delivered',)
    assert database.execute("SELECT city,source_order_from FROM marts.dim_customer_history WHERE customer_id='c1' ORDER BY source_order_from").fetchall() == [('sao paulo', 1), ('campinas', 31), ('santos', 51), ('campinas', 61)]

    # Duplicate delivery under a new filename must not duplicate rows or history.
    before_replay = contents()
    load('redelivery.csv', changes)
    build('build')
    assert contents() == before_replay

    # Details alone must refresh order totals; delete the final remaining detail rows.
    # Reinsert previously deleted parent/customer keys in a later batch.
    load('details-and-reinsert.csv', [event('order_items', {**item, 'price': 80}, 70, 'D'),
                                    event('order_payments', {**payment, 'payment_value': 85}, 71, 'D'),
                                    event('customers', spare_customer, 72),
                                    event('orders', {**order, 'order_id': 'o3'}, 73)])
    before_failure = contents()
    broken = tmp_path / 'broken_project'
    shutil.copytree(ROOT / 'dbt', broken, ignore=shutil.ignore_patterns('target', 'logs', 'dbt_packages', 'profiles.yml'))
    model = broken / 'models/marts/fct_orders.sql'
    # Fail after dbt has built/swapped the replacement, within its transaction.
    model.write_text("{{ config(post_hook='select 1/0') }}\n" + model.read_text())
    build('run', '--select', 'fct_orders', project=broken, success=False)
    assert contents() == before_failure
    # A normal retry rebuilds from all raw events; no model checkpoint is needed.
    build('build')
    assert database.execute("SELECT item_count,payment_count,has_items,has_payments,order_total,payment_total FROM marts.fct_orders WHERE order_id='o1'").fetchone() == (0, 0, False, False, 0, 0)
    assert database.execute('SELECT count(*) FROM marts.fct_order_items').fetchone() == (0,)
    assert database.execute('SELECT count(*) FROM marts.fct_order_payments').fetchone() == (0,)
    assert database.execute("SELECT is_deleted,is_current FROM marts.dim_customer_history WHERE customer_id='c2' ORDER BY source_order_from").fetchall() == [(False, False), (True, False), (False, True)]

    rebuilt = contents()
    raw_before = database.execute('SELECT source_file,row_count FROM raw.loaded_files ORDER BY source_file').fetchall()
    build('build')
    assert contents() == rebuilt
    assert database.execute('SELECT source_file,row_count FROM raw.loaded_files ORDER BY source_file').fetchall() == raw_before
