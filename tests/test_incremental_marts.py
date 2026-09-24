"""Compare successive incremental builds with full rebuilds, using DMS-format fixtures."""
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
         'fct_order_status_history', 'fct_order_items', 'fct_order_payments')


def event(table, data, sequence, op='I'):
    timestamp = (datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=sequence)).isoformat()
    values = [str(data.get(c, timestamp if c == 'updated_at' else NULL)) for c in source_columns(table)]
    return [op, table, 'ecommerce', *values, '0/ABC', str(sequence), timestamp]


@pytest.mark.integration
def test_incremental_batches_match_full_rebuild_and_recover_atomically(database, tmp_path):
    initialize_raw(database)
    profiles = tmp_path / 'profiles'
    profiles.mkdir()
    shutil.copyfile(ROOT / 'dbt/profiles.yml.example', profiles / 'profiles.yml')

    def build(*args, project=ROOT / 'dbt', success=True):
        database.commit()  # Release read locks before dbt replaces relations on full refresh.
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

    def contents(physical=False):
        extra = ', xmin::text, ctid::text' if physical else ''
        return {m: sorted(database.execute(f'SELECT *{extra} FROM marts.{m}').fetchall(), key=repr)
                for m in MARTS}

    def checkpoints():
        return {m: database.execute('SELECT source_file FROM marts.processed_files WHERE model_name=%s ORDER BY 1', (m,)).fetchall()
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
    initial = contents(physical=True)
    assert database.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='marts' AND table_type='BASE TABLE' ORDER BY 1").fetchall() == [(m,) for m in sorted((*MARTS, 'processed_files'))]
    assert all(files == [('baseline.csv',)] for files in checkpoints().values())

    # A no-input build must leave every stored row untouched, not silently rebuild tables.
    build('build')
    assert contents(physical=True) == initial

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
    # Other customers/orders and their history retain their original physical rows.
    now = contents(physical=True)
    for mart, key, index in [('dim_customers', 'c3', 0), ('dim_customer_history', 'c3', 1),
                             ('fct_orders', 'o2', 0), ('fct_order_status_history', 'o2', 1)]:
        assert [r for r in now[mart] if r[index] == key] == [r for r in initial[mart] if r[index] == key]

    # Lower-sequence files arriving later can remove an old history boundary and add others.
    load('late.csv', [event('orders', {**order, 'status': 'delivered'}, 30, 'U'),
                      event('customers', changed_customer, 31, 'U'),
                      event('customers', {**customer, 'city': 'santos'}, 51, 'U')])
    build('build')
    assert database.execute("SELECT source_order_from FROM marts.fct_order_status_history WHERE order_id='o1' ORDER BY 1").fetchall() == [(10,), (20,), (30,)]
    assert database.execute("SELECT city,source_order_from FROM marts.dim_customer_history WHERE customer_id='c1' ORDER BY source_order_from").fetchall() == [('sao paulo', 1), ('campinas', 31), ('santos', 51), ('campinas', 61)]

    # Duplicate delivery under a new filename acknowledges that file without rewriting marts.
    before_replay = contents(physical=True)
    load('redelivery.csv', changes)
    build('build')
    assert contents(physical=True) == before_replay
    assert all(('redelivery.csv',) in files for files in checkpoints().values())

    # Details alone must refresh order totals; delete the final remaining detail rows.
    # Reinsert previously deleted parent/customer keys in a later batch.
    load('details-and-reinsert.csv', [event('order_items', {**item, 'price': 80}, 70, 'D'),
                                    event('order_payments', {**payment, 'payment_value': 85}, 71, 'D'),
                                    event('customers', spare_customer, 72),
                                    event('orders', {**order, 'order_id': 'o3'}, 73)])
    before_failure = contents(physical=True)
    before_checkpoint = checkpoints()
    broken = tmp_path / 'broken_project'
    shutil.copytree(ROOT / 'dbt', broken, ignore=shutil.ignore_patterns('target', 'logs', 'dbt_packages', 'profiles.yml'))
    hook = broken / 'macros/incremental_files.sql'
    # Inject an actual SQL failure after checkpoint insertion, within the model transaction.
    hook.write_text(hook.read_text().replace(
        "select '{{ this.identifier }}', source_file from {{ cdc_temp('pending') }};",
        "select '{{ this.identifier }}', source_file from {{ cdc_temp('pending') }};\n    select 1/0;"))
    build('run', '--select', 'fct_orders', project=broken, success=False)
    assert contents(physical=True) == before_failure
    assert checkpoints() == before_checkpoint
    # A successful selected model advances only its own checkpoint. Other marts
    # must still process these files when the entire graph is built next.
    build('run', '--select', 'fct_orders')
    partial = checkpoints()
    assert ('details-and-reinsert.csv',) in partial['fct_orders']
    for mart in MARTS:
        if mart != 'fct_orders':
            assert partial[mart] == before_checkpoint[mart]
    build('build')
    assert database.execute("SELECT item_count,payment_count,has_items,has_payments,order_total,payment_total FROM marts.fct_orders WHERE order_id='o1'").fetchone() == (0, 0, False, False, 0, 0)
    assert database.execute('SELECT count(*) FROM marts.fct_order_items').fetchone() == (0,)
    assert database.execute('SELECT count(*) FROM marts.fct_order_payments').fetchone() == (0,)
    assert database.execute("SELECT is_deleted,is_current FROM marts.dim_customer_history WHERE customer_id='c2' ORDER BY source_order_from").fetchall() == [(False, False), (True, False), (False, True)]

    incremental = contents()
    ledger = checkpoints()
    # A selected full refresh cannot erase another model's progress.
    build('run', '--select', 'fct_orders', '--full-refresh')
    assert contents() == incremental
    assert checkpoints() == ledger
    build('build', '--full-refresh')
    assert contents() == incremental
    assert checkpoints() == ledger
    rebuilt = contents(physical=True)
    build('build')
    assert contents(physical=True) == rebuilt
