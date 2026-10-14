"""Source values reach raw unchanged; SQL staging owns type conversion failures."""
from datetime import datetime, timezone
from decimal import Decimal
import os
import shutil
import subprocess

import psycopg
import pytest

from olist_cdc.db import ROOT
from olist_cdc.events import NULL, parse_csv
from olist_cdc.schema import SOURCE_COLUMNS, source_columns
from olist_cdc.warehouse import initialize_raw, load_local
from test_events import csv_text
from test_mart_rebuilds import event


@pytest.mark.integration
def test_raw_strings_staging_types_and_invalid_values(database, tmp_path):
    initialize_raw(database)
    rows = [
        event('customers', dict(customer_id='c1', postal_code='00123', city='São Paulo, "Centro"\nAndar 2', state=''), 1),
        event('orders', dict(order_id='o1', customer_id='c1', purchased_at='2017-01-02'), 2),
        event('order_items', dict(order_item_key='o1:1', order_id='o1', order_item_id='1', price='50.00', freight_value='5.00'), 3),
        event('order_payments', dict(payment_key='o1:1', order_id='o1', payment_sequential='1', payment_installments='2', payment_value='55.00', updated_at='2026-01-01T03:00:00.123456+03:00'), 4),
    ]
    load_local(database, parse_csv(csv_text(rows), 'typed.csv'), 'typed.csv')
    database.commit()
    shutil.copyfile(ROOT/'dbt/profiles.yml.example', tmp_path/'profiles.yml')
    result = subprocess.run([str(ROOT/'.venv/bin/dbt'), 'run', '--select', 'path:models/staging',
        '--project-dir', str(ROOT/'dbt'), '--profiles-dir', str(tmp_path), '--target', 'local'],
        capture_output=True, text=True, timeout=120,
        env={**os.environ, 'WAREHOUSE_DATABASE': database.info.dbname,
             'DBT_SEND_ANONYMOUS_USAGE_STATS': 'false',
             'DBT_TARGET_PATH': str(tmp_path/'target'), 'DBT_LOG_PATH': str(tmp_path/'logs')})
    assert result.returncode == 0, result.stdout + result.stderr
    types = {(t,c):k for t,c,k in database.execute("SELECT table_name,column_name,data_type FROM information_schema.columns WHERE table_schema='raw'")}
    assert all(types[t,c] == 'character varying' for t in SOURCE_COLUMNS for c in source_columns(t))
    assert database.execute('SELECT payment_value,payment_installments,updated_at FROM raw.order_payments').fetchone() == ('55.00','2','2026-01-01T03:00:00.123456+03:00')
    assert database.execute('SELECT payment_value,payment_installments,updated_at FROM staging.stg_order_payments').fetchone() == (Decimal('55.00'),2,datetime(2026,1,1,0,0,0,123456,tzinfo=timezone.utc))
    assert database.execute('SELECT purchased_at FROM staging.stg_orders').fetchone() == (datetime(2017,1,2),)
    assert database.execute('SELECT postal_code,city,state,customer_unique_id FROM staging.stg_customers').fetchone() == ('00123',rows[0][6],'',None)
    database.commit()
    # Values load into raw successfully, but querying their staged type fails.
    for column, value in [('payment_value','0.001'), ('payment_value','1000000000000.00'),
                          ('payment_value','not-a-number'), ('payment_installments','1.5'),
                          ('updated_at','invalid-date')]:
        database.execute(f'UPDATE raw.order_payments SET {column}=%s', (value,))
        assert database.execute(f'SELECT {column} FROM raw.order_payments').fetchone() == (value,)
        with pytest.raises(psycopg.Error):
            with database.transaction():
                database.execute(f'SELECT {column} FROM staging.stg_order_payments').fetchall()
        database.rollback()
    # A timezone must not be silently discarded from a source TIMESTAMP.
    database.execute("UPDATE raw.orders SET purchased_at='2017-01-01T12:00:00+03:00'")
    with pytest.raises(psycopg.Error):
        with database.transaction():
            database.execute('SELECT purchased_at FROM staging.stg_orders').fetchall()
    database.rollback()
    # Naive DMS timestamps stay UTC even for queries from another timezone.
    database.execute("UPDATE raw.order_payments SET updated_at='2026-01-01 00:00:00.123456'")
    database.execute("SET TIME ZONE 'Asia/Tokyo'")
    assert database.execute('SELECT updated_at FROM staging.stg_order_payments').fetchone()[0] == datetime(2026,1,1,0,0,0,123456,tzinfo=timezone.utc)
