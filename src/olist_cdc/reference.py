"""Small reference exports, separate from backups and CDC checkpoints."""
import csv
from datetime import datetime, timezone
from decimal import Decimal
import io
import json
from pathlib import Path
from uuid import uuid4

from psycopg.rows import dict_row

from olist_cdc.seed import TABLES
from olist_cdc.events import NULL

ROOT = Path(__file__).resolve().parents[2]
META = ('seed_runs', 'simulation_steps')


def document(origin, tables):
    return {
        'exported_at': datetime.now(timezone.utc).isoformat(),
        'origin': origin,
        'scope': 'One connected order example and complete available project metadata; not a database backup.',
        'source_schema_sql': (ROOT / 'sql/source.sql').read_text(),
        'tables': tables,
    }


def export_live(connection):
    """Read a consistent snapshot without modifying the source."""
    tables = {}
    with connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        connection.execute("SET LOCAL TIME ZONE 'UTC'")
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute('''SELECT o.* FROM ecommerce.orders o
                WHERE EXISTS (SELECT 1 FROM ecommerce.order_items i WHERE i.order_id=o.order_id)
                AND EXISTS (SELECT 1 FROM ecommerce.order_payments p WHERE p.order_id=o.order_id)
                ORDER BY o.order_id LIMIT 1''')
            order = cursor.fetchone()
            tables['ecommerce.orders'] = {'status': 'exported', 'rows': [order] if order else []}
            for table, field, value in (
                ('customers', 'customer_id', order['customer_id'] if order else None),
                ('order_items', 'order_id', order['order_id'] if order else None),
                ('order_payments', 'order_id', order['order_id'] if order else None),
            ):
                # Table/field names are fixed above, never external input.
                cursor.execute(f'SELECT * FROM ecommerce.{table} WHERE {field}=%s ORDER BY 1', (value,))
                tables[f'ecommerce.{table}'] = {'status': 'exported', 'rows': cursor.fetchall()}
            for table in META:
                cursor.execute(f'SELECT * FROM project_meta.{table} ORDER BY 1, 2')
                tables[f'project_meta.{table}'] = {'status': 'exported', 'rows': cursor.fetchall()}
    return document('live PostgreSQL snapshot', tables)


def recover_snapshot(s3, bucket, prefix):
    """Stream retained DMS snapshots; keep only a connected example in memory."""
    def rows(table):
        landing = f'{prefix.rstrip("/")}/ecommerce/{table}/'
        for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=landing):
            for obj in page.get('Contents', []):
                if not obj['Key'].endswith('.csv'):
                    continue
                body = s3.get_object(Bucket=bucket, Key=obj['Key'])['Body']
                with io.TextIOWrapper(body, encoding='utf-8', newline='') as stream:
                    for row in csv.reader(stream):
                        columns = [*TABLES[table], 'updated_at']
                        if len(row) != len(columns) + 4 or row[0] != 'I':
                            raise ValueError(f'Unexpected snapshot layout: {obj["Key"]}')
                        yield dict(zip(columns, (None if v == NULL else v for v in row[1:1 + len(columns)]))), obj['Key']

    item_rows = rows('order_items')
    try:
        item, _ = next(item_rows)
    finally:
        item_rows.close()
    order, order_key = next((r, k) for r, k in rows('orders') if r['order_id'] == item['order_id'])
    customer, customer_key = next((r, k) for r, k in rows('customers') if r['customer_id'] == order['customer_id'])
    payments = [(r, k) for r, k in rows('order_payments') if r['order_id'] == order['order_id']]
    items = [(r, k) for r, k in rows('order_items') if r['order_id'] == order['order_id']]
    tables = {}
    for table, pairs in [('customers', [(customer, customer_key)]), ('orders', [(order, order_key)]),
                         ('order_items', items), ('order_payments', payments)]:
        tables[f'ecommerce.{table}'] = {
            'status': 'recovered_snapshot', 'rows': [r for r, _ in pairs],
            'source_objects': sorted({f's3://{bucket}/{k}' for _, k in pairs}),
        }
    for table in META:
        tables[f'project_meta.{table}'] = {
            'status': 'unavailable', 'rows': None,
            'reason': 'Not included in retained CDC files; deleted source rows cannot be recovered.',
        }
    result = document('retained DMS initial snapshots; values preserved as CSV strings, not current state', tables)
    result['schema_note'] = 'SQL is the current repository schema supplied for reference.'
    return result


def encode(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f'Unsupported reference value: {type(value)}')


def save_reference(s3, bucket, result, directory=None):
    payload = json.dumps(result, default=encode, indent=2) + '\n'
    key = f'reference/{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}.json'
    s3.put_object(Bucket=bucket, Key=key, Body=payload.encode(), ContentType='application/json',
                  ServerSideEncryption='AES256')
    if directory is not None:
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        (directory / 'source-reference.json').write_text(payload)
    return f's3://{bucket}/{key}'


def preserve_live(session, bucket, host):
    from dotenv import load_dotenv
    from olist_cdc.db import connect
    import os
    load_dotenv(ROOT / '.env.cloud', override=True)
    os.environ['POSTGRES_HOST'] = host
    with connect() as connection:
        result = export_live(connection)
    return save_reference(session.client('s3'), bucket, result, ROOT / 'data/reference')
