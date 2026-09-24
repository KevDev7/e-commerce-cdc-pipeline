"""Compare every current source field with Redshift after a quiescent demo batch."""
from datetime import datetime, timezone
from itertools import zip_longest
import json
from pathlib import Path

from dotenv import load_dotenv

from olist_cdc.cloud_load import warehouse_connection
from olist_cdc.db import connect
from olist_cdc.events import source_columns
from olist_cdc.seed import TABLES

ROOT = Path(__file__).resolve().parents[1]


def canonical(value):
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc).isoformat()
    return None if value is None else str(value)


def compare_rows(table, source_rows, target_rows):
    """Compare ordered iterators without persisting or collecting their datasets."""
    missing = object()
    count = 0
    for count, (expected, actual) in enumerate(zip_longest(source_rows, target_rows, fillvalue=missing), 1):
        if expected is missing or actual is missing:
            side = 'source' if expected is missing else 'warehouse'
            raise AssertionError(f'{table}: {side} ended before row {count}')
        if [canonical(v) for v in expected] != [canonical(v) for v in actual]:
            raise AssertionError(f'{table}: first mismatch at row {count}, source key {expected[0]}')
    return {'count': count, 'all_fields_match': True}


def main():
    load_dotenv(ROOT / '.env.cloud', override=True)
    with connect() as source, warehouse_connection() as target:
        source.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        result = {}
        for table in TABLES:
            columns = ','.join(source_columns(table))
            source_rows = source.execute(f'SELECT {columns} FROM ecommerce.{table} ORDER BY 1')
            cursor = target.cursor()
            cursor.execute(f'SELECT {columns} FROM intermediate.int_{table}_current ORDER BY 1')
            result[table] = compare_rows(table, source_rows, cursor)
            cursor.close()
        target.commit()
    path = ROOT / 'data/cloud-reconciliation.json'
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps({'verified_at': datetime.now(timezone.utc).isoformat(), 'tables': result}, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
