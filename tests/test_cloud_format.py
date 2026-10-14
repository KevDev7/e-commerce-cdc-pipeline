import csv
import io

import pytest

from olist_cdc.cloud_load import snapshot_table, validate_raw_schema
from olist_cdc.copy_csv import normalized_csv
from olist_cdc.events import Event, NULL, parse_csv
from olist_cdc.schema import RAW_METADATA, SOURCE_COLUMNS, source_columns
from test_events import csv_text, customer_row


def test_copy_csv_preserves_source_strings_null_empty_text_and_metadata():
    row = customer_row(sequence='12345678901234567890123456789012345', city='São Paulo, "Centro"\nAndar 2')
    row[4] = NULL
    row[7] = ''
    row[8] = '2026-01-01T03:00:00.123456+03:00'
    row[-1] = '2026-01-01T03:00:00.123456+03:00'
    events = parse_csv(csv_text([row]), 's3://bucket/cdc/file.csv')
    body = normalized_csv(events, 'customers')
    restored = dict(zip(source_columns('customers') + list(RAW_METADATA),
                        next(csv.reader(io.StringIO(body.decode(), newline='')))))
    assert restored['city'] == row[6]
    assert restored['state'] == ''
    assert restored['customer_unique_id'] == NULL
    assert restored['postal_code'] == '00123'
    assert '_source_lsn' not in restored
    assert restored['_source_order'] == row[10]
    assert restored['updated_at'] == row[8]  # Source timestamp is unchanged.
    assert restored['_commit_at'] == '2026-01-01T00:00:00.123456+00:00'
    assert restored['_is_snapshot'] == 'false'
    assert body == normalized_csv(events, 'customers')


def payment_event(value):
    return Event('order_payments', ['key','order','1','credit_card','2',value,'invalid-date',
        'cdc:order_payments:123','I',123,'2026-01-01T00:00:00Z',False,'s3://bucket/file.csv'])


def test_source_type_errors_are_preserved_for_staging():
    for value in ('999999999999.99', '0.001', '1000000000000.00', 'not-a-number'):
        row = next(csv.reader(io.StringIO(normalized_csv([payment_event(value)], 'order_payments').decode())))
        assert row[5] == value
        assert row[6] == 'invalid-date'


def test_invalid_metadata_or_changed_shape_cannot_produce_copy_file():
    event = payment_event('10.01'); event.values[-3] = 'invalid'
    with pytest.raises(ValueError):
        normalized_csv([event], 'order_payments')
    with pytest.raises(ValueError, match='shape'):
        normalized_csv([payment_event('10.01')], 'customers')


def test_capture_paths_distinguish_snapshot_from_cdc_and_reject_unexpected_tables():
    assert snapshot_table('raw/initial-load/customers/LOAD00000001.csv', 'raw') == 'customers'
    assert snapshot_table('raw/cdc/20260922.csv', 'raw') is None
    with pytest.raises(ValueError, match='Unexpected'):
        snapshot_table('raw/initial-load/unknown/LOAD00000001.csv', 'raw')


def test_copy_csv_retains_duplicate_deliveries_for_staging_to_deduplicate():
    events = parse_csv(csv_text([customer_row(), customer_row()]), 'repeated.csv')
    rows = list(csv.reader(io.StringIO(normalized_csv(events, 'customers').decode())))
    assert len(rows) == 2
    assert rows[0] == rows[1]


def test_loader_refuses_old_typed_raw_schema():
    class Cursor:
        def execute(self, query): pass
        def fetchall(self): return self.rows
    cursor = Cursor()
    cursor.rows = [(table, col, 'character varying') for table in SOURCE_COLUMNS for col in source_columns(table)]
    validate_raw_schema(cursor)
    cursor.rows = [(t, c, 'timestamp with time zone' if c == 'updated_at' else k) for t,c,k in cursor.rows]
    with pytest.raises(RuntimeError, match='fresh raw baseline'):
        validate_raw_schema(cursor)
