from datetime import datetime, timezone
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from olist_cdc.cloud_load import snapshot_table
from olist_cdc.events import Event, NULL, RAW_METADATA, parse_csv, source_columns
from olist_cdc.parquet import normalized_parquet
from test_events import csv_text, customer_row


def test_parquet_preserves_null_empty_text_postal_sequence_and_utc_microseconds():
    row = customer_row(sequence='12345678901234567890123456789012345', city='São Paulo, "Centro"\nAndar 2')
    row[4] = NULL
    row[7] = ''
    row[8] = '2026-01-01T03:00:00.123456+03:00'
    event = parse_csv(csv_text([row]), 's3://bucket/cdc/file.csv')[0]
    body = normalized_parquet([event], 'customers')
    table = pq.read_table(pa.BufferReader(body))
    restored = table.to_pylist()[0]
    assert table.column_names == source_columns('customers') + list(RAW_METADATA)
    assert restored['city'] == row[6]
    assert restored['state'] == ''
    assert restored['customer_unique_id'] is None
    assert restored['postal_code'] == '00123'
    assert '_source_lsn' not in restored
    assert restored['_source_order'] == Decimal(row[10])
    assert restored['updated_at'] == datetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=timezone.utc)
    assert restored['_is_snapshot'] is False
    assert body == normalized_parquet([event], 'customers')
    assert pq.ParquetFile(pa.BufferReader(body)).metadata.row_group(0).column(0).compression == 'ZSTD'


def payment_event(value):
    return Event('order_payments', ['key','order',1,'credit_card',2,value,'2026-01-01T00:00:00Z',
        'cdc:order_payments:123','I',123,'2026-01-01T00:00:00Z',False,'s3://bucket/file.csv'])


def test_decimal_money_is_exact_and_excess_scale_or_precision_is_rejected():
    table = pq.read_table(pa.BufferReader(normalized_parquet([payment_event('999999999999.99')], 'order_payments')))
    assert table.to_pylist()[0]['payment_value'] == Decimal('999999999999.99')
    for value in ('0.001', '1000000000000.00'):
        with pytest.raises(pa.ArrowInvalid):
            normalized_parquet([payment_event(value)], 'order_payments')


def test_invalid_timestamp_or_changed_shape_cannot_produce_copy_file():
    event = payment_event('10.01'); event.values[6] = 'invalid'
    with pytest.raises(ValueError):
        normalized_parquet([event], 'order_payments')
    with pytest.raises(ValueError, match='shape'):
        normalized_parquet([payment_event('10.01')], 'customers')


def test_capture_paths_distinguish_snapshot_from_cdc_and_reject_unexpected_tables():
    assert snapshot_table('raw/initial-load/customers/LOAD00000001.csv', 'raw') == 'customers'
    assert snapshot_table('raw/cdc/20260922.csv', 'raw') is None
    with pytest.raises(ValueError, match='Unexpected'):
        snapshot_table('raw/initial-load/unknown/LOAD00000001.csv', 'raw')
