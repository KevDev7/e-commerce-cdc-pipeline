import io
import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from olist_cdc.reference import recover_snapshot, save_reference
from olist_cdc.seed import TABLES


def test_recovery_links_rows_and_does_not_invent_metadata():
    values = {
        'customers': ['c1', 'p1', '00123', 'city', 'SP'],
        'orders': ['o1', 'c1', 'delivered', '2017-01-01', '', '', '', '2017-02-01'],
        'order_items': ['o1:1', 'o1', '1', 'p1', 's1', '2017-01-01', '1.00', '2.00'],
        'order_payments': ['o1:1', 'o1', '1', 'credit_card', '1', '3.00'],
    }
    objects = {f'prefix/ecommerce/{t}/LOAD.csv': ','.join(['I', *v, '2026-01-01', '', '', '2026-01-02'])+'\n'
               for t, v in values.items()}
    s3 = SimpleNamespace(
        get_paginator=lambda name: SimpleNamespace(paginate=lambda **kw: [{'Contents': [{'Key': k} for k in objects if k.startswith(kw['Prefix'])]}]),
        get_object=lambda **kw: {'Body': io.BytesIO(objects[kw['Key']].encode())},
    )
    result = recover_snapshot(s3, 'bucket', 'prefix')
    tables = result['tables']
    assert tables['ecommerce.orders']['rows'][0]['customer_id'] == tables['ecommerce.customers']['rows'][0]['customer_id']
    assert tables['ecommerce.order_payments']['rows'][0]['order_id'] == 'o1'
    assert tables['ecommerce.customers']['rows'][0]['postal_code'] == '00123'
    assert tables['project_meta.seed_runs']['rows'] is None
    assert tables['project_meta.simulation_steps']['status'] == 'unavailable'


def test_reference_upload_preserves_money_and_times_and_failure_is_not_success(tmp_path):
    calls = []
    result = {'value': Decimal('58.90'), 'time': datetime(2026, 1, 1, tzinfo=timezone.utc)}
    uri = save_reference(SimpleNamespace(put_object=lambda **kw: calls.append(kw)), 'bucket', result, tmp_path)
    assert uri.startswith('s3://bucket/reference/')
    assert calls[0]['ServerSideEncryption'] == 'AES256'
    assert json.loads((tmp_path/'source-reference.json').read_text())['value'] == '58.90'
    def fail(**kw):
        raise RuntimeError('S3 unavailable')
    with pytest.raises(RuntimeError):
        save_reference(SimpleNamespace(put_object=fail), 'bucket', result, tmp_path/'failed')
    assert not (tmp_path/'failed').exists()
