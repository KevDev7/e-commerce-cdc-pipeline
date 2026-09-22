"""Reconcile the actual Olist seed with the local snapshot fixture and marts.

Run after init, seed and build_local_warehouse, before simulating new source changes.
This is local evidence only, never an assertion that AWS DMS was exercised.
"""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

from olist_cdc.db import ROOT, connect
from olist_cdc.events import source_columns
from olist_cdc.seed import TABLES, SHA256, URL


def fingerprint(rows):
    digest = hashlib.sha256()
    count = 0
    for row in rows:
        values = [v.astimezone(timezone.utc).isoformat() if isinstance(v,datetime) and v.tzinfo
                  else None if v is None else str(v) for v in row]
        digest.update(json.dumps(values,separators=(',',':')).encode()+b'\n')
        count += 1
    return dict(rows=count, sha256=digest.hexdigest())


def main():
    if os.environ.get('POSTGRES_HOST','127.0.0.1') not in ('127.0.0.1','localhost'):
        raise ValueError('This evidence command is local-only')
    result = dict(source_url=URL, archive_sha256=SHA256,
                  verified_at=datetime.now(timezone.utc).isoformat(),
                  validation='Local PostgreSQL snapshot fixture; not AWS CDC',tables={},marts={})
    with connect() as source, connect(os.environ.get('WAREHOUSE_DATABASE','olist_warehouse')) as target:
        for table in TABLES:
            columns=','.join(source_columns(table))
            expected=fingerprint(source.execute(f'SELECT {columns} FROM ecommerce.{table} ORDER BY 1'))
            actual=fingerprint(target.execute(f'SELECT {columns} FROM analytics_intermediate.int_{table}_current ORDER BY 1'))
            assert actual == expected, (table,actual,expected)
            result['tables'][table]=actual
        for table in ('dim_customers','dim_customer_history','fct_orders','fct_order_status_history','fct_order_items','fct_order_payments'):
            result['marts'][table]=target.execute(f'SELECT count(*) FROM analytics_marts.{table}').fetchone()[0]
        result['orders_without_items']=target.execute('SELECT count(*) FROM analytics_marts.fct_orders WHERE NOT has_items').fetchone()[0]
        result['orders_without_payments']=target.execute('SELECT count(*) FROM analytics_marts.fct_orders WHERE NOT has_payments').fetchone()[0]
        assert result['orders_without_items']==775
        assert result['orders_without_payments']==1
        expected_items=source.execute('SELECT sum(price),sum(freight_value) FROM ecommerce.order_items').fetchone()
        expected_payments=source.execute('SELECT sum(payment_value) FROM ecommerce.order_payments').fetchone()[0]
        actual=target.execute('SELECT sum(item_total),sum(freight_total),sum(payment_total) FROM analytics_marts.fct_orders').fetchone()
        assert actual==(*expected_items,expected_payments)
        result['order_grain_totals']=dict(zip(('items','freight','payments'),map(str,actual)))
    built=json.loads((ROOT/'dbt/target/run_results.json').read_text())['results']
    assert all(r['status'] in ('success','pass') for r in built)
    result['dbt_models']=sum(r['unique_id'].startswith('model.') for r in built)
    result['dbt_tests']=sum(r['unique_id'].startswith('test.') for r in built)
    manifest=json.loads((ROOT/'dbt/target/manifest.json').read_text())
    result['dbt_incremental_models']=sorted(
        manifest['nodes'][r['unique_id']]['name'] for r in built
        if r['unique_id'].startswith('model.')
        and manifest['nodes'][r['unique_id']]['config']['materialized']=='incremental')
    destination=ROOT/'docs/evidence/olist-local-validation.json'
    destination.parent.mkdir(parents=True,exist_ok=True)
    destination.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))


if __name__=='__main__':
    main()
