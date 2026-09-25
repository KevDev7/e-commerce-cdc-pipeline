"""Check the simulated lifecycle against real Redshift raw events and marts."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from dotenv import load_dotenv
from olist_cdc.cloud_load import warehouse_connection
from olist_cdc.simulate import ids

ROOT = Path(__file__).resolve().parents[1]


def verify(cursor, scenario):
    key = ids(scenario)

    def rows(sql, params=()):
        cursor.execute(sql, params)
        return [tuple(row) for row in cursor.fetchall()]

    assert rows('''SELECT status,item_total,freight_total,payment_total,item_count,payment_count
                   FROM marts.fct_orders WHERE order_id=%s''', (key['order'],)) == [
                       ('delivered', 100, 10, 110, 2, 2)]
    assert rows('''SELECT status FROM marts.fct_order_status_history
                   WHERE order_id=%s ORDER BY source_order_from''', (key['order'],)) == [
                       ('created',), ('approved',), ('shipped',), ('delivered',)]
    assert rows('''SELECT city FROM marts.dim_customer_history
                   WHERE customer_id=%s ORDER BY source_order_from''', (key['customer'],)) == [
                       ('sao paulo',), ('campinas',)]
    for table, column, identity in (
        ('dim_customers', 'customer_id', key['delete-customer']),
        ('fct_orders', 'order_id', key['delete-order']),
    ):
        assert rows(f'SELECT count(*) FROM marts.{table} WHERE {column}=%s', (identity,)) == [(0,)]
    assert rows('''SELECT is_deleted FROM marts.dim_customer_history
                   WHERE customer_id=%s ORDER BY source_order_from''', (key['delete-customer'],)) == [(False,), (True,)]
    assert rows('''SELECT count(*) FROM "raw".orders WHERE order_id=%s AND status='ROLLBACK_SENTINEL' ''',
                (key['order'],)) == [(0,)]
    customers = {row[0]: row[1] for row in rows(
        """SELECT o.order_id,c.city FROM marts.fct_orders o
           JOIN marts.dim_customers c ON o.customer_id=c.customer_id
           WHERE o.order_id IN (%s,%s)""", (key['order'], key['repeat-order']))}
    assert customers == {key['order']: 'campinas', key['repeat-order']: 'campinas'}
    operations = {}
    for table, column, identities in (
        ('customers', 'customer_id', (key['customer'], key['delete-customer'])),
        ('orders', 'order_id', (key['order'], key['delete-order'], key['repeat-order'])),
        ('order_items', 'order_id', (key['order'], key['delete-order'])),
        ('order_payments', 'order_id', (key['order'], key['delete-order'])),
    ):
        placeholders = ','.join(['%s'] * len(identities))
        for op, count, unique in rows(f'''SELECT _op,count(*),count(DISTINCT _event_id)
                    FROM "raw".{table} WHERE {column} IN ({placeholders}) AND NOT _is_snapshot GROUP BY _op''', identities):
            assert count == unique
            operations[op] = operations.get(op, 0) + count
    assert operations == {'I': 9, 'U': 4, 'D': 2}, operations
    return dict(scenario=scenario, operations=operations, lifecycle_and_totals=True,
                hard_deletes=True, rollback_excluded=True, unique_event_ids=True,
                current_customer_city='campinas', customer_attribute_history=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('scenarios', nargs='+')
    args = parser.parse_args()
    load_dotenv(ROOT/'.env.cloud', override=True)
    with warehouse_connection() as connection:
        cursor = connection.cursor()
        results = [verify(cursor, scenario) for scenario in args.scenarios]
        connection.commit()
    result = dict(verified_at=datetime.now(timezone.utc).isoformat(), scenarios=results)
    (ROOT/'data/cloud-scenarios.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
