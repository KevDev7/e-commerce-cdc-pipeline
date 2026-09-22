"""Create and verify a follow-up order for one completed simulated customer.

Run `write` after the scenario's address correction, then capture/load/build.
`verify` checks the old and new orders against their observed customer versions.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from dotenv import load_dotenv
from olist_cdc.cloud_load import warehouse_connection
from olist_cdc.db import connect
from olist_cdc.simulate import ids

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['write', 'verify'])
    parser.add_argument('--scenario', required=True)
    args = parser.parse_args()
    load_dotenv(ROOT/'.env.cloud', override=True)
    original = ids(args.scenario)
    followup = ids(args.scenario+'-repeat')['order']
    if args.command == 'write':
        with connect() as connection:
            assert connection.execute('SELECT city FROM ecommerce.customers WHERE customer_id=%s',
                                      (original['customer'],)).fetchone() == ('campinas',)
            cursor = connection.execute('''INSERT INTO ecommerce.orders
                (order_id,customer_id,status,purchased_at)
                VALUES (%s,%s,'created',CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')
                ON CONFLICT (order_id) DO NOTHING''', (followup,original['customer']))
            print('Simulated follow-up order inserted:', cursor.rowcount)
        return
    with warehouse_connection() as connection:
        cursor = connection.cursor()
        cursor.execute('''SELECT o.order_id,o.customer_history_status,h.city,h.customer_version_id
            FROM analytics_marts.fct_orders o LEFT JOIN analytics_marts.dim_customer_history h
            ON o.customer_version_id=h.customer_version_id WHERE o.order_id IN (%s,%s)''',
            (original['order'], followup))
        rows = {row[0]:row[1:] for row in cursor.fetchall()}
        assert tuple(rows[original['order']][:2]) == ('matched','sao paulo'), rows
        assert tuple(rows[followup][:2]) == ('matched','campinas'), rows
        assert rows[original['order']][2] != rows[followup][2]
        cursor.execute("""SELECT count(*) FROM analytics_marts.fct_orders
                       WHERE customer_history_status='creation_not_captured'
                         AND customer_version_id IS NULL""")
        historical = cursor.fetchone()[0]
        assert historical == 99441
        connection.commit()
    result = dict(verified_at=datetime.now(timezone.utc).isoformat(), scenario=args.scenario,
                  original_order_city='sao paulo', followup_order_city='campinas',
                  distinct_observed_versions=True, historical_orders_without_invented_version=historical)
    (ROOT/'data/customer-join.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
