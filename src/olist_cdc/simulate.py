"""Simulated new orders, clearly separate from Olist's real historical seed.

Python commits SQL transactions. PostgreSQL, not Python, generates the WAL.
"""
import re
from uuid import UUID, uuid5

import psycopg

NAMESPACE = UUID('ac97c34a-c917-48cc-aed0-81c37f3db319')
PHASES = ('open', 'approve', 'ship', 'deliver', 'correct', 'repeat-order', 'create-delete-test', 'delete-test', 'rollback-test')


def ids(scenario):
    if not re.fullmatch(r'[a-zA-Z0-9_-]{1,50}', scenario):
        raise ValueError('Scenario names must be 1–50 letters, digits, underscores or hyphens')
    return {kind: uuid5(NAMESPACE, f'{scenario}:{kind}').hex
            for kind in ('customer', 'person', 'order', 'product', 'seller', 'delete-customer', 'delete-order', 'repeat-order')}


def run_phase(connection, scenario, phase):
    if phase not in PHASES:
        raise ValueError(f'Unknown phase: {phase}')
    key = ids(scenario)
    with connection.transaction():
        connection.execute('SELECT pg_advisory_xact_lock(8174202)')
        completed = {r[0] for r in connection.execute(
            'SELECT phase FROM project_meta.simulation_steps WHERE scenario=%s', (scenario,))}
        if phase in completed:
            return dict(scenario=scenario, phase=phase, status='already_completed')
        index = PHASES.index(phase)
        if index and PHASES[index - 1] not in completed:
            raise ValueError(f'Run phase {PHASES[index - 1]} before {phase}')
        if phase == 'open':
            connection.execute("""INSERT INTO ecommerce.customers
                (customer_id,customer_unique_id,postal_code,city,state)
                VALUES (%s,%s,'01000','sao paulo','SP')""", (key['customer'],key['person']))
            connection.execute("""INSERT INTO ecommerce.orders (order_id,customer_id,status,purchased_at,estimated_delivery_at)
                VALUES (%s,%s,'created',CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo',
                    (CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo') + interval '7 days')""", (key['order'],key['customer']))
            for line in (1, 2):
                connection.execute("""INSERT INTO ecommerce.order_items
                    (order_item_key,order_id,order_item_id,product_id,seller_id,shipping_limit_at,price,freight_value)
                    VALUES (%s,%s,%s,%s,%s,(CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')+interval '2 days',50,5)""",
                    (key['order']+':'+str(line),key['order'],line,key['product'],key['seller']))
        elif phase == 'approve':
            # Two payment entries for two items exercise independent aggregation.
            for seq, amount, method in ((1,60,'credit_card'),(2,50,'voucher')):
                connection.execute("""INSERT INTO ecommerce.order_payments
                    (payment_key,order_id,payment_sequential,payment_type,payment_installments,payment_value)
                    VALUES (%s,%s,%s,%s,1,%s)""", (key['order']+':'+str(seq),key['order'],seq,method,amount))
            connection.execute("""UPDATE ecommerce.orders SET status='approved',
                approved_at=CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo' WHERE order_id=%s""", (key['order'],))
        elif phase == 'ship':
            connection.execute("""UPDATE ecommerce.orders SET status='shipped',
                carrier_delivered_at=CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo' WHERE order_id=%s""", (key['order'],))
        elif phase == 'deliver':
            connection.execute("""UPDATE ecommerce.orders SET status='delivered',
                customer_delivered_at=CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo' WHERE order_id=%s""", (key['order'],))
        elif phase == 'correct':
            connection.execute("UPDATE ecommerce.customers SET city='campinas',postal_code='13000' WHERE customer_id=%s", (key['customer'],))
        elif phase == 'repeat-order':
            # A second order shares the same current customer record.
            connection.execute("""INSERT INTO ecommerce.orders (order_id,customer_id,status,purchased_at)
                VALUES (%s,%s,'created',CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')""",
                (key['repeat-order'],key['customer']))
        elif phase == 'create-delete-test':
            connection.execute("""INSERT INTO ecommerce.customers (customer_id,customer_unique_id,city,state)
                VALUES (%s,%s,'disposable CDC test','SP')""", (key['delete-customer'],key['delete-customer']))
            connection.execute("""INSERT INTO ecommerce.orders (order_id,customer_id,status,purchased_at)
                VALUES (%s,%s,'created',CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')""", (key['delete-order'],key['delete-customer']))
        elif phase == 'delete-test':
            # Delete only the disposable records from this scenario, never Olist history.
            connection.execute('DELETE FROM ecommerce.orders WHERE order_id=%s', (key['delete-order'],))
            connection.execute('DELETE FROM ecommerce.customers WHERE customer_id=%s', (key['delete-customer'],))
        elif phase == 'rollback-test':
            with connection.transaction():
                connection.execute("UPDATE ecommerce.orders SET status='ROLLBACK_SENTINEL' WHERE order_id=%s", (key['order'],))
                raise psycopg.Rollback()
        connection.execute('INSERT INTO project_meta.simulation_steps (scenario,phase) VALUES (%s,%s)', (scenario,phase))
    return dict(scenario=scenario,phase=phase,status='completed')
