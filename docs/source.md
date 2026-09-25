# Olist source contract

Source: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), version 2, published under CC BY-NC-SA 4.0. The version-specific download URL is in `src/olist_cdc/seed.py`. Loading checks the selected CSV filenames, required columns and source constraints; it does not require an exact ZIP checksum. Cloud preparation downloads the original ZIP temporarily, retains a copy in S3, and removes the local temporary file. Datasets are never committed.

This is real, anonymized historical business data, not a live API or event stream. The application database is a portfolio reconstruction. New source activity is simulated through real SQL transactions; PostgreSQL produces the WAL read by DMS.

## Historical dataset size

The four selected CSVs contain **415,418 historical rows** before simulated
activity. The complete original Olist ZIP is retained in S3.

| Source table | Historical rows | Warehouse marts |
|---|---:|---|
| customers | 99,441 | dim_customers; dim_customer_history |
| orders | 99,441 | fct_orders |
| order_items | 112,650 | fct_order_items |
| order_payments | 103,886 | fct_order_payments |

## Selected tables

- **customers:** customer_id, customer_unique_id, postal_code, city, state.
- **orders:** order_id, customer_id, status, purchased_at, approved_at, carrier_delivered_at, customer_delivered_at, estimated_delivery_at.
- **order_items:** order_item_key, order_id, order_item_id, product_id, seller_id, shipping_limit_at, price, freight_value.
- **order_payments:** payment_key, order_id, payment_sequential, payment_type, payment_installments, payment_value.

All tables also have database audit `updated_at`. This timestamp is not a CDC watermark. All use `REPLICA IDENTITY FULL`; primary and foreign keys enforce source relationships.

`customer_id` identifies the customer record associated with an order, while `customer_unique_id` links a person across orders. We preserve both. We do not merge address records into a supposed historical person dimension. Product and seller IDs remain available, but their dimension files are outside this scoped implementation.

Item/payment rows have composite natural keys. We derive `order_item_key = order_id || ':' || order_item_id` and `payment_key = order_id || ':' || payment_sequential`, preserve the original components, and enforce uniqueness and key consistency. No generated business values are inserted into the historical seed.

## Preservation and limitations

The archive's business timestamps have no timezone offset. Store them as `timestamp` and preserve their literal values. PostgreSQL audit and CDC commit times use `timestamptz`. New simulated business timestamps explicitly use America/Sao_Paulo; that is a simulation choice, not a claim about the export's timezone.

Empty CSV values become SQL NULL. Zip-code prefixes remain strings, preserving leading zeroes. Money uses decimal types. Missing delivery/approval dates and orders without item/payment records are retained. No rule forces payment totals to equal item plus freight totals: they differ in the actual data.

Source seeding is atomic and recorded once under `olist-kaggle-v2`. Retrying never overwrites subsequent changes. Start a fresh database for a different seed or source schema. The inspected counts and anomalies are in [the migration record](olist-migration.md).

The seed ledger now uses `seed_id` instead of an archive hash. This source setup
is for a fresh demo database; an older source ledger is not migrated in place.
