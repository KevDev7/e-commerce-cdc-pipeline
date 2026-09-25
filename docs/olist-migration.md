# Olist migration

The owner selected Olist to replace Synthea on 2026-09-22. This migration preserves the WAL → DMS → S3 → Redshift architecture, five-minute Airflow batches, atomic loading, replay and history safeguards. The preceding Synthea implementation remains in Git history at `04bd80e`.

## Inspected source

Publisher: Olist, [Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), dataset version 2. Downloaded the actual ZIP from Kaggle's dataset download endpoint and inspected the CSVs locally. SHA-256: `967e41e04fc306fe604e2a693f488995a8b41e5047418f8a5c8e4abd6deca784`.

| Table | Rows | Natural key |
|---|---:|---|
| customers | 99,441 | customer_id |
| orders | 99,441 | order_id |
| order_items | 112,650 | order_id + order_item_id |
| order_payments | 103,886 | order_id + payment_sequential |

All four natural keys are unique. All orders reference existing customers; every item and payment references an existing order. There are 96,096 distinct customer_unique_id values. `customer_id` describes a customer record associated with an order; it must not be mistaken for the cross-order person identifier.

775 orders have no items and one order has no payment. Approval timestamps are absent for 160 orders, carrier timestamps for 1,783, and delivery timestamps for 2,965. Preserve these upstream gaps. Order status counts: delivered 96,478; invoiced 314; shipped 1,107; processing 301; unavailable 609; canceled 625; created 5; approved 2.

## Implementation scope

- Use customers, orders, order items and payments; retain product/seller IDs without adding product/seller dimensions in this iteration.
- Preserve original IDs. Derive item/payment row keys from their two natural-key components, keeping both components and enforcing their uniqueness.
- Keep the export's timezone-less business timestamps as `timestamp`; do not invent a timezone. Capture/audit timestamps remain `timestamptz`.
- Simulate explicitly labeled new orders, split payments, status/address updates and disposable hard deletes through SQL. PostgreSQL generates the actual WAL; Python never fabricates CDC logs.
- Track observed customer-record address history after capture begins. Do not claim that it reconstructs pre-capture address history or resolves a person's historical addresses across orders.
- Aggregate items and payments separately to order grain before joining, preventing multi-item/multi-payment fan-out. Missing payment is visible, not an assertion that money settled.
- Use fresh local databases, capture prefix and checkpoints; never mix Olist events with archived Synthea events.
- Preserve old AWS evidence as historical Synthea evidence. Olist needs its own paid AWS validation before claiming end-to-end cloud verification.
- Retain the existing `synthea-cdc` AWS profile/resource ownership labels and GitHub repository identity for continuity; changing those is separate from changing the data source. No paid infrastructure is provisioned by this migration.

## Repository rename

On September 25, 2026, the project was renamed to **E-commerce CDC Pipeline**
with repository `KevDev7/e-commerce-cdc-pipeline`. The `olist-cdc` Python package
and CLI still identify the implemented Olist dataset. AWS profile, stack and
resource ownership names remain `synthea-cdc` so existing cloud resources stay
accessible. Historical evidence retains the original repository URLs.
