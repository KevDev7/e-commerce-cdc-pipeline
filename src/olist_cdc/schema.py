"""Shared source/event contract, independent of source setup and database drivers."""

# Business columns in source order. The source adds updated_at after these fields.
SOURCE_COLUMNS = {
    "customers": ("customer_id", "customer_unique_id", "postal_code", "city", "state"),
    "orders": ("order_id", "customer_id", "status", "purchased_at", "approved_at",
               "carrier_delivered_at", "customer_delivered_at", "estimated_delivery_at"),
    "order_items": ("order_item_key", "order_id", "order_item_id", "product_id",
                    "seller_id", "shipping_limit_at", "price", "freight_value"),
    "order_payments": ("payment_key", "order_id", "payment_sequential", "payment_type",
                       "payment_installments", "payment_value"),
}

TYPES = {
    **{name: "timestamp" for name in ("purchased_at", "approved_at", "carrier_delivered_at",
        "customer_delivered_at", "estimated_delivery_at", "shipping_limit_at")},
    "updated_at": "timestamptz",
    **{name: "numeric(14,2)" for name in ("price", "freight_value", "payment_value")},
    **{name: "integer" for name in ("order_item_id", "payment_sequential", "payment_installments")},
}

# DMS appends these fields after the source columns; raw retains a normalized envelope.
DMS_METADATA = ("_source_order", "_commit_at")
RAW_METADATA = ("_event_id", "_op", "_source_order", "_commit_at", "_is_snapshot", "_source_file")


def source_columns(table):
    return [*SOURCE_COLUMNS[table], "updated_at"]
