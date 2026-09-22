"""Explicitly typed COPY files, in the same column order as raw_ddl()."""
from datetime import datetime, timezone
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

from olist_cdc.events import RAW_METADATA, source_columns
from olist_cdc.warehouse import TYPES


def field_type(name):
    kind = TYPES.get(name)
    if name == "_source_order":
        return pa.decimal128(35, 0)
    if kind == "numeric(14,2)":
        return pa.decimal128(14, 2)
    if kind == "integer":
        return pa.int32()
    if kind == "timestamp":
        return pa.timestamp("us")
    if kind == "timestamptz" or name == "_commit_at":
        return pa.timestamp("us", tz="UTC")
    if name == "_is_snapshot":
        return pa.bool_()
    return pa.string()


def value_for(value, kind):
    if value is None:
        return None
    if pa.types.is_decimal(kind):
        return Decimal(value)
    if pa.types.is_integer(kind):
        return int(value)
    if pa.types.is_timestamp(kind):
        parsed = datetime.fromisoformat(value)
        if kind.tz:
            # DMS's timezone-less audit timestamps represent UTC.
            return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
        if parsed.tzinfo is not None:
            raise ValueError("Business timestamps must preserve their timezone-less source values")
        return parsed
    return value


def typed_table(events, table):
    columns = source_columns(table) + list(RAW_METADATA)
    required = set(RAW_METADATA) - {"_source_lsn"}
    schema = pa.schema([pa.field(name, field_type(name), nullable=name not in required) for name in columns])
    for event in events:
        if event.table != table or len(event.values) != len(columns):
            raise ValueError(f"Unexpected event shape for {table}")
    arrays = []
    for index, field in enumerate(schema):
        values = [value_for(event.values[index], field.type) for event in events]
        if not field.nullable and any(value is None for value in values):
            raise ValueError(f"Required CDC metadata is missing: {field.name}")
        arrays.append(pa.array(values, type=field.type, safe=True))
    return pa.Table.from_arrays(arrays, schema=schema)


def normalized_parquet(events, table):
    sink = pa.BufferOutputStream()
    pq.write_table(typed_table(events, table), sink, compression="zstd", version="2.6",
                   coerce_timestamps="us", allow_truncated_timestamps=False)
    return sink.getvalue().to_pybytes()
