"""Prepare per-table CSV COPY inputs without converting source column values."""
import csv
from datetime import datetime, timezone
import io

from olist_cdc.events import NULL
from olist_cdc.schema import RAW_METADATA, source_columns


def normalized_csv(events, table):
    columns = source_columns(table) + list(RAW_METADATA)
    sink = io.StringIO(newline="")
    writer = csv.writer(sink, lineterminator="\n")
    for event in events:
        if event.table != table or len(event.values) != len(columns):
            raise ValueError(f"Unexpected event shape for {table}")
        values = list(event.values)
        if any(value is None for value in values[-6:]):
            raise ValueError("Required CDC metadata is missing")
        # Only the CDC envelope is normalized. Naive DMS audit times are UTC.
        commit_at = datetime.fromisoformat(values[-3])
        if commit_at.tzinfo is None:
            commit_at = commit_at.replace(tzinfo=timezone.utc)
        values[-3] = commit_at.astimezone(timezone.utc).isoformat()
        values[-2] = "true" if values[-2] else "false"
        writer.writerow([NULL if value is None else value for value in values])
    return sink.getvalue().encode("utf-8")
