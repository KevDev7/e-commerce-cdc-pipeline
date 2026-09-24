"""Parse the documented DMS CSV format into a small, explicit event contract."""
import csv
from dataclasses import dataclass
import io

from olist_cdc.seed import TABLES

# DMS transformations append these columns after the original source columns.
# Olist source/metadata ordering was verified against real DMS files; see docs/validation.md.
DMS_METADATA = ("_source_lsn", "_source_order", "_commit_at")
RAW_METADATA = ("_event_id", "_op", "_source_lsn", "_source_order", "_commit_at", "_is_snapshot", "_source_file")
NULL = "__OLIST_NULL__"


def source_columns(table):
    return [*TABLES[table], "updated_at"]


@dataclass
class Event:
    table: str
    values: list


def parse_csv(text, source_file, *, snapshot_table=None):
    """A file is parsed completely before any warehouse write is attempted."""
    events = []
    seen = set()
    for line, row in enumerate(csv.reader(io.StringIO(text, newline="")), 1):
        if not row:
            continue
        if snapshot_table is None:
            if len(row) < 3:
                raise ValueError(f"{source_file}:{line}: missing CDC operation/table/schema")
            operation, table, schema, *fields = row
            if schema != "ecommerce":
                raise ValueError(f"Unexpected schema {schema}")
        else:
            operation, *fields = row
            table = snapshot_table
        if table not in TABLES or operation not in ("I", "U", "D"):
            raise ValueError(f"{source_file}:{line}: unsupported table or operation")
        expected = len(source_columns(table)) + len(DMS_METADATA)
        if len(fields) != expected:
            raise ValueError(f"{source_file}:{line}: expected {expected} fields, received {len(fields)}; check DMS column order")
        values = [None if value == NULL else value for value in fields[:-3]]
        position, sequence, commit_at = fields[-3:]
        if not values[0]:
            raise ValueError(f"{source_file}:{line}: missing primary key")
        snapshot = snapshot_table is not None
        if snapshot:
            if operation != "I":
                raise ValueError("Snapshot files must contain only inserts")
            sequence = "0"
            event_id = f"snapshot:{table}:{values[0]}"
        else:
            if not sequence.isdigit() or len(sequence) > 35 or int(sequence) == 0 or not position:
                raise ValueError(f"{source_file}:{line}: CDC requires source position and a positive change sequence")
            event_id = f"cdc:{table}:{sequence}"
        if not commit_at or commit_at == NULL:
            raise ValueError(f"{source_file}:{line}: missing commit timestamp")
        if event_id in seen:
            raise ValueError(f"{source_file}:{line}: repeated event identity inside file")
        seen.add(event_id)
        events.append(Event(table, values + [event_id, operation, position or None,
                      int(sequence), commit_at, snapshot, source_file]))
    return events
