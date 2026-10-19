"""Parse the documented DMS CSV format into a small, explicit event contract."""
import csv
from dataclasses import dataclass
from hashlib import sha256
import io

from olist_cdc.schema import DMS_METADATA, SOURCE_COLUMNS, source_columns

# DMS transformations append these columns after the original source columns.
# Olist source/metadata ordering was verified against real DMS files; see docs/testing.md.
NULL = "__OLIST_NULL__"


@dataclass
class Event:
    table: str
    values: list


def parse_csv(text, source_file, *, snapshot_table=None):
    """A file is parsed completely before any warehouse write is attempted."""
    events = []
    for line, row in enumerate(csv.reader(io.StringIO(text, newline="")), 1):
        if not row:
            continue
        if snapshot_table is None:
            if len(row) < 3:
                raise ValueError(f"{source_file}:{line}: missing CDC operation/table/schema")
            operation, table, schema, *fields = row
            # Retained captures use the source name; the target mapping aliases it.
            if schema not in ("ecommerce", "initial-load"):
                raise ValueError(f"Unexpected schema {schema}")
        else:
            operation, *fields = row
            table = snapshot_table
        if table not in SOURCE_COLUMNS or operation not in ("I", "U", "D"):
            raise ValueError(f"{source_file}:{line}: unsupported table or operation")
        expected = len(source_columns(table)) + len(DMS_METADATA)
        # Retained captures have an extra _source_lsn before the two metadata
        # columns. Ignore it so historical replay produces identical events.
        if len(fields) not in (expected, expected + 1):
            raise ValueError(f"{source_file}:{line}: expected {expected} fields (or {expected + 1} for legacy captures), received {len(fields)}; check DMS column order")
        values = [None if value == NULL else value for value in fields[:len(source_columns(table))]]
        sequence, commit_at = fields[-2:]
        snapshot = snapshot_table is not None
        if snapshot:
            if operation != "I":
                raise ValueError("Snapshot files must contain only inserts")
            sequence = "0"
            if values[0]:
                event_id = f"snapshot:{table}:{values[0]}"
            else:
                # No source key is available. Preserve each bad row for staging
                # validation, using a stable file/line identity instead of merging
                # unrelated rows under a shared missing-key identity.
                location = sha256(f"{source_file}:{line}".encode()).hexdigest()
                event_id = f"snapshot-missing-key:{table}:{location}"
        else:
            if not sequence.isdigit() or len(sequence) > 35 or int(sequence) == 0:
                raise ValueError(f"{source_file}:{line}: CDC requires a positive change sequence")
            event_id = f"cdc:{table}:{sequence}"
        if not commit_at or commit_at == NULL:
            raise ValueError(f"{source_file}:{line}: missing commit timestamp")
        events.append(Event(table, values + [event_id, operation,
                      int(sequence), commit_at, snapshot, source_file]))
    return events
