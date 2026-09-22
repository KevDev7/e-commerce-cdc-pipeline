"""Compare every current source field with Redshift after a quiescent demo batch."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env.cloud", override=True)
from olist_cdc.cloud_load import warehouse_connection
from olist_cdc.db import connect
from olist_cdc.events import source_columns
from olist_cdc.seed import TABLES


def canonical(value):
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc).isoformat()
    return None if value is None else str(value)


def fingerprint(rows):
    digest = hashlib.sha256()
    count = 0
    for row in rows:
        digest.update(json.dumps([canonical(value) for value in row], separators=(",", ":")).encode() + b"\n")
        count += 1
    return {"count": count, "sha256": digest.hexdigest()}


with connect() as source, warehouse_connection() as target:
    source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
    result = {}
    for table in TABLES:
        columns = ",".join(source_columns(table))
        source_rows = source.execute(f"SELECT {columns} FROM ecommerce.{table} ORDER BY 1")
        cursor = target.cursor()
        cursor.execute(f"SELECT {columns} FROM analytics_intermediate.int_{table}_current ORDER BY 1")
        actual, expected = fingerprint(cursor), fingerprint(source_rows)
        if actual != expected:
            raise AssertionError(f"{table} does not reconcile: source={expected}, warehouse={actual}")
        result[table] = actual
    target.commit()
    path = ROOT / "data/cloud-reconciliation.json"
    path.write_text(json.dumps({"verified_at": datetime.now(timezone.utc).isoformat(), "tables": result}, indent=2))
    print(json.dumps(result, indent=2))
