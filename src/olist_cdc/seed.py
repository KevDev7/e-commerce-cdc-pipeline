import csv
import hashlib
import io
from pathlib import Path
import urllib.request
import zipfile

from psycopg import sql
from psycopg.types.json import Jsonb

URL = "https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce?datasetVersionNumber=2"
SHA256 = "967e41e04fc306fe604e2a693f488995a8b41e5047418f8a5c8e4abd6deca784"
# Derived row keys are first in source column order; all natural-key parts remain.
TABLES = {
    "customers": {"customer_id": "customer_id", "customer_unique_id": "customer_unique_id",
                  "postal_code": "customer_zip_code_prefix", "city": "customer_city", "state": "customer_state"},
    "orders": {"order_id": "order_id", "customer_id": "customer_id", "status": "order_status",
               "purchased_at": "order_purchase_timestamp", "approved_at": "order_approved_at",
               "carrier_delivered_at": "order_delivered_carrier_date", "customer_delivered_at": "order_delivered_customer_date",
               "estimated_delivery_at": "order_estimated_delivery_date"},
    "order_items": {"order_item_key": None, "order_id": "order_id", "order_item_id": "order_item_id",
                    "product_id": "product_id", "seller_id": "seller_id", "shipping_limit_at": "shipping_limit_date",
                    "price": "price", "freight_value": "freight_value"},
    "order_payments": {"payment_key": None, "order_id": "order_id", "payment_sequential": "payment_sequential",
                       "payment_type": "payment_type", "payment_installments": "payment_installments", "payment_value": "payment_value"},
}


def mapped_values(table, row):
    values = [row[source] or None for source in TABLES[table].values() if source]
    if table in ("order_items", "order_payments"):
        part = "order_item_id" if table == "order_items" else "payment_sequential"
        values.insert(0, row["order_id"] + ":" + str(int(row[part])))
    return values


def download(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        temporary = path.with_suffix(".partial")
        try:
            with urllib.request.urlopen(URL, timeout=60) as response, temporary.open("wb") as output:
                while chunk := response.read(1024 * 1024):
                    output.write(chunk)
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != SHA256:
        raise ValueError(f"Sample checksum changed ({actual}); inspect the new sample before updating the pin.")


def load(connection, archive: Path):
    archive_hash = hashlib.sha256(archive.read_bytes()).hexdigest()
    if archive_hash != SHA256:
        raise ValueError("Only the inspected, pinned Olist archive is accepted by this seed loader.")
    with connection.transaction():
        connection.execute("SET LOCAL TIME ZONE 'UTC'")
        connection.execute("SELECT pg_advisory_xact_lock(8174201)")
        previous = connection.execute(
            "SELECT row_counts FROM project_meta.seed_runs WHERE archive_sha256=%s", (archive_hash,)
        ).fetchone()
        if previous:
            return {"status": "already_loaded", "row_counts": previous[0]}
        for table in TABLES:
            count = connection.execute(sql.SQL("SELECT count(*) FROM ecommerce.{}").format(sql.Identifier(table))).fetchone()[0]
            if count:
                raise ValueError("Source tables are not empty. Seed once into a fresh database; no records were changed.")
        counts = {}
        with zipfile.ZipFile(archive) as zipped:
            for table, columns in TABLES.items():
                filename = "olist_" + table + "_dataset.csv"
                matches = [name for name in zipped.namelist() if Path(name).name == filename]
                if len(matches) != 1:
                    raise ValueError(f"Expected exactly one {filename}")
                query = sql.SQL("COPY ecommerce.{} ({}) FROM STDIN").format(
                    sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, columns))
                )
                counts[table] = 0
                with zipped.open(matches[0]) as raw, io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as stream:
                    reader = csv.DictReader(stream)
                    missing = {v for v in columns.values() if v} - set(reader.fieldnames or [])
                    if missing:
                        raise ValueError(f"{filename} missing columns: {sorted(missing)}")
                    with connection.cursor().copy(query) as copy:
                        for row in reader:
                            values = mapped_values(table, row)
                            copy.write_row(values)
                            counts[table] += 1
        connection.execute("INSERT INTO project_meta.seed_runs (archive_sha256,row_counts) VALUES (%s,%s)",
                           (archive_hash, Jsonb(counts)))
    return {"status": "loaded", "row_counts": counts}
