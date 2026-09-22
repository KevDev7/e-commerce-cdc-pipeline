"""Load immutable DMS S3 files through Redshift COPY, with an atomic file ledger."""
import hashlib
import json
import logging
import os

import boto3
import redshift_connector
from psycopg import sql

from olist_cdc.events import parse_csv
from olist_cdc.parquet import normalized_parquet
from olist_cdc.seed import TABLES
from olist_cdc.warehouse import raw_ddl

log = logging.getLogger(__name__)


def aws_session():
    return boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "synthea-cdc"),
                         region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))


def warehouse_connection():
    return redshift_connector.connect(
        host=os.environ["REDSHIFT_HOST"], database="olist",
        user=os.environ["REDSHIFT_USER"], password=os.environ["REDSHIFT_PASSWORD"],
        ssl=True, sslmode="verify-full", timeout=30,
    )


def snapshot_table(key, prefix):
    parts = key.removeprefix(prefix.rstrip("/") + "/").split("/")
    if len(parts) == 3 and parts[0] == "ecommerce" and parts[1] in TABLES and parts[2].startswith("LOAD"):
        return parts[1]
    if len(parts) == 2 and parts[0] == "cdc":
        return None
    raise ValueError(f"Unexpected capture path: {key}")


def check_capture():
    dms = aws_session().client("dms")
    arn = os.environ["DMS_TASK_ARN"]
    task = dms.describe_replication_tasks(Filters=[{"Name": "replication-task-arn", "Values": [arn]}])["ReplicationTasks"][0]
    if task["Status"] != "running":
        raise RuntimeError(f"Capture task is {task['Status']}: {task.get('LastFailureMessage', '')}")
    tables = dms.describe_table_statistics(ReplicationTaskArn=arn)["TableStatistics"]
    completed = {t["TableName"] for t in tables if t["SchemaName"] == "ecommerce" and t["TableState"] == "Table completed"}
    if completed != set(TABLES):
        raise RuntimeError(f"Initial load is not complete: {sorted(completed)}")
    log.info("Capture running; initial load complete for all four source tables")


def load_file(connection, s3, bucket, key, role, *, metrics=None):
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    digest = hashlib.sha256(body).hexdigest()
    source = f"s3://{bucket}/{key}"
    cursor = connection.cursor()
    try:
        cursor.execute('LOCK TABLE "raw".loaded_files')
        cursor.execute('SELECT content_sha256 FROM "raw".loaded_files WHERE source_file=%s', (source,))
        previous = cursor.fetchone()
        if previous:
            if previous[0] != digest:
                raise ValueError(f"Previously loaded file changed: {key}")
            connection.commit()
            if metrics is not None:
                metrics["files_already_loaded"] = metrics.get("files_already_loaded", 0) + 1
            return 0
        events = parse_csv(body.decode(), source, snapshot_table=snapshot_table(key, os.environ.get("CAPTURE_PREFIX", "olist-v1")))
        for table in TABLES:
            batch = [e for e in events if e.table == table]
            if not batch:
                continue
            # COPY input is derived; the original DMS file remains untouched.
            source_identity = hashlib.sha256(source.encode()).hexdigest()
            staging_key = f"copy-ready/parquet-v1/{source_identity}/{digest}/{table}.parquet"
            s3.put_object(Bucket=bucket, Key=staging_key, Body=normalized_parquet(batch, table), ServerSideEncryption="AES256")
            cursor.execute(f'CREATE TEMP TABLE incoming_{table} (LIKE "raw".{table})')
            copy_sql = sql.SQL("COPY {} FROM {} IAM_ROLE {} FORMAT AS PARQUET").format(
                sql.Identifier("incoming_" + table), sql.Literal(f"s3://{bucket}/{staging_key}"),
                sql.Literal(role)).as_string()
            cursor.execute(copy_sql)
            cursor.execute(f'INSERT INTO "raw".{table} SELECT i.* FROM incoming_{table} i WHERE NOT EXISTS '
                           f'(SELECT 1 FROM "raw".{table} r WHERE r._event_id=i._event_id)')
            cursor.execute(f"DROP TABLE incoming_{table}")
        cursor.execute('INSERT INTO "raw".loaded_files (source_file,content_sha256,row_count) VALUES (%s,%s,%s)', (source, digest, len(events)))
        connection.commit()
        if metrics is not None:
            metrics['files_committed'] = metrics.get('files_committed', 0) + 1
            for event in events:
                name = 'snapshot_rows' if event.values[-2] else 'input_' + event.values[-6]
                metrics[name] = metrics.get(name, 0) + 1
        log.info("Processed %s: %s incoming events (existing event identities are skipped)", key, len(events))
        return len(events)
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def load_pending(*, metrics=None):
    if metrics is None:
        metrics = {}
    session = aws_session()
    s3 = session.client("s3")
    bucket = os.environ["S3_BUCKET"]
    prefix = os.environ.get("CAPTURE_PREFIX", "olist-v1").rstrip("/") + "/"
    keys = sorted(obj["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
                  for obj in page.get("Contents", []) if obj["Key"].endswith(".csv"))
    if not keys:
        raise RuntimeError("No DMS files found; check capture before loading")
    connection = warehouse_connection()
    try:
        cursor = connection.cursor()
        for statement in raw_ddl():
            cursor.execute(statement)
        connection.commit()
        metrics["files_listed"] = len(keys)
        total = sum(load_file(connection, s3, bucket, key, os.environ["REDSHIFT_COPY_ROLE"], metrics=metrics) for key in keys)
        log.info("Inspected %s files; processed %s incoming events from previously unseen files", len(keys), total)
    finally:
        connection.close()


def report():
    connection = warehouse_connection()
    try:
        cursor = connection.cursor()
        counts = {}
        for table in ("dim_customers", "dim_customer_history", "fct_orders", "fct_order_status_history", "fct_order_items", "fct_order_payments"):
            cursor.execute(f"SELECT count(*) FROM analytics_marts.{table}")
            counts[table] = cursor.fetchone()[0]
        log.info("Populated marts: %s", json.dumps(counts))
        connection.commit()
        return counts
    finally:
        connection.close()
