"""Load immutable DMS S3 files through Redshift COPY, with an atomic file ledger."""
import json
import logging
import os

import boto3
import redshift_connector
from psycopg import sql

from olist_cdc.events import NULL, parse_csv
from olist_cdc.copy_csv import normalized_csv
from olist_cdc.schema import SOURCE_COLUMNS, source_columns
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


def validate_raw_schema(cursor):
    """Refuse to reuse the earlier typed raw schema with this string-based loader."""
    cursor.execute("SELECT table_name,column_name,data_type FROM information_schema.columns "
                   "WHERE table_schema='raw'")
    types = {(table, column): kind for table, column, kind in cursor.fetchall()}
    if any(types.get((table, column)) != 'character varying'
           for table in SOURCE_COLUMNS for column in source_columns(table)):
        raise RuntimeError("Raw source columns must be VARCHAR. Rebuild a fresh raw baseline "
                           "from retained captures; see docs/warehouse.md#upgrade-to-string-based-raw")


def snapshot_table(key, prefix):
    parts = key.removeprefix(prefix.rstrip("/") + "/").split("/")
    if len(parts) == 3 and parts[0] == "initial-load" and parts[1] in SOURCE_COLUMNS and parts[2].startswith("LOAD"):
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
    failed = [t["TableName"] for t in tables
              if t.get("TableState") in ("Table error", "Table cancelled")]
    if failed:
        raise RuntimeError(f"Capture tables failed: {sorted(failed)}")
    if task.get("MigrationType") == "cdc":
        # CDC-only restore runs reuse an independently reconciled warehouse baseline.
        # Such a task does not execute a full load or report its completion states.
        log.info("CDC-only capture running; no full load is performed by this task")
        return
    completed = {t["TableName"] for t in tables if t["SchemaName"] == "ecommerce" and t["TableState"] == "Table completed"}
    if completed != set(SOURCE_COLUMNS):
        raise RuntimeError(f"Initial load is not complete: {sorted(completed)}")
    log.info("Capture running; initial load complete for all four source tables")


def load_file(connection, s3, bucket, key, role, *, metrics=None):
    source = f"s3://{bucket}/{key}"
    cursor = connection.cursor()
    try:
        cursor.execute('LOCK TABLE "raw".loaded_files')
        cursor.execute('SELECT 1 FROM "raw".loaded_files WHERE source_file=%s', (source,))
        previous = cursor.fetchone()
        if previous:
            connection.commit()
            if metrics is not None:
                metrics["files_already_loaded"] = metrics.get("files_already_loaded", 0) + 1
            return 0
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        snapshot = snapshot_table(key, os.environ.get("CAPTURE_PREFIX", "raw"))
        events = parse_csv(body.decode(), source, snapshot_table=snapshot)
        for table in SOURCE_COLUMNS:
            batch = [e for e in events if e.table == table]
            if not batch:
                continue
            # COPY input is derived; the original DMS file remains untouched.
            relative_key = key.removeprefix(os.environ.get("CAPTURE_PREFIX", "raw").rstrip("/") + "/")
            if snapshot:
                staging_key = f"copy-ready/{relative_key.removesuffix('.csv')}.csv"
            else:
                staging_key = f"copy-ready/{relative_key.removesuffix('.csv')}/{table}.csv"
            s3.put_object(Bucket=bucket, Key=staging_key, Body=normalized_csv(batch, table), ServerSideEncryption="AES256")
            copy_sql = sql.SQL("COPY {} FROM {} IAM_ROLE {} FORMAT AS CSV NULL AS {} TIMEFORMAT 'auto'").format(
                sql.Identifier("raw", table), sql.Literal(f"s3://{bucket}/{staging_key}"),
                sql.Literal(role), sql.Literal(NULL)).as_string()
            cursor.execute(copy_sql)
        cursor.execute('INSERT INTO "raw".loaded_files (source_file,row_count) VALUES (%s,%s)', (source, len(events)))
        connection.commit()
        if metrics is not None:
            metrics['files_committed'] = metrics.get('files_committed', 0) + 1
            for event in events:
                name = 'snapshot_rows' if event.values[-2] else 'input_' + event.values[-5]
                metrics[name] = metrics.get(name, 0) + 1
        log.info("Processed %s: %s delivered events (staging deduplicates event identities)", key, len(events))
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
    prefix = os.environ.get("CAPTURE_PREFIX", "raw").rstrip("/") + "/"
    keys = sorted(obj["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
                  for obj in page.get("Contents", []) if obj["Key"].endswith(".csv"))
    if not keys:
        raise RuntimeError("No DMS files found; check capture before loading")
    connection = warehouse_connection()
    try:
        cursor = connection.cursor()
        for statement in raw_ddl():
            cursor.execute(statement)
        validate_raw_schema(cursor)
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
        for table in ("dim_customers", "dim_customer_history", "fct_orders", "fct_order_items", "fct_order_payments"):
            cursor.execute(f"SELECT count(*) FROM marts.{table}")
            counts[table] = cursor.fetchone()[0]
        log.info("Populated marts: %s", json.dumps(counts))
        connection.commit()
        return counts
    finally:
        connection.close()
