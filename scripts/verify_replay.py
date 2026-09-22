"""Redeliver a real CDC file under another name, then retry the whole batch."""
import json
import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env.cloud", override=True)
from synthea_cdc.cloud_load import aws_session, load_pending, warehouse_connection
from synthea_cdc.seed import TABLES


def counts():
    connection = warehouse_connection()
    try:
        cursor = connection.cursor()
        result = {}
        for table in TABLES:
            cursor.execute(f'SELECT count(*), count(distinct _event_id) FROM "raw".{table}')
            result[table] = list(cursor.fetchone())
        connection.commit()
        return result
    finally:
        connection.close()


before = counts()
s3 = aws_session().client("s3")
bucket = os.environ["S3_BUCKET"]
prefix = os.environ.get("CAPTURE_PREFIX", "capture-v1") + "/cdc/"
originals = sorted(x["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
                   for x in page.get("Contents", []) if x["Key"].split("/")[-1].startswith("CDC_TXN-"))
if not originals:
    raise RuntimeError("Capture a real CDC file before replay testing")
original = originals[0]
replayed = prefix + "replay-" + original.split("/")[-1]
s3.copy_object(Bucket=bucket, Key=replayed, CopySource={"Bucket": bucket, "Key": original}, ServerSideEncryption="AES256")
load_pending()
after_redelivery = counts()
load_pending()
after_retry = counts()
assert before == after_redelivery == after_retry
assert all(total == unique for total, unique in before.values())
result = {"before": before, "after_redelivery": after_redelivery, "after_retry": after_retry,
          "replayed_key": replayed}
(ROOT / "data/cloud-replay.json").write_text(json.dumps(result, indent=2))
print("Replay and whole-batch retry preserved all raw event counts and identities:", json.dumps(before))
