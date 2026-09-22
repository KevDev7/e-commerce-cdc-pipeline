"""Inspect or control the one project DMS task, without restarting it per batch."""
import argparse
import json
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env.cloud", override=True)
import os
from synthea_cdc.cloud_load import aws_session

parser = argparse.ArgumentParser()
parser.add_argument("command", choices=["status", "start", "stop", "resume"])
args = parser.parse_args()
client = aws_session().client("dms")
arn = os.environ["DMS_TASK_ARN"]
if args.command == "status":
    task = client.describe_replication_tasks(Filters=[{"Name": "replication-task-arn", "Values": [arn]}])["ReplicationTasks"][0]
    print(json.dumps({"status": task["Status"], "failure": task.get("LastFailureMessage"),
                      "statistics": task.get("ReplicationTaskStats"),
                      "tables": client.describe_table_statistics(ReplicationTaskArn=arn)["TableStatistics"]}, default=str, indent=2))
elif args.command == "stop":
    client.stop_replication_task(ReplicationTaskArn=arn)
    print("Capture stopping")
else:
    client.start_replication_task(ReplicationTaskArn=arn, StartReplicationTaskType="start-replication" if args.command == "start" else "resume-processing")
    print("Capture", args.command, "requested")
