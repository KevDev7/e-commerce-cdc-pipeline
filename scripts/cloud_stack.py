"""Explicit, non-blocking lifecycle commands for the project's temporary AWS stack."""
import argparse
from datetime import datetime, timezone
import importlib.util
import ipaddress
import json
import os
from pathlib import Path
import secrets
import urllib.request

import boto3
from botocore.exceptions import ClientError
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / "data/cloud-state.json"
ENV = ROOT / ".env.cloud"
STACK = "synthea-cdc-demo"
SESSION = boto3.Session(profile_name="synthea-cdc", region_name="us-east-1")
CF = SESSION.client("cloudformation")


def template():
    spec = importlib.util.spec_from_file_location("project_template", ROOT / "infra/template.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return json.dumps(module.template())


def write_env(values):
    # Open with restrictive permissions before writing any secret.
    fd = os.open(ENV, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.chmod(ENV, 0o600)
    with os.fdopen(fd, "w") as stream:
        stream.write("".join(f"{key}={value}\n" for key, value in values.items()))


def stack():
    state = json.loads(STATE.read_text())
    result = CF.describe_stacks(StackName=state["stack_id"])["Stacks"][0]
    if {t["Key"]: t["Value"] for t in result["Tags"]}.get("Project") != "synthea-cdc":
        raise RuntimeError("Stack ownership check failed")
    return result


def outputs():
    return {x["OutputKey"]: x["OutputValue"] for x in stack().get("Outputs", [])}


def create(allowance_usd):
    if STATE.exists():
        raise RuntimeError("A deployment record already exists; inspect it before another paid run")
    account = SESSION.client("sts").get_caller_identity()["Account"]
    ec2 = SESSION.client("ec2")
    vpc = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])["Vpcs"][0]["VpcId"]
    subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc]}])["Subnets"]
    selected = sorted([s for s in subnets if s["AvailabilityZone"] in ("us-east-1a", "us-east-1b", "us-east-1c")], key=lambda s: s["AvailabilityZone"])
    if len(selected) != 3:
        raise RuntimeError("Expected three default public subnets; inspect network before deployment")
    ip = str(ipaddress.IPv4Address(urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10).read().decode().strip()))
    env = {"AWS_PROFILE": "synthea-cdc", "AWS_DEFAULT_REGION": "us-east-1", "CAPTURE_PREFIX": "raw",
           "POSTGRES_DB": "olist", "POSTGRES_PORT": "5432", "POSTGRES_USER": "cdc_owner", "POSTGRES_SSLMODE": "require",
           "POSTGRES_PASSWORD": "Aa1" + secrets.token_hex(16), "DMS_PASSWORD": "Bb2" + secrets.token_hex(16),
           "REDSHIFT_USER": "warehouse_owner", "REDSHIFT_DATABASE": "olist", "REDSHIFT_PASSWORD": "Cc3" + secrets.token_hex(16)}
    write_env(env)
    parameters = {"DatasetBucketName": "olist-cdc-" + secrets.token_hex(6), "Vpc": vpc, "Subnets": ",".join(s["SubnetId"] for s in selected), "ClientCidr": ip + "/32", "EnableWarehouse": "false",
                  "SourcePassword": env["POSTGRES_PASSWORD"], "DmsPassword": env["DMS_PASSWORD"], "WarehousePassword": env["REDSHIFT_PASSWORD"]}
    iam = SESSION.client("iam")
    for key, name in (("DmsVpcRole", "dms-vpc-role"), ("DmsLogRole", "dms-cloudwatch-logs-role")):
        try:
            iam.get_role(RoleName=name)
            parameters["Create" + key] = "false"
        except iam.exceptions.NoSuchEntityException:
            parameters["Create" + key] = "true"
    body = template()
    CF.validate_template(TemplateBody=body)
    response = CF.create_stack(StackName=STACK, TemplateBody=body,
        Parameters=[{"ParameterKey": k, "ParameterValue": v} for k, v in parameters.items()],
        Capabilities=["CAPABILITY_NAMED_IAM"], Tags=[{"Key": "Project", "Value": "synthea-cdc"}],
        OnFailure="DELETE", TimeoutInMinutes=40)
    STATE.parent.mkdir(exist_ok=True)
    STATE.write_text(json.dumps({"stack_id": response["StackId"], "account": account,
                                 "created_at": datetime.now(timezone.utc).isoformat(), "allowance_usd": allowance_usd}, indent=2))
    print("Creation started. Use status; no capture or Redshift queries start automatically.")


def sync_env():
    result = outputs()
    if not {"SourceHost", "TaskArn", "S3Bucket", "CopyRoleArn"}.issubset(result):
        raise RuntimeError("Stack outputs are not ready; wait for CREATE_COMPLETE or UPDATE_COMPLETE")
    values = dict(dotenv_values(ENV))
    for key, output in {"POSTGRES_HOST": "SourceHost", "DMS_TASK_ARN": "TaskArn", "S3_BUCKET": "S3Bucket",
                        "REDSHIFT_COPY_ROLE": "CopyRoleArn", "REDSHIFT_HOST": "WarehouseHost"}.items():
        if output in result:
            values[key] = result[output]
    write_env(values)
    print("Updated .env.cloud (secret values are not displayed)")


def enable_warehouse():
    if json.loads(STATE.read_text()).get("mode") == "restored":
        raise RuntimeError("Restored stacks already define warehouse compute; do not replace their template")
    current = stack()
    parameters = [{"ParameterKey": p["ParameterKey"], **({"ParameterValue": "true"} if p["ParameterKey"] == "EnableWarehouse" else {"UsePreviousValue": True})} for p in current["Parameters"]]
    CF.update_stack(StackName=current["StackId"], TemplateBody=template(), Parameters=parameters, Capabilities=["CAPABILITY_NAMED_IAM"])
    print("Stack update started with warehouse enabled; set usage-limit before running queries")


def usage_limit():
    rs = SESSION.client("redshift-serverless")
    workgroup = rs.get_workgroup(workgroupName="synthea-cdc")["workgroup"]
    if workgroup["baseCapacity"] != 4 or workgroup["maxCapacity"] != 4:
        raise RuntimeError("Unexpected Redshift capacity; inspect before running queries")
    arn = workgroup["workgroupArn"]
    existing = rs.list_usage_limits(resourceArn=arn)["usageLimits"]
    if not existing:
        rs.create_usage_limit(resourceArn=arn, usageType="serverless-compute", amount=6,
                              period="monthly", breachAction="deactivate")
    print("Redshift limited to 4 RPUs and 6 RPU-hours/month ($2.25 compute at the checked rate). Other charges are additional.")


def status():
    current = stack()
    print(current["StackStatus"])
    events = CF.describe_stack_events(StackName=current["StackId"])["StackEvents"][:12]
    for event in reversed(events):
        print(event["LogicalResourceId"], event["ResourceStatus"], event.get("ResourceStatusReason", ""))


def connections():
    result = outputs()
    dms = SESSION.client("dms")
    existing = {c["EndpointArn"]: c for c in dms.describe_connections(
        Filters=[{"Name": "replication-instance-arn", "Values": [result["ReplicationArn"]]}])["Connections"]}
    for key, label in (("SourceEndpointArn", "PostgreSQL"), ("TargetEndpointArn", "S3")):
        connection = existing.get(result[key], {})
        state = connection.get("Status")
        if state not in ("successful", "testing"):
            dms.test_connection(ReplicationInstanceArn=result["ReplicationArn"], EndpointArn=result[key])
            state = "testing"
        print(label, state, connection.get("LastFailureMessage", ""))


def delete():
    current = stack()
    result = outputs()
    dms = SESSION.client("dms")
    if "TaskArn" in result:
        task = dms.describe_replication_tasks(Filters=[{"Name": "replication-task-arn", "Values": [result["TaskArn"]]}])["ReplicationTasks"]
        if task and task[0]["Status"] == "running":
            dms.stop_replication_task(ReplicationTaskArn=result["TaskArn"])
            print("Stopping capture; rerun delete after task is stopped")
            return
        if task and task[0]["Status"] in ("starting", "stopping"):
            raise RuntimeError("Wait until capture stops before deleting")
        if task:
            state = json.loads(STATE.read_text())
            state["log_group"] = json.loads(task[0]["ReplicationTaskSettings"])["Logging"].get("CloudWatchLogGroup")
            STATE.write_text(json.dumps(state, indent=2))
    if "S3Bucket" in result:
        deployed = CF.get_template(StackName=current["StackId"])["TemplateBody"]
        if isinstance(deployed, str):
            deployed = json.loads(deployed)
        state = json.loads(STATE.read_text())
        external = deployed.get("Metadata", {}).get("RetainedData", {})
        for logical_id, policy in (("Bucket", "Retain"), ("Source", "Snapshot"), ("Namespace", "Retain"), ("CopyRole", "Retain")):
            resource = deployed["Resources"].get(logical_id)
            if resource is None and state.get("mode") == "restored" and logical_id != "Source":
                field = {"Bucket": "bucket", "Namespace": "namespace", "CopyRole": "copy_role"}[logical_id]
                expected = state.get("retained_" + field)
                if expected and external.get(field) == expected:
                    continue  # The restore stack does not own or delete this retained resource.
            if resource is None or resource.get("DeletionPolicy") != policy:
                raise RuntimeError(f"Update {logical_id} to DeletionPolicy={policy} before teardown; retained data must survive")
        bucket = result["S3Bucket"]
        state = json.loads(STATE.read_text())
        state["retained_bucket"] = bucket
        state["retained_region"] = "us-east-1"
        state["capture_prefix"] = "raw"
        state["retained_copy_role"] = result.get("CopyRoleArn")
        state["retained_namespace"] = "synthea-cdc" if "WarehouseHost" in result else None
        state["source_snapshot_instance"] = "synthea-cdc-source"
        STATE.write_text(json.dumps(state, indent=2))
        print(f"Retaining s3://{bucket}/ (source archive, original captures and derived Parquet)")
    CF.delete_stack(StackName=current["StackId"])
    print("Compute deletion started; retaining S3, the Redshift namespace and an RDS snapshot. Verify DELETE_COMPLETE and the snapshot before declaring retention complete.")



def cleanup_logs():
    if stack()["StackStatus"] != "DELETE_COMPLETE":
        raise RuntimeError("Finish stack deletion before removing its DMS logs")
    group = json.loads(STATE.read_text()).get("log_group")
    if group and group.startswith("dms-tasks-synthea-cdc-"):
        logs = SESSION.client("logs")
        try:
            logs.delete_log_group(logGroupName=group)
        except logs.exceptions.ResourceNotFoundException:
            pass
    print("Project DMS log cleanup complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["validate", "create", "status", "env", "warehouse", "usage-limit", "connections", "delete", "cleanup-logs"])
    parser.add_argument("--allowance-usd", type=float, help="User-approved allowance for this one-time run (create only)")
    args = parser.parse_args()
    if args.command == "create" and (args.allowance_usd is None or args.allowance_usd <= 0):
        parser.error("create requires a positive --allowance-usd agreed with the user")
    if args.command == "validate":
        CF.validate_template(TemplateBody=template())
        print("CloudFormation template valid")
    else:
        {"create": lambda: create(args.allowance_usd), "status": status, "env": sync_env, "warehouse": enable_warehouse,
         "usage-limit": usage_limit, "connections": connections, "delete": delete, "cleanup-logs": cleanup_logs}[args.command]()
