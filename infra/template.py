"""Small CloudFormation template; temporary compute and a retained private S3 dataset bucket."""
import json
from pathlib import Path

HERE = Path(__file__).parent
ref = lambda name: {"Ref": name}
att = lambda name, attribute: {"Fn::GetAtt": [name, attribute]}
sub = lambda value: {"Fn::Sub": value}
TAGS = [{"Key": "Project", "Value": "synthea-cdc"}]


def role(service, statements):
    return {"AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"}]},
        "Policies": [{"PolicyName": "project-access", "PolicyDocument": {"Version": "2012-10-17", "Statement": statements}}], "Tags": TAGS}


def template():
    resources = {}
    def add(name, kind, properties, **extra):
        resources[name] = {"Type": "AWS::" + kind, "Properties": properties, **extra}
    add("Bucket", "S3::Bucket", {
        "PublicAccessBlockConfiguration": {x: True for x in ("BlockPublicAcls", "BlockPublicPolicy", "IgnorePublicAcls", "RestrictPublicBuckets")},
        "BucketEncryption": {"ServerSideEncryptionConfiguration": [{"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]}, "Tags": TAGS},
        DeletionPolicy="Retain", UpdateReplacePolicy="Retain")
    add("DmsRole", "IAM::Role", role("dms.amazonaws.com", [
        {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"], "Resource": att("Bucket", "Arn")},
        {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:PutObjectTagging"], "Resource": sub("${Bucket.Arn}/olist-v1/*")},
    ]))
    add("CopyRole", "IAM::Role", role(["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"], [
        {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"], "Resource": att("Bucket", "Arn")},
        {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": sub("${Bucket.Arn}/copy-ready/*")},
    ]))
    for name, role_name, policy in (("DmsVpcRole", "dms-vpc-role", "AmazonDMSVPCManagementRole"),
                                    ("DmsLogRole", "dms-cloudwatch-logs-role", "AmazonDMSCloudWatchLogsRole")):
        properties = role("dms.amazonaws.com", [])
        properties.pop("Policies")
        properties.update(RoleName=role_name, ManagedPolicyArns=[f"arn:aws:iam::aws:policy/service-role/{policy}"])
        add(name, "IAM::Role", properties, Condition="Create" + name)
    add("DmsSecurityGroup", "EC2::SecurityGroup", {"GroupDescription": "Olist DMS outbound access", "VpcId": ref("Vpc"), "Tags": TAGS})
    add("SourceSecurityGroup", "EC2::SecurityGroup", {
        "GroupDescription": "Olist PostgreSQL: local developer and DMS only", "VpcId": ref("Vpc"), "Tags": TAGS,
        "SecurityGroupIngress": [{"IpProtocol": "tcp", "FromPort": 5432, "ToPort": 5432, "CidrIp": ref("ClientCidr")},
                                 {"IpProtocol": "tcp", "FromPort": 5432, "ToPort": 5432, "SourceSecurityGroupId": ref("DmsSecurityGroup")}],
    })
    add("WarehouseSecurityGroup", "EC2::SecurityGroup", {
        "GroupDescription": "Olist Redshift: local developer only", "VpcId": ref("Vpc"), "Tags": TAGS,
        "SecurityGroupIngress": [{"IpProtocol": "tcp", "FromPort": 5439, "ToPort": 5439, "CidrIp": ref("ClientCidr")}],
    }, Condition="WarehouseEnabled")
    add("SourceSubnets", "RDS::DBSubnetGroup", {"DBSubnetGroupDescription": "Olist demo", "SubnetIds": ref("Subnets"), "Tags": TAGS})
    add("DmsSubnets", "DMS::ReplicationSubnetGroup", {"ReplicationSubnetGroupDescription": "Olist demo", "SubnetIds": ref("Subnets"), "Tags": TAGS}, DependsOn="DmsVpcRole")
    add("SourceParameters", "RDS::DBParameterGroup", {"Description": "Olist WAL capture", "Family": "postgres17", "Parameters": {"rds.logical_replication": "1"}, "Tags": TAGS})
    add("Source", "RDS::DBInstance", {
        "DBInstanceIdentifier": "synthea-cdc-source", "DBInstanceClass": "db.t4g.micro", "Engine": "postgres", "EngineVersion": "17.11",
        "DBName": "olist", "MasterUsername": "cdc_owner", "MasterUserPassword": ref("SourcePassword"),
        "AllocatedStorage": "20", "StorageType": "gp3", "StorageEncrypted": True,
        "DBSubnetGroupName": ref("SourceSubnets"), "DBParameterGroupName": ref("SourceParameters"),
        "VPCSecurityGroups": [ref("SourceSecurityGroup")], "MultiAZ": False, "PubliclyAccessible": True,
        "BackupRetentionPeriod": 0, "DeleteAutomatedBackups": True, "DeletionProtection": False,
        "EnablePerformanceInsights": False, "Tags": TAGS,
    }, DeletionPolicy="Delete", UpdateReplacePolicy="Delete")
    add("Replication", "DMS::ReplicationInstance", {
        "ReplicationInstanceIdentifier": "synthea-cdc-replication", "ReplicationInstanceClass": "dms.t3.small",
        "EngineVersion": "3.6.1", "AllocatedStorage": 5, "MultiAZ": False, "PubliclyAccessible": True,
        "ReplicationSubnetGroupIdentifier": ref("DmsSubnets"), "VpcSecurityGroupIds": [ref("DmsSecurityGroup")], "Tags": TAGS,
    }, DependsOn=["DmsVpcRole", "DmsLogRole"])
    add("SourceEndpoint", "DMS::Endpoint", {
        "EndpointIdentifier": "synthea-cdc-source", "EndpointType": "source", "EngineName": "postgres",
        "ServerName": att("Source", "Endpoint.Address"), "Port": 5432, "DatabaseName": "olist",
        "Username": "dms_reader", "Password": ref("DmsPassword"), "SslMode": "require", "Tags": TAGS,
        "PostgreSqlSettings": {"PluginName": "test_decoding", "CaptureDdls": False},
    })
    settings = json.loads((HERE / "dms-s3-settings.example.json").read_text())
    settings.update(ServiceAccessRoleArn=att("DmsRole", "Arn"), BucketName=ref("Bucket"))
    add("TargetEndpoint", "DMS::Endpoint", {"EndpointIdentifier": "synthea-cdc-s3", "EndpointType": "target", "EngineName": "s3", "S3Settings": settings, "Tags": TAGS})
    add("Capture", "DMS::ReplicationTask", {
        "ReplicationTaskIdentifier": "synthea-cdc-capture", "MigrationType": "full-load-and-cdc",
        "ReplicationInstanceArn": ref("Replication"), "SourceEndpointArn": ref("SourceEndpoint"), "TargetEndpointArn": ref("TargetEndpoint"),
        "TableMappings": (HERE / "dms-table-mappings.json").read_text(), "Tags": TAGS,
        "ReplicationTaskSettings": json.dumps({
            "Logging": {"EnableLogging": True},
            "FullLoadSettings": {"MaxFullLoadSubTasks": 2, "CommitRate": 10000, "TargetTablePrepMode": "DO_NOTHING"},
            "TargetMetadata": {"SupportLobs": False},
            "ErrorBehavior": {"DataErrorPolicy": "STOP_TASK", "TableErrorPolicy": "STOP_TASK", "DataTruncationErrorPolicy": "STOP_TASK"},
        }),
    })
    add("Namespace", "RedshiftServerless::Namespace", {
        "NamespaceName": "synthea-cdc", "DbName": "olist", "AdminUsername": "warehouse_owner", "AdminUserPassword": ref("WarehousePassword"),
        "IamRoles": [att("CopyRole", "Arn")], "DefaultIamRoleArn": att("CopyRole", "Arn"), "Tags": TAGS,
    }, Condition="WarehouseEnabled", DeletionPolicy="Delete", UpdateReplacePolicy="Delete")
    add("Workgroup", "RedshiftServerless::Workgroup", {
        "WorkgroupName": "synthea-cdc", "NamespaceName": ref("Namespace"), "BaseCapacity": 4, "MaxCapacity": 4,
        "PricePerformanceTarget": {"Status": "DISABLED"},
        "PubliclyAccessible": True, "SubnetIds": ref("Subnets"), "SecurityGroupIds": [ref("WarehouseSecurityGroup")],
        "ConfigParameters": [{"ParameterKey": "require_ssl", "ParameterValue": "true"},
                             {"ParameterKey": "max_query_execution_time", "ParameterValue": "300"}], "Tags": TAGS,
    }, Condition="WarehouseEnabled")
    parameters = {"Vpc": {"Type": "AWS::EC2::VPC::Id"}, "Subnets": {"Type": "List<AWS::EC2::Subnet::Id>"},
                  "ClientCidr": {"Type": "String"}, "EnableWarehouse": {"Type": "String", "Default": "false", "AllowedValues": ["true", "false"]}}
    for key in ("SourcePassword", "DmsPassword", "WarehousePassword"):
        parameters[key] = {"Type": "String", "NoEcho": True, "MinLength": 16}
    conditions = {"WarehouseEnabled": {"Fn::Equals": [ref("EnableWarehouse"), "true"]}}
    for role_name in ("DmsVpcRole", "DmsLogRole"):
        parameters["Create" + role_name] = {"Type": "String", "AllowedValues": ["true", "false"]}
        conditions["Create" + role_name] = {"Fn::Equals": [ref("Create" + role_name), "true"]}
    outputs = {"S3Bucket": {"Value": ref("Bucket")}, "SourceHost": {"Value": att("Source", "Endpoint.Address")},
               "TaskArn": {"Value": ref("Capture")}, "ReplicationArn": {"Value": ref("Replication")},
               "SourceEndpointArn": {"Value": ref("SourceEndpoint")}, "TargetEndpointArn": {"Value": ref("TargetEndpoint")},
               "CopyRoleArn": {"Value": att("CopyRole", "Arn")},
               "WarehouseHost": {"Value": att("Workgroup", "Workgroup.Endpoint.Address"), "Condition": "WarehouseEnabled"}}
    return {"AWSTemplateFormatVersion": "2010-09-09", "Description": "Temporary Olist CDC portfolio test", "Parameters": parameters,
            "Conditions": conditions, "Resources": resources, "Outputs": outputs}


if __name__ == "__main__":
    print(json.dumps(template(), indent=2))
