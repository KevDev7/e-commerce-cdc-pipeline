"""Restore temporary compute around the retained baseline, without seeding again."""
import argparse
from datetime import datetime, timezone
import ipaddress
import json
import secrets
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import dotenv_values
from infra.restore import restore_template
from scripts import cloud_stack as cloud


def create(allowance):
    if allowance <= 0:
        raise ValueError('A positive approved allowance is required')
    previous = json.loads(cloud.STATE.read_text())
    if cloud.stack()['StackStatus'] != 'DELETE_COMPLETE':
        raise RuntimeError('Previous stack must be completely deleted first')
    session = cloud.SESSION
    snapshot = session.client('rds').describe_db_snapshots(
        DBSnapshotIdentifier=previous['retained_source_snapshot'])['DBSnapshots'][0]
    if snapshot['Status'] != 'available':
        raise RuntimeError('Retained snapshot is not available')
    session.client('redshift-serverless').get_namespace(namespaceName=previous['retained_namespace'])
    session.client('s3').head_bucket(Bucket=previous['retained_bucket'])
    ec2 = session.client('ec2')
    vpc = ec2.describe_vpcs(Filters=[{'Name':'is-default','Values':['true']}])['Vpcs'][0]['VpcId']
    subnets = ec2.describe_subnets(Filters=[{'Name':'vpc-id','Values':[vpc]}])['Subnets']
    chosen = sorted([x for x in subnets if x['AvailabilityZone'] in ('us-east-1a','us-east-1b','us-east-1c')], key=lambda x:x['AvailabilityZone'])
    if len(chosen) != 3:
        raise RuntimeError('Expected three public default subnets')
    ip = str(ipaddress.IPv4Address(urllib.request.urlopen('https://checkip.amazonaws.com',timeout=10).read().decode().strip()))
    env = dict(dotenv_values(cloud.ENV))
    env.update(POSTGRES_DB='olist',POSTGRES_PORT='5432',POSTGRES_USER='cdc_owner',POSTGRES_SSLMODE='require',
               POSTGRES_PASSWORD='Aa1'+secrets.token_hex(16),DMS_PASSWORD='Bb2'+secrets.token_hex(16))
    parameters = dict(Vpc=vpc,Subnets=','.join(x['SubnetId'] for x in chosen),ClientCidr=ip+'/32',
                      EnableWarehouse='true',DmsPassword=env['DMS_PASSWORD'])
    iam=session.client('iam')
    for key,name in [('DmsVpcRole','dms-vpc-role'),('DmsLogRole','dms-cloudwatch-logs-role')]:
        try:
            iam.get_role(RoleName=name);parameters['Create'+key]='false'
        except iam.exceptions.NoSuchEntityException:
            parameters['Create'+key]='true'
    body=json.dumps(restore_template(previous))
    cloud.CF.validate_template(TemplateBody=body)
    archive=cloud.ROOT/'data'/'retained-before-five-cycles.json'
    if archive.exists():
        raise RuntimeError('Restore archive exists; inspect before starting another run')
    archive.write_text(json.dumps(previous,indent=2)+'\n')
    cloud.write_env(env)
    result=cloud.CF.create_stack(StackName=cloud.STACK,TemplateBody=body,
        Parameters=[{'ParameterKey':k,'ParameterValue':v} for k,v in parameters.items()],
        Capabilities=['CAPABILITY_NAMED_IAM'],Tags=[{'Key':'Project','Value':'synthea-cdc'}],
        OnFailure='DELETE',TimeoutInMinutes=60)
    state={k:v for k,v in previous.items() if k.startswith('retained_')}
    state.update(stack_id=result['StackId'],created_at=datetime.now(timezone.utc).isoformat(),
                 allowance_usd=allowance,mode='restored',account=previous['account'])
    cloud.STATE.write_text(json.dumps(state,indent=2)+'\n')
    print('Restore started; no full load or business writes have started')


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('command',choices=['create','reset-password'])
    p.add_argument('--allowance-usd',type=float)
    a=p.parse_args()
    if a.command=='create':
        if a.allowance_usd is None:p.error('An approved --allowance-usd is required')
        create(a.allowance_usd)
    else:
        if json.loads(cloud.STATE.read_text()).get('mode')!='restored':
            raise RuntimeError('This command is only for the restored project source')
        cloud.SESSION.client('rds').modify_db_instance(DBInstanceIdentifier='synthea-cdc-source',
            MasterUserPassword=dotenv_values(cloud.ENV)['POSTGRES_PASSWORD'],ApplyImmediately=True)
        print('Restored source password reset requested; secret not displayed')
