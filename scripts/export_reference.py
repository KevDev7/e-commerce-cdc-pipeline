"""Preserve real source rows in S3 and a small ignored local reference file."""
import argparse
import boto3
from olist_cdc.reference import ROOT, preserve_live, recover_snapshot, save_reference

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--bucket', required=True)
parser.add_argument('--region', default='us-east-1')
source = parser.add_mutually_exclusive_group(required=True)
source.add_argument('--host', help='Live RDS host; credentials come from .env.cloud')
source.add_argument('--recover-prefix', help='Recover business samples from retained DMS snapshots')
args = parser.parse_args()
session = boto3.Session(profile_name='synthea-cdc', region_name=args.region)
if args.host:
    uri = preserve_live(session, args.bucket, args.host)
else:
    s3 = session.client('s3')
    uri = save_reference(s3, args.bucket, recover_snapshot(s3, args.bucket, args.recover_prefix), ROOT / 'data/reference')
print(uri)
print(ROOT / 'data/reference/source-reference.json')
