"""Temporary compute for a reconciled retained Olist baseline; no new full load."""
from infra.template import template


def restore_template(retained):
    body = template()
    resources = body['Resources']
    bucket = retained['retained_bucket']
    namespace = retained['retained_namespace']
    copy_role = retained['retained_copy_role']
    for name in ('Bucket', 'Namespace', 'CopyRole'):
        del resources[name]
    source = resources['Source']['Properties']
    for name in ('DBName', 'MasterUsername', 'MasterUserPassword', 'StorageEncrypted'):
        source.pop(name, None)
    source['DBSnapshotIdentifier'] = retained['retained_source_snapshot']
    resources['Workgroup']['Properties']['NamespaceName'] = namespace
    resources['Capture']['Properties']['MigrationType'] = 'cdc'
    resources['TargetEndpoint']['Properties']['S3Settings']['BucketName'] = bucket
    statements = resources['DmsRole']['Properties']['Policies'][0]['PolicyDocument']['Statement']
    statements[0]['Resource'] = f'arn:aws:s3:::{bucket}'
    statements[1]['Resource'] = f'arn:aws:s3:::{bucket}/raw/*'
    body['Outputs']['S3Bucket']['Value'] = bucket
    body['Outputs']['CopyRoleArn']['Value'] = copy_role
    for name in ('DatasetBucketName', 'SourcePassword', 'WarehousePassword'):
        del body['Parameters'][name]
    body['Metadata'] = {'RetainedData': {
        'bucket': bucket, 'namespace': namespace, 'copy_role': copy_role,
        'source_snapshot': retained['retained_source_snapshot']}}
    body['Description'] = 'Temporary Olist compute restored from a retained baseline; CDC only'
    return body
