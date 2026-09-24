"""Teardown must retain cloud data without downloading it to the workstation."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from infra.template import template


def test_data_survives_while_compute_is_disposable():
    resources = template()['Resources']
    assert resources['Bucket']['DeletionPolicy'] == 'Retain'
    assert resources['Bucket']['UpdateReplacePolicy'] == 'Retain'
    assert resources['Source']['DeletionPolicy'] == 'Snapshot'
    assert resources['Source']['UpdateReplacePolicy'] == 'Snapshot'
    assert resources['Namespace']['DeletionPolicy'] == 'Retain'
    assert resources['CopyRole']['DeletionPolicy'] == 'Retain'
    assert resources['Workgroup'].get('DeletionPolicy', 'Delete') == 'Delete'
    assert resources['Replication'].get('DeletionPolicy', 'Delete') == 'Delete'


@pytest.mark.parametrize('missing_policy', [None, 'Bucket', 'Source', 'Namespace', 'CopyRole'])
def test_teardown_preserves_bucket_and_refuses_legacy_delete_policy(monkeypatch, tmp_path, missing_policy):
    calls = []
    body = template()
    if missing_policy:
        body['Resources'][missing_policy].pop('DeletionPolicy')
    cf = SimpleNamespace(get_template=lambda **kw: {'TemplateBody': body},
                         delete_stack=lambda **kw: calls.append(kw))
    # No S3 client is supplied: teardown must not download or delete objects.
    session = SimpleNamespace(client=lambda service: {'cloudformation': cf, 'dms': object()}[service])
    monkeypatch.setattr('boto3.Session', lambda **kw: session)
    spec = importlib.util.spec_from_file_location('cloud_stack_test', Path('scripts/cloud_stack.py'))
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    module.STATE = tmp_path/'state.json'; module.STATE.write_text('{}')
    module.stack = lambda: {'StackId': 'project-stack'}
    module.outputs = lambda: {'S3Bucket': 'project-bucket', 'SourceHost': 'source-host'}
    if missing_policy is None:
        module.delete()
        assert calls == [{'StackName': 'project-stack'}]
        assert json.loads(module.STATE.read_text())['retained_bucket'] == 'project-bucket'
    else:
        with pytest.raises(RuntimeError, match='DeletionPolicy'):
            module.delete()
        assert calls == []


def test_restore_reuses_cloud_data_without_full_load_or_owning_retained_resources():
    from infra.restore import restore_template
    body = restore_template({'retained_bucket': 'existing-bucket',
        'retained_namespace': 'existing-warehouse', 'retained_copy_role': 'existing-role',
        'retained_source_snapshot': 'existing-snapshot'})
    resources = body['Resources']
    assert not {'Bucket', 'Namespace', 'CopyRole'} & resources.keys()
    assert resources['Source']['DeletionPolicy'] == 'Snapshot'
    props = resources['Source']['Properties']
    assert props['DBSnapshotIdentifier'] == 'existing-snapshot'
    assert not {'MasterUsername', 'MasterUserPassword', 'DBName'} & props.keys()
    assert resources['Capture']['Properties']['MigrationType'] == 'cdc'
    assert resources['Workgroup']['Properties']['NamespaceName'] == 'existing-warehouse'
    assert resources['TargetEndpoint']['Properties']['S3Settings']['BucketName'] == 'existing-bucket'
    assert 'Fn::GetAtt": ["Bucket"' not in json.dumps(body)
