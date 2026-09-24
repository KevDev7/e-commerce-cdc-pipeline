"""Teardown must retain cloud data without downloading it to the workstation."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from infra.template import template


def test_bucket_is_retained_but_source_and_warehouse_are_disposable():
    resources = template()['Resources']
    assert resources['Bucket']['DeletionPolicy'] == 'Retain'
    assert resources['Bucket']['UpdateReplacePolicy'] == 'Retain'
    assert resources['Source']['DeletionPolicy'] == 'Delete'
    assert resources['Namespace']['DeletionPolicy'] == 'Delete'


@pytest.mark.parametrize('retain', [True, False])
def test_teardown_preserves_bucket_and_refuses_legacy_delete_policy(monkeypatch, tmp_path, retain):
    calls = []
    body = template()
    if not retain:
        body['Resources']['Bucket'].pop('DeletionPolicy')
    cf = SimpleNamespace(get_template=lambda **kw: {'TemplateBody': body},
                         delete_stack=lambda **kw: calls.append(kw))
    # No S3 client is supplied: teardown must not download or delete objects.
    session = SimpleNamespace(client=lambda service: {'cloudformation': cf, 'dms': object()}[service])
    monkeypatch.setattr('boto3.Session', lambda **kw: session)
    spec = importlib.util.spec_from_file_location('cloud_stack_test', Path('scripts/cloud_stack.py'))
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    module.STATE = tmp_path/'state.json'; module.STATE.write_text('{}')
    module.stack = lambda: {'StackId': 'project-stack'}
    module.outputs = lambda: {'S3Bucket': 'project-bucket'}
    if retain:
        module.delete()
        assert calls == [{'StackName': 'project-stack'}]
        assert json.loads(module.STATE.read_text())['retained_bucket'] == 'project-bucket'
    else:
        with pytest.raises(RuntimeError, match='Retain'):
            module.delete()
        assert calls == []


def test_export_failure_prevents_source_deletion(monkeypatch, tmp_path):
    calls = []
    cf = SimpleNamespace(get_template=lambda **kw: {'TemplateBody': template()},
                         delete_stack=lambda **kw: calls.append(kw))
    session = SimpleNamespace(client=lambda service: {'cloudformation': cf, 'dms': object()}[service])
    monkeypatch.setattr('boto3.Session', lambda **kw: session)
    spec = importlib.util.spec_from_file_location('cloud_stack_reference_test', Path('scripts/cloud_stack.py'))
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    module.STATE = tmp_path/'state.json'; module.STATE.write_text('{}')
    module.stack = lambda: {'StackId': 'project-stack'}
    module.outputs = lambda: {'S3Bucket': 'project-bucket', 'SourceHost': 'source-host'}
    def fail(*args):
        raise RuntimeError('export failed')
    monkeypatch.setattr('olist_cdc.reference.preserve_live', fail)
    with pytest.raises(RuntimeError, match='export failed'):
        module.delete()
    assert calls == []
    module.delete(skip_reference_reason='Source was never initialized')
    assert len(calls) == 1
    assert json.loads(module.STATE.read_text())['reference_export_skipped'] == 'Source was never initialized'
