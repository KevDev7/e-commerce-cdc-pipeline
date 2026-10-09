"""Verify the compute guard against stubbed AWS responses, without AWS calls."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import botocore.session
from botocore.stub import Stubber
import pytest


ARN = 'arn:aws:redshift-serverless:us-east-1:123456789012:workgroup/00000000-0000-0000-0000-000000000000'
LIMIT = dict(resourceArn=ARN, usageType='serverless-compute', amount=6,
             period='monthly', breachAction='deactivate')


@pytest.fixture
def guard(monkeypatch):
    client = botocore.session.get_session().create_client(
        'redshift-serverless', region_name='us-east-1',
        aws_access_key_id='testing', aws_secret_access_key='testing')
    session = SimpleNamespace(client=lambda name: {
        'cloudformation': object(), 'redshift-serverless': client}[name])
    monkeypatch.setattr('boto3.Session', lambda **kwargs: session)
    spec = importlib.util.spec_from_file_location('usage_limit_test', Path('scripts/cloud_stack.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with Stubber(client) as stub:
        stub.add_response('get_workgroup', {'workgroup': {
            'workgroupArn': ARN, 'baseCapacity': 4, 'maxCapacity': 4}},
            {'workgroupName': 'synthea-cdc'})
        yield module.usage_limit, stub
        stub.assert_no_pending_responses()


@pytest.mark.parametrize('different', [
    {'amount': 1000}, {'period': 'daily'}, {'breachAction': 'log'},
    {'usageType': 'cross-region-datasharing'},
])
def test_incorrect_existing_limit_is_not_reported_as_protection(guard, capsys, different):
    run, stub = guard
    stub.add_response('list_usage_limits', {'usageLimits': [{**LIMIT, **different}]},
                      {'resourceArn': ARN})
    with pytest.raises(RuntimeError, match='usage limit'):
        run()
    assert not capsys.readouterr().out


@pytest.mark.parametrize('amount', [6, 3])
def test_expected_or_stricter_existing_limit_is_verified_without_changes(guard, capsys, amount):
    run, stub = guard
    stub.add_response('list_usage_limits', {'usageLimits': [{**LIMIT, 'amount': amount}]},
                      {'resourceArn': ARN})
    run()
    assert 'verified' in capsys.readouterr().out


def test_missing_limit_is_created_and_verified(guard, capsys):
    run, stub = guard
    stub.add_response('list_usage_limits', {}, {'resourceArn': ARN})
    stub.add_response('create_usage_limit', {'usageLimit': LIMIT}, LIMIT)
    run()
    assert 'verified' in capsys.readouterr().out


def test_unexpected_created_limit_is_not_reported_as_protection(guard, capsys):
    run, stub = guard
    stub.add_response('list_usage_limits', {}, {'resourceArn': ARN})
    stub.add_response('create_usage_limit', {'usageLimit': {**LIMIT, 'breachAction': 'log'}}, LIMIT)
    with pytest.raises(RuntimeError, match='usage limit'):
        run()
    assert not capsys.readouterr().out
