from types import SimpleNamespace
import pytest
from olist_cdc import cloud_load


def check(monkeypatch, migration, tables, status='running'):
    dms = SimpleNamespace(
        describe_replication_tasks=lambda **kw: {'ReplicationTasks': [
            {'Status': status, 'MigrationType': migration}]},
        describe_table_statistics=lambda **kw: {'TableStatistics': tables})
    monkeypatch.setenv('DMS_TASK_ARN', 'test-task')
    monkeypatch.setattr(cloud_load, 'aws_session', lambda: SimpleNamespace(client=lambda name: dms))
    cloud_load.check_capture()


def test_cdc_only_does_not_require_a_new_full_load(monkeypatch):
    check(monkeypatch, 'cdc', [])


def test_new_full_load_still_requires_all_four_tables(monkeypatch):
    with pytest.raises(RuntimeError, match='Initial load is not complete'):
        check(monkeypatch, 'full-load-and-cdc', [])


def test_cdc_only_still_rejects_capture_failures(monkeypatch):
    with pytest.raises(RuntimeError, match='Capture tables failed'):
        check(monkeypatch, 'cdc', [{'TableName': 'orders', 'TableState': 'Table error'}])
    with pytest.raises(RuntimeError, match='Capture task is failed'):
        check(monkeypatch, 'cdc', [], status='failed')
