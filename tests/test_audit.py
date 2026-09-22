import json
import subprocess

import pytest

from olist_cdc.audit import audit_step, connect_audit


@pytest.fixture
def audit(monkeypatch,tmp_path):
    monkeypatch.setenv('BATCH_AUDIT_PATH',str(tmp_path/'audit.sqlite'))
    monkeypatch.setenv('BATCH_RUN_ID','run-one')
    monkeypatch.setenv('BATCH_ATTEMPT','1')


def records(query):
    connection=connect_audit()
    try:
        return [dict(r) for r in connection.execute(query)]
    finally:
        connection.close()


def test_failed_build_retry_preserves_attempts_and_final_success(audit,monkeypatch):
    with audit_step('load') as details:details['files_committed']=2
    with pytest.raises(subprocess.CalledProcessError):
        with audit_step('build'):
            raise subprocess.CalledProcessError(7,'dbt')
    assert records('SELECT * FROM batch_runs')[0]['status']=='failed'
    monkeypatch.setenv('BATCH_ATTEMPT','2')
    with audit_step('build'):pass
    assert records('SELECT * FROM batch_runs')[0]['status']=='incomplete'
    with audit_step('complete'):pass
    run=records('SELECT * FROM batch_runs')[0]
    assert run['status']=='success' and run['dbt_status']=='success'
    attempts=records("SELECT * FROM task_attempts WHERE step='build' ORDER BY attempt")
    assert [r['status'] for r in attempts]==['failed','success']
    assert json.loads(attempts[0]['details'])['exit_code']==7
    assert all(r['duration_seconds']>=0 for r in attempts)


def test_quiet_run_is_skipped_without_dbt(audit):
    with audit_step('check'):pass
    with audit_step('pending') as details:details['_skip']=True
    row=records('SELECT * FROM batch_runs')[0]
    assert row['status']=='skipped' and row['dbt_status'] is None


def test_partial_metrics_survive_failure_without_storing_exception_message(audit):
    with pytest.raises(RuntimeError):
        with audit_step('load') as details:
            details.update(files_committed=1,input_I=4)
            raise RuntimeError('potentially sensitive connection details')
    row=records('SELECT * FROM task_attempts')[0]
    assert json.loads(row['details'])==dict(files_committed=1,input_I=4,error_type='RuntimeError')
    assert row['status']=='failed'


def test_incomplete_work_is_not_reported_as_success(audit):
    with audit_step('check'):
        assert records('SELECT * FROM batch_runs')[0]['status']=='incomplete'
    assert records('SELECT * FROM batch_runs')[0]['status']=='incomplete'


def test_clearing_a_task_does_not_reuse_old_completion(audit,monkeypatch):
    with audit_step('build'):pass
    with audit_step('complete'):pass
    monkeypatch.setenv('BATCH_ATTEMPT','2')
    with audit_step('build'):
        assert records('SELECT * FROM batch_runs')[0]['status']=='incomplete'
    assert records('SELECT * FROM batch_runs')[0]['status']=='incomplete'
    with audit_step('complete'):pass
    assert records('SELECT * FROM batch_runs')[0]['status']=='success'
