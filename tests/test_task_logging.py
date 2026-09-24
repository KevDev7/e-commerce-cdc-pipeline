"""Failures must propagate while committed-input metrics remain inspectable."""
import json
import logging
import subprocess

import pytest
from olist_cdc.task_logging import log_step


def summaries(caplog):
    return [json.loads(r.getMessage().removeprefix('Task summary: '))
            for r in caplog.records if r.name == 'olist_cdc.task_logging']


def test_failure_logs_partial_metrics_and_retry_separately(caplog, monkeypatch):
    caplog.set_level(logging.INFO)
    monkeypatch.setenv('BATCH_RUN_ID', 'run-one')
    monkeypatch.setenv('BATCH_ATTEMPT', '1')
    with pytest.raises(subprocess.CalledProcessError):
        with log_step('load') as details:
            details.update(files_committed=1, input_I=4)
            raise subprocess.CalledProcessError(7, 'sensitive-command')
    monkeypatch.setenv('BATCH_ATTEMPT', '2')
    with log_step('load') as details:
        details['files_already_loaded'] = 1
    failed, retried = summaries(caplog)
    assert failed['status'] == 'failed' and failed['attempt'] == '1'
    assert failed['details'] == dict(files_committed=1, input_I=4,
                                     error_type='CalledProcessError', exit_code=7)
    assert retried['status'] == 'success' and retried['attempt'] == '2'
    assert retried['run_id'] == failed['run_id'] == 'run-one'
    assert 'sensitive-command' not in caplog.text


def test_manual_quiet_run_logs_without_creating_an_audit_database(caplog, monkeypatch, tmp_path):
    caplog.set_level(logging.INFO)
    monkeypatch.delenv('BATCH_RUN_ID', raising=False)
    monkeypatch.chdir(tmp_path)
    with log_step('pending') as details:
        details['_skip'] = True
    row = summaries(caplog)[0]
    assert row['status'] == 'skipped' and row['run_id'] is None
    assert row['details'] == {} and row['duration_seconds'] >= 0
    assert list(tmp_path.iterdir()) == []
