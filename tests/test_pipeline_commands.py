"""Exercise the shared command runner's skip, failure and acknowledgement boundaries."""
import subprocess
import pytest
from scripts import run_cloud
from test_microbatch import capture


def test_shared_runner_retries_failed_build_and_skips_completed_batch(monkeypatch, tmp_path, capture):
    monkeypatch.setattr(run_cloud, 'ROOT', tmp_path)
    monkeypatch.setenv('BATCH_RUN_ID', 'one')
    calls = []
    monkeypatch.setattr(run_cloud, 'check_capture', lambda: calls.append('check'))
    monkeypatch.setattr(run_cloud, 'load_pending', lambda **kw: calls.append('load'))
    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(7, 'dbt')
    monkeypatch.setattr(run_cloud.subprocess, 'run', fail)
    for step in ('check', 'pending', 'load'):
        assert run_cloud.main([step]) is None
    with pytest.raises(subprocess.CalledProcessError):
        run_cloud.main(['build'])
    checkpoint = tmp_path/'data/olist-microbatch/completed.json'
    assert not checkpoint.exists()
    assert run_cloud.main(['pending']) is None
    monkeypatch.setattr(run_cloud.subprocess, 'run', lambda *a, **kw: calls.append('build'))
    for step in ('build', 'complete'):
        assert run_cloud.main([step]) is None
    assert checkpoint.exists() and calls == ['check', 'load', 'build']
    assert run_cloud.main(['pending']) == 99


def test_shared_runner_preserves_full_refresh_build(monkeypatch):
    calls = []
    monkeypatch.setattr(run_cloud.subprocess, 'run', lambda *a, **kw: calls.append((a, kw)))
    run_cloud.main(['build', '--full-refresh'])
    assert '--full-refresh' in calls[0][0][0]
    assert 'build' in calls[0][0][0] and calls[0][1]['check']
    with pytest.raises(SystemExit) as error:
        run_cloud.main(['complete', '--full-refresh'])
    assert error.value.code == 2 and len(calls) == 1


@pytest.mark.parametrize('step', ['pending', 'complete'])
def test_gate_commands_require_an_explicit_run_id(monkeypatch, step):
    monkeypatch.delenv('BATCH_RUN_ID', raising=False)
    with pytest.raises(SystemExit) as error:
        run_cloud.main([step])
    assert error.value.code == 2
