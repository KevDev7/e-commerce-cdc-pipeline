"""Run with Airflow's Python in an isolated container; never contact AWS.

Real DAG/task state transitions, with cloud commands replaced by shell fixtures.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shlex
import sqlite3
import tempfile

from airflow.models import DagBag
from airflow.utils.state import DagRunState, TaskInstanceState

root = Path(__file__).resolve().parents[1]
bag = DagBag(str(root / 'dags'), include_examples=False)
assert not bag.import_errors, bag.import_errors
dag = bag.get_dag('olist_cdc')
assert dag.schedule_interval == '*/5 * * * *'
assert not dag.catchup and dag.max_active_runs == 1 and dag.is_paused_upon_creation
expected = ['check', 'pending', 'load', 'build', 'report', 'complete']
assert [task.task_id for task in dag.topological_sort()] == expected
for first, second in zip(expected, expected[1:]):
    assert dag.get_task(first).downstream_task_ids == {second}

audit_dir = tempfile.TemporaryDirectory()
audit_path = str(Path(audit_dir.name) / 'audit.sqlite')
start = datetime(2026, 9, 22, tzinfo=timezone.utc)
for index, (name, gate_exit, build_exit) in enumerate([
    ('new-files', 0, 0), ('quiet', 99, 0), ('failed-build', 0, 7), ('recovery', 0, 0),
]):
    for task in dag.tasks:
        exit_code = gate_exit if task.task_id == 'pending' else build_exit if task.task_id == 'build' else 0
        task.env.update(PYTHONPATH=str(root/'src'), BATCH_AUDIT_PATH=audit_path)
        fixture = f"""import os, subprocess, sys
from olist_cdc.audit import audit_step
assert os.environ['BATCH_RUN_ID'] and int(os.environ['BATCH_ATTEMPT']) >= 1
with audit_step({task.task_id!r}) as details:
    if {exit_code} == 99: details['_skip'] = True
    elif {exit_code}: raise subprocess.CalledProcessError({exit_code}, 'fixture')
sys.exit({exit_code})
"""
        task.bash_command = 'python -c ' + shlex.quote(fixture)
        task.retries = 0
    run = dag.test(execution_date=start + timedelta(minutes=5 * index))
    states = {ti.task_id: ti.state for ti in run.get_task_instances()}
    if name == 'quiet':
        assert run.state == DagRunState.SUCCESS, states
        assert states['check'] == TaskInstanceState.SUCCESS
        assert all(states[t] == TaskInstanceState.SKIPPED for t in expected[1:]), states
    elif name == 'failed-build':
        assert run.state == DagRunState.FAILED, states
        assert states['build'] == TaskInstanceState.FAILED
        assert states['complete'] == TaskInstanceState.UPSTREAM_FAILED, states
    else:
        assert run.state == DagRunState.SUCCESS, states
        assert all(states[t] == TaskInstanceState.SUCCESS for t in expected), states
    with sqlite3.connect(audit_path) as audit:
        status, dbt_status = audit.execute('SELECT status,dbt_status FROM batch_runs WHERE run_id=?',(run.run_id,)).fetchone()
    assert status == ('skipped' if name=='quiet' else 'failed' if name=='failed-build' else 'success'), status
    assert dbt_status == (None if name=='quiet' else 'failed' if name=='failed-build' else 'success'), dbt_status
    print(f'PASS {name}: {states}; audit={status}')
print('PASS: schedule, single active run, paused default, new files, idle skip, failure and recovery')
