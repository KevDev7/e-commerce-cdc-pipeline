"""Run with Airflow's Python in an isolated container; never contact AWS.

Real DAG/task state transitions, with cloud commands replaced by shell fixtures.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shlex

from airflow.models import DagBag
from airflow.utils.state import DagRunState, TaskInstanceState

root = Path(__file__).resolve().parents[1]
bag = DagBag(str(root / 'dags'), include_examples=False)
assert not bag.import_errors, bag.import_errors
dag = bag.get_dag('olist_cdc')
assert dag.schedule_interval == '*/5 * * * *'
assert not dag.catchup and dag.max_active_runs == 1 and dag.is_paused_upon_creation
expected = ['check', 'pending', 'load', 'build', 'complete']
assert [task.task_id for task in dag.topological_sort()] == expected
for step in expected:
    task = dag.get_task(step)
    assert task.bash_command.endswith('scripts/run_cloud.py ' + step)
    assert task.skip_on_exit_code == ([99] if step == 'pending' else [])
for first, second in zip(expected, expected[1:]):
    assert dag.get_task(first).downstream_task_ids == {second}

start = datetime(2026, 9, 22, tzinfo=timezone.utc)
for index, (name, gate_exit, build_exit) in enumerate([
    ('new-files', 0, 0), ('quiet', 99, 0), ('failed-build', 0, 7), ('recovery', 0, 0),
]):
    for task in dag.tasks:
        exit_code = gate_exit if task.task_id == 'pending' else build_exit if task.task_id == 'build' else 0
        task.env.update(PYTHONPATH=str(root/'src'))
        fixture = f"""import os, subprocess, sys
from olist_cdc.task_logging import log_step
assert os.environ['BATCH_RUN_ID'] and int(os.environ['BATCH_ATTEMPT']) >= 1
with log_step({task.task_id!r}) as details:
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
    print(f'PASS {name}: {states}')
print('PASS: schedule, single active run, paused default, new files, idle skip, failure and recovery')
