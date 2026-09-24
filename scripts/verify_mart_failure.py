"""Prove rollback and retry of an incremental order mart on the real Redshift target.

Run with Airflow paused, after loading a batch containing new order events and
before building its marts. Fault injection modifies a temporary dbt project only.
"""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
from olist_cdc.cloud_load import warehouse_connection


def state():
    with warehouse_connection() as connection:
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM marts.fct_orders ORDER BY order_id')
        digest = hashlib.sha256()
        count = 0
        for row in cursor:
            digest.update(json.dumps(row, default=str, separators=(',', ':')).encode() + b'\n')
            count += 1
        cursor.execute('SELECT source_file FROM marts.fct_orders__files ORDER BY source_file')
        files = [row[0] for row in cursor]
        cursor.execute('''SELECT count(*) FROM "raw".orders o WHERE NOT EXISTS (
            SELECT 1 FROM marts.fct_orders__files f WHERE f.source_file=o._source_file)''')
        pending = cursor.fetchone()[0]
        connection.commit()
    return dict(rows=count, sha256=digest.hexdigest(), files=files, pending_order_events=pending)


def run(project, output, work):
    with output.open('w') as log:
        return subprocess.run(
            [str(Path(sys.executable).with_name('dbt')), 'run', '--select', 'fct_orders',
             '--target', 'redshift', '--project-dir', str(project), '--profiles-dir', str(ROOT/'dbt'),
             '--no-send-anonymous-usage-stats'], stdout=log, stderr=subprocess.STDOUT,
            env={**os.environ, 'DBT_TARGET_PATH':str(work/'target'), 'DBT_LOG_PATH':str(work/'logs')},
            timeout=300).returncode


def main():
    if not (ROOT/'.env.cloud').exists():
        raise RuntimeError('No active cloud environment; provision an authorized demo first')
    load_dotenv(ROOT/'.env.cloud', override=True)
    before = state()
    if before['pending_order_events'] == 0:
        raise ValueError('Load new order events before this test, without building the marts')
    evidence = ROOT/'data/mart-failure'
    evidence.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='olist-dbt-failure-') as directory:
        work = Path(directory)
        project = work/'project'
        shutil.copytree(ROOT/'dbt', project,
                        ignore=shutil.ignore_patterns('target','logs','profiles.yml','dbt_packages'))
        hook = project/'macros/incremental_files.sql'
        original = hook.read_text()
        needle = "select source_file from {{ cdc_temp('pending') }};"
        if original.count(needle) != 1:
            raise RuntimeError('Checkpoint hook changed; review fault injection before running')
        hook.write_text(original.replace(needle, needle+'\n    select 1/0;'))
        failed_log = evidence/'failure.log'
        code = run(project, failed_log, work)
        if code == 0 or not any(message in failed_log.read_text().lower()
                                for message in ('division by zero', 'divide by zero')):
            raise AssertionError('Expected SQL failure was not observed; inspect the saved log')
        after_failure = state()
        assert after_failure == before, 'Mart or checkpoint changed despite failed SQL'
        assert run(ROOT/'dbt', evidence/'retry.log', work) == 0, 'Retry failed; inspect saved log'
    after_retry = state()
    assert after_retry['pending_order_events'] == 0
    assert after_retry['sha256'] != before['sha256'], 'Fixture did not change business rows'
    result = dict(verified_at=datetime.now(timezone.utc).isoformat(), target='Redshift',
                  model='fct_orders', rollback_preserved_rows_and_checkpoint=True,
                  retry_applied_changes=True, pending_order_events=before['pending_order_events'],
                  before_rows=before['rows'], after_retry_rows=after_retry['rows'],
                  before_sha256=before['sha256'], after_retry_sha256=after_retry['sha256'])
    (ROOT/'data/mart-failure.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
