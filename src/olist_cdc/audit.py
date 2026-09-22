"""Small local audit tables; no AWS calls and no additional service."""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import time

DEFAULT_PATH = Path(__file__).resolve().parents[2] / 'data/olist-microbatch/audit.sqlite'


def connect_audit():
    path = Path(os.environ.get('BATCH_AUDIT_PATH', DEFAULT_PATH))
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.executescript('''
        CREATE TABLE IF NOT EXISTS task_attempts (
            run_id TEXT NOT NULL, step TEXT NOT NULL, attempt INTEGER NOT NULL,
            started_at TEXT NOT NULL, finished_at TEXT, duration_seconds REAL,
            status TEXT NOT NULL, details TEXT NOT NULL,
            PRIMARY KEY(run_id,step,attempt)
        );
        CREATE VIEW IF NOT EXISTS batch_runs AS
        WITH ranked AS (
            SELECT *,row_number() OVER (PARTITION BY run_id,step ORDER BY attempt DESC) AS rank
            FROM task_attempts
        ), latest AS (SELECT * FROM ranked WHERE rank=1)
        SELECT run_id,
            (SELECT min(a.started_at) FROM task_attempts a WHERE a.run_id=latest.run_id) AS started_at,
            max(finished_at) AS last_finished_at,
            round((julianday(max(finished_at))-julianday((SELECT min(a.started_at)
                FROM task_attempts a WHERE a.run_id=latest.run_id)))*86400,3) AS elapsed_seconds,
            CASE WHEN max(status='failed') THEN 'failed'
                 WHEN max(status='incomplete') THEN 'incomplete'
                 WHEN max(CASE WHEN step='complete' AND status='success' THEN finished_at END)
                      >= max(finished_at) THEN 'success'
                 WHEN max(step='pending' AND status='skipped') THEN 'skipped'
                 ELSE 'incomplete' END AS status,
            max(CASE WHEN step='build' THEN status END) AS dbt_status,
            (SELECT coalesce(sum(json_extract(a.details,'$.files_committed')),0)
                FROM task_attempts a WHERE a.run_id=latest.run_id) AS files_committed,
            (SELECT coalesce(sum(json_extract(a.details,'$.input_I')),0)
                FROM task_attempts a WHERE a.run_id=latest.run_id) AS input_inserts,
            (SELECT coalesce(sum(json_extract(a.details,'$.input_U')),0)
                FROM task_attempts a WHERE a.run_id=latest.run_id) AS input_updates,
            (SELECT coalesce(sum(json_extract(a.details,'$.input_D')),0)
                FROM task_attempts a WHERE a.run_id=latest.run_id) AS input_deletes,
            (SELECT coalesce(sum(json_extract(a.details,'$.snapshot_rows')),0)
                FROM task_attempts a WHERE a.run_id=latest.run_id) AS snapshot_rows
        FROM latest GROUP BY run_id;
    ''')
    return connection


@contextmanager
def audit_step(step):
    """Airflow provides a run ID and attempt; ungrouped manual commands only log."""
    details = {}
    run_id = os.environ.get('BATCH_RUN_ID')
    if not run_id:
        yield details
        return
    attempt = int(os.environ.get('BATCH_ATTEMPT', '1'))
    started = datetime.now(timezone.utc).isoformat()
    timer = time.monotonic()
    connection = connect_audit()
    try:
        connection.execute('INSERT OR REPLACE INTO task_attempts VALUES (?,?,?,?,NULL,NULL,?,?)',
                           (run_id,step,attempt,started,'incomplete','{}'))
        connection.commit()
        status = 'success'
        try:
            yield details
            if details.pop('_skip', False):
                status = 'skipped'
        except BaseException as error:
            status = 'failed'
            # Exception messages can contain SQL or connection details. Keep those in existing task logs.
            details['error_type'] = type(error).__name__
            if hasattr(error, 'returncode'):
                details['exit_code'] = error.returncode
            raise
        finally:
            connection.execute('''UPDATE task_attempts SET finished_at=?,duration_seconds=?,status=?,details=?
                WHERE run_id=? AND step=? AND attempt=?''',
                (datetime.now(timezone.utc).isoformat(),round(time.monotonic()-timer,3),status,
                 json.dumps(details,sort_keys=True),run_id,step,attempt))
            connection.commit()
    finally:
        connection.close()
