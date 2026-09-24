"""Validation-tool behavior only; these do not claim to execute AWS services."""
from datetime import datetime, timezone, timedelta
import io
import os
import signal
import subprocess
import sys
import time

import pytest

from scripts.check_freshness import find_probe, run_step, within_budget
from scripts.verify_bootstrap import overlaps
from test_events import csv_text, customer_row


def test_bootstrap_requires_commit_to_fit_inside_table_load():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    table = dict(FullLoadStartTime=start, FullLoadEndTime=start + timedelta(seconds=10))
    commit = dict(before_commit=(start + timedelta(seconds=2)).isoformat(),
                  after_commit=(start + timedelta(seconds=3)).isoformat())
    assert overlaps(commit, table)
    assert not overlaps(commit, {})
    assert not overlaps({**commit, 'before_commit': start.isoformat()}, table)
    assert not overlaps({**commit, 'after_commit': (start + timedelta(seconds=11)).isoformat()}, table)


def test_freshness_fails_when_deadline_expires():
    with pytest.raises(TimeoutError, match='threshold'):
        within_budget(time.monotonic() - 10, 1)


def test_step_timeout_stops_nested_work(tmp_path):
    pid_file = tmp_path / 'child.pid'
    heartbeat = tmp_path / 'heartbeat'
    child = f'''import os, pathlib, time
pathlib.Path({str(pid_file)!r}).write_text(str(os.getpid()))
while True:
    pathlib.Path({str(heartbeat)!r}).write_text(str(time.monotonic_ns()))
    time.sleep(0.02)
'''
    wrapper = f'import subprocess,sys; subprocess.run([sys.executable,"-c",{child!r}],check=True)'
    try:
        with pytest.raises(subprocess.TimeoutExpired):
            run_step([sys.executable, '-c', wrapper], timeout=2)
        assert pid_file.exists() and heartbeat.exists(), 'Nested work must actually start'
        time.sleep(0.1)
        stopped_at = heartbeat.read_text()
        time.sleep(0.2)
        assert heartbeat.read_text() == stopped_at, 'Nested work continued after timeout'
    finally:
        # Keep a failing regression test from leaving its child running.
        if pid_file.exists():
            try:
                os.kill(int(pid_file.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_step_preserves_command_failure():
    with pytest.raises(subprocess.CalledProcessError) as error:
        run_step([sys.executable, '-c', 'raise SystemExit(7)'], timeout=5)
    assert error.value.returncode == 7


def test_freshness_requires_specific_probe_not_any_recent_file():
    class S3Fixture:
        def __init__(self):
            self.rows = [customer_row(customer_id='unrelated')]

        def get_paginator(self, _):
            return self

        def paginate(self, **_):
            return [{'Contents': [{'Key': 'raw/dms/olist/cdc/test.csv'}]}]

        def get_object(self, **_):
            return {'Body': io.BytesIO(csv_text(self.rows).encode())}

    s3 = S3Fixture()
    assert find_probe(s3, 'bucket', 'raw/dms/olist/cdc/', 'probe', set()) is None
    s3.rows.append(customer_row(customer_id='probe', sequence='2'))
    assert find_probe(s3, 'bucket', 'raw/dms/olist/cdc/', 'probe', set()) == 'raw/dms/olist/cdc/test.csv'
