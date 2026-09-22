"""Validation-tool behavior only; these do not claim to execute AWS services."""
from datetime import datetime, timezone, timedelta
import io
import time

import pytest

from scripts.check_freshness import find_probe, within_budget
from scripts.verify_bootstrap import overlaps
from test_events import csv_text, patient_row


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


def test_freshness_requires_specific_probe_not_any_recent_file():
    class S3Fixture:
        def __init__(self):
            self.rows = [patient_row(patient_id='unrelated')]

        def get_paginator(self, _):
            return self

        def paginate(self, **_):
            return [{'Contents': [{'Key': 'capture-v1/cdc/test.csv'}]}]

        def get_object(self, **_):
            return {'Body': io.BytesIO(csv_text(self.rows).encode())}

    s3 = S3Fixture()
    assert find_probe(s3, 'bucket', 'capture-v1/cdc/', 'probe', set()) is None
    s3.rows.append(patient_row(patient_id='probe', sequence='2'))
    assert find_probe(s3, 'bucket', 'capture-v1/cdc/', 'probe', set()) == 'capture-v1/cdc/test.csv'
