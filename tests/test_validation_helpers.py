"""Bootstrap timing validation; this does not claim to execute AWS services."""
from datetime import datetime, timezone, timedelta

from scripts.verify_bootstrap import overlaps


def test_bootstrap_requires_commit_to_fit_inside_table_load():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    table = dict(FullLoadStartTime=start, FullLoadEndTime=start + timedelta(seconds=10))
    commit = dict(before_commit=(start + timedelta(seconds=2)).isoformat(),
                  after_commit=(start + timedelta(seconds=3)).isoformat())
    assert overlaps(commit, table)
    assert not overlaps(commit, {})
    assert not overlaps({**commit, 'before_commit': start.isoformat()}, table)
    assert not overlaps({**commit, 'after_commit': (start + timedelta(seconds=11)).isoformat()}, table)
