"""Exercise the consolidated verifier against actual dbt models and tiny fixtures.

SQL source writes are real. Change envelopes here are explicit test fixtures;
DMS capture is tested separately in AWS.
"""
import os
import shutil
import subprocess

import pytest

from olist_cdc.db import ROOT
from olist_cdc.events import parse_csv, source_columns
from olist_cdc.seed import TABLES
from olist_cdc.simulate import PHASES, run_phase
from olist_cdc.warehouse import initialize_raw, load_local
from scripts.verify_cloud_scenario import verify
from test_events import csv_text
from test_incremental_marts import event


@pytest.mark.integration
def test_standard_scenario_includes_both_customer_versions(database, tmp_path):
    initialize_raw(database)
    previous = {table: {} for table in TABLES}
    changes = []
    for phase in PHASES:
        run_phase(database, 'combined-demo', phase)
        for table in TABLES:
            columns = source_columns(table)
            current = {row[0]: dict(zip(columns, row)) for row in database.execute(
                f"SELECT {','.join(columns)} FROM ecommerce.{table} ORDER BY 1")}
            for key in sorted(previous[table].keys() | current.keys()):
                before, after = previous[table].get(key), current.get(key)
                if before == after:
                    continue
                op = 'I' if before is None else 'D' if after is None else 'U'
                # Fixture helper represents absent optional values using the CSV null token.
                values = {k: v for k, v in (after or before).items() if v is not None}
                changes.append(event(table, values, len(changes) + 1, op))
            previous[table] = current
    assert len(changes) == 15
    body = csv_text(changes)
    load_local(database, parse_csv(body, 'scenario.csv'), 'scenario.csv')
    database.commit()
    shutil.copyfile(ROOT/'dbt/profiles.yml.example', tmp_path/'profiles.yml')
    result = subprocess.run([str(ROOT/'.venv/bin/dbt'), 'build',
        '--project-dir', str(ROOT/'dbt'), '--profiles-dir', str(tmp_path), '--target', 'local'],
        capture_output=True, text=True, timeout=120,
        env={**os.environ, 'WAREHOUSE_DATABASE': database.info.dbname,
             'DBT_SEND_ANONYMOUS_USAGE_STATS': 'false',
             'DBT_TARGET_PATH': str(tmp_path/'target'), 'DBT_LOG_PATH': str(tmp_path/'logs')})
    assert result.returncode == 0, result.stdout[-8000:] + result.stderr[-2000:]
    checked = verify(database.cursor(), 'combined-demo')
    assert checked['operations'] == {'I': 9, 'U': 4, 'D': 2}
    assert checked['distinct_observed_versions']
