"""Missing source keys reach raw, then fail staging checks for all four tables."""
import json
import os
import shutil
import subprocess

import pytest

from olist_cdc.db import ROOT
from olist_cdc.events import NULL, parse_csv
from olist_cdc.schema import SOURCE_COLUMNS, source_columns
from olist_cdc.warehouse import initialize_raw, load_local
from test_events import csv_text
from test_mart_rebuilds import event


@pytest.mark.integration
def test_missing_keys_are_retained_and_block_dbt_downstream(database, tmp_path):
    initialize_raw(database)
    for table in SOURCE_COLUMNS:
        key = source_columns(table)[0]
        cdc = [event(table, {key: NULL}, 1), event(table, {key: ''}, 2, 'D'),
               event(table, {key: 'valid-key'}, 3)]
        snapshots = [[row[0], *row[3:]] for row in cdc[:2]]
        snapshots[1][0] = 'I'  # Full-load snapshots contain inserts only.
        for name, rows, snapshot in [(f'{table}-cdc.csv', cdc, None),
                                      (f'{table}/LOAD.csv', snapshots, table)]:
            parsed = parse_csv(csv_text(rows), name, snapshot_table=snapshot)
            assert load_local(database, parsed, name) == 'loaded'
            database.commit()
            assert load_local(database, parsed, name) == 'already_loaded'
            database.commit()
        assert database.execute(f'SELECT count(*),count(DISTINCT _event_id) FROM raw.{table}').fetchone() == (5, 5)
        assert database.execute(f'SELECT count(*) FROM raw.{table} WHERE {key} IS NULL OR {key}=\'\'').fetchone() == (4,)
    database.commit()
    shutil.copyfile(ROOT/'dbt/profiles.yml.example', tmp_path/'profiles.yml')
    result = subprocess.run([str(ROOT/'.venv/bin/dbt'), 'build', '--project-dir', str(ROOT/'dbt'),
        '--profiles-dir', str(tmp_path), '--target', 'local'], capture_output=True, text=True,
        timeout=120, env={**os.environ, 'WAREHOUSE_DATABASE': database.info.dbname,
            'DBT_SEND_ANONYMOUS_USAGE_STATS': 'false', 'DBT_TARGET_PATH': str(tmp_path/'target'),
            'DBT_LOG_PATH': str(tmp_path/'logs')})
    assert result.returncode == 1, result.stdout + result.stderr
    results = json.loads((tmp_path/'target/run_results.json').read_text())['results']
    failures = [r for r in results if 'required_source_key_' in r['unique_id']]
    assert len(failures) == 4
    assert all(r['status'] == 'fail' and r['failures'] == 4 for r in failures), failures
    marts = [r for r in results if r['unique_id'].startswith('model.') and '.fct_' in r['unique_id']]
    assert len(marts) == 3 and all(r['status'] == 'skipped' for r in marts)
    for table in SOURCE_COLUMNS:
        key = source_columns(table)[0]
        assert database.execute(f'SELECT count(*) FROM staging.stg_{table}').fetchone() == (5,)
        assert database.execute(f'SELECT count(*) FROM raw.{table} WHERE {key} IS NULL OR {key}=\'\'').fetchone() == (4,)
