"""The source simulator and ingestion share a driver-independent schema contract."""
import subprocess
import sys

import pytest

from olist_cdc import seed
from olist_cdc.schema import SOURCE_COLUMNS, source_columns


@pytest.mark.parametrize('table', SOURCE_COLUMNS)
def test_seed_mapping_matches_source_column_order(table):
    assert tuple(seed.CSV_COLUMNS[table]) == SOURCE_COLUMNS[table]
    assert source_columns(table) == [*SOURCE_COLUMNS[table], 'updated_at']


def test_event_conversion_is_independent_of_seed_and_warehouse_loaders():
    result = subprocess.run([sys.executable, '-c', '''
import sys
import olist_cdc.events
import olist_cdc.parquet
assert 'olist_cdc.seed' not in sys.modules
assert 'olist_cdc.warehouse' not in sys.modules
'''], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
