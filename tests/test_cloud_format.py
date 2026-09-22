import csv
import gzip
import io

import pytest

from olist_cdc.cloud_load import normalized_csv, snapshot_table
from olist_cdc.events import Event, NULL


def test_copy_csv_preserves_null_empty_quotes_and_multiline_values():
    values = [None, "", 'Boston, "Central"\nFloor 2', True, False, "00123", 12345678901234567890123456789012345]
    result = normalized_csv([Event("customers", values)])
    row = next(csv.reader(io.StringIO(gzip.decompress(result).decode(), newline="")))
    assert row == [NULL, "", values[2], "true", "false", "00123", str(values[-1])]
    assert result == normalized_csv([Event("customers", values)])


def test_capture_paths_distinguish_snapshot_from_cdc_and_reject_unexpected_tables():
    assert snapshot_table("olist-v1/ecommerce/customers/LOAD00000001.csv", "olist-v1") == "customers"
    assert snapshot_table("olist-v1/cdc/20260922.csv", "olist-v1") is None
    with pytest.raises(ValueError, match="Unexpected"):
        snapshot_table("olist-v1/ecommerce/unknown/LOAD00000001.csv", "olist-v1")
