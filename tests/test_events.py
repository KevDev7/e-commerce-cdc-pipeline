import csv
import io

import pytest

from olist_cdc.events import NULL, parse_csv
from olist_cdc.warehouse import initialize_raw, load_local


def customer_row(sequence="1", operation="I", city="sao paulo", customer_id="c1"):
    return [operation,"customers","ecommerce",customer_id,"person1","00123",city,"SP",
            "2026-01-01T00:00:00Z","0/12345",sequence,"2026-01-01T00:00:00Z"]


def csv_text(rows):
    buffer = io.StringIO(newline=""); csv.writer(buffer).writerows(rows)
    return buffer.getvalue()


def test_csv_handles_quotes_commas_and_null_without_losing_empty_strings():
    row = customer_row(city='sao paulo, "Central"'); row[5] = NULL; row[7] = ""
    event = parse_csv(csv_text([row]), "cdc/file.csv")[0]
    assert event.values[2] is None
    assert event.values[3] == 'sao paulo, "Central"'
    assert event.values[4] == ""


def test_missing_order_and_schema_changes_are_rejected():
    with pytest.raises(ValueError, match="change sequence"):
        parse_csv(csv_text([customer_row(sequence="")]), "bad.csv")
    with pytest.raises(ValueError, match="expected"):
        parse_csv(csv_text([customer_row()+["extra-column"]]), "bad.csv")


@pytest.mark.integration
def test_file_retry_and_redelivery_do_not_duplicate_events(database):
    initialize_raw(database)
    text = csv_text([customer_row()])
    events = parse_csv(text,"one.csv")
    assert load_local(database,events,"one.csv") == "loaded"
    assert load_local(database,events,"one.csv") == "already_loaded"
    assert load_local(database,parse_csv(text,"two.csv"),"two.csv") == "loaded"
    assert database.execute("SELECT count(*) FROM raw.customers").fetchone()[0] == 1
    database.commit()


@pytest.mark.integration
def test_failed_file_does_not_leave_partial_rows_or_a_checkpoint(database):
    import psycopg
    initialize_raw(database)
    rows = [customer_row(),customer_row(sequence="2",customer_id="p2")]
    rows[1][8] = "invalid-date"
    events = parse_csv(csv_text(rows),"bad.csv")
    with pytest.raises(psycopg.Error):
        load_local(database,events,"bad.csv")
    assert database.execute("SELECT count(*) FROM raw.customers").fetchone()[0] == 0
    assert database.execute("SELECT count(*) FROM raw.loaded_files").fetchone()[0] == 0
