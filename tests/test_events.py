import csv
import hashlib
import io

import pytest

from synthea_cdc.events import NULL, parse_csv
from synthea_cdc.warehouse import initialize_raw, load_local


def patient_row(sequence="1", operation="I", city="Boston", patient_id="p1"):
    return [operation,"patients","healthcare",patient_id,"1990-01-01",NULL,"F",city,"MA","02108",
            "2026-01-01T00:00:00Z","0/12345",sequence,"2026-01-01T00:00:00Z"]


def csv_text(rows):
    buffer = io.StringIO(newline=""); csv.writer(buffer).writerows(rows)
    return buffer.getvalue()


def test_csv_handles_quotes_commas_and_null_without_losing_empty_strings():
    row = patient_row(city='Boston, "Central"'); row[9] = ""
    event = parse_csv(csv_text([row]), "cdc/file.csv")[0]
    assert event.values[2] is None
    assert event.values[4] == 'Boston, "Central"'
    assert event.values[6] == ""


def test_missing_order_and_schema_changes_are_rejected():
    with pytest.raises(ValueError, match="change sequence"):
        parse_csv(csv_text([patient_row(sequence="")]), "bad.csv")
    with pytest.raises(ValueError, match="expected"):
        parse_csv(csv_text([patient_row()+["extra-column"]]), "bad.csv")


@pytest.mark.integration
def test_file_retry_and_redelivery_do_not_duplicate_events(database):
    initialize_raw(database)
    text = csv_text([patient_row()]); digest=hashlib.sha256(text.encode()).hexdigest()
    events = parse_csv(text,"one.csv")
    assert load_local(database,events,"one.csv",digest) == "loaded"
    assert load_local(database,events,"one.csv",digest) == "already_loaded"
    assert load_local(database,parse_csv(text,"two.csv"),"two.csv",digest) == "loaded"
    assert database.execute("SELECT count(*) FROM raw.patients").fetchone()[0] == 1
    database.commit()
    with pytest.raises(ValueError,match="changed"):
        load_local(database,events,"one.csv","different-content")


@pytest.mark.integration
def test_failed_file_does_not_leave_partial_rows_or_a_checkpoint(database):
    import psycopg
    initialize_raw(database)
    rows = [patient_row(),patient_row(sequence="2",patient_id="p2")]
    rows[1][4] = "invalid-date"
    events = parse_csv(csv_text(rows),"bad.csv")
    with pytest.raises(psycopg.Error):
        load_local(database,events,"bad.csv","hash")
    assert database.execute("SELECT count(*) FROM raw.patients").fetchone()[0] == 0
    assert database.execute("SELECT count(*) FROM raw.loaded_files").fetchone()[0] == 0
