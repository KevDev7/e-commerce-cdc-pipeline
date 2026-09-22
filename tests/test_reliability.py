"""Local database guarantees; these tests do not stand in for DMS/Redshift validation."""
import hashlib
import os

import psycopg
import pytest
from psycopg import sql

from synthea_cdc.db import connect
from synthea_cdc.events import NULL, parse_csv
from synthea_cdc.warehouse import initialize_raw, load_local
from test_events import csv_text, patient_row


@pytest.mark.integration
def test_exported_snapshot_and_wal_cover_concurrent_insert_update_delete(database):
    database.autocommit = True
    database.execute("""INSERT INTO healthcare.patients
        (patient_id,birth_date,gender,city) VALUES
        ('00000000-0000-0000-0000-000000000001','1990-01-01','F','Boston'),
        ('00000000-0000-0000-0000-000000000002','1990-01-01','F','Delete me')""")
    slot = "snapshot_" + database.info.dbname.removeprefix("synthea_test_")
    # The replication protocol exports the snapshot associated with the WAL start.
    params = database.info.get_parameters()
    params.update(password=os.environ['POSTGRES_PASSWORD'], replication="database")
    with psycopg.connect(**params, autocommit=True) as replication:
        try:
            result = replication.execute(sql.SQL(
                "CREATE_REPLICATION_SLOT {} LOGICAL test_decoding (SNAPSHOT 'export')"
            ).format(sql.Identifier(slot))).fetchone()
            with connect(database.info.dbname) as snapshot:
                snapshot.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                snapshot.execute(sql.SQL("SET TRANSACTION SNAPSHOT {}").format(sql.Literal(result[2])))
                before = snapshot.execute("SELECT patient_id,city FROM healthcare.patients ORDER BY 1").fetchall()
                with database.transaction():
                    database.execute("UPDATE healthcare.patients SET city='Cambridge' WHERE city='Boston'")
                    database.execute("DELETE FROM healthcare.patients WHERE city='Delete me'")
                    database.execute("""INSERT INTO healthcare.patients (patient_id,birth_date,gender,city)
                        VALUES ('00000000-0000-0000-0000-000000000003','1990-01-01','F','New patient')""")
                assert snapshot.execute("SELECT patient_id,city FROM healthcare.patients ORDER BY 1").fetchall() == before
            records = database.execute("SELECT data FROM pg_logical_slot_get_changes(%s,NULL,NULL)", (slot,)).fetchall()
            changes = [r[0] for r in records if r[0].startswith("table healthcare.patients:")]
            assert len(changes) == 3
            assert [s.split(": ")[1] for s in changes] == ["UPDATE", "DELETE", "INSERT"]
            assert "city[character varying]:'Cambridge'" in changes[0].split("new-tuple:")[1]
            assert database.execute("SELECT city FROM healthcare.patients ORDER BY patient_id").fetchall() == [("Cambridge",), ("New patient",)]
        finally:
            # End the exported snapshot before dropping its slot.
            replication.close()
            database.execute("SELECT pg_drop_replication_slot(%s)", (slot,))


@pytest.mark.integration
def test_failure_after_first_table_rolls_back_whole_file_then_retry_succeeds(database):
    initialize_raw(database)
    rows = [patient_row(), ["I", "encounters", "healthcare", "e1", "p1",
        "2026-01-01T00:00:00Z", NULL, "ambulatory", "invalid-number",
        "2026-01-01T00:00:00Z", "0/12345", "2", "2026-01-01T00:00:00Z"]]
    body = csv_text(rows)
    with pytest.raises(psycopg.Error):
        load_local(database, parse_csv(body, "multi.csv"), "multi.csv", hashlib.sha256(body.encode()).hexdigest())
    # patients was inserted before the encounters COPY failed.
    assert database.execute('SELECT count(*) FROM "raw".patients').fetchone()[0] == 0
    assert database.execute('SELECT count(*) FROM "raw".loaded_files').fetchone()[0] == 0
    database.commit()
    rows[1][8] = "100"
    body = csv_text(rows)
    digest = hashlib.sha256(body.encode()).hexdigest()
    assert load_local(database, parse_csv(body, "multi.csv"), "multi.csv", digest) == "loaded"
    assert load_local(database, parse_csv(body, "multi.csv"), "multi.csv", digest) == "already_loaded"
    assert database.execute('SELECT count(*) FROM "raw".patients').fetchone()[0] == 1
    assert database.execute('SELECT count(*) FROM "raw".encounters').fetchone()[0] == 1
