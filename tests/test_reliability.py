"""Local database guarantees; these tests do not stand in for DMS/Redshift validation."""
import os

import psycopg
import pytest
from psycopg import sql

from olist_cdc.db import connect
from olist_cdc.events import NULL, parse_csv
from olist_cdc.warehouse import initialize_raw, load_local
from test_events import csv_text, customer_row


@pytest.mark.integration
def test_exported_snapshot_and_wal_cover_concurrent_insert_update_delete(database):
    database.autocommit = True
    database.execute("""INSERT INTO ecommerce.customers
        (customer_id,customer_unique_id,city) VALUES
        ('00000000000000000000000000000001','person','Boston'),
        ('00000000000000000000000000000002','person','Delete me')""")
    slot = "snapshot_" + database.info.dbname.removeprefix("olist_test_")
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
                before = snapshot.execute("SELECT customer_id,city FROM ecommerce.customers ORDER BY 1").fetchall()
                with database.transaction():
                    database.execute("UPDATE ecommerce.customers SET city='Cambridge' WHERE city='Boston'")
                    database.execute("DELETE FROM ecommerce.customers WHERE city='Delete me'")
                    database.execute("""INSERT INTO ecommerce.customers (customer_id,customer_unique_id,city)
                        VALUES ('00000000000000000000000000000003','person','New customer')""")
                assert snapshot.execute("SELECT customer_id,city FROM ecommerce.customers ORDER BY 1").fetchall() == before
            records = database.execute("SELECT data FROM pg_logical_slot_get_changes(%s,NULL,NULL)", (slot,)).fetchall()
            changes = [r[0] for r in records if r[0].startswith("table ecommerce.customers:")]
            assert len(changes) == 3
            assert [s.split(": ")[1] for s in changes] == ["UPDATE", "DELETE", "INSERT"]
            assert "city[character varying]:'Cambridge'" in changes[0].split("new-tuple:")[1]
            assert database.execute("SELECT city FROM ecommerce.customers ORDER BY customer_id").fetchall() == [("Cambridge",), ("New customer",)]
        finally:
            # End the exported snapshot before dropping its slot.
            replication.close()
            database.execute("SELECT pg_drop_replication_slot(%s)", (slot,))


@pytest.mark.integration
def test_failure_after_first_table_rolls_back_whole_file_then_retry_succeeds(database):
    initialize_raw(database)
    rows = [customer_row(), ["I", "order_payments", "ecommerce", "o1:1", "o1",
        "1", "credit_card", "1", "invalid-number",
        "2026-01-01T00:00:00Z", "0/12345", "2", "2026-01-01T00:00:00Z"]]
    body = csv_text(rows)
    with pytest.raises(psycopg.Error):
        load_local(database, parse_csv(body, "multi.csv"), "multi.csv")
    # customers was inserted before the order_payments COPY failed.
    assert database.execute('SELECT count(*) FROM "raw".customers').fetchone()[0] == 0
    assert database.execute('SELECT count(*) FROM "raw".loaded_files').fetchone()[0] == 0
    database.commit()
    rows[1][8] = "100"
    body = csv_text(rows)
    assert load_local(database, parse_csv(body, "multi.csv"), "multi.csv") == "loaded"
    assert load_local(database, parse_csv(body, "multi.csv"), "multi.csv") == "already_loaded"
    assert database.execute('SELECT count(*) FROM "raw".customers').fetchone()[0] == 1
    assert database.execute('SELECT count(*) FROM "raw".order_payments').fetchone()[0] == 1
