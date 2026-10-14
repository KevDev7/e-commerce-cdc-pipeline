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
    assert event.values[-6] == "cdc:customers:1"


def test_missing_order_and_schema_changes_are_rejected():
    with pytest.raises(ValueError, match="change sequence"):
        parse_csv(csv_text([customer_row(sequence="")]), "bad.csv")
    with pytest.raises(ValueError, match="expected"):
        parse_csv(csv_text([customer_row()+["extra-column"]]), "bad.csv")


@pytest.mark.integration
def test_file_retry_is_skipped_but_redelivery_is_preserved_in_raw(database):
    initialize_raw(database)
    text = csv_text([customer_row()])
    events = parse_csv(text,"one.csv")
    assert load_local(database,events,"one.csv") == "loaded"
    assert load_local(database,events,"one.csv") == "already_loaded"
    assert load_local(database,parse_csv(text,"two.csv"),"two.csv") == "loaded"
    assert database.execute("SELECT count(*) FROM raw.customers").fetchone()[0] == 2
    database.commit()


@pytest.mark.integration
def test_failed_file_does_not_leave_partial_rows_or_a_checkpoint(database):
    import psycopg
    initialize_raw(database)
    rows = [customer_row(),customer_row(sequence="2",customer_id="p2")]
    rows[1][6] = "x" * 257  # Exceeds raw VARCHAR storage; not a source type check.
    events = parse_csv(csv_text(rows),"bad.csv")
    with pytest.raises(psycopg.Error):
        load_local(database,events,"bad.csv")
    assert database.execute("SELECT count(*) FROM raw.customers").fetchone()[0] == 0
    assert database.execute("SELECT count(*) FROM raw.loaded_files").fetchone()[0] == 0


@pytest.mark.integration
def test_readable_snapshot_identity_fits_long_composite_key(database):
    initialize_raw(database)
    order_id = 'a' * 32
    key = order_id + ':2147483647'
    timestamp = '2026-01-01T00:00:00Z'
    body = csv_text([['I', key, order_id, '2147483647', 'credit_card', '1', '10.00',
                      timestamp, '', '0', timestamp]])
    events = parse_csv(body, 'payments/LOAD.csv', snapshot_table='order_payments')
    identity = 'snapshot:order_payments:' + key
    assert events[0].values[-6] == identity and len(identity) > 64
    load_local(database, events, 'payments/LOAD.csv')
    assert database.execute('SELECT _event_id FROM raw.order_payments').fetchone() == (identity,)


def test_target_schema_alias_preserves_cdc_identity_and_operation():
    row = customer_row(sequence='123', operation='U')
    original = parse_csv(csv_text([row]), 'raw/cdc/test.csv')[0]
    row[2] = 'initial-load'
    renamed = parse_csv(csv_text([row]), 'raw/cdc/test.csv')[0]
    assert renamed == original
    assert renamed.values[-2] is False  # The schema label does not make this a snapshot.
    row[2] = 'unrelated-schema'
    with pytest.raises(ValueError, match='Unexpected schema'):
        parse_csv(csv_text([row]), 'raw/cdc/test.csv')


def test_repeated_event_ids_are_preserved_by_parser():
    rows = [customer_row(), customer_row(), customer_row(city='campinas')]
    events = parse_csv(csv_text(rows), 'repeated.csv')
    assert len(events) == 3
    assert len({e.values[-6] for e in events}) == 1
    assert [e.values[3] for e in events] == ['sao paulo', 'sao paulo', 'campinas']


def test_cdc_missing_keys_are_preserved_with_source_event_identity():
    for key in (NULL, ''):
        for operation in ('I', 'U', 'D'):
            body = csv_text([customer_row(customer_id=key, operation=operation)])
            parsed = parse_csv(body, 'missing-key.csv')[0]
            assert parsed.values[0] == (None if key == NULL else '')
            assert parsed.values[-6] == 'cdc:customers:1'
            assert parsed.values[-5] == operation


def test_missing_snapshot_keys_get_stable_distinct_file_line_identities():
    rows = [[r[0], *r[3:]] for r in [customer_row(customer_id=NULL),
                                    customer_row(customer_id=NULL), customer_row(customer_id='')]]
    body = csv_text(rows)
    parsed = parse_csv(body, 'customers/LOAD.csv', snapshot_table='customers')
    identities = [e.values[-6] for e in parsed]
    assert len(set(identities)) == 3
    assert all(i.startswith('snapshot-missing-key:customers:') and len(i) <= 128 for i in identities)
    assert parsed == parse_csv(body, 'customers/LOAD.csv', snapshot_table='customers')
    other = parse_csv(body, 'customers/OTHER.csv', snapshot_table='customers')
    assert not set(identities) & {e.values[-6] for e in other}
    assert [e.values[0] for e in parsed] == [None, None, '']
