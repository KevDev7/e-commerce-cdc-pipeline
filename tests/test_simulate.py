import pytest
from olist_cdc.simulate import PHASES, ids, run_phase


@pytest.mark.integration
def test_order_lifecycle_and_retry(database):
    key=ids('lifecycle-test')
    for phase in PHASES:
        assert run_phase(database,'lifecycle-test',phase)['status']=='completed'
    for phase in PHASES:
        assert run_phase(database,'lifecycle-test',phase)['status']=='already_completed'
    assert database.execute('SELECT status FROM ecommerce.orders WHERE order_id=%s',(key['order'],)).fetchone()==('delivered',)
    assert database.execute('SELECT sum(payment_value) FROM ecommerce.order_payments').fetchone()[0]==110
    assert database.execute('SELECT sum(price+freight_value) FROM ecommerce.order_items').fetchone()[0]==110
    assert database.execute('SELECT city FROM ecommerce.customers').fetchall()==[('campinas',)]
    assert database.execute('SELECT count(*) FROM ecommerce.orders').fetchone()[0]==1


@pytest.mark.integration
def test_wal_contains_committed_changes_in_order_but_not_rollback(database):
    database.autocommit=True
    slot='test_'+ids(database.info.dbname)['customer']
    database.execute("SELECT * FROM pg_create_logical_replication_slot(%s,'test_decoding')",(slot,))
    try:
        for phase in PHASES:run_phase(database,'wal-test',phase)
        records=[r[0] for r in database.execute('SELECT data FROM pg_logical_slot_get_changes(%s,NULL,NULL)',(slot,))]
        changes=[r for r in records if r.startswith('table ecommerce.')]
        assert len(changes)==14
        assert sum(': INSERT:' in r for r in changes)==8
        assert sum(': UPDATE:' in r for r in changes)==4
        assert sum(': DELETE:' in r for r in changes)==2
        assert not any('ROLLBACK_SENTINEL' in r for r in records)
        updates=[r.split('new-tuple:')[1] for r in changes if r.startswith('table ecommerce.orders: UPDATE:')]
        for text,status in zip(updates,('approved','shipped','delivered')):
            assert f"status[character varying]:'{status}'" in text
        for phase in PHASES:run_phase(database,'wal-test',phase)
        assert not database.execute('SELECT data FROM pg_logical_slot_get_changes(%s,NULL,NULL)',(slot,)).fetchall()
    finally:
        database.execute('SELECT pg_drop_replication_slot(%s)',(slot,))


@pytest.mark.integration
def test_out_of_order_phase_does_not_create_data(database):
    with pytest.raises(ValueError,match='Run phase open'):
        run_phase(database,'bad-order','approve')
    assert database.execute('SELECT count(*) FROM ecommerce.orders').fetchone()[0]==0
