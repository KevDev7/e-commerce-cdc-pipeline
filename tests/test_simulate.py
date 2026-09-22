import pytest

from synthea_cdc.simulate import PHASES, ids, run_phase


@pytest.mark.integration
def test_billing_lifecycle_and_retry(database):
    scenario = "lifecycle-test"
    key = ids(scenario)
    for phase in PHASES:
        assert run_phase(database, scenario, phase)["status"] == "completed"
    for phase in PHASES:
        assert run_phase(database, scenario, phase)["status"] == "already_completed"
    assert database.execute("SELECT status,outstanding_primary FROM healthcare.claims WHERE claim_id=%s", (key["claim"],)).fetchone() == ("CLOSED", 0)
    assert database.execute("SELECT sum(payments),sum(amount) FILTER (WHERE transaction_type='CHARGE') FROM healthcare.claim_transactions").fetchone() == (100, 100)
    assert database.execute("SELECT city FROM healthcare.patients").fetchall() == [("Cambridge",)]
    assert database.execute("SELECT count(*) FROM healthcare.encounters").fetchone()[0] == 1
    assert database.execute("SELECT count(*) FROM healthcare.claim_transactions").fetchone()[0] == 2


@pytest.mark.integration
def test_wal_contains_committed_changes_in_order_but_not_rollback(database):
    database.autocommit = True
    slot = "test_" + ids(database.info.dbname)["patient"].hex
    database.execute("SELECT * FROM pg_create_logical_replication_slot(%s,'test_decoding')", (slot,))
    try:
        for phase in PHASES:
            run_phase(database, "wal-test", phase)
        records = [r[0] for r in database.execute("SELECT data FROM pg_logical_slot_get_changes(%s,NULL,NULL)", (slot,))]
        changes = [r for r in records if r.startswith("table healthcare.")]
        assert len(changes) == 13
        assert sum(": INSERT:" in r for r in changes) == 7
        assert sum(": UPDATE:" in r for r in changes) == 4
        assert sum(": DELETE:" in r for r in changes) == 2
        assert not any("ROLLBACK_SENTINEL" in r for r in records)
        claim_changes = [r for r in changes if r.startswith("table healthcare.claims:")]
        assert "'OPEN'" in claim_changes[0]
        assert "new-tuple:" in claim_changes[1] and "status[text]:'BILLED'" in claim_changes[1].split("new-tuple:")[1]
        assert "status[text]:'CLOSED'" in claim_changes[2].split("new-tuple:")[1]
        for phase in PHASES:
            run_phase(database, "wal-test", phase)
        assert not database.execute("SELECT data FROM pg_logical_slot_get_changes(%s,NULL,NULL)", (slot,)).fetchall()
    finally:
        database.execute("SELECT pg_drop_replication_slot(%s)", (slot,))


@pytest.mark.integration
def test_out_of_order_phase_does_not_create_data(database):
    with pytest.raises(ValueError, match="Run phase open"):
        run_phase(database, "bad-order", "bill")
    assert database.execute("SELECT count(*) FROM healthcare.claims").fetchone()[0] == 0
