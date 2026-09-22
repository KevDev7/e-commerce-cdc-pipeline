"""Small, explicit business transactions. PostgreSQL produces the WAL, not Python."""
import re
from uuid import UUID, uuid5

import psycopg

NAMESPACE = UUID("92c98e48-6a65-4ab7-af6a-f5f52f4bf15c")
PHASES = ("open", "bill", "pay", "correct", "create-delete-test", "delete-test", "rollback-test")


def ids(scenario):
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,50}", scenario):
        raise ValueError("Scenario names must be 1–50 letters, digits, underscores or hyphens")
    return {kind: uuid5(NAMESPACE, f"{scenario}:{kind}")
            for kind in ("patient", "encounter", "claim", "charge", "payment", "delete-patient", "delete-encounter")}


def run_phase(connection, scenario, phase):
    if phase not in PHASES:
        raise ValueError(f"Unknown phase: {phase}")
    key = ids(scenario)
    with connection.transaction():
        connection.execute("SELECT pg_advisory_xact_lock(8174202)")
        completed = {r[0] for r in connection.execute(
            "SELECT phase FROM project_meta.simulation_steps WHERE scenario=%s", (scenario,)
        )}
        if phase in completed:
            return {"scenario": scenario, "phase": phase, "status": "already_completed"}
        index = PHASES.index(phase)
        if index and PHASES[index - 1] not in completed:
            raise ValueError(f"Run phase {PHASES[index - 1]} before {phase}")
        if phase == "open":
            connection.execute("""INSERT INTO healthcare.patients
                (patient_id,birth_date,gender,city,state,postal_code)
                VALUES (%s,'1990-01-01','F','Boston','Massachusetts','02108')""", (key["patient"],))
            connection.execute("""INSERT INTO healthcare.encounters
                (encounter_id,patient_id,started_at,encounter_class,total_claim_cost)
                VALUES (%s,%s,CURRENT_TIMESTAMP,'ambulatory',100)""", (key["encounter"], key["patient"]))
            connection.execute("""INSERT INTO healthcare.claims
                (claim_id,patient_id,encounter_id,status,outstanding_primary,outstanding_secondary,outstanding_patient)
                VALUES (%s,%s,%s,'OPEN',100,0,0)""", (key["claim"],key["patient"],key["encounter"]))
            connection.execute("""INSERT INTO healthcare.claim_transactions
                (transaction_id,claim_id,patient_id,transaction_type,amount,posted_at,payments,adjustments,transfers,outstanding)
                VALUES (%s,%s,%s,'CHARGE',100,CURRENT_TIMESTAMP,0,0,0,100)""", (key["charge"],key["claim"],key["patient"]))
        elif phase == "bill":
            connection.execute("UPDATE healthcare.encounters SET ended_at=CURRENT_TIMESTAMP WHERE encounter_id=%s", (key["encounter"],))
            connection.execute("UPDATE healthcare.claims SET status='BILLED' WHERE claim_id=%s", (key["claim"],))
        elif phase == "pay":
            connection.execute("""INSERT INTO healthcare.claim_transactions
                (transaction_id,claim_id,patient_id,transaction_type,amount,posted_at,payments,adjustments,transfers,outstanding)
                VALUES (%s,%s,%s,'PAYMENT',100,CURRENT_TIMESTAMP,100,0,0,0)""", (key["payment"],key["claim"],key["patient"]))
            connection.execute("UPDATE healthcare.claims SET status='CLOSED',outstanding_primary=0 WHERE claim_id=%s", (key["claim"],))
        elif phase == "correct":
            connection.execute("UPDATE healthcare.patients SET city='Cambridge',postal_code='02139' WHERE patient_id=%s", (key["patient"],))
        elif phase == "create-delete-test":
            connection.execute("""INSERT INTO healthcare.patients
                (patient_id,birth_date,gender,city,state,postal_code)
                VALUES (%s,'1990-01-01','F','Disposable test record','TEST','00000')""", (key["delete-patient"],))
            connection.execute("""INSERT INTO healthcare.encounters
                (encounter_id,patient_id,started_at,encounter_class,total_claim_cost)
                VALUES (%s,%s,CURRENT_TIMESTAMP,'test',0)""", (key["delete-encounter"],key["delete-patient"]))
        elif phase == "delete-test":
            # Delete only the disposable records created by this scenario.
            connection.execute("DELETE FROM healthcare.encounters WHERE encounter_id=%s", (key["delete-encounter"],))
            connection.execute("DELETE FROM healthcare.patients WHERE patient_id=%s", (key["delete-patient"],))
        elif phase == "rollback-test":
            with connection.transaction():
                connection.execute("UPDATE healthcare.claims SET status='ROLLBACK_SENTINEL' WHERE claim_id=%s", (key["claim"],))
                raise psycopg.Rollback()
        connection.execute("INSERT INTO project_meta.simulation_steps (scenario,phase) VALUES (%s,%s)", (scenario,phase))
    return {"scenario": scenario, "phase": phase, "status": "completed"}

