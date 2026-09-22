"""Warehouse behavior tests use explicit DMS-format fixtures, not fake WAL claims."""
import hashlib
import os
import shutil
import subprocess

import pytest

from synthea_cdc.db import ROOT
from synthea_cdc.events import NULL, parse_csv, source_columns
from synthea_cdc.warehouse import initialize_raw, load_local
from test_events import csv_text


@pytest.mark.integration
def test_marts_handle_late_files_deletes_history_and_multiple_payments(database, tmp_path):
    initialize_raw(database)
    time0 = "2026-01-01T00:00:00Z"
    time1 = "2026-01-02T00:00:00Z"

    def row(table, data, sequence, op="I", timestamp=time0):
        values = [str(data.get(c, time0 if c=="updated_at" else NULL)) for c in source_columns(table)]
        return [op,table,"healthcare",*values,"0/ABC",str(sequence),timestamp]

    patient = dict(patient_id="p1",birth_date="1990-01-01",gender="F",city="Boston",state="MA",postal_code="02108")
    encounter = dict(encounter_id="e1",patient_id="p1",started_at=time0,encounter_class="ambulatory",total_claim_cost="100")
    claim = dict(claim_id="c1",patient_id="p1",encounter_id="e1",status="OPEN",outstanding_primary="100")
    charge = dict(transaction_id="t1",claim_id="c1",patient_id="p1",transaction_type="CHARGE",amount="100",posted_at=time0)
    payment = dict(transaction_id="t2",claim_id="c1",patient_id="p1",transaction_type="PAYMENT",payments="50",posted_at=time0)
    earlier = [row("patients",patient,1),row("patients",{**patient,"patient_id":"p2"},2),
               row("encounters",encounter,3),row("claims",claim,4),row("claim_transactions",charge,5),
               row("claim_transactions",payment,6),row("claim_transactions",{**payment,"transaction_id":"t3"},7),
               row("claims",{**claim,"status":"BILLED"},8,"U")]
    later = [row("claims",{**claim,"status":"CLOSED","outstanding_primary":"0"},9,"U",time1),
             row("patients",{**patient,"city":"Cambridge"},10,"U",time1),
             row("patients",{**patient,"patient_id":"p2"},11,"D",time1)]
    for name, rows in [("later.csv",later),("earlier.csv",earlier),("replayed.csv",earlier)]:
        text=csv_text(rows)
        load_local(database,parse_csv(text,name),name,hashlib.sha256(text.encode()).hexdigest())
    profiles = tmp_path/"profiles"; profiles.mkdir()
    shutil.copyfile(ROOT/"dbt/profiles.yml.example",profiles/"profiles.yml")
    result = subprocess.run([str(ROOT/".venv/bin/dbt"),"build","--project-dir",str(ROOT/"dbt"),
        "--profiles-dir",str(profiles),"--target","local"],capture_output=True,text=True,
        env={**os.environ,"WAREHOUSE_DATABASE":database.info.dbname,"DBT_SEND_ANONYMOUS_USAGE_STATS":"false",
             "DBT_TARGET_PATH":str(tmp_path/"target"),"DBT_LOG_PATH":str(tmp_path/"logs")},timeout=120)
    assert result.returncode==0, result.stdout[-8000:]+result.stderr[-2000:]
    assert database.execute("SELECT claim_id,status,charge_total,payment_total,financial_entry_count FROM analytics_marts.fct_claims").fetchall()==[("c1","CLOSED",100,100,3)]
    assert database.execute("SELECT patient_id,city FROM analytics_marts.dim_patients").fetchall()==[("p1","Cambridge")]
    history=database.execute("SELECT city,is_current FROM analytics_marts.dim_patient_history WHERE patient_id='p1' ORDER BY source_order_from").fetchall()
    assert history==[("Boston",False),("Cambridge",True)]
    assert database.execute("SELECT count(*) FROM analytics_marts.dim_patient_history WHERE patient_id='p2' AND is_current").fetchone()[0]==0
    assert database.execute("SELECT count(*) FROM raw.claim_transactions").fetchone()[0]==3
