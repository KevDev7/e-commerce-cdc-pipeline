import csv
import hashlib
import io
from pathlib import Path
import urllib.request
import zipfile

from psycopg import sql
from psycopg.types.json import Jsonb

URL = "https://synthetichealth.github.io/synthea-sample-data/downloads/latest/synthea_sample_data_csv_latest.zip"
# Pin the inspected sample, because the upstream "latest" URL can change.
SHA256 = "d61417b551e5b0997c33851b339c157421751f0ea68c18ea686ceb1850907c35"
TABLES = {
    "patients": {
        "patient_id": "Id", "birth_date": "BIRTHDATE", "death_date": "DEATHDATE",
        "gender": "GENDER", "city": "CITY", "state": "STATE", "postal_code": "ZIP",
    },
    "encounters": {
        "encounter_id": "Id", "patient_id": "PATIENT", "started_at": "START",
        "ended_at": "STOP", "encounter_class": "ENCOUNTERCLASS", "total_claim_cost": "TOTAL_CLAIM_COST",
    },
    "claims": {
        "claim_id": "Id", "patient_id": "PATIENTID", "encounter_id": "APPOINTMENTID",
        "status": "STATUS1", "outstanding_primary": "OUTSTANDING1",
        "outstanding_secondary": "OUTSTANDING2", "outstanding_patient": "OUTSTANDINGP",
    },
    "claim_transactions": {
        "transaction_id": "ID", "claim_id": "CLAIMID", "patient_id": "PATIENTID",
        "transaction_type": "TYPE", "amount": "AMOUNT", "posted_at": "FROMDATE",
        "payments": "PAYMENTS", "adjustments": "ADJUSTMENTS", "transfers": "TRANSFERS",
        "outstanding": "OUTSTANDING",
    },
}


def download(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        temporary = path.with_suffix(".partial")
        try:
            with urllib.request.urlopen(URL, timeout=60) as response, temporary.open("wb") as output:
                while chunk := response.read(1024 * 1024):
                    output.write(chunk)
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != SHA256:
        raise ValueError(f"Sample checksum changed ({actual}); inspect the new sample before updating the pin.")


def load(connection, archive: Path):
    archive_hash = hashlib.sha256(archive.read_bytes()).hexdigest()
    if archive_hash != SHA256:
        raise ValueError("Only the inspected, pinned Synthea archive is accepted by this seed loader.")
    with connection.transaction():
        connection.execute("SET LOCAL TIME ZONE 'UTC'")
        connection.execute("SELECT pg_advisory_xact_lock(8174201)")
        previous = connection.execute(
            "SELECT row_counts FROM project_meta.seed_runs WHERE archive_sha256=%s", (archive_hash,)
        ).fetchone()
        if previous:
            return {"status": "already_loaded", "row_counts": previous[0]}
        for table in TABLES:
            count = connection.execute(sql.SQL("SELECT count(*) FROM healthcare.{}").format(sql.Identifier(table))).fetchone()[0]
            if count:
                raise ValueError("Source tables are not empty. Seed once into a fresh database; no records were changed.")
        counts = {}
        with zipfile.ZipFile(archive) as zipped:
            for table, columns in TABLES.items():
                filename = "claims_transactions.csv" if table == "claim_transactions" else table + ".csv"
                matches = [name for name in zipped.namelist() if Path(name).name == filename]
                if len(matches) != 1:
                    raise ValueError(f"Expected exactly one {filename}")
                query = sql.SQL("COPY healthcare.{} ({}) FROM STDIN").format(
                    sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, columns))
                )
                counts[table] = 0
                with zipped.open(matches[0]) as raw, io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as stream:
                    reader = csv.DictReader(stream)
                    missing = set(columns.values()) - set(reader.fieldnames or [])
                    if missing:
                        raise ValueError(f"{filename} missing columns: {sorted(missing)}")
                    with connection.cursor().copy(query) as copy:
                        for row in reader:
                            values = [row[source] or None for source in columns.values()]
                            copy.write_row(values)
                            counts[table] += 1
        connection.execute("INSERT INTO project_meta.seed_runs (archive_sha256,row_counts) VALUES (%s,%s)",
                           (archive_hash, Jsonb(counts)))
    return {"status": "loaded", "row_counts": counts}
