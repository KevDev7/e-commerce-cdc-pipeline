import csv
import hashlib
import io
import zipfile

import psycopg
import pytest

from synthea_cdc import seed

PATIENT = "00000000-0000-0000-0000-000000000001"
ENCOUNTER = "00000000-0000-0000-0000-000000000002"
CLAIM = "00000000-0000-0000-0000-000000000003"
TRANSACTION = "00000000-0000-0000-0000-000000000004"


@pytest.fixture
def archive(tmp_path, monkeypatch):
    def make(*, orphan=False):
        rows = {
            "patients": [PATIENT, "1990-01-01", "", "F", "Boston", "Massachusetts", "02108"],
            "encounters": [ENCOUNTER, PATIENT, "2020-01-01T12:00:00Z", "2020-01-01T12:30:00Z", "ambulatory", "100.25"],
            "claims": [CLAIM, PATIENT, ENCOUNTER, "CLOSED", "", "", ""],
            "claim_transactions": [TRANSACTION, PATIENT if orphan else CLAIM, PATIENT, "CHARGE", "100.25", "2020-01-01T12:00:00Z", "", "", "", ""],
        }
        path = tmp_path / "synthea.zip"
        with zipfile.ZipFile(path, "w") as z:
            for table, mapping in seed.TABLES.items():
                stream = io.StringIO(newline="")
                writer = csv.writer(stream)
                writer.writerow(mapping.values()); writer.writerow(rows[table])
                filename = "claims_transactions" if table == "claim_transactions" else table
                z.writestr("csv/" + filename + ".csv", stream.getvalue())
        monkeypatch.setattr(seed, "SHA256", hashlib.sha256(path.read_bytes()).hexdigest())
        return path
    return make


@pytest.mark.integration
def test_seed_preserves_nulls_and_retries_without_resetting_source(database, archive):
    path = archive()
    first = seed.load(database, path)
    assert first["row_counts"] == dict.fromkeys(seed.TABLES, 1)
    database.execute("UPDATE healthcare.patients SET city='Cambridge'")
    database.commit()
    assert seed.load(database, path)["status"] == "already_loaded"
    assert database.execute("SELECT city FROM healthcare.patients").fetchone()[0] == "Cambridge"
    assert database.execute("SELECT outstanding_primary FROM healthcare.claims").fetchone()[0] is None


@pytest.mark.integration
def test_invalid_relationship_rolls_back_entire_seed(database, archive):
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        seed.load(database, archive(orphan=True))
    assert database.execute("SELECT count(*) FROM healthcare.patients").fetchone()[0] == 0
    assert database.execute("SELECT count(*) FROM project_meta.seed_runs").fetchone()[0] == 0


def test_changed_upstream_archive_is_rejected(tmp_path):
    path = tmp_path / "unexpected.zip"
    path.write_bytes(b"unexpected archive")
    with pytest.raises(ValueError, match="checksum changed"):
        seed.download(path)
