"""Inject a failure in the actual Redshift loader immediately before its ledger write."""
import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
from synthea_cdc.cloud_load import aws_session, load_file, warehouse_connection
from synthea_cdc.events import parse_csv
from synthea_cdc.seed import TABLES
from synthea_cdc.warehouse import raw_ddl


class InjectedFailure(RuntimeError):
    pass


class FailBeforeLedger:
    """Test-only connection proxy; all COPY/INSERT operations reach the real database."""
    def __init__(self, connection):
        self.connection = connection
        self.inserted_tables = []
        self.triggered = False

    def cursor(self):
        owner = self
        cursor = self.connection.cursor()

        class Cursor:
            def execute(self, statement, *args):
                if statement.startswith('INSERT INTO "raw".loaded_files'):
                    owner.triggered = True
                    raise InjectedFailure("Deliberate failure after raw INSERTs, before ledger/commit")
                result = cursor.execute(statement, *args)
                if statement.startswith('INSERT INTO "raw".'):
                    owner.inserted_tables.append(statement.split()[2])
                return result

            def __getattr__(self, name):
                return getattr(cursor, name)

        return Cursor()

    def __getattr__(self, name):
        return getattr(self.connection, name)


def counts(connection):
    cursor = connection.cursor()
    try:
        result = {}
        for table in TABLES:
            cursor.execute(f'SELECT count(*),count(DISTINCT _event_id) FROM "raw".{table}')
            result[table] = list(cursor.fetchone())
        cursor.execute('SELECT count(*) FROM "raw".loaded_files')
        result['loaded_files'] = cursor.fetchone()[0]
        connection.commit()
        return result
    finally:
        cursor.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("key", help="One actual, as-yet-unloaded CDC S3 key containing new events")
    args = parser.parse_args()
    if not (ROOT / '.env.cloud').exists():
        raise RuntimeError('No active cloud environment; provision an authorized demo first')
    load_dotenv(ROOT / '.env.cloud', override=True)
    prefix = os.environ.get("CAPTURE_PREFIX", "capture-v1") + "/cdc/"
    if not args.key.startswith(prefix) or not args.key.endswith('.csv'):
        raise ValueError("Select an actual CDC file from this capture prefix")
    bucket = os.environ["S3_BUCKET"]
    s3 = aws_session().client("s3")
    source = f"s3://{bucket}/{args.key}"
    events = parse_csv(s3.get_object(Bucket=bucket, Key=args.key)['Body'].read().decode(), source)
    if not events:
        raise ValueError("An empty file cannot demonstrate recovery")
    connection = warehouse_connection()
    try:
        cursor = connection.cursor()
        for statement in raw_ddl():
            cursor.execute(statement)
        connection.commit()
        cursor.execute('SELECT count(*) FROM "raw".loaded_files WHERE source_file=%s', (source,))
        if cursor.fetchone()[0]:
            raise ValueError("File already loaded; choose an unseen file")
        # Establish the expected number of new event identities before injection.
        expected = {table: 0 for table in TABLES}
        for table in TABLES:
            ids = [e.values[-7] for e in events if e.table == table]
            if ids:
                placeholders = ','.join(['%s'] * len(ids))
                cursor.execute(f'SELECT count(DISTINCT _event_id) FROM "raw".{table} WHERE _event_id IN ({placeholders})', tuple(ids))
                expected[table] = len(ids) - cursor.fetchone()[0]
        connection.commit()
        if not sum(expected.values()):
            raise ValueError("All events already exist; this would not test rollback of new rows")
        before = counts(connection)
        fault = FailBeforeLedger(connection)
        try:
            load_file(fault, s3, bucket, args.key, os.environ["REDSHIFT_COPY_ROLE"])
        except InjectedFailure:
            pass
        if not fault.triggered or not fault.inserted_tables:
            raise AssertionError("Injection point was not exercised after raw writes")
        after_failure = counts(connection)
        assert after_failure == before, (before, after_failure)
        load_file(connection, s3, bucket, args.key, os.environ["REDSHIFT_COPY_ROLE"])
        after_retry = counts(connection)
        for table in TABLES:
            assert after_retry[table] == [x + expected[table] for x in before[table]]
        assert after_retry['loaded_files'] == before['loaded_files'] + 1
        assert load_file(connection, s3, bucket, args.key, os.environ["REDSHIFT_COPY_ROLE"]) == 0
        assert counts(connection) == after_retry
        result = dict(source_key=args.key, inserted_tables_before_failure=fault.inserted_tables,
                      before=before, after_failure=after_failure, after_retry=after_retry,
                      second_retry_unchanged=True)
        (ROOT / 'data/load-failure.json').write_text(json.dumps(result, indent=2))
        print(json.dumps(result, indent=2))
    finally:
        connection.close()


if __name__ == '__main__':
    main()
