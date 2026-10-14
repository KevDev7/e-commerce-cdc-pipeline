"""Inject a failure in the actual Redshift loader immediately before its ledger write."""
import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
from olist_cdc.cloud_load import aws_session, load_file, validate_raw_schema, warehouse_connection
from olist_cdc.events import parse_csv
from olist_cdc.schema import SOURCE_COLUMNS
from olist_cdc.warehouse import raw_ddl


class InjectedFailure(RuntimeError):
    pass


class FailBeforeLedger:
    """Test-only connection proxy; all COPY/INSERT operations reach the real database."""
    def __init__(self, connection):
        self.connection = connection
        self.copied_tables = []
        self.triggered = False

    def cursor(self):
        owner = self
        cursor = self.connection.cursor()

        class Cursor:
            def execute(self, statement, *args):
                if statement.startswith('INSERT INTO "raw".loaded_files'):
                    owner.triggered = True
                    raise InjectedFailure("Deliberate failure after raw COPYs, before ledger/commit")
                result = cursor.execute(statement, *args)
                if statement.startswith('COPY "raw".'):
                    owner.copied_tables.append(statement.split()[1])
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
        for table in SOURCE_COLUMNS:
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
    parser.add_argument("key", help="One actual, as-yet-unloaded CDC S3 key")
    args = parser.parse_args()
    if not (ROOT / '.env.cloud').exists():
        raise RuntimeError('No active cloud environment; provision an authorized demo first')
    load_dotenv(ROOT / '.env.cloud', override=True)
    prefix = os.environ.get("CAPTURE_PREFIX", "raw") + "/cdc/"
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
        validate_raw_schema(cursor)
        connection.commit()
        cursor.execute('SELECT count(*) FROM "raw".loaded_files WHERE source_file=%s', (source,))
        if cursor.fetchone()[0]:
            raise ValueError("File already loaded; choose an unseen file")
        # Raw retains every row from this new file, including repeated identities.
        expected_rows = {table: sum(e.table == table for e in events) for table in SOURCE_COLUMNS}
        expected_unique = {}
        for table in SOURCE_COLUMNS:
            identities = {e.values[-6] for e in events if e.table == table}
            if identities:
                placeholders = ','.join(['%s'] * len(identities))
                cursor.execute(f'SELECT count(DISTINCT _event_id) FROM "raw".{table} WHERE _event_id IN ({placeholders})', tuple(identities))
                expected_unique[table] = len(identities) - cursor.fetchone()[0]
            else:
                expected_unique[table] = 0
        connection.commit()
        before = counts(connection)
        fault = FailBeforeLedger(connection)
        try:
            load_file(fault, s3, bucket, args.key, os.environ["REDSHIFT_COPY_ROLE"])
        except InjectedFailure:
            pass
        if not fault.triggered or not fault.copied_tables:
            raise AssertionError("Injection point was not exercised after raw writes")
        after_failure = counts(connection)
        assert after_failure == before, (before, after_failure)
        load_file(connection, s3, bucket, args.key, os.environ["REDSHIFT_COPY_ROLE"])
        after_retry = counts(connection)
        for table in SOURCE_COLUMNS:
            assert after_retry[table] == [before[table][0] + expected_rows[table],
                                          before[table][1] + expected_unique[table]]
        assert after_retry['loaded_files'] == before['loaded_files'] + 1
        assert load_file(connection, s3, bucket, args.key, os.environ["REDSHIFT_COPY_ROLE"]) == 0
        assert counts(connection) == after_retry
        result = dict(source_key=args.key, copied_tables_before_failure=fault.copied_tables,
                      before=before, after_failure=after_failure, after_retry=after_retry,
                      second_retry_unchanged=True)
        (ROOT / 'data/load-failure.json').write_text(json.dumps(result, indent=2))
        print(json.dumps(result, indent=2))
    finally:
        connection.close()


if __name__ == '__main__':
    main()
