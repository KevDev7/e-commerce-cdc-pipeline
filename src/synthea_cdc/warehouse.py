"""Atomic local raw loads; the same raw schema is used by the Redshift loader."""
from psycopg import sql

from synthea_cdc.events import RAW_METADATA, source_columns
from synthea_cdc.seed import TABLES

TYPES = {
    "birth_date": "date", "death_date": "date", "started_at": "timestamptz", "ended_at": "timestamptz",
    "posted_at": "timestamptz", "updated_at": "timestamptz", "total_claim_cost": "numeric(14,2)",
    "outstanding_primary": "numeric(14,2)", "outstanding_secondary": "numeric(14,2)",
    "outstanding_patient": "numeric(14,2)", "amount": "numeric(14,2)", "payments": "numeric(14,2)",
    "adjustments": "numeric(14,2)", "transfers": "numeric(14,2)", "outstanding": "numeric(14,2)",
}


def raw_ddl():
    statements = ['CREATE SCHEMA IF NOT EXISTS "raw"', """CREATE TABLE IF NOT EXISTS "raw".loaded_files
        (source_file varchar(2048) NOT NULL, content_sha256 varchar(64) NOT NULL,
         loaded_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP, row_count bigint NOT NULL)"""]
    for table in TABLES:
        fields = [f"{name} {TYPES.get(name, 'varchar(256)')}" for name in source_columns(table)]
        fields += ["_event_id varchar(64) NOT NULL", "_op varchar(1) NOT NULL", "_source_lsn varchar(128)",
                   "_source_order numeric(35,0) NOT NULL", "_commit_at timestamptz NOT NULL",
                   "_is_snapshot boolean NOT NULL", "_source_file varchar(2048) NOT NULL"]
        statements.append(f'CREATE TABLE IF NOT EXISTS "raw".{table} (' + ",".join(fields) + ")")
    return statements


def initialize_raw(connection):
    for statement in raw_ddl():
        connection.execute(statement)
    connection.commit()


def load_local(connection, events, source_file, content_sha256):
    """Reference load for PostgreSQL integration tests. No AWS behavior is mocked."""
    with connection.transaction():
        # Serialize loaders; the ledger and raw records commit together.
        connection.execute("LOCK TABLE raw.loaded_files IN EXCLUSIVE MODE")
        previous = connection.execute("SELECT content_sha256 FROM raw.loaded_files WHERE source_file=%s", (source_file,)).fetchone()
        if previous:
            if previous[0] != content_sha256:
                raise ValueError("A previously loaded source file changed; inspect it before continuing")
            return "already_loaded"
        for table in TABLES:
            batch = [event.values for event in events if event.table == table]
            if not batch:
                continue
            temporary = sql.Identifier("incoming_" + table)
            connection.execute(sql.SQL("CREATE TEMP TABLE {} (LIKE raw.{}) ON COMMIT DROP").format(temporary, sql.Identifier(table)))
            columns = source_columns(table) + list(RAW_METADATA)
            query = sql.SQL("COPY {} ({}) FROM STDIN").format(temporary, sql.SQL(",").join(map(sql.Identifier, columns)))
            with connection.cursor().copy(query) as copy:
                for values in batch:
                    copy.write_row(values)
            connection.execute(sql.SQL("INSERT INTO raw.{} SELECT i.* FROM {} i WHERE NOT EXISTS (SELECT 1 FROM raw.{} r WHERE r._event_id=i._event_id)").format(
                sql.Identifier(table), temporary, sql.Identifier(table)))
        connection.execute("INSERT INTO raw.loaded_files (source_file,content_sha256,row_count) VALUES (%s,%s,%s)",
                           (source_file,content_sha256,len(events)))
    return "loaded"
