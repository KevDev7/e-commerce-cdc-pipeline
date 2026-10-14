"""Atomic local raw loads; the same raw schema is used by the Redshift loader."""
from psycopg import sql

from olist_cdc.schema import RAW_METADATA, SOURCE_COLUMNS, TYPES, source_columns


def raw_ddl():
    statements = ['CREATE SCHEMA IF NOT EXISTS "raw"', """CREATE TABLE IF NOT EXISTS "raw".loaded_files
        (source_file varchar(2048) NOT NULL,
         loaded_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP, row_count bigint NOT NULL)"""]
    for table in SOURCE_COLUMNS:
        fields = [f"{name} {TYPES.get(name, 'varchar(256)')}" for name in source_columns(table)]
        fields += ["_event_id varchar(128) NOT NULL", "_op varchar(1) NOT NULL",
                   "_source_order numeric(35,0) NOT NULL", "_commit_at timestamptz NOT NULL",
                   "_is_snapshot boolean NOT NULL", "_source_file varchar(2048) NOT NULL"]
        statements.append(f'CREATE TABLE IF NOT EXISTS "raw".{table} (' + ",".join(fields) + ")")
    return statements


def initialize_raw(connection):
    for statement in raw_ddl():
        connection.execute(statement)
    connection.commit()


def load_local(connection, events, source_file):
    """Reference load for PostgreSQL integration tests. No AWS behavior is mocked."""
    with connection.transaction():
        # Serialize loaders; the ledger and raw records commit together.
        connection.execute("LOCK TABLE raw.loaded_files IN EXCLUSIVE MODE")
        previous = connection.execute("SELECT 1 FROM raw.loaded_files WHERE source_file=%s", (source_file,)).fetchone()
        if previous:
            return "already_loaded"
        for table in SOURCE_COLUMNS:
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
            connection.execute(sql.SQL("INSERT INTO raw.{} SELECT * FROM {}").format(
                sql.Identifier(table), temporary))
        connection.execute("INSERT INTO raw.loaded_files (source_file,row_count) VALUES (%s,%s)",
                           (source_file,len(events)))
    return "loaded"
