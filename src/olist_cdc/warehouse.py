"""Atomic local raw loads; the same raw schema is used by the Redshift loader."""
from psycopg import sql

from olist_cdc.events import RAW_METADATA, source_columns
from olist_cdc.seed import TABLES

TYPES = {
    **{name: "timestamp" for name in ("purchased_at", "approved_at", "carrier_delivered_at",
        "customer_delivered_at", "estimated_delivery_at", "shipping_limit_at")},
    "updated_at": "timestamptz",
    **{name: "numeric(14,2)" for name in ("price", "freight_value", "payment_value")},
    **{name: "integer" for name in ("order_item_id", "payment_sequential", "payment_installments")},
}


def raw_ddl():
    statements = ['CREATE SCHEMA IF NOT EXISTS "raw"', """CREATE TABLE IF NOT EXISTS "raw".loaded_files
        (source_file varchar(2048) NOT NULL,
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


def load_local(connection, events, source_file):
    """Reference load for PostgreSQL integration tests. No AWS behavior is mocked."""
    with connection.transaction():
        # Serialize loaders; the ledger and raw records commit together.
        connection.execute("LOCK TABLE raw.loaded_files IN EXCLUSIVE MODE")
        previous = connection.execute("SELECT 1 FROM raw.loaded_files WHERE source_file=%s", (source_file,)).fetchone()
        if previous:
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
        connection.execute("INSERT INTO raw.loaded_files (source_file,row_count) VALUES (%s,%s)",
                           (source_file,len(events)))
    return "loaded"
