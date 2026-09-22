"""Exercise dbt on real seed records using an explicitly labeled snapshot fixture.

This is not CDC extraction. Actual cloud capture is AWS DMS and awaits deployment.
Existing warehouse state is retained; a changed fixture must use a fresh test DB.
"""
import csv
from datetime import datetime, timezone
import hashlib
import io
import os
from pathlib import Path
import shutil
import subprocess

from psycopg import sql

from synthea_cdc.db import ROOT, connect
from synthea_cdc.events import NULL, parse_csv, source_columns
from synthea_cdc.seed import TABLES
from synthea_cdc.warehouse import initialize_raw, load_local


def main():
    name = os.environ.get("WAREHOUSE_DATABASE", "synthea_warehouse")
    if os.environ.get("POSTGRES_HOST", "127.0.0.1") not in ("127.0.0.1", "localhost"):
        raise ValueError("This fixture builder is local-only")
    with connect("postgres", autocommit=True) as admin:
        if not admin.execute("SELECT 1 FROM pg_database WHERE datname=%s", (name,)).fetchone():
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    with connect(name) as target:
        initialize_raw(target)
        if target.execute("SELECT count(*) FROM raw.loaded_files").fetchone()[0]:
            print("Existing local fixture retained; rebuilding models from captured raw data.")
            target.commit()
        else:
            target.commit()
            observed = datetime.now(timezone.utc).isoformat()
            with connect() as source:
                source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                for table in TABLES:
                    rows = source.execute(sql.SQL("SELECT {} FROM healthcare.{} ORDER BY 1").format(
                        sql.SQL(",").join(map(sql.Identifier,source_columns(table))), sql.Identifier(table)))
                    stream = io.StringIO(newline=""); writer = csv.writer(stream)
                    for row in rows:
                        writer.writerow(["I", *[NULL if value is None else str(value) for value in row], "", "", observed])
                    text = stream.getvalue(); file = f"local-snapshot/{table}/LOAD00000001.csv"
                    events = parse_csv(text,file,snapshot_table=table)
                    result=load_local(target,events,file,hashlib.sha256(text.encode()).hexdigest())
                    print(f"{table}: {len(events)} rows, {result}")
    shutil.copyfile(ROOT/"dbt/profiles.yml.example",ROOT/"dbt/profiles.yml")
    subprocess.run([str(ROOT/".venv/bin/dbt"),"build","--project-dir",str(ROOT/"dbt"),"--profiles-dir",str(ROOT/"dbt"),"--target","local"],check=True,
                   env={**os.environ,"DBT_SEND_ANONYMOUS_USAGE_STATS":"false"})


if __name__ == "__main__":
    main()
