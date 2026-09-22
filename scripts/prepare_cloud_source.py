"""Load the RDS source and grant the DMS reader only capture privileges."""
import os
from tempfile import TemporaryDirectory
from pathlib import Path

from dotenv import load_dotenv
from psycopg import sql

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env.cloud", override=True)
from olist_cdc.db import connect, initialize
from olist_cdc.seed import SHA256, download, load
from olist_cdc.cloud_load import aws_session

# Only a temporary seed ZIP touches disk; both databases run in AWS.
with TemporaryDirectory(prefix="olist-seed-") as temporary:
    archive = Path(temporary) / "source.zip"
    download(archive)
    aws_session().client("s3").upload_file(str(archive), os.environ["S3_BUCKET"],
        f"source/olist-v2/{SHA256}.zip", ExtraArgs={"ServerSideEncryption": "AES256"})
    with connect() as connection:
        initialize(connection)
        print(load(connection, archive))
        if not connection.execute("SELECT 1 FROM pg_roles WHERE rolname='dms_reader'").fetchone():
            connection.execute(sql.SQL("CREATE USER dms_reader PASSWORD {}").format(sql.Literal(os.environ["DMS_PASSWORD"])))
        connection.execute("GRANT rds_replication TO dms_reader")
        connection.execute("GRANT CONNECT ON DATABASE olist TO dms_reader")
        connection.execute("GRANT USAGE ON SCHEMA ecommerce TO dms_reader")
        connection.execute("GRANT SELECT ON ALL TABLES IN SCHEMA ecommerce TO dms_reader")
        connection.commit()
        print("RDS seed ready; DMS reader granted SELECT and logical replication")
