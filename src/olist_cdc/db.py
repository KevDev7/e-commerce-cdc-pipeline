import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")


def connect(database=None, *, autocommit=False):
    return psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "127.0.0.1"),
        port=os.environ.get("POSTGRES_PORT", "55432"),
        user=os.environ.get("POSTGRES_USER", "cdc_owner"),
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=database or os.environ.get("POSTGRES_DB", "olist"),
        sslmode=os.environ.get("POSTGRES_SSLMODE", "prefer"),
        autocommit=autocommit,
    )


def initialize(connection):
    connection.execute((ROOT / "sql/source.sql").read_text())
    connection.commit()
