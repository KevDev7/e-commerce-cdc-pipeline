import os
from uuid import uuid4

import pytest
from psycopg import sql

from synthea_cdc.db import connect, initialize


def pytest_addoption(parser):
    parser.addoption("--integration", action="store_true", help="Run tests against local Docker PostgreSQL")


@pytest.fixture
def database(request):
    if not request.config.getoption("--integration"):
        pytest.skip("Pass --integration with the local PostgreSQL service running")
    if os.environ.get("POSTGRES_HOST", "127.0.0.1") not in ("127.0.0.1", "localhost"):
        pytest.fail("Integration tests create temporary databases only on local PostgreSQL")
    name = "synthea_test_" + uuid4().hex
    with connect("postgres", autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            with connect(name) as connection:
                initialize(connection)
                yield connection
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))

