# Development checks

The full-data demonstration uses RDS and Redshift. Follow the [AWS runbook](run-cloud.md).
GitHub CI runs small generated PostgreSQL fixtures, the dbt graph and real Airflow
task-state checks. It does not download Olist or use AWS credentials.

## Offline checks

With Python 3.12 and uv installed, from the repository root:

```sh
uv sync --locked
uv run pytest -q
```

Tests requiring PostgreSQL are skipped unless `--integration` is supplied.
The offline suite covers CSV/Parquet conversion, command routing, the Airflow
batch completion marker, task logs and validation helpers. Integration tests
exercise rebuilt marts, history, deletes, replay and SQL failure/retry.

## Disposable integration fixtures

CI is the default place to run these. To reproduce them locally, use a disposable
PostgreSQL test instance with logical replication enabled and set the connection
environment variables from `.env.example` to that instance. Then run:

```sh
uv run pytest --integration -q
```

The tests create small temporary databases and drop them afterward. They generate
their own seed archives and CDC records; no Olist download or full local warehouse
is required. Stop and remove the disposable PostgreSQL instance when done.
`.github/workflows/checks.yml` contains the complete CI setup and Airflow smoke command.

The production source loader and Parquet conversion are shared with these tests.
The local raw loader is a test implementation for PostgreSQL; actual cloud capture
and Redshift COPY are verified separately in the dated [AWS results](validation.md).
