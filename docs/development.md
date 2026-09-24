# Optional local development


The normal demo uses cloud databases and retains datasets in S3. These optional commands create full local data copies; skip them when keeping data off the workstation. CI runs integration fixtures on GitHub.

Requires Docker Desktop, Python 3.12 and uv. Copy `.env.example` to `.env` and set a local development password. Existing Synthea users should use `POSTGRES_DB=olist`; the Olist Compose project has a separate data volume.

```sh
uv sync --locked
docker compose up -d --wait postgres
uv run olist-cdc init
uv run olist-cdc seed
uv run python scripts/build_local_warehouse.py
uv run python scripts/validate_local_olist.py
uv run pytest --integration -q
```

The local warehouse builder is explicitly a snapshot fixture, not a CDC extractor. It preserves an existing fixture; it does not follow later source mutations. Actual capture in AWS is DMS. The tests independently verify actual PostgreSQL WAL and exercise downstream DMS-format event fixtures.

To simulate new source activity:

```sh
uv run olist-cdc simulate --scenario demo-001
# Or execute phases individually:
uv run olist-cdc simulate --scenario demo-002 --phase open
```

Phases are open → approve → ship → deliver → correct → create-delete-test → delete-test → rollback-test. Retrying a phase is idempotent. Simulated IDs are deterministic and their scenario is recorded in project metadata. Hard deletes target only the disposable test records. `docker compose stop` stops the local database without deleting its data.


For the normal demonstration, follow the [AWS runbook](run-cloud.md).
