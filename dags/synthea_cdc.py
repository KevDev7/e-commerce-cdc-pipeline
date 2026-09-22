"""DMS captures continuously; this DAG batches downstream work on demand."""
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "synthea_cdc", start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None, catchup=False, max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    dagrun_timeout=timedelta(minutes=20),
    description="Validate capture, load S3 events, build and test Redshift marts",
    tags=["synthea", "cdc", "portfolio"],
) as dag:
    tasks = [BashOperator(
        task_id=step,
        bash_command=f"/opt/pipeline/bin/python /opt/project/scripts/run_cloud.py {step}",
        execution_timeout=timedelta(minutes=10),
    ) for step in ("check", "load", "build", "report")]
    for upstream, downstream in zip(tasks, tasks[1:]):
        upstream >> downstream
