"""Continuous DMS capture with five-minute downstream batches during demos."""
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "olist_cdc", start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="*/5 * * * *", catchup=False, max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    dagrun_timeout=timedelta(minutes=20),
    description="Every five minutes: check capture, process new files, build and test marts",
    tags=["olist", "cdc", "portfolio"],
) as dag:
    steps = ("check", "pending", "load", "build", "complete")
    tasks = {step: BashOperator(
        task_id=step,
        bash_command=f"/opt/pipeline/bin/python /opt/project/scripts/run_cloud.py {step}",
        env={"BATCH_RUN_ID": "{{ run_id }}", "BATCH_ATTEMPT": "{{ ti.try_number }}"}, append_env=True,
        skip_on_exit_code=99 if step == "pending" else None,
        execution_timeout=timedelta(minutes=2 if step in ("pending", "complete") else 10),
    ) for step in steps}
    for first, second in zip(steps, steps[1:]):
        tasks[first] >> tasks[second]
