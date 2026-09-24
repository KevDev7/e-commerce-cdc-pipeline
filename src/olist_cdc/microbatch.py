"""A local completion checkpoint for the single-active-run Airflow demo."""
import hashlib
import json
import logging
import os
from pathlib import Path

from olist_cdc.cloud_load import aws_session

log = logging.getLogger(__name__)


def pending_path(directory, run_id):
    # Airflow run IDs are data, never shell commands or filesystem paths.
    return directory / (hashlib.sha256(run_id.encode()).hexdigest() + '.json')


def prepare_batch(directory, run_id):
    """Compare S3 with the last successfully built batch without waking Redshift."""
    directory = Path(directory)
    bucket = os.environ['S3_BUCKET']
    prefix = os.environ.get('CAPTURE_PREFIX', 'raw/dms/olist').rstrip('/') + '/'
    s3 = aws_session().client('s3')
    objects = sorted(
        (obj['Key'], obj['ETag'], obj['Size'])
        for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get('Contents', []) if obj['Key'].endswith('.csv')
    )
    if not objects:
        raise RuntimeError('No DMS files found; check capture before loading')
    manifest = json.dumps(dict(bucket=bucket, prefix=prefix,
                               warehouse=os.environ['REDSHIFT_HOST'], objects=objects), sort_keys=True)
    checkpoint = directory / 'completed.json'
    if checkpoint.exists() and checkpoint.read_text() == manifest:
        log.info('No new capture files since the last successful batch; skipping warehouse work')
        return False
    directory.mkdir(parents=True, exist_ok=True)
    pending_path(directory, run_id).write_text(manifest)
    log.info('Capture changed or no completion checkpoint exists; processing batch')
    return True


def complete_batch(directory, run_id):
    """Advance only after loading and dbt build/tests have succeeded."""
    directory = Path(directory)
    # Atomic replacement leaves the old checkpoint intact if acknowledgement fails.
    pending = pending_path(directory, run_id)
    temporary = pending.with_suffix('.tmp')
    temporary.write_text(pending.read_text())
    temporary.replace(directory / 'completed.json')
    log.info('Batch completion checkpoint saved')
