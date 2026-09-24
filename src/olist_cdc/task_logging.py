"""Task summaries in ordinary logs; Airflow owns execution history."""
from contextlib import contextmanager
import json
import logging
import os
import time

log = logging.getLogger(__name__)


@contextmanager
def log_step(step):
    details = {}
    started = time.monotonic()
    status = 'success'
    try:
        yield details
        if details.pop('_skip', False):
            status = 'skipped'
    except BaseException as error:
        status = 'failed'
        details['error_type'] = type(error).__name__
        if hasattr(error, 'returncode'):
            details['exit_code'] = error.returncode
        raise
    finally:
        log.info('Task summary: %s', json.dumps(dict(
            step=step, run_id=os.environ.get('BATCH_RUN_ID'),
            attempt=os.environ.get('BATCH_ATTEMPT'), status=status,
            duration_seconds=round(time.monotonic()-started, 3), details=details,
        ), sort_keys=True))
