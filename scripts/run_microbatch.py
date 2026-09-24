"""Airflow's S3-only gate and post-build acknowledgement."""
import argparse
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from olist_cdc.task_logging import log_step
from olist_cdc.microbatch import complete_batch, prepare_batch

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / '.env.cloud', override=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('step', choices=['pending', 'complete'])
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    directory = ROOT / 'data' / 'olist-microbatch'
    run_id = os.environ['BATCH_RUN_ID']
    skip = False
    with log_step(args.step) as details:
        if args.step == 'pending':
            skip = not prepare_batch(directory, run_id)
            details['_skip'] = skip
        else:
            complete_batch(directory, run_id)
    if skip:
        raise SystemExit(99)  # Downstream tasks skip on quiet runs.
