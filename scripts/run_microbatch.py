"""Airflow's S3-only gate and post-build acknowledgement."""
import argparse
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from synthea_cdc.microbatch import complete_batch, prepare_batch

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / '.env.cloud', override=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('step', choices=['pending', 'complete'])
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    directory = ROOT / 'data' / 'microbatch'
    run_id = os.environ['BATCH_RUN_ID']
    if args.step == 'pending':
        if not prepare_batch(directory, run_id):
            raise SystemExit(99)  # BashOperator skips all dependent tasks.
    else:
        complete_batch(directory, run_id)
