"""Commands shared by manual validation and the Airflow DAG."""
import argparse
import logging
import os
from pathlib import Path
import subprocess
import sys

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / '.env.cloud', override=True)
from olist_cdc.task_logging import log_step
from olist_cdc.cloud_load import check_capture, load_pending, report
from olist_cdc.microbatch import complete_batch, prepare_batch


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('step', choices=['check','pending','load','build','report','complete'])
    args = parser.parse_args(argv)
    if args.step in ('pending', 'complete') and not os.environ.get('BATCH_RUN_ID'):
        parser.error('BATCH_RUN_ID is required for pending/complete; use the same ID for both')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    with log_step(args.step) as details:
        if args.step in ('pending', 'complete'):
            directory = ROOT / 'data' / 'olist-microbatch'
            run_id = os.environ['BATCH_RUN_ID']
            if args.step == 'pending':
                if not prepare_batch(directory, run_id):
                    details['_skip'] = True
                    return 99
            else:
                complete_batch(directory, run_id)
        elif args.step == 'build':
            details['mart_materialization'] = 'table'
            subprocess.run([str(Path(sys.executable).with_name('dbt')), 'build', '--target', 'redshift',
                            '--project-dir', str(ROOT/'dbt'), '--profiles-dir', str(ROOT/'dbt'),
                            '--no-send-anonymous-usage-stats'], check=True)
        elif args.step == 'load':
            load_pending(metrics=details)
        elif args.step == 'report':
            details['mart_rows'] = report()
        else:
            check_capture()


if __name__ == '__main__':
    raise SystemExit(main())
