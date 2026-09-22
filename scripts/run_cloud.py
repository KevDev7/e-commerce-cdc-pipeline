"""Commands shared by manual validation and the Airflow DAG."""
import argparse
import logging
from pathlib import Path
import subprocess
import sys

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / '.env.cloud', override=True)
from olist_cdc.audit import audit_step
from olist_cdc.cloud_load import check_capture, load_pending, report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('step', choices=['check','load','build','report'])
    parser.add_argument('--full-refresh', action='store_true', help='Rebuild all marts from retained raw events (build only)')
    args = parser.parse_args()
    if args.full_refresh and args.step != 'build':
        parser.error('--full-refresh is only valid with build')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    with audit_step(args.step) as details:
        if args.step == 'build':
            details['full_refresh'] = args.full_refresh
            subprocess.run([str(Path(sys.executable).with_name('dbt')), 'build', '--target', 'redshift',
                            '--project-dir', str(ROOT/'dbt'), '--profiles-dir', str(ROOT/'dbt'),
                            '--no-send-anonymous-usage-stats', *(['--full-refresh'] if args.full_refresh else [])], check=True)
        elif args.step == 'load':
            load_pending(metrics=details)
        elif args.step == 'report':
            details['mart_rows'] = report()
        else:
            check_capture()


if __name__ == '__main__':
    main()
