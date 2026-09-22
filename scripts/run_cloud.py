"""Commands shared by manual validation and the Airflow DAG."""
import argparse
import logging
from pathlib import Path
import subprocess
import sys

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env.cloud", override=True)
from olist_cdc.cloud_load import check_capture, load_pending, report

parser = argparse.ArgumentParser()
parser.add_argument("step", choices=["check", "load", "build", "report"])
args = parser.parse_args()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
if args.step == "build":
    subprocess.run([str(Path(sys.executable).with_name("dbt")), "build", "--target", "redshift",
                    "--project-dir", str(ROOT / "dbt"), "--profiles-dir", str(ROOT / "dbt"),
                    "--no-send-anonymous-usage-stats"], check=True)
else:
    {"check": check_capture, "load": load_pending, "report": report}[args.step]()
