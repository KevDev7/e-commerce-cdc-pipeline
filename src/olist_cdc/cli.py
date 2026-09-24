"""Simulate business activity in the prepared cloud source."""
import argparse
import json
import logging

from dotenv import load_dotenv
from olist_cdc.db import ROOT, connect
from olist_cdc.simulate import PHASES, run_phase


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Simulate orders in the project's RDS source")
    parser.add_argument("--scenario", default="demo-001")
    parser.add_argument("--phase", choices=["all", *PHASES], default="all")
    args = parser.parse_args()
    if not load_dotenv(ROOT / ".env.cloud", override=True):
        parser.error("Missing .env.cloud; prepare the project cloud source first")
    with connect() as connection:
        phases = PHASES if args.phase == "all" else [args.phase]
        result = [run_phase(connection, args.scenario, phase) for phase in phases]
    logging.info("%s", json.dumps(result))


if __name__ == "__main__":
    main()
