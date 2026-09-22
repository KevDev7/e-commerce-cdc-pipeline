import argparse
import json
import logging
from pathlib import Path

from synthea_cdc.db import connect, initialize
from synthea_cdc.seed import download, load
from synthea_cdc.simulate import PHASES, run_phase


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Synthea CDC project commands")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init", help="Create the four source tables and project metadata")
    seed = sub.add_parser("seed", help="Download and load the pinned Synthea sample once")
    seed.add_argument("--archive", type=Path, default=Path("data/synthea.zip"))
    simulate = sub.add_parser("simulate", help="Commit a small, repeatable healthcare billing scenario")
    simulate.add_argument("--scenario", default="demo-001")
    simulate.add_argument("--phase", choices=["all", *PHASES], default="all")
    args = parser.parse_args()
    if args.command == "seed":
        download(args.archive)
    with connect() as connection:
        if args.command == "init":
            initialize(connection)
            result = {"status": "initialized"}
        elif args.command == "seed":
            result = load(connection, args.archive)
        else:
            phases = PHASES if args.phase == "all" else [args.phase]
            result = [run_phase(connection, args.scenario, phase) for phase in phases]
    logging.info("%s", json.dumps(result))


if __name__ == "__main__":
    main()
