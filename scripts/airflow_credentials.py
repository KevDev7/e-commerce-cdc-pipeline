"""Give the demo container a one-hour session for this project's AWS profile only."""
import configparser
import os
from pathlib import Path

import boto3

directory = Path(__file__).resolve().parents[1] / ".aws"
directory.mkdir(mode=0o700, exist_ok=True)
session = boto3.Session(profile_name="synthea-cdc", region_name="us-east-1")
credentials = session.client("sts").get_session_token(DurationSeconds=3600)["Credentials"]
config = configparser.ConfigParser()
config["synthea-cdc"] = {"aws_access_key_id": credentials["AccessKeyId"],
                         "aws_secret_access_key": credentials["SecretAccessKey"],
                         "aws_session_token": credentials["SessionToken"]}
path = directory / "credentials"
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
os.chmod(path, 0o600)
with os.fdopen(fd, "w") as stream:
    config.write(stream)
(directory / "config").write_text("[profile synthea-cdc]\nregion=us-east-1\n")
print("Wrote an ignored project-only session file; expires", credentials["Expiration"].isoformat())
