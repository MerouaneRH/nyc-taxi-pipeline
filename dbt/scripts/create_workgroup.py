"""Create the Athena workgroup used by dbt (idempotent). See ADR 0007.

Usage: AWS_PROFILE=nyc-taxi uv run python dbt/scripts/create_workgroup.py
"""
import os

import boto3
from botocore.exceptions import ClientError

NAME = os.environ.get("DBT_ATHENA_WORKGROUP", "nyc-taxi-dbt")
BUCKET = os.environ.get("S3_BUCKET", "merouane-nyc-taxi-raw-euw3-2026")
REGION = os.environ.get("AWS_REGION", "eu-west-3")
SCAN_CAP_BYTES = 5 * 1024**3  # 5 GiB per query
ENGINE = "Athena engine version 3"
OUTPUT = f"s3://{BUCKET}/athena-results/"


def main() -> None:
    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "nyc-taxi"), region_name=REGION)
    athena = session.client("athena")
    try:
        athena.create_work_group(
            Name=NAME,
            Description="dbt runs with a per-query scan cap",
            Configuration={
                "ResultConfiguration": {"OutputLocation": OUTPUT},
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": False,
                "BytesScannedCutoffPerQuery": SCAN_CAP_BYTES,
                "EngineVersion": {"SelectedEngineVersion": ENGINE},
            },
        )
        print(f"created workgroup {NAME}")
    except ClientError as err:
        if "already created" not in err.response["Error"]["Message"]:
            raise
        athena.update_work_group(
            WorkGroup=NAME,
            ConfigurationUpdates={
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": False,
                "BytesScannedCutoffPerQuery": SCAN_CAP_BYTES,
                "ResultConfigurationUpdates": {"OutputLocation": OUTPUT},
                "EngineVersionUpdates": {"SelectedEngineVersion": ENGINE},
            },
        )
        print(f"updated workgroup {NAME}")


if __name__ == "__main__":
    main()
