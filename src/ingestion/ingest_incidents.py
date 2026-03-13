"""
ingest_incidents.py

Uploads the full Metro Nashville incidents CSV from the local
`nashville-crime-platform` folder into the S3 data lake:

    s3://nashville-crime-platform/raw/incidents/incidents_2025.csv
"""

import datetime as dt
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


# CONFIGURATION

# S3 bucket name
BUCKET_NAME = "nashville-crime-platform"

# Local path to the CSV on your Mac.
# Use the folder this script is in as the base directory.
LOCAL_BASE_DIR = Path(__file__).resolve().parent
LOCAL_FILE_NAME = "incidents_2025.csv"
LOCAL_FILE_PATH = LOCAL_BASE_DIR / LOCAL_FILE_NAME

# Target S3 key (path inside the bucket)
S3_KEY = f"raw/incidents/{LOCAL_FILE_NAME}"


def main() -> None:
    print("=== MNPD Incidents Ingestion (Raw Zone) Started ===")

    # Log a UTC timestamp for the run (nice for debugging later)
    run_ts = dt.datetime.utcnow().isoformat() + "Z"
    print(f"Run timestamp (UTC): {run_ts}")

    print(f"Local file: {LOCAL_FILE_PATH}")
    print(f"Target S3 URI: s3://{BUCKET_NAME}/{S3_KEY}")

    # Check that the file actually exists locally
    if not LOCAL_FILE_PATH.exists():
        print(f"ERROR: Local file not found: {LOCAL_FILE_PATH}")
        return

    # Create S3 client
    s3_client = boto3.client("s3")

    try:
        # Upload the file
        s3_client.upload_file(
            Filename=str(LOCAL_FILE_PATH),
            Bucket=BUCKET_NAME,
            Key=S3_KEY,
        )
        print("Upload complete!")
    except ClientError as e:
        print("ERROR: Upload to S3 failed.")
        print(e)
        return

    print("=== Incidents Ingestion Complete ===")


if __name__ == "__main__":
    main()




