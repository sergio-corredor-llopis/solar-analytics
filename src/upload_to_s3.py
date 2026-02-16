"""
Upload Parquet files to S3.
Usage:
    python src/upload_to_s3.py --test     # Upload one file to verify
    python src/upload_to_s3.py            # Upload all 131 files
"""

import boto3
import argparse
from pathlib import Path

BUCKET = "solar-analytics-raw-scl-dev"
LOCAL_BASE = Path("data/staging/monthly")
S3_PREFIX = "raw/monthly"


def get_parquet_files():
    """Find all Parquet files in local staging directory."""
    files = sorted(LOCAL_BASE.glob("year=*/month=*/*.parquet"))
    return files


def get_s3_key(local_path):
    """Convert local path to S3 key.
    
    data/staging/monthly/year=2013/month=02/solar_data_2013_02.parquet
    → raw/monthly/year=2013/month=02/solar_data_2013_02.parquet
    """
    relative = local_path.relative_to(LOCAL_BASE)
    return f"{S3_PREFIX}/{relative.as_posix()}"


def upload_files(test_mode=False):
    """Upload Parquet files to S3."""
    s3 = boto3.client("s3", region_name="eu-central-1")
    
    files = get_parquet_files()
    
    if not files:
        print(f"ERROR: No Parquet files found in {LOCAL_BASE}")
        return
    
    print(f"Found {len(files)} Parquet files")
    
    if test_mode:
        files = files[:1]
        print(f"TEST MODE: Uploading only {files[0].name}")
    
    uploaded = 0
    failed = 0
    
    for file_path in files:
        s3_key = get_s3_key(file_path)
        try:
            s3.upload_file(str(file_path), BUCKET, s3_key)
            uploaded += 1
            print(f"  ✅ [{uploaded}/{len(files)}] {s3_key}")
        except Exception as e:
            failed += 1
            print(f"  ❌ FAILED: {s3_key} — {e}")
    
    print(f"\nDone: {uploaded} uploaded, {failed} failed, {len(files)} total")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload Parquet files to S3")
    parser.add_argument("--test", action="store_true", help="Upload one file only")
    args = parser.parse_args()
    
    upload_files(test_mode=args.test)