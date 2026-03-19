"""
load_to_bigquery.py
Loads raw Parquet files from S3 into BigQuery table: solar_raw.raw_solar_readings
Run once to populate BigQuery. Safe to re-run (overwrites existing data).
"""

import os
import io
import boto3
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ── Config ────────────────────────────────────────────────────────────────────

GCP_KEY_PATH   = r"C:\Users\sergi\Downloads\dbt-tutorial-488323-07de3550e18f.json"
GCP_PROJECT    = "dbt-tutorial-488323"
BQ_DATASET     = "solar_raw"
BQ_TABLE       = "raw_solar_readings"

S3_BUCKET      = "solar-analytics-raw-scl-dev"
AWS_REGION     = "eu-central-1"

CHUNK_SIZE     = 50  # files per BigQuery upload batch

# ── Clients ───────────────────────────────────────────────────────────────────

print("Connecting to AWS S3...")
s3 = boto3.client("s3", region_name=AWS_REGION)

print("Connecting to BigQuery...")
credentials = service_account.Credentials.from_service_account_file(GCP_KEY_PATH)
bq = bigquery.Client(project=GCP_PROJECT, credentials=credentials)

# ── List all Parquet files in S3 ──────────────────────────────────────────────

print("Listing Parquet files in S3...")
paginator = s3.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=S3_BUCKET)

s3_keys = []
for page in pages:
    for obj in page.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            s3_keys.append(obj["Key"])

print(f"Found {len(s3_keys)} Parquet files.")

# ── Load files in chunks ──────────────────────────────────────────────────────

table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # overwrite on first chunk
    source_format=bigquery.SourceFormat.PARQUET,
    autodetect=True,
)

total_rows = 0

for i in range(0, len(s3_keys), CHUNK_SIZE):
    chunk_keys = s3_keys[i : i + CHUNK_SIZE]
    chunk_num  = (i // CHUNK_SIZE) + 1
    total_chunks = (len(s3_keys) + CHUNK_SIZE - 1) // CHUNK_SIZE
    print(f"\nChunk {chunk_num}/{total_chunks} — reading {len(chunk_keys)} files from S3...")

    # Read all files in this chunk into a single DataFrame
    dfs = []
    for key in chunk_keys:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        df = pd.read_parquet(io.BytesIO(response["Body"].read()))
        # Add partition columns from the S3 path if not already present
        # Path format: staging/monthly/year=YYYY/month=MM/filename.parquet
        parts = key.split("/")
        for part in parts:
            if part.startswith("year=") and "year" not in df.columns:
                df["year"] = int(part.split("=")[1])
            elif part.startswith("month=") and "month" not in df.columns:
                df["month"] = int(part.split("=")[1])
        df["source_file"] = key  # track which file each row came from
        dfs.append(df)

    chunk_df = pd.concat(dfs, ignore_index=True)

    # Sanitize column names: replace any character that isn't alphanumeric or _ with _
    import re
    chunk_df.columns = [re.sub(r"[^\w]", "_", col) for col in chunk_df.columns]
    # Remove leading digits (BigQuery doesn't allow columns starting with a number)
    chunk_df.columns = [f"col_{col}" if col[0].isdigit() else col for col in chunk_df.columns]

    print(f"  Rows in chunk: {len(chunk_df):,} | Columns: {len(chunk_df.columns)}")

    # After first chunk, append instead of overwrite
    if i > 0:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    print(f"  Uploading to BigQuery...")
    job = bq.load_table_from_dataframe(chunk_df, table_ref, job_config=job_config)
    job.result()  # wait for completion

    total_rows += len(chunk_df)
    print(f"  ✅ Chunk {chunk_num} done. Running total: {total_rows:,} rows")

# ── Final verification ────────────────────────────────────────────────────────

print(f"\n{'='*60}")
table = bq.get_table(table_ref)
print(f"✅ Load complete!")
print(f"   Table:  {table_ref}")
print(f"   Rows:   {table.num_rows:,}")
print(f"   Size:   {table.num_bytes / 1024 / 1024:.1f} MB")
print(f"   Schema: {len(table.schema)} columns")
print(f"{'='*60}")
