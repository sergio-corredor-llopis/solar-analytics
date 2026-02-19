from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import sys
sys.path.insert(0, '/opt/airflow/src')
from validate_parquet import validate_parquet_quality

default_args = {
    "owner": "sergio",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def convert_csv_to_parquet(**context):
    """Simulate CSV to Parquet conversion and pass metadata downstream."""
    result = {
        "files_converted": 131,
        "output_format": "parquet",
        "columns_per_file": 108,
        "years_covered": list(range(2013, 2024)),
        "status": "success",
    }
    context["ti"].xcom_push(key="conversion_result", value=result)
    print(f"Converted {result['files_converted']} files to {result['output_format']}")
    print(f"Columns per file: {result['columns_per_file']}")
    print(f"Years: {result['years_covered'][0]}-{result['years_covered'][-1]}")
    return result


def verify_conversion(**context):
    """Pull conversion metadata and validate it."""
    conversion = context["ti"].xcom_pull(
        task_ids="convert_csv_to_parquet", key="conversion_result"
    )

    checks = {}

    # Check 1: File count
    checks["file_count"] = conversion["files_converted"] == 131
    print(f"Check 1 - File count (131): {'PASS' if checks['file_count'] else 'FAIL'}")

    # Check 2: Column count
    checks["column_count"] = conversion["columns_per_file"] == 108
    print(f"Check 2 - Columns (108): {'PASS' if checks['column_count'] else 'FAIL'}")

    # Check 3: Format
    checks["format"] = conversion["output_format"] == "parquet"
    print(f"Check 3 - Format (parquet): {'PASS' if checks['format'] else 'FAIL'}")

    # Check 4: Year range
    checks["year_range"] = (
        conversion["years_covered"][0] == 2013
        and conversion["years_covered"][-1] == 2023
    )
    print(f"Check 4 - Years (2013-2023): {'PASS' if checks['year_range'] else 'FAIL'}")

    all_passed = all(checks.values())
    print(f"\nAll checks passed: {all_passed}")

    if not all_passed:
        raise ValueError(f"Verification failed: {checks}")

    context["ti"].xcom_push(key="verification_result", value=checks)
    return checks


def upload_to_s3(**context):
    """Simulate S3 upload using verified conversion data."""
    verification = context["ti"].xcom_pull(
        task_ids="verify_conversion", key="verification_result"
    )

    if not all(verification.values()):
        raise ValueError("Cannot upload: verification checks failed")

    conversion = context["ti"].xcom_pull(
        task_ids="convert_csv_to_parquet", key="conversion_result"
    )

    result = {
        "bucket": "solar-analytics-raw-scl-dev",
        "prefix": "raw/monthly/",
        "files_uploaded": conversion["files_converted"],
        "region": "eu-central-1",
        "status": "success",
    }
    context["ti"].xcom_push(key="upload_result", value=result)
    print(f"Uploaded {result['files_uploaded']} files to s3://{result['bucket']}/{result['prefix']}")
    return result


def verify_s3_upload(**context):
    """Validate S3 upload results."""
    upload = context["ti"].xcom_pull(
        task_ids="upload_to_s3", key="upload_result"
    )

    checks = {}

    # Check 1: File count matches
    checks["file_count"] = upload["files_uploaded"] == 131
    print(f"Check 1 - Files uploaded (131): {'PASS' if checks['file_count'] else 'FAIL'}")

    # Check 2: Correct bucket
    checks["bucket"] = upload["bucket"] == "solar-analytics-raw-scl-dev"
    print(f"Check 2 - Bucket name: {'PASS' if checks['bucket'] else 'FAIL'}")

    # Check 3: Correct region
    checks["region"] = upload["region"] == "eu-central-1"
    print(f"Check 3 - Region: {'PASS' if checks['region'] else 'FAIL'}")

    all_passed = all(checks.values())
    print(f"\nAll S3 checks passed: {all_passed}")
    print(f"\n{'='*50}")
    print(f"PIPELINE COMPLETE")
    print(f"{'='*50}")

    if not all_passed:
        raise ValueError(f"S3 verification failed: {checks}")

    return checks


with DAG(
    dag_id="solar_pipeline",
    default_args=default_args,
    description="Solar Analytics: CSV to Parquet, validate, upload to S3, verify S3 upload",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["solar", "pipeline"],
) as dag:

    task_convert = PythonOperator(
        task_id="convert_csv_to_parquet",
        python_callable=convert_csv_to_parquet,
    )

    task_verify = PythonOperator(
        task_id="verify_conversion",
        python_callable=verify_conversion,
    )

    task_validate = PythonOperator(
        task_id="validate_quality",
        python_callable=validate_parquet_quality,
        op_kwargs={"input_dir": "/opt/airflow/data/staging/monthly"},
    )

    task_upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    task_verify_s3 = PythonOperator(
        task_id="verify_s3_upload",
        python_callable=verify_s3_upload,
    )

    task_convert >> task_verify >> task_validate >> task_upload >> task_verify_s3