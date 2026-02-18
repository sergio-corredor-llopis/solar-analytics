# Solar Performance Analytics Platform

Cloud-based pipeline for ISO 61724-compliant solar monitoring analysis.

## Project Overview

Migration of a paid university research collaboration project ("Beca de colaboración" at UPM) — 10 years of solar performance data — from CSV to a modern cloud data stack.

## Author

Sergio — Electrical Engineer transitioning to Data Engineering
Target: Data Engineer in Zurich (Energy Sector)

## Data

| Type | Count | Date Range |
|------|-------|------------|
| Monthly CSVs | 131 | Feb 2013 → Dec 2023 |
| Auxiliary CSVs | 2 | — |
| Auxiliary XLSX | 13 | — |

## Architecture

| Layer | Technology | Status |
|-------|------------|--------|
| Storage | AWS S3 | ✅ |
| Infrastructure | Terraform | ✅ |
| Orchestration | Airflow | 🔜 |
| Transform | Spark + dbt | 🔜 |
| Visualization | Streamlit | 🔜 |

## S3 Structure

solar-analytics-raw-scl-dev/
├── raw/
│ ├── monthly/ # 131 monthly CSV files
│ │ └── year=YYYY/
│ │ └── month=MM/
│ └── auxiliary/
│ ├── csv/ # 2 auxiliary CSVs
│ └── xlsx/ # 13 auxiliary XLSX
├── staging/
│ └── monthly/ # Cleaned Parquet files
└── curated/
└── performance_ratio/ # Final analytics


## Infrastructure

### Resources Created

| Resource | Name |
|----------|------|
| S3 Bucket | `solar-analytics-raw-scl-dev` |
| IAM Role | `solar-analytics-pipeline-role-dev` |
| IAM Policy | `solar-analytics-s3-access-dev` |

### Security

- ✅ S3 versioning enabled
- ✅ S3 public access blocked
- ✅ IAM role with least-privilege S3 access

### Deploy

```bash
cd terraform
terraform init
terraform apply

## Data Pipeline

### Source Data
- 131 monthly CSV files (Feb 2013 → Dec 2023)
- Format: UTF-16 LE, tab-separated, European decimals
- Pattern: `YYYY MM [Spanish Month] Todos los Inversores.csv`

### Conversion Script

```bash
# Install dependencies
pip install pandas pyarrow

# Convert one file
python src/data_conversion.py

### Output Structure
data/staging/monthly/
└── year=2013/
└── month=02/
└── solar_data_2013_02.parquet

### Data Documentation
See [docs/data_dictionary.md](docs/data_dictionary.md) for column descriptions.

## Architecture

### Pipeline Overview
CSV (raw) → Parquet → Validation → S3 Upload → S3 Verification

### Local Development

The pipeline runs on Apache Airflow, containerized with Docker:

| Service | Purpose |
|---------|---------|
| PostgreSQL | Metadata database |
| Airflow Webserver | UI (localhost:8080) |
| Airflow Scheduler | Task orchestration |

### Setup

```bash
cd airflow
docker compose up airflow-init   # First time only
docker compose up -d             # Start services
Access the UI at http://localhost:8080 (admin/admin)

Pipeline DAG: solar_pipeline
Task	Description
convert_csv_to_parquet	Convert 131 monthly CSVs to Parquet format
verify_conversion	Validate file count, columns, format, year range
upload_to_s3	Upload to s3://solar-analytics-raw-scl-dev/
verify_s3_upload	Validate upload count, bucket, region
Tasks communicate via XCom. If any validation fails, downstream tasks are blocked.

## Project Structure
solar-analytics/
├── terraform/
│ ├── provider.tf
│ ├── variables.tf
│ ├── main.tf
│ ├── outputs.tf
│ └── iam.tf
├── src/
│ ├── data_conversion.py
│ ├── verify_conversion.py
│ ├── upload_to_s3.py
│ └── verify_s3_upload.py
├── airflow/
│ ├── docker-compose.yaml
│ └── dags/
│ └── solar_pipeline_dag.py
├── docs/
│ └── data_dictionary.md
├── requirements.txt
├── README.md
└── .gitignore