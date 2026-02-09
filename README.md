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