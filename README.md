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
| Orchestration | Airflow (Docker) | ✅ |
| Validation | Custom (physical bounds) | ✅ |
| Transform | Spark + dbt | 🔜 |
| Visualization | Streamlit | 🔜 |

## S3 Structure

```
solar-analytics-raw-scl-dev/
├── raw/
│   ├── monthly/                  # 131 monthly CSV files
│   │   └── year=YYYY/
│   │       └── month=MM/
│   └── auxiliary/
│       ├── csv/                  # 2 auxiliary CSVs
│       └── xlsx/                 # 13 auxiliary XLSX
├── staging/
│   └── monthly/                  # Cleaned Parquet files
└── curated/
    └── performance_ratio/        # Final analytics
```

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
```

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
```

### Output Structure
```
data/staging/monthly/
└── year=2013/
    └── month=02/
        └── solar_data_2013_02.parquet
```

### Data Documentation
See [docs/data_dictionary.md](docs/data_dictionary.md) for column descriptions.

## Pipeline

### Overview
```
CSV (raw) → Parquet → Validation → Quality Check → S3 Upload → S3 Verification
```

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
```

Access the UI at http://localhost:8080 (admin/admin)

### Pipeline DAG: `solar_pipeline`

| Task | Description |
|------|-------------|
| `convert_csv_to_parquet` | Convert 131 monthly CSVs to Parquet format |
| `verify_conversion` | Validate file count, columns, format, year range |
| `validate_quality` | Physical bounds checking against ISO/AEMET limits |
| `upload_to_s3` | Upload to s3://solar-analytics-raw-scl-dev/ |
| `verify_s3_upload` | Validate upload count, bucket, region |

Tasks communicate via XCom. If any validation fails, downstream tasks are blocked — bad data never reaches S3.

## Data Quality

### Validation Approach

The `validate_quality` task performs 5 checks on every Parquet file:

| Check | Type | Action on Failure |
|-------|------|-------------------|
| File count (131) | Critical | ❌ Stop pipeline |
| Schema consistency (108 columns) | Critical | ❌ Stop pipeline |
| No empty files | Critical | ❌ Stop pipeline |
| Timestamp integrity | Warning | ⚠️ Log, continue |
| Physical bounds | Warning | ⚠️ Log, continue |

### Physical Bounds

Envelope bounds across all 13 solar systems, derived from ISO standards and AEMET historical data for Madrid:

| Variable | Min | Max | Unit |
|----------|-----|-----|------|
| Irradiance (G_H, G_M) | 0 | 1,500 | W/m² |
| Ambient Temperature (T_U) | -17.4 | 50 | °C |
| Module Temperature (T_M) | -17.4 | 70 | °C |
| Inverter Temperature (T_WR) | -17.4 | 80 | °C |
| AC Power (P_AC) | 0 | 5,880 | W |
| DC Voltage (U_DC) | 0 | 548.5 | V |
| AC Voltage (U_AC) | 0 | 280 | V |

Per-system validation with auxiliary reference files planned for the dbt layer.

### Findings (1.1M rows, 131 files)

| Finding | Severity | Detail |
|---------|----------|--------|
| T_M3 sensor failure | High | Thousands of sub-minimum readings across most of the 10-year period |
| T_M2 sensor failure | High | Failed ~Aug 2021, stuck above 70°C through Dec 2022 |
| T_WR(11) runs hot | Low | Consistently highest inverter temperature every summer |
| T_WR(8), T_WR(3), T_WR(10) | Low | Seasonal summer exceedances, expected behavior |
| All T_M below -17.4°C | Medium | System-wide events in Apr 2019, Jan 2022 (communication loss) |
| U_DC(12) above 548.5V | Low | 6 total readings across 10 years |

These findings match the known sensor issues from the original university research. Temperature correction from ambient (T_U) is planned for the dbt transformation layer.

## Project Structure

```
solar-analytics/
├── terraform/
│   ├── provider.tf
│   ├── variables.tf
│   ├── main.tf
│   ├── outputs.tf
│   └── iam.tf
├── src/
│   ├── data_conversion.py
│   ├── verify_conversion.py
│   ├── upload_to_s3.py
│   ├── verify_s3_upload.py
│   └── validate_parquet.py
├── airflow/
│   ├── docker-compose.yaml
│   └── dags/
│       └── solar_pipeline_dag.py
├── docs/
│   └── data_dictionary.md
├── requirements.txt
├── README.md
└── .gitignore
```