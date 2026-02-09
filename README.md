# Solar Performance Analytics Platform

Cloud-based pipeline for ISO 61724-compliant solar monitoring analysis.

## Project Overview

Migration of a paid university research collaboration project ("Beca de colaboración" at UPM) — 10 years of solar performance data — from CSV/Excel to a modern cloud data stack.

## Author

Sergio Corredor Llopis — Electrical Engineer transitioning to Data Engineering  
Target: Data Engineer in Zurich (Energy Sector)

## Architecture

| Layer | Technology |
|-------|------------|
| Storage | AWS S3 |
| Orchestration | Airflow (coming) |
| Transform | Spark + dbt (coming) |
| Visualization | Streamlit (coming) |
| Infrastructure | Terraform |

## Current Status

- [x] S3 raw data bucket
- [ ] S3 folder structure
- [ ] Data migration (CSV → Parquet)
- [ ] Airflow setup
- [ ] dbt models
- [ ] Streamlit dashboard

## Infrastructure

```bash
cd terraform
terraform init
terraform apply