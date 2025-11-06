# Airflow ETL/ELT Portfolio

[![Python 3.12](https://img.shields.io/badge/python-3.12-3776AB.svg?logo=python)](https://www.python.org/downloads/)
[![Apache Airflow 2.9.2](https://img.shields.io/badge/airflow-2.9.2-017CEE.svg?logo=apacheairflow)](https://airflow.apache.org/)
[![Flask API Service](https://img.shields.io/badge/flask-3.0_API_service-000000.svg?logo=flask)](https://flask.palletsprojects.com/) <!-- API container uses Flask 3.0.x; Airflow image bundles Flask 2.2.x internally -->
[![Pandas 2.2.2](https://img.shields.io/badge/pandas-2.2.2-150458.svg?logo=pandas)](https://pandas.pydata.org/)
[![Postgres 13 (Meta DB)](https://img.shields.io/badge/postgres-13-4169E1.svg?logo=postgresql)](https://www.postgresql.org/)
[![SQLite (Pipeline Store)](https://img.shields.io/badge/sqlite-storage-044A64.svg?logo=sqlite)](https://www.sqlite.org/)
[![BigQuery](https://img.shields.io/badge/google_bigquery-ingestion-1A73E8.svg?logo=googlecloud)](https://cloud.google.com/bigquery)
[![GCS](https://img.shields.io/badge/google_cloud_storage-ingestion-4285F4.svg?logo=googlecloud)](https://cloud.google.com/storage)
[![SQLAlchemy 1.4.52](https://img.shields.io/badge/sqlalchemy-1.4.52-CC0000.svg)](https://www.sqlalchemy.org/)
[![Docker Compose](https://img.shields.io/badge/docker%20compose-dev_env-1D63ED.svg?logo=docker)](https://docs.docker.com/compose/)
[![Faker 25.x](https://img.shields.io/badge/faker-synthetic_data-FF6F00.svg)](https://faker.readthedocs.io/)


Engineering reference implementation of two Airflow data pipelines over research-style tabular datasets: (1) CSV → GCS → BigQuery external tables; (2) REST API → Pandas transforms → SQLite upsert with dynamic schema.

**Features**
- Orchestration: Airflow LocalExecutor, task dependency & retries.
- Sources: Local CSV, synthetic REST (Flask).
- Destinations: GCS objects, BigQuery external tables, local SQLite store.
- Transformation: Pandas normalization, dtype → SQL type mapping.
- Schema Evolution: Automatic column adds + idempotent upsert via SQLAlchemy (SQLite dialect).
- Cloud Integration: GCP operators (bucket, dataset, external table, checks).
- Containerized Dev: Docker Compose (webserver, scheduler, Postgres metadata, API service).
- Synthetic Data: Faker entities (materials, suppliers, experiments, orders).

---

## Quick Start
```powershell
git clone https://github.com/devopscicdgit/AirflowR-D-Orchestrator.git
cd AirflowR-D-Orchestrator
./scripts/1-environment-setup.ps1
./scripts/2-generate-api-datasets.ps1
./scripts/3-init-airflow.ps1
# Airflow UI: http://localhost:8080 (admin/admin)
```

---

## Operations
| Component | Purpose | Access / Command |
|-----------|---------|------------------|
| Webserver | UI, DAG monitoring | http://localhost:8080 (admin/admin) |
| Scheduler | Executes scheduled tasks | `docker compose logs scheduler` |
| Postgres (metadata) | Airflow state storage | `docker compose exec postgres psql -U airflow -d airflow` |
| Logs | Task run logs | `./logs/<dag_id>/<task_id>/<execution_date>/1.log` or UI |
| API Service | Data source endpoints | http://localhost:5000 (`/api/materials` etc.) |
| Airflow CLI | DAG/task management | `docker compose exec webserver airflow dags list` |

### Run DAGs
Trigger via UI or CLI:
```powershell
docker compose exec webserver airflow dags trigger materials_experiments_ingestion
docker compose exec webserver airflow dags trigger materials_orders_experiments_etl_api
```
Stop environment (optional):
```powershell
./scripts/4-teardown.ps1
```

## Data Verification
### SQLite (API ETL Output)
```powershell
sqlite3 ./data/api_database.db ".tables"
sqlite3 ./data/api_database.db ".schema experiments"
sqlite3 ./data/api_database.db "SELECT COUNT(*) FROM experiments;"
sqlite3 ./data/api_database.db "SELECT * FROM experiments LIMIT 5;"
```
### GCP (CSV Ingestion)
1. Bucket exists and file uploaded (GCS Console or `gsutil ls gs://<bucket>/`).
2. BigQuery dataset & external table created.
3. Query sample:
```sql
SELECT * FROM `<PROJECT>.<DATASET>.experiments_table` LIMIT 10;
```
4. Optional Airflow data quality task: BigQueryCheckOperator after external table creation.

## Configuration & Security
- Set secrets/keys via environment or Airflow Connections (avoid committing JSON keys).
- `DEVELOPMENT_MODE=true` (env) will skip actual GCP resource creation while allowing local parse/testing.
- Recommended Variables:
```powershell
docker compose exec webserver airflow variables set GCP_PROJECT_ID your-project
docker compose exec webserver airflow variables set GCS_BUCKET your-bucket
docker compose exec webserver airflow variables set BQ_DATASET materials_experiments_dataset
```
- Add service account key filename (e.g., `gcp-key.json`) to `.gitignore`; prefer mounting a secret file not copying into image.
- Change default admin password after first login.

## Testing & CI
- Pytest for `data_helpers.py` (schema evolution & upsert logic).
- CI (GitHub Actions workflow `ci.yml`): installs deps, parses DAGs, runs pytest.
- Future: add data quality assertions & lint (ruff/flake8).

## Notes
- Python 3.12 required (ensure `py -3.12` available on Windows).
- Synthetic data for development/demo only.

## Future Enhancements
- Parameterize GCP resources fully via Variables/Env.
- Add additional destinations (e.g., Parquet, S3) & TaskGroup patterns.
- Integrate secret manager (GCP Secret Manager / Vault).
