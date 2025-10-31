# Airflow ETL/ELT Portfolio

[![Python 3.12](https://img.shields.io/badge/python-3.12-3776AB.svg?logo=python)](https://www.python.org/downloads/)
[![Apache Airflow 2.9.1](https://img.shields.io/badge/airflow-2.9.1-017CEE.svg?logo=apacheairflow)](https://airflow.apache.org/) <!-- Container image version; requirements pinned to 2.9.2 planned upgrade -->
[![Flask 3.0 API Service](https://img.shields.io/badge/flask-3.0_API-000000.svg?logo=flask)](https://flask.palletsprojects.com/)
[![Pandas 2.2.2](https://img.shields.io/badge/pandas-2.2.2-150458.svg?logo=pandas)](https://pandas.pydata.org/)
[![Postgres 13 (Meta DB)](https://img.shields.io/badge/postgres-13-4169E1.svg?logo=postgresql)](https://www.postgresql.org/)
[![SQLite (Pipeline Store)](https://img.shields.io/badge/sqlite-storage-044A64.svg?logo=sqlite)](https://www.sqlite.org/)
[![BigQuery](https://img.shields.io/badge/google_bigquery-ingestion-1A73E8.svg?logo=googlecloud)](https://cloud.google.com/bigquery)
[![GCS](https://img.shields.io/badge/google_cloud_storage-ingestion-4285F4.svg?logo=googlecloud)](https://cloud.google.com/storage)
[![SQLAlchemy 1.4](https://img.shields.io/badge/sqlalchemy-1.4-CC0000.svg)](https://www.sqlalchemy.org/)
[![Docker Compose](https://img.shields.io/badge/docker%20compose-dev_env-1D63ED.svg?logo=docker)](https://docs.docker.com/compose/)
[![Faker 25.x](https://img.shields.io/badge/faker-synthetic_data-FF6F00.svg)](https://faker.readthedocs.io/)


This repository demonstrates a professional **Airflow ETL/ELT pipeline portfolio**, including end-to-end orchestration, transformation, and cloud integration for **research/academic-style datasets**.

Key highlights:

- **ETL/ELT Pipelines:** Extract, Transform/Enrich, Load data across multiple destinations  
- **Research & Academic Data Handling:** Experimental materials, suppliers, orders, and experiments  
- **API-Based Extraction:** Synthetic datasets via Flask API  
- **Data Loading:** SQLite, Google Cloud Storage (GCS), BigQuery  
- **Workflow Automation:** Scheduling, monitoring, and task dependencies via Airflow  
- **Containerized Environment:** Docker Compose for reproducibility and consistent dev setups  

This portfolio demonstrates skills in **data pipeline design, workflow automation, cloud integration, and research-oriented data processing**, making it ideal for **Data Engineering, MLOps, or AI/ML pipeline roles**.

---

## Tech Stack & Tools

- **Languages:** Python 3.12  
- **Workflow Orchestration:** Apache Airflow 2.x  
- **Data Storage:** SQLite, GCS, BigQuery  
- **Containerization:** Docker, Docker Compose  
- **Data Processing:** Pandas, Faker  
- **Version Control & Development:** Git, VS Code DevContainer  

---

## Getting Started

1. **Clone the repository**
   ```powershell
   git clone https://github.com/devopscicdgit/AirflowR-D-Orchestrator.git
   cd AirflowR-D-Orchestrator
   ```
2. Setup Python environment
    ```powershell
    .\scripts\1-environment-setup.ps1
    ```
3. Generate synthetic datasets
    ```powershell
    .\scripts\2-generate-api-datasets.ps1
    ```
4. Initialize Airflow environment
    ```powershell
    .\scripts\3-init-airflow.ps1
    ```
    Access Airflow webserver at http://localhost:8080

    Default credentials: admin / admin
    ### Accessing Airflow Environments

    Once the environment is running, you can interact with the following components:

    | Component    | Purpose                               | Access / Notes |
    |-------------|---------------------------------------|----------------|
    | **Webserver** | DAG monitoring, task management       | [http://localhost:8080](http://localhost:8080)  <br>**Default credentials:** `admin / admin` |
    | **Scheduler** | Executes and schedules DAG tasks      | Runs as a background process <br>Monitor via logs: <br>`docker-compose logs scheduler` |
    | **Postgres DB** | Stores metadata (DAG state, users, connections) | CLI access: <br>`docker-compose exec postgres psql -U airflow -d airflow` |
    | **Logs** | Task execution details | Located at: <br>`./logs/<dag_id>/<task_id>/<execution_date>/1.log` <br>Or view via Web UI |
    | **API Service** | Provides synthetic datasets | [http://localhost:5000](http://localhost:5000) <br>Available endpoints: <br>`/api/suppliers` <br>`/api/materials` <br>`/api/experiments` <br>`/api/orders` |
    | **Airflow CLI** | Command-line DAG management | Run commands inside webserver container: <br>`docker-compose exec webserver airflow <command>    # Example: airflow dags list, airflow tasks test    ` |


 5. Run DAGs   
    materials_experiments_ingestion: Upload CSV → GCS → BigQuery

    materials_orders_experiments_etl_api: API → SQLite ETL
6. Teardown environment (optional)
    ```powershell
    .\scripts\4-teardown.ps1
    ```
    Preserves data/datasets for future use

## Notes

- Adjust GCP project IDs and credentials in DAGs before deployment.

- Ensure Python 3.12 is installed and available via py -3.12.

- Synthetic datasets are for portfolio demonstration and learning purposes only.