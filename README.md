# Airflow ETL/ELT Portfolio

<p> <img alt="Python" src="https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white"> <img alt="Airflow" src="https://img.shields.io/badge/Airflow-2.9.1_(imgs)%2F2.9.2_req-017CEE?logo=apacheairflow&logoColor=white"> <img alt="Flask" src="https://img.shields.io/badge/Flask-3.0_API-000000?logo=flask&logoColor=white"> <img alt="Pandas" src="https://img.shields.io/badge/Pandas-2.2.2-150458?logo=pandas&logoColor=white"> <img alt="Postgres" src="https://img.shields.io/badge/Postgres-13-4169E1?logo=postgresql&logoColor=white"> <img alt="SQLite" src="https://img.shields.io/badge/SQLite-Pipeline_DB-044A64?logo=sqlite&logoColor=white"> <img alt="BigQuery" src="https://img.shields.io/badge/Google-BigQuery-1A73E8?logo=googlecloud&logoColor=white"> <img alt="GCS" src="https://img.shields.io/badge/Google-Cloud_Storage-4285F4?logo=googlecloud&logoColor=white"> <img alt="SQLAlchemy" src="https://img.shields.io/badge/SQLAlchemy-1.4-CC0000"> <img alt="Docker Compose" src="https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white"> <img alt="Faker" src="https://img.shields.io/badge/Faker-25.x-FF6F00"> </p>


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
