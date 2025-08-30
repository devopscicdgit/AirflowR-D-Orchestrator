# Airflow ETL/ELT Portfolio

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
   git clone https://github.com/yourusername/airflow-portfolio.git
   cd airflow-portfolio
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