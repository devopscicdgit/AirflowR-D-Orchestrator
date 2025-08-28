from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateExternalTableOperator

default_args = {
    "owner": "jamesxu",
}

with DAG(
    dag_id="materials_experiments_ingestion",
    description="ETL pipeline: upload experimental materials dataset to GCS and create BigQuery external table",
    default_args=default_args,
    start_date=datetime(2025, 8, 21),  # ✅ adjust if you want immediate run
    schedule_interval="@once",
    catchup=False,
    tags=["gcp", "bigquery", "gcs", "materials", "etl", "demo"],
) as dag:

    start_ingestion = EmptyOperator(task_id="start_ingestion")

    # Create bucket in GCS
    create_materials_bucket = GCSCreateBucketOperator(
        task_id="create_materials_bucket",
        bucket_name="james-materials-experiments-bucket",
        gcp_conn_id="my_gcs_conn",
    )

    # Upload CSV dataset to GCS
    upload_experiments_csv = LocalFilesystemToGCSOperator(
        task_id="upload_experiments_csv",
        src="/opt/airflow/data/datasets/materials-experiments.csv",
        dst="materials_experiments.csv",
        bucket="james-materials-experiments-bucket",
        gcp_conn_id="my_gcs_conn",
    )

    # Create BigQuery dataset
    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_materials_dataset",
        dataset_id="materials_experiments_dataset",
        gcp_conn_id="my_gcs_conn",
    )

    join_tasks = EmptyOperator(task_id="join_tasks")

    # Create BigQuery external table pointing to GCS
    create_bq_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_bq_external_table",
        table_resource={
            "tableReference": {
                "projectId": "decent-creek-469617-g7",  # 🔑 replace with your project ID
                "datasetId": "materials_experiments_dataset",
                "tableId": "experiments_table",
            },
            "externalDataConfiguration": {
            "sourceFormat": "CSV",
            "sourceUris": ["gs://james-materials-experiments-bucket/materials_experiments.csv"],
            "schema": {
                "fields": [
                    {"name": "date", "type": "DATE", "mode": "NULLABLE"},
                    {"name": "experiment_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "material_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "pressure_atm", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "researcher", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "result_yield_pct", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "temperature_c", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "category", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "density_g_cm3", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "melting_point_c", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "supplier_id", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "tensile_strength_mpa", "type": "FLOAT", "mode": "NULLABLE"},
                ]
            },
            "csvOptions": {
                "skipLeadingRows": 1
            }
        }

        },
        gcp_conn_id="my_gcs_conn",
    )

    end_ingestion = EmptyOperator(task_id="end_ingestion")

    # Task dependencies
    [create_materials_bucket >> upload_experiments_csv] >> join_tasks
    create_bq_dataset >> join_tasks
    join_tasks >> create_bq_external_table >> end_ingestion
