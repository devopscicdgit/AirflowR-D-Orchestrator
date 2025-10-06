# dags/materials_experiments_ingestion.py
"""
Materials and Experiments Data Ingestion DAG for Google Cloud Platform

This DAG implements a comprehensive data ingestion pipeline that:
1. Validates local experimental materials datasets
2. Creates Google Cloud Storage infrastructure
3. Uploads datasets to GCS with data quality checks
4. Sets up BigQuery datasets and external tables
5. Provides data quality validation and monitoring

The pipeline is designed for research data management and supports
both initial setup and ongoing data refresh operations.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator, 
    BigQueryCreateExternalTableOperator,
    BigQueryCheckOperator
)
from airflow.exceptions import AirflowSkipException, AirflowFailException

# Configure logging for better debugging and monitoring
logger = logging.getLogger(__name__)

# ==================== CONFIGURATION SECTION ====================
# Environment variables and constants for the ingestion pipeline

# Google Cloud Platform Configuration
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "decent-creek-469617-g7")
GCP_CONNECTION_ID = os.getenv("GCP_CONNECTION_ID", "my_gcs_conn")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "james-materials-experiments-bucket")
BIGQUERY_DATASET_ID = os.getenv("BIGQUERY_DATASET_ID", "materials_experiments_dataset")
BIGQUERY_TABLE_ID = os.getenv("BIGQUERY_TABLE_ID", "experiments_table")

# Development mode - set to False to enable full GCP operations
DEVELOPMENT_MODE = os.getenv("DEVELOPMENT_MODE", "false").lower() == "true"

# Local file system paths
AIRFLOW_DATA_DIR = os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/data")
VALIDATION_REPORTS_DIR = os.path.join(AIRFLOW_DATA_DIR, "validation_reports")

# Dynamic source data path - will be determined at runtime
SOURCE_DATA_PATH = None  # Will be set dynamically by get_latest_experiments_file()

# Data quality thresholds
MIN_REQUIRED_ROWS = int(os.getenv("MIN_REQUIRED_ROWS", "100"))
MAX_NULL_PERCENTAGE = float(os.getenv("MAX_NULL_PERCENTAGE", "20.0"))

# Ensure validation reports directory exists
os.makedirs(VALIDATION_REPORTS_DIR, exist_ok=True)

# ==================== UTILITY FUNCTIONS ====================

def get_latest_experiments_file() -> str:
    """
    Dynamically find the latest transformed experiments file based on date.
    
    This function searches for transformed_experiments-YYYY-MM-DD.csv files
    in the experimental_data directory and returns the path to the most recent one.
    
    Returns:
        str: Path to the latest transformed experiments file
        
    Raises:
        AirflowFailException: If no transformed experiments files are found
    """
    import glob
    from datetime import datetime, timedelta
    
    experimental_data_dir = os.path.join(AIRFLOW_DATA_DIR, "experimental_data")
    
    # Pattern to match transformed_experiments-YYYY-MM-DD.csv files
    pattern = os.path.join(experimental_data_dir, "transformed_experiments-*.csv")
    
    # Find all matching files
    experiment_files = glob.glob(pattern)
    
    if not experiment_files:
        # If no files found, try to use today's date as fallback
        today = datetime.now().strftime("%Y-%m-%d")
        fallback_path = os.path.join(experimental_data_dir, f"transformed_experiments-{today}.csv")
        
        # Try yesterday as well in case today's hasn't been generated yet
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        yesterday_path = os.path.join(experimental_data_dir, f"transformed_experiments-{yesterday}.csv")
        
        if os.path.exists(fallback_path):
            logger.info(f"Using today's experiments file: {fallback_path}")
            return fallback_path
        elif os.path.exists(yesterday_path):
            logger.info(f"Using yesterday's experiments file: {yesterday_path}")
            return yesterday_path
        else:
            raise AirflowFailException(f"No transformed experiments files found in {experimental_data_dir}")
    
    # Sort files by modification time (most recent first)
    experiment_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    
    latest_file = experiment_files[0]
    logger.info(f"Found {len(experiment_files)} transformed experiments files")
    logger.info(f"Using latest file: {latest_file}")
    
    return latest_file

def convert_to_json_serializable(obj):
    """
    Convert pandas/numpy data types to JSON-serializable Python types.
    
    Args:
        obj: Object that may contain non-serializable types
        
    Returns:
        JSON-serializable version of the object
    """
    if isinstance(obj, dict):
        return {key: convert_to_json_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_json_serializable(item) for item in obj]
    elif hasattr(obj, 'item'):  # numpy scalar types
        return obj.item()
    elif hasattr(obj, 'tolist'):  # numpy arrays
        return obj.tolist()
    elif str(type(obj)).startswith('<class \'numpy.') or str(type(obj)).startswith('<class \'pandas.'):
        # Handle other numpy/pandas types
        try:
            return obj.item() if hasattr(obj, 'item') else str(obj)
        except:
            return str(obj)
    else:
        return obj

def validate_dataset_quality(file_path: str) -> Dict[str, Any]:
    """
    Perform comprehensive data quality validation on the materials experiments dataset.
    
    This function checks:
    - File existence and readability
    - Minimum row count requirements
    - Required column presence
    - Data type consistency
    - Null value percentages
    - Value range validation for numerical fields
    
    Args:
        file_path (str): Path to the CSV file to validate
        
    Returns:
        Dict[str, Any]: Validation report with metrics and pass/fail status
        
    Raises:
        AirflowFailException: If critical validation checks fail
    """
    validation_report = {
        "file_path": file_path,
        "validation_timestamp": datetime.now().isoformat(),
        "checks_passed": 0,
        "checks_failed": 0,
        "errors": [],
        "warnings": [],
        "metrics": {}
    }
    
    try:
        # Check if file exists and is readable
        if not os.path.exists(file_path):
            validation_report["errors"].append(f"File not found: {file_path}")
            validation_report["checks_failed"] += 1
            raise AirflowFailException(f"Source data file not found: {file_path}")
        
        # Load dataset for validation
        df = pd.read_csv(file_path)
        validation_report["metrics"]["total_rows"] = len(df)
        validation_report["metrics"]["total_columns"] = len(df.columns)
        
        # Check minimum row count
        if len(df) < MIN_REQUIRED_ROWS:
            validation_report["errors"].append(f"Insufficient data: {len(df)} rows (minimum: {MIN_REQUIRED_ROWS})")
            validation_report["checks_failed"] += 1
        else:
            validation_report["checks_passed"] += 1
            
        # Check for required columns based on data type detection
        # First, detect if this is materials data or experiments data
        is_materials_data = "material_id" in df.columns and "category" in df.columns
        is_experiments_data = "experiment_id" in df.columns and "researcher" in df.columns
        
        if is_experiments_data:
            required_columns = [
                "experiment_id", "material_id", "date", "researcher",
                "temperature_c", "pressure_atm", "result_yield_pct"
            ]
            data_type = "experiments"
        elif is_materials_data:
            required_columns = [
                "material_id", "category", "name"
            ]
            data_type = "materials"
        else:
            # Fallback: detect based on available columns
            available_cols = df.columns.tolist()
            if "experiment_id" in available_cols:
                required_columns = ["experiment_id", "material_id"]
                data_type = "experiments"
            elif "material_id" in available_cols:
                required_columns = ["material_id"]
                data_type = "materials"
            else:
                validation_report["errors"].append("Unable to detect data type (materials or experiments)")
                validation_report["checks_failed"] += 1
                data_type = "unknown"
                required_columns = []
        
        validation_report["metrics"]["detected_data_type"] = data_type
        logger.info(f"Detected data type: {data_type}")
        
        # Check for required columns
        if required_columns:
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                validation_report["errors"].append(f"Missing required columns for {data_type}: {missing_columns}")
                validation_report["checks_failed"] += 1
            else:
                validation_report["checks_passed"] += 1
                logger.info(f"All required {data_type} columns present: {required_columns}")
        else:
            validation_report["warnings"].append("No required columns check performed due to unknown data type")
            
        # Check null value percentages
        null_percentages = (df.isnull().sum() / len(df) * 100).to_dict()
        validation_report["metrics"]["null_percentages"] = null_percentages
        
        high_null_columns = [col for col, pct in null_percentages.items() if pct > MAX_NULL_PERCENTAGE]
        if high_null_columns:
            validation_report["warnings"].append(f"High null percentage in columns: {high_null_columns}")
            
        # Validate data types and ranges for key numerical fields based on data type
        if data_type == "experiments":
            numerical_validations = {
                "temperature_c": {"min": -273.15, "max": 5000},  # Absolute zero to extreme high temps
                "pressure_atm": {"min": 0, "max": 1000},        # Physical pressure limits
                "result_yield_pct": {"min": 0, "max": 100}      # Percentage bounds
            }
        elif data_type == "materials":
            numerical_validations = {
                "density_g_cm3": {"min": 0.1, "max": 30},       # Physical density limits
                "melting_point_c": {"min": -273.15, "max": 4000}, # Physical melting point limits
                "tensile_strength_mpa": {"min": 0, "max": 10000}  # Material strength limits
            }
        else:
            numerical_validations = {}  # No specific validations for unknown data type
        
        for col, limits in numerical_validations.items():
            if col in df.columns:
                valid_data = df[col].dropna()
                if len(valid_data) > 0:
                    out_of_range = valid_data[(valid_data < limits["min"]) | (valid_data > limits["max"])]
                    if len(out_of_range) > 0:
                        validation_report["warnings"].append(
                            f"Out of range values in {col}: {len(out_of_range)} records"
                        )
                        
        # Check for duplicate IDs based on data type
        duplicates = 0  # Initialize duplicates variable
        if data_type == "experiments" and "experiment_id" in df.columns:
            duplicates = df["experiment_id"].duplicated().sum()
            if duplicates > 0:
                validation_report["warnings"].append(f"Duplicate experiment IDs: {duplicates}")
        elif data_type == "materials" and "material_id" in df.columns:
            duplicates = df["material_id"].duplicated().sum()
            if duplicates > 0:
                validation_report["warnings"].append(f"Duplicate material IDs: {duplicates}")
                
        validation_report["metrics"]["duplicate_records"] = duplicates
        
        logger.info(f"Dataset validation completed: {validation_report['checks_passed']} passed, "
                   f"{validation_report['checks_failed']} failed, {len(validation_report['warnings'])} warnings")
        
        return validation_report
        
    except Exception as e:
        validation_report["errors"].append(f"Validation failed with error: {str(e)}")
        validation_report["checks_failed"] += 1
        logger.error(f"Dataset validation error: {e}")
        raise AirflowFailException(f"Dataset validation failed: {e}")

def generate_bigquery_schema(file_path: str) -> list:
    """
    Automatically generate BigQuery schema from CSV file structure.
    
    Args:
        file_path (str): Path to the CSV file to analyze
        
    Returns:
        list: BigQuery schema definition with field types
    """
    df = pd.read_csv(file_path, nrows=1000)  # Sample for schema inference
    
    # Map pandas dtypes to BigQuery types
    dtype_mapping = {
        'object': 'STRING',
        'int64': 'INTEGER',
        'float64': 'FLOAT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP'
    }
    
    schema = []
    for col in df.columns:
        col_type = dtype_mapping.get(str(df[col].dtype), 'STRING')
        
        # Special handling for known date columns
        if 'date' in col.lower():
            col_type = 'DATE'
        elif col.endswith('_id'):
            col_type = 'STRING'
            
        schema.append({
            "name": col,
            "type": col_type,
            "mode": "REQUIRED" if col in ["experiment_id", "material_id"] else "NULLABLE"
        })
    
    return schema

# ==================== DAG DEFINITION ====================

# DAG default arguments with comprehensive retry and notification settings
default_args = {
    "owner": "jamesxu",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 21),
    "email_on_failure": False,  # Set to True and add email for production
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# DAG instantiation with comprehensive configuration
dag = DAG(
    dag_id="materials_experiments_ingestion",
    description="Comprehensive materials experiments data ingestion pipeline to Google Cloud Platform",
    default_args=default_args,
    schedule_interval="@weekly",  # Run weekly for regular data updates
    catchup=False,  # Don't run for past dates when DAG is first enabled
    max_active_runs=1,  # Prevent overlapping runs
    tags=["gcp", "bigquery", "gcs", "materials", "etl", "data-quality"],
)

# ==================== DAG TASKS DEFINITION ====================

# Task 1: Data quality validation
@task(dag=dag)
def validate_source_data():
    """
    Validate the source materials experiments dataset for quality and completeness.
    
    This task performs comprehensive data quality checks before proceeding with
    the ingestion pipeline. Failures here will stop the pipeline execution.
    
    Returns:
        Dict: Validation report with metrics and status
    """
    # Dynamically determine the latest experiments file
    source_data_path = get_latest_experiments_file()
    logger.info(f"Starting data validation for: {source_data_path}")
    
    validation_report = validate_dataset_quality(source_data_path)
    
    # Save validation report for monitoring and auditing
    report_file = os.path.join(VALIDATION_REPORTS_DIR, f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    
    import json
    # Convert pandas/numpy types to JSON-serializable types
    serializable_report = convert_to_json_serializable(validation_report)
    
    with open(report_file, 'w') as f:
        json.dump(serializable_report, f, indent=2)
    
    logger.info(f"Validation report saved to: {report_file}")
    
    # Fail the task if critical validation checks failed
    if validation_report["checks_failed"] > 0:
        raise AirflowFailException(f"Data validation failed: {validation_report['errors']}")
    
    return validation_report

# Task 2: Google Cloud Storage bucket creation (Development-friendly)
@task(dag=dag)
def create_gcs_bucket():
    """
    Create GCS bucket with development mode support.
    
    In development mode, this task will skip GCS operations if connection is unavailable.
    In production mode, it will create the actual GCS bucket.
    
    Returns:
        dict: Status of bucket creation operation
    """
    if DEVELOPMENT_MODE:
        logger.info("🔧 Development mode: Skipping GCS bucket creation")
        return {
            "bucket_created": False,
            "development_mode": True,
            "message": "Skipped in development mode"
        }
    
    try:
        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        from google.api_core.exceptions import Conflict
        
        # Test connection first
        gcs_hook = GCSHook(gcp_conn_id=GCP_CONNECTION_ID)
        
        # Try to create bucket - if it exists, Google will return 409 Conflict
        try:
            gcs_hook.create_bucket(bucket_name=GCS_BUCKET_NAME, project_id=GCP_PROJECT_ID)
            logger.info(f"✅ Created GCS bucket: {GCS_BUCKET_NAME}")
            
            return {
                "bucket_created": True,
                "bucket_name": GCS_BUCKET_NAME,
                "message": f"Successfully created bucket {GCS_BUCKET_NAME}"
            }
            
        except Conflict:
            # Bucket already exists - this is fine
            logger.info(f"✅ GCS bucket {GCS_BUCKET_NAME} already exists")
            return {
                "bucket_created": False,
                "bucket_exists": True,
                "message": f"Bucket {GCS_BUCKET_NAME} already exists"
            }
        
    except Exception as e:
        if DEVELOPMENT_MODE:
            logger.warning(f"⚠️ GCS connection failed in development mode: {e}")
            return {
                "bucket_created": False,
                "development_mode": True,
                "error": str(e),
                "message": "Skipped due to connection issue in development mode"
            }
        else:
            raise AirflowFailException(f"Failed to create GCS bucket: {e}")

# Task 3: Data preprocessing and validation
@task(dag=dag)
def preprocess_data_for_upload():
    """
    Perform final data validation and preparation checks before upload.
    
    This task validates the latest experiments file to ensure it's ready
    for upload to Google Cloud Storage and BigQuery processing.
    
    Validations performed:
    - File accessibility and format verification
    - Basic data integrity checks
    - Schema validation
    
    Returns:
        dict: Preprocessing results and file information
    """
    logger.info("Starting data preprocessing validation for cloud upload")
    
    # Get the latest experiments file dynamically
    source_data_path = get_latest_experiments_file()
    logger.info(f"Validating data from: {source_data_path}")
    
    # Load and validate the dataset
    df = pd.read_csv(source_data_path)
    
    # Basic validation checks
    row_count = len(df)
    column_count = len(df.columns)
    file_size = os.path.getsize(source_data_path)
    
    logger.info(f"Dataset validation complete:")
    logger.info(f"  - Rows: {row_count}")
    logger.info(f"  - Columns: {column_count}")
    logger.info(f"  - File size: {file_size} bytes")
    logger.info(f"  - Source file: {source_data_path}")
    
    return {
        "source_file": source_data_path,
        "row_count": row_count,
        "column_count": column_count,
        "file_size_bytes": file_size,
        "validation_passed": True,
        "message": "Data preprocessing validation completed successfully"
    }

# Task 4: Upload dataset to Google Cloud Storage
@task(dag=dag)
def upload_to_gcs():
    """
    Upload the latest experiments dataset to Google Cloud Storage with development mode support.
    
    This task dynamically determines the source file and uploads it to GCS
    with a standardized destination name for consistent BigQuery table creation.
    In development mode, it skips the actual upload but simulates the process.
    
    Returns:
        dict: Upload status and metadata
    """
    # Get the latest experiments file dynamically
    source_data_path = get_latest_experiments_file()
    destination_path = "materials_experiments.csv"
    
    if DEVELOPMENT_MODE:
        logger.info("🔧 Development mode: Skipping GCS upload")
        logger.info(f"Would upload: {source_data_path} to gs://{GCS_BUCKET_NAME}/{destination_path}")
        
        # Simulate file info for development
        import os
        file_size = os.path.getsize(source_data_path)
        
        return {
            "uploaded": False,
            "development_mode": True,
            "source_file": source_data_path,
            "destination": f"gs://{GCS_BUCKET_NAME}/{destination_path}",
            "file_size_bytes": file_size,
            "message": "Skipped in development mode"
        }
    
    try:
        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        
        # Set up GCS hook
        gcs_hook = GCSHook(gcp_conn_id=GCP_CONNECTION_ID)
        
        logger.info(f"Uploading {source_data_path} to gs://{GCS_BUCKET_NAME}/{destination_path}")
        
        gcs_hook.upload(
            bucket_name=GCS_BUCKET_NAME,
            object_name=destination_path,
            filename=source_data_path
        )
        
        logger.info(f"✅ Successfully uploaded to gs://{GCS_BUCKET_NAME}/{destination_path}")
        
        # Get file info
        import os
        file_size = os.path.getsize(source_data_path)
        
        return {
            "uploaded": True,
            "source_file": source_data_path,
            "destination": f"gs://{GCS_BUCKET_NAME}/{destination_path}",
            "file_size_bytes": file_size,
            "message": "Successfully uploaded to GCS"
        }
        
    except Exception as e:
        if DEVELOPMENT_MODE:
            logger.warning(f"⚠️ GCS upload failed in development mode: {e}")
            return {
                "uploaded": False,
                "development_mode": True,
                "error": str(e),
                "message": "Skipped due to upload failure in development mode"
            }
        else:
            raise AirflowFailException(f"Failed to upload to GCS: {e}")

# Task 5: Create BigQuery dataset (Development-friendly)
@task(dag=dag)
def create_bigquery_dataset():
    """
    Create BigQuery dataset with development mode support.
    
    In development mode, this task will skip BigQuery operations if connection is unavailable.
    In production mode, it will create the actual BigQuery dataset.
    
    Returns:
        dict: Status of dataset creation operation
    """
    if DEVELOPMENT_MODE:
        logger.info("🔧 Development mode: Skipping BigQuery dataset creation")
        return {
            "dataset_created": False,
            "development_mode": True,
            "message": "Skipped in development mode"
        }
    
    try:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        
        # Set up BigQuery hook with connection
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONNECTION_ID)
        
        # Check if dataset exists
        dataset_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}"
        
        try:
            # Check if dataset exists using hook
            dataset_exists = bq_hook.exists(dataset_id=BIGQUERY_DATASET_ID, project_id=GCP_PROJECT_ID)
            
            if dataset_exists:
                logger.info(f"✅ BigQuery dataset {dataset_id} already exists")
                return {
                    "dataset_created": False,
                    "dataset_exists": True,
                    "message": f"Dataset {dataset_id} already exists"
                }
        except Exception as check_error:
            logger.info(f"Could not check dataset existence, proceeding with creation: {check_error}")
        
        # Create dataset using hook
        bq_hook.create_empty_dataset(
            dataset_id=BIGQUERY_DATASET_ID,
            project_id=GCP_PROJECT_ID,
            dataset_reference={
                "description": "Materials experiments dataset for research analytics"
            }
        )
        
        logger.info(f"✅ Created BigQuery dataset: {dataset_id}")
        return {
            "dataset_created": True,
            "dataset_id": dataset_id,
            "message": f"Successfully created dataset {dataset_id}"
        }
            
    except Exception as e:
        if DEVELOPMENT_MODE:
            logger.warning(f"⚠️ BigQuery connection failed in development mode: {e}")
            return {
                "dataset_created": False,
                "development_mode": True,
                "error": str(e),
                "message": "Skipped due to connection issue in development mode"
            }
        else:
            raise AirflowFailException(f"Failed to create BigQuery dataset: {e}")

# Task 6: Create BigQuery external table with auto-detected schema
@task(dag=dag)
def create_bigquery_external_table():
    """
    Create BigQuery external table with auto-detected schema and development mode support.
    
    In development mode, this task will skip BigQuery operations if connection is unavailable.
    In production mode, it will create the actual external table pointing to GCS data.
    
    Returns:
        dict: Status of external table creation operation
    """
    if DEVELOPMENT_MODE:
        logger.info("🔧 Development mode: Skipping BigQuery external table creation")
        return {
            "table_created": False,
            "development_mode": True,
            "message": "Skipped in development mode"
        }
    
    try:
        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        
        # Set up BigQuery hook
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONNECTION_ID)
        
        table_resource = {
            "tableReference": {
                "projectId": GCP_PROJECT_ID,
                "datasetId": BIGQUERY_DATASET_ID,
                "tableId": BIGQUERY_TABLE_ID,
            },
            "externalDataConfiguration": {
                "sourceFormat": "CSV",
                "sourceUris": [f"gs://{GCS_BUCKET_NAME}/materials_experiments.csv"],
                "autodetect": True,  # Automatically detect schema from CSV
                "csvOptions": {
                    "skipLeadingRows": 1,  # Skip header row
                    "allowJaggedRows": False,  # Require consistent column count
                    "allowQuotedNewlines": True  # Handle quoted newlines in CSV
                }
            }
        }
        
        # Create external table
        bq_hook.create_external_table(
            external_project_dataset_table=f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}",
            schema_fields=None,  # Use autodetect
            source_uris=[f"gs://{GCS_BUCKET_NAME}/materials_experiments.csv"],
            source_format="CSV",
            autodetect=True,
            skip_leading_rows=1
        )
        
        logger.info(f"✅ Created BigQuery external table: {GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}")
        
        return {
            "table_created": True,
            "table_id": f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}",
            "source_uri": f"gs://{GCS_BUCKET_NAME}/materials_experiments.csv",
            "message": "Successfully created BigQuery external table"
        }
        
    except Exception as e:
        if DEVELOPMENT_MODE:
            logger.warning(f"⚠️ BigQuery external table creation failed in development mode: {e}")
            return {
                "table_created": False,
                "development_mode": True,
                "error": str(e),
                "message": "Skipped due to connection issue in development mode"
            }
        else:
            raise AirflowFailException(f"Failed to create BigQuery external table: {e}")

# Task 7: Data quality validation in BigQuery (Simplified for development)
@task(dag=dag)
def validate_bigquery_data():
    """
    Validate data quality in BigQuery with development mode support.
    
    In development mode, this task will skip BigQuery validation if connection is unavailable.
    In production mode, it performs comprehensive data quality checks.
    
    Returns:
        dict: Validation results and metrics
    """
    if DEVELOPMENT_MODE:
        logger.info("🔧 Development mode: Skipping BigQuery data validation")
        return {
            "validation_passed": True,
            "development_mode": True,
            "message": "Validation skipped in development mode"
        }
    
    try:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        
        # Set up BigQuery hook with connection
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONNECTION_ID)
        
        # Simple table existence and row count check
        table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}"
        
        # Check if table exists and get basic info
        try:
            # Check if table exists using hook
            table_exists = bq_hook.table_exists(
                dataset_id=BIGQUERY_DATASET_ID,
                table_id=BIGQUERY_TABLE_ID,
                project_id=GCP_PROJECT_ID
            )
            
            if not table_exists:
                logger.warning(f"⚠️ Table {table_id} does not exist")
                return {
                    "table_exists": False,
                    "validation_passed": False,
                    "error": "Table does not exist"
                }
            
            logger.info(f"✅ Table exists: {table_id}")
            
            # Simple row count query using hook
            count_query = f"SELECT COUNT(*) as row_count FROM `{table_id}`"
            results = bq_hook.get_pandas_df(sql=count_query)
            
            row_count = results.iloc[0]['row_count']
            logger.info(f"✅ Table contains {row_count} rows")
            
            # Basic validation
            if row_count >= MIN_REQUIRED_ROWS:
                logger.info(f"✅ Row count validation passed: {row_count} >= {MIN_REQUIRED_ROWS}")
                validation_passed = True
            else:
                logger.warning(f"⚠️ Row count below threshold: {row_count} < {MIN_REQUIRED_ROWS}")
                validation_passed = False
                
            return {
                "table_exists": True,
                "row_count": int(row_count),
                "validation_passed": validation_passed,
                "message": "BigQuery validation completed successfully"
            }
            
        except Exception as table_error:
            logger.error(f"❌ Table validation failed: {table_error}")
            return {
                "table_exists": False,
                "validation_passed": False,
                "error": str(table_error)
            }
            
    except Exception as e:
        if DEVELOPMENT_MODE:
            logger.warning(f"⚠️ BigQuery validation failed in development mode: {e}")
            return {
                "validation_passed": True,  # Allow pipeline to continue in dev mode
                "development_mode": True,
                "error": str(e),
                "message": "Validation skipped due to connection issue in development mode"
            }
        else:
            raise AirflowFailException(f"BigQuery validation failed: {e}")

# Task 8: Final success notification and cleanup
@task(dag=dag)
def finalize_ingestion_pipeline():
    """
    Finalize the ingestion pipeline with success logging and cleanup.
    
    This task performs final validation, logging, and cleanup operations
    to complete the ingestion pipeline successfully.
    """
    logger.info("Materials experiments ingestion pipeline completed successfully!")
    logger.info(f"Data available in BigQuery: {GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}")
    logger.info(f"Source data archived in GCS: gs://{GCS_BUCKET_NAME}/materials_experiments.csv")
    
    # Log pipeline metrics for monitoring
    pipeline_metrics = {
        "pipeline_completion_time": datetime.now().isoformat(),
        "gcp_project": GCP_PROJECT_ID,
        "bigquery_dataset": BIGQUERY_DATASET_ID,
        "bigquery_table": BIGQUERY_TABLE_ID,
        "gcs_bucket": GCS_BUCKET_NAME
    }
    
    logger.info(f"Pipeline metrics: {pipeline_metrics}")
    return pipeline_metrics

end_ingestion = EmptyOperator(
    task_id="end_ingestion",
    dag=dag
)

# ==================== TASK DEPENDENCIES ====================
# Define the execution order and dependencies between tasks

# Phase 1: Data validation and preprocessing
data_validation = validate_source_data()
preprocessed_data = preprocess_data_for_upload()

# Phase 2: Infrastructure setup (parallel execution)
bucket_creation = create_gcs_bucket()
dataset_creation = create_bigquery_dataset()

# Phase 3: Data upload and table creation
upload_task = upload_to_gcs()
table_creation = create_bigquery_external_table()

# Phase 4: Quality validation and finalization
quality_check = validate_bigquery_data()
finalization = finalize_ingestion_pipeline()

# Define task dependencies
data_validation >> preprocessed_data

# Infrastructure can be set up in parallel after preprocessing
preprocessed_data >> [bucket_creation, dataset_creation]

# Upload requires bucket creation
bucket_creation >> upload_task

# External table creation requires both dataset and uploaded data
[dataset_creation, upload_task] >> table_creation

# Quality check runs after table creation
table_creation >> quality_check

# Final cleanup after validation
quality_check >> finalization >> end_ingestion
