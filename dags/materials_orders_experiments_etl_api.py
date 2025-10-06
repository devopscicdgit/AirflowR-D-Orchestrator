# dags/materials_orders_experiments_etl_api.py
"""
Airflow DAG for API-driven ETL pipeline that processes materials, orders, and experiments data.

This DAG performs the following operations:
1. Extract: Fetches data from REST API endpoints for suppliers, materials, orders, and experiments
2. Transform: Enriches orders and experiments data by joining with supplier and material information
3. Load: Upserts the transformed data into a SQLite database with automatic schema evolution

The pipeline is designed to handle daily incremental loads with robust error handling
and data validation throughout the process.
"""

import os
import re
import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

from utils.data_helpers import setup_database_connection, upsert, SQL_TYPE_MAPPING

# Initialize logging configuration for the DAG
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ------- CONFIGURATION SECTION -------
# Environment variables and directory setup for the ETL pipeline

# Primary data directory - can be overridden via environment variable
AIRFLOW_DATA_DIR = os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/data")

# Directory for storing experimental/transformed data files during ETL process
DATA_DIR = os.path.join(AIRFLOW_DATA_DIR, "experimental_data")

# SQLite database file path for storing final processed data
DATABASE_FILE = os.path.join(AIRFLOW_DATA_DIR, "api_database.db")

# API configuration - base URL for the materials management API
API_URL = os.getenv("MATERIALS_API_URL", "http://api-services:5000/api")  # Configurable via environment

# API version for endpoint routing (future-proofing)
API_VERSION = "v1"

# Ensure data directory exists before pipeline execution
os.makedirs(DATA_DIR, exist_ok=True)


def _sanitize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sanitize DataFrame column names for database compatibility.
    
    This function performs the following operations:
    1. Converts column names to lowercase
    2. Replaces non-alphanumeric characters with underscores
    3. Strips leading/trailing underscores
    4. Handles empty column names by replacing with 'col'
    5. Removes duplicate columns (keeps first occurrence)
    
    Args:
        df (pd.DataFrame): Input DataFrame with potentially problematic column names
        
    Returns:
        pd.DataFrame: DataFrame with sanitized column names safe for SQL operations
    """
    df = df.copy()
    
    # Sanitize each column name: lowercase, replace special chars, handle empty names
    df.columns = [
        re.sub(r"[^\w]+", "_", str(c).strip().lower()).strip("_") or "col" for c in df.columns
    ]
    
    # Remove exact duplicate column names, keeping first occurrence
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def _safe_json_to_df(payload):
    """
    Safely convert various JSON API response formats into a pandas DataFrame.
    
    Handles multiple common API response patterns:
    1. List of dictionaries: [{"id": 1, "name": "Item1"}, {"id": 2, "name": "Item2"}]
    2. Dictionary with data array: {"data": [...], "meta": {...}}
    3. Dictionary with items/results array: {"items": [...]} or {"results": [...]}
    4. Single dictionary record: {"id": 1, "name": "Item1"}
    5. None/empty responses
    
    Args:
        payload: API response payload (list, dict, or None)
        
    Returns:
        pd.DataFrame: Normalized DataFrame from the JSON payload, empty DataFrame if conversion fails
    """
    if payload is None:
        return pd.DataFrame()
        
    if isinstance(payload, list):
        # Direct list of records - most common API pattern
        return pd.json_normalize(payload)
        
    if isinstance(payload, dict):
        # Try common wrapper patterns where actual data is nested
        for key in ("data", "items", "results"):
            if key in payload and isinstance(payload[key], list):
                return pd.json_normalize(payload[key])
                
        # If no wrapper found, treat the entire dict as a single record
        return pd.json_normalize([payload])
        
    # Fallback for unexpected payload types
    return pd.DataFrame()


# ---------------- AIRFLOW DAG DEFINITION ----------------

@dag(
    dag_id="materials_orders_experiments_etl_api",
    description="API-driven ETL pipeline for materials, orders, and experiments data with SQLite storage",
    schedule_interval="@daily",  # Runs once per day
    start_date=datetime(2025, 8, 10),  # When the DAG should start running
    catchup=False,  # Don't run for past dates when DAG is first enabled
    default_args={
        "owner": "jamesxu",  # DAG owner for monitoring and notifications
        "retries": 2,  # Number of automatic retries on task failure
        "retry_delay": timedelta(minutes=2)  # Wait time between retries
    },
    tags=["etl", "api", "sqlite"],  # Tags for DAG organization and filtering
)
def materials_orders_experiments_etl_api():
    """
    Main DAG function defining the ETL pipeline workflow.
    
    This DAG implements a three-stage ETL process:
    1. Extract: Fetch data from API endpoints
    2. Transform: Enrich and normalize the data
    3. Load: Store data in SQLite database
    """

    @task()
    def extract(**kwargs):
        """
        EXTRACT PHASE: Fetch data from API endpoints and save as CSV files.
        
        This task performs the following operations:
        1. Creates a date-specific directory for storing raw data
        2. Fetches data from four API endpoints:
           - suppliers (static reference data)
           - materials (static reference data)  
           - orders (filtered by order_date)
           - experiments (filtered by date)
        3. Converts JSON responses to DataFrames with error handling
        4. Sanitizes column names for downstream processing
        5. Saves each dataset as a CSV file in the date directory
        
        Args:
            **kwargs: Airflow context variables including logical_date
            
        Returns:
            dict: Mapping of dataset names to their CSV file paths
        """
        # Get the execution date for this DAG run
        logical_date = kwargs["logical_date"]
        date = logical_date.strftime("%Y-%m-%d")
        
        # Create date-specific directory for organizing daily data extracts
        run_dir = os.path.join(DATA_DIR, date)
        os.makedirs(run_dir, exist_ok=True)

        def get_json(endpoint, params=None, retries=3, timeout=8):
            """
            Robust API client with retry logic and error handling.
            
            Args:
                endpoint (str): API endpoint path (e.g., 'suppliers', 'orders')
                params (dict, optional): Query parameters for the request
                retries (int): Maximum number of retry attempts
                timeout (int): Request timeout in seconds
                
            Returns:
                dict/list: Parsed JSON response
                
            Raises:
                Exception: If all retry attempts fail
            """
            url = f"{API_URL}/{endpoint}"
            
            for attempt in range(1, retries + 1):
                try:
                    resp = requests.get(url, params=params, timeout=timeout)
                    resp.raise_for_status()  # Raise exception for HTTP error codes
                    return resp.json()
                except Exception as e:
                    logger.warning("Fetch attempt %d failed for %s : %s", attempt, url, e)
                    if attempt == retries:
                        raise  # Re-raise exception if all retries exhausted
            return []

        # Dictionary to store processed datasets
        datasets = {}

        # Fetch reference data (suppliers & materials) - no date filtering needed
        # These are typically static or slowly-changing dimension tables
        suppliers_payload = get_json("suppliers")
        materials_payload = get_json("materials")

        # Fetch transactional data (orders & experiments) - filtered by date
        # These contain the daily business transactions we want to process
        orders_payload = get_json("orders", params={"order_date": date})
        experiments_payload = get_json("experiments", params={"date": date})

        # Convert API responses to DataFrames using safe conversion
        # This handles various JSON response formats and provides empty DataFrames for failures
        df_suppliers = _safe_json_to_df(suppliers_payload)
        df_materials = _safe_json_to_df(materials_payload)
        df_orders = _safe_json_to_df(orders_payload)
        df_experiments = _safe_json_to_df(experiments_payload)

        # Sanitize column names to ensure database compatibility
        # This prevents issues with special characters, spaces, and reserved SQL keywords
        df_suppliers = _sanitize_cols(df_suppliers)
        df_materials = _sanitize_cols(df_materials)
        df_orders = _sanitize_cols(df_orders)
        df_experiments = _sanitize_cols(df_experiments)

        # Save each dataset as CSV and build file path mapping for downstream tasks
        files = {}
        for name, df in (
            ("suppliers", df_suppliers),
            ("materials", df_materials),
            ("orders", df_orders),
            ("experiments", df_experiments),
        ):
            path = os.path.join(run_dir, f"{name}.csv")
            df.to_csv(path, index=False)
            logger.info("Fetched %s rows for %s -> %s", len(df), name, path)
            files[name] = path

        return files

    @task()
    def transform(file_paths: dict, **kwargs):
        """
        TRANSFORM PHASE: Enrich and normalize data by joining reference tables.
        
        This task performs the following operations:
        1. Loads CSV files from the extract phase
        2. Validates that required datasets are available
        3. Adds prefixes to supplier/material columns to prevent naming conflicts
        4. Joins orders with supplier and material data for enrichment
        5. Joins experiments with material data for enrichment
        6. Saves enriched datasets as transformed CSV files
        
        The join strategy:
        - Orders are enriched with both supplier and material information
        - Experiments are enriched with material information only
        - Column prefixing prevents _x/_y suffix collisions during joins
        
        Args:
            file_paths (dict): Mapping of dataset names to CSV file paths from extract task
            **kwargs: Airflow context variables including logical_date
            
        Returns:
            dict: Mapping of transformed dataset names to their file paths
            
        Raises:
            AirflowSkipException: If required datasets are missing or empty
        """
        date = kwargs["logical_date"].strftime("%Y-%m-%d")

        def read_df(path, name):
            """
            Safely read CSV file into DataFrame with error handling.
            
            Args:
                path (str): File path to read
                name (str): Dataset name for logging
                
            Returns:
                pd.DataFrame: Loaded and sanitized DataFrame, empty if file missing/corrupt
            """
            try:
                if not path or not os.path.exists(path):
                    logger.warning("Missing file for %s: %s", name, path)
                    return pd.DataFrame()
                    
                df = pd.read_csv(path)
                df = _sanitize_cols(df)  # Ensure consistent column naming
                logger.info("%s loaded: %d rows, cols=%s", name, len(df), list(df.columns)[:10])
                return df
            except Exception as e:
                logger.exception("Failed reading %s from %s: %s", name, path, e)
                return pd.DataFrame()

        # Load all datasets from extract phase
        suppliers = read_df(file_paths.get("suppliers"), "suppliers")
        materials = read_df(file_paths.get("materials"), "materials")
        orders = read_df(file_paths.get("orders"), "orders")
        experiments = read_df(file_paths.get("experiments"), "experiments")

        # Validate that core reference data is available
        # Without suppliers, materials, and orders, the enrichment process cannot proceed
        if suppliers.empty or materials.empty or orders.empty:
            raise AirflowSkipException("Missing required datasets (suppliers/materials/orders); skipping transform.")

        # COLUMN PREFIXING STRATEGY:
        # Add prefixes to supplier/material columns (except join keys) to prevent conflicts
        # This ensures clear attribution of fields in the final enriched datasets
        # Join keys (supplier_id, material_id) remain unprefixed for joining
        
        suppliers_pref = suppliers.rename(
            columns={c: ("supplier_" + c if c != "supplier_id" else "supplier_id") for c in suppliers.columns}
        )
        materials_pref = materials.rename(
            columns={c: ("material_" + c if c != "material_id" else "material_id") for c in materials.columns}
        )

        # ORDERS ENRICHMENT:
        # Join orders with supplier data first, then with material data
        # This creates a denormalized view with all related information
        enriched_orders = orders.merge(suppliers_pref, on="supplier_id", how="left")
        enriched_orders = enriched_orders.merge(materials_pref, on="material_id", how="left")

        # Clean up any remaining duplicate columns (safety measure)
        enriched_orders = enriched_orders.loc[:, ~enriched_orders.columns.duplicated()]

        # Save enriched orders dataset
        orders_file = os.path.join(DATA_DIR, f"transformed_orders-{date}.csv")
        enriched_orders.to_csv(orders_file, index=False)
        logger.info("Saved transformed orders: %s (%d rows)", orders_file, len(enriched_orders))

        # EXPERIMENTS ENRICHMENT:
        # Process experiments if available (may be empty for some dates)
        experiments_file = None
        if not experiments.empty:
            # Join experiments with material data for enrichment
            enriched_experiments = experiments.merge(materials_pref, on="material_id", how="left")
            enriched_experiments = enriched_experiments.loc[:, ~enriched_experiments.columns.duplicated()]
            
            experiments_file = os.path.join(DATA_DIR, f"transformed_experiments-{date}.csv")
            enriched_experiments.to_csv(experiments_file, index=False)
            logger.info("Saved transformed experiments: %s (%d rows)", experiments_file, len(enriched_experiments))
        else:
            logger.info("No experiments to transform for %s", date)

        return {"orders": orders_file, "experiments": experiments_file}

    @task()
    def load(transformed: dict):
        """
        LOAD PHASE: Store transformed data in SQLite database with upsert capability.
        
        This task performs the following operations:
        1. Loads transformed CSV files from the transform phase
        2. Sanitizes column names for SQL compatibility
        3. Sets up database connections with automatic schema evolution
        4. Performs upsert operations to handle both inserts and updates
        5. Uses appropriate primary keys for each dataset to prevent duplicates
        
        Database Design:
        - Orders table: Composite primary key (order_id, material_id) for order line items
        - Experiments table: Simple primary key (experiment_id) for unique experiments
        - Schema evolution: Automatically adds new columns if detected in data
        - Upsert logic: Updates existing records or inserts new ones based on primary keys
        
        Args:
            transformed (dict): Mapping of dataset names to transformed CSV file paths
        """
        
        # ORDERS DATA LOADING
        orders_path = transformed.get("orders")
        if orders_path and os.path.exists(orders_path):
            df_orders = pd.read_csv(orders_path)
            
            if df_orders.empty:
                logger.info("No orders rows to load.")
            else:
                # Ensure column names are SQL-safe (remove special characters, etc.)
                df_orders.columns = [
                    re.sub(r"[^\w]+", "_", str(c).strip().lower()).strip("_") or "col" 
                    for c in df_orders.columns
                ]
                
                # Build column type mapping for database schema creation/evolution
                # Maps pandas dtypes to SQL types for proper column definitions
                extra = {
                    col: SQL_TYPE_MAPPING.get(str(dtype), SQL_TYPE_MAPPING.get("object")) 
                    for col, dtype in df_orders.dtypes.items()
                }
                
                # Setup database connection and table with composite primary key
                # order_id + material_id ensures uniqueness for order line items
                engine_orders, table_orders = setup_database_connection(
                    DATABASE_FILE, "orders", 
                    extra_columns=extra, 
                    primary_keys=["order_id", "material_id"]
                )
                
                # Perform upsert operation (insert new, update existing)
                upsert(engine_orders, table_orders, df_orders, primary_keys=["order_id", "material_id"])
                logger.info("Orders load finished.")
        else:
            logger.warning("No transformed orders file found; skipping orders load.")

        # EXPERIMENTS DATA LOADING
        experiments_path = transformed.get("experiments")
        if experiments_path and os.path.exists(experiments_path):
            df_exp = pd.read_csv(experiments_path)
            
            if df_exp.empty:
                logger.info("No experiments rows to load.")
            else:
                # Sanitize column names for SQL compatibility
                df_exp.columns = [
                    re.sub(r"[^\w]+", "_", str(c).strip().lower()).strip("_") or "col" 
                    for c in df_exp.columns
                ]
                
                # Build column type mapping for schema evolution
                extra = {
                    col: SQL_TYPE_MAPPING.get(str(dtype), SQL_TYPE_MAPPING.get("object")) 
                    for col, dtype in df_exp.dtypes.items()
                }
                
                # Setup database connection and table with simple primary key
                # experiment_id should be unique per experiment record
                engine_exp, table_exp = setup_database_connection(
                    DATABASE_FILE, "experiments", 
                    extra_columns=extra, 
                    primary_keys=["experiment_id"]
                )
                
                # Perform upsert operation
                upsert(engine_exp, table_exp, df_exp, primary_keys=["experiment_id"])
                logger.info("Experiments load finished.")
        else:
            logger.info("No transformed experiments file; skipping experiments load.")

    # ---------------- DAG TASK ORCHESTRATION ----------------
    # Define the task dependencies and data flow for the ETL pipeline
    
    # Step 1: Extract data from API endpoints and save as CSV files
    files = extract()
    
    # Step 2: Transform and enrich the data by joining reference tables
    transformed = transform(files)
    
    # Step 3: Load the transformed data into SQLite database
    load(transformed)


# Instantiate the DAG to make it available to the Airflow scheduler
materials_orders_experiments_etl_api()
