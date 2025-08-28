# dags/materials_orders_experiments_etl_api.py

import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from sqlalchemy import create_engine
import logging

from utils.data_helpers import setup_database_connection, upsert, SQL_TYPE_MAPPING

# --- Config ---
AIRFLOW_DATA_DIR = "/opt/airflow/data"
DATA_DIR = f"{AIRFLOW_DATA_DIR}/experimental_data"
DATABASE_FILE = f"{AIRFLOW_DATA_DIR}/api_database.db"
API_URL = "http://api-services:5000/api"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --- DAG ---
@dag(
    description="ETL pipeline for materials, orders, and experiments via API into SQLite",
    schedule="@daily",
    default_args={
        "owner": "jamesxu",
        "depends_on_past": False,
        "start_date": datetime(2025, 8, 10),
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    },
    catchup=False,
    tags=["etl", "materials", "orders", "experiments", "sqlite"],
)
def materials_orders_experiments_etl_api():

    # -------------------
    @task()
    def extract(**kwargs):
        """Extract datasets from API and save CSVs."""
        date = kwargs["logical_date"].strftime("%Y-%m-%d")
        os.makedirs(DATA_DIR, exist_ok=True)

        def fetch_json(url, retries=3, timeout=5):
            for i in range(retries):
                try:
                    r = requests.get(url, timeout=timeout)
                    r.raise_for_status()
                    return r.json()
                except Exception as e:
                    if i < retries - 1:
                        continue
                    else:
                        raise

        datasets = {
            "suppliers": fetch_json(f"{API_URL}/suppliers"),
            "materials": fetch_json(f"{API_URL}/materials"),
            "orders": fetch_json(f"{API_URL}/orders?order_date={date}"),
            "experiments": fetch_json(f"{API_URL}/experiments?date={date}"),
        }

        file_paths = {}
        for name, data in datasets.items():
            file = f"{DATA_DIR}/{name}-{date}.csv"
            pd.DataFrame(data).to_csv(file, index=False)
            file_paths[name] = file
            logger.info(f"Extracted {len(data)} rows for {name}")

        return file_paths

    # -------------------
    @task()
    def transform(file_paths: dict, **kwargs):
        """Transform and enrich orders and experiments with robust handling."""
        date = kwargs["logical_date"].strftime("%Y-%m-%d")

        # --- Load CSVs safely ---
        def safe_read_csv(path, name):
            try:
                df = pd.read_csv(path)
                if df.empty:
                    logger.warning(f"{name} CSV is empty: {path}")
                else:
                    logger.info(f"Loaded {len(df)} rows from {name} CSV: {path}")
                return df
            except FileNotFoundError:
                logger.warning(f"{name} CSV not found: {path}")
                return pd.DataFrame()
            except Exception as e:
                logger.error(f"Error reading {name} CSV: {path} -> {e}")
                return pd.DataFrame()

        suppliers = safe_read_csv(file_paths.get("suppliers", ""), "Suppliers")
        materials = safe_read_csv(file_paths.get("materials", ""), "Materials")
        orders = safe_read_csv(file_paths.get("orders", ""), "Orders")
        experiments = safe_read_csv(file_paths.get("experiments", ""), "Experiments")

        # --- Validate required datasets ---
        if suppliers.empty or materials.empty or orders.empty:
            raise AirflowSkipException("One or more required datasets empty; skipping transform.")

        # --- Enrich Orders ---
        enriched_orders = orders.merge(
            suppliers, on="supplier_id", how="left", suffixes=("", "_supplier")
        ).merge(
            materials, on="material_id", how="left", suffixes=("", "_material")
        )

        orders_file = f"{DATA_DIR}/transformed_orders-{date}.csv"
        enriched_orders.to_csv(orders_file, index=False)
        logger.info(f"Transformed {len(enriched_orders)} orders, saved to {orders_file}")

        # --- Enrich Experiments ---
        if not experiments.empty:
            enriched_experiments = experiments.merge(
                materials, on="material_id", how="left", suffixes=("", "_material")
            )
            experiments_file = f"{DATA_DIR}/transformed_experiments-{date}.csv"
            enriched_experiments.to_csv(experiments_file, index=False)
            logger.info(f"Transformed {len(enriched_experiments)} experiments, saved to {experiments_file}")
        else:
            experiments_file = None
            logger.info("No experiments to transform.")

        return {"orders": orders_file, "experiments": experiments_file}


    # -------------------
    @task()
    def load(transformed_files: dict):
        """Load transformed orders and experiments into SQLite with upsert."""
        engine = create_engine(f"sqlite:///{DATABASE_FILE}")

        # --- Load Orders ---
        orders_file = transformed_files.get("orders")
        df_orders = pd.read_csv(orders_file)
        if df_orders.empty:
            logger.warning("No orders to load")
        else:
            conn, table = setup_database_connection(
                table_name="orders",
                extra_columns={col: SQL_TYPE_MAPPING.get(str(dtype), SQL_TYPE_MAPPING["object"]) 
                               for col, dtype in df_orders.dtypes.items()},
                primary_keys=["order_id", "material_id"]
            )
            upsert(conn, table, df_orders)

        # --- Load Experiments ---
        experiments_file = transformed_files.get("experiments")
        if experiments_file and os.path.exists(experiments_file):
            df_exp = pd.read_csv(experiments_file)
            if not df_exp.empty:
                conn, table = setup_database_connection(
                    table_name="experiments",
                    extra_columns={col: SQL_TYPE_MAPPING.get(str(dtype), SQL_TYPE_MAPPING["object"]) 
                                   for col, dtype in df_exp.dtypes.items()},
                    primary_keys=["experiment_id"]
                )
                upsert(conn, table, df_exp)
            else:
                logger.warning("No experiments to load")
        else:
            logger.info("Experiments file missing or empty")

    # --- DAG Flow ---
    extracted = extract()
    transformed = transform(extracted)
    load(transformed)


materials_orders_experiments_etl_api()
