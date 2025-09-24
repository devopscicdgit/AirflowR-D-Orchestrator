# dags/materials_orders_experiments_etl_api.py

import os
import re
import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

from utils.data_helpers import setup_database_connection, upsert, SQL_TYPE_MAPPING

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ------- CONFIG -------
AIRFLOW_DATA_DIR = os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/data")
DATA_DIR = os.path.join(AIRFLOW_DATA_DIR, "experimental_data")
DATABASE_FILE = os.path.join(AIRFLOW_DATA_DIR, "api_database.db")
API_URL = os.getenv("MATERIALS_API_URL", "http://api-services:5000/api")  # set in your env
API_VERSION = "v1"  # if your API uses versions

os.makedirs(DATA_DIR, exist_ok=True)


def _sanitize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        re.sub(r"[^\w]+", "_", str(c).strip().lower()).strip("_") or "col" for c in df.columns
    ]
    # remove exact duplicates keeping first occurrence
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def _safe_json_to_df(payload):
    """
    Convert an API response payload into a DataFrame.
    Accepts list-of-dicts, dict-of-lists, or single dict.
    """
    if payload is None:
        return pd.DataFrame()
    if isinstance(payload, list):
        return pd.json_normalize(payload)
    if isinstance(payload, dict):
        # try common patterns
        # e.g. {"data": [...]} or {"items": [...]}
        for k in ("data", "items", "results"):
            if k in payload and isinstance(payload[k], list):
                return pd.json_normalize(payload[k])
        # otherwise treat dict as single record
        return pd.json_normalize([payload])
    return pd.DataFrame()


# ---------------- DAG ----------------
@dag(
    dag_id="materials_orders_experiments_etl_api",
    description="API-driven ETL for materials / orders / experiments -> single SQLite DB",
    schedule_interval="@daily",
    start_date=datetime(2025, 8, 10),
    catchup=False,
    default_args={"owner": "jamesxu", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["etl", "api", "sqlite"],
)
def materials_orders_experiments_etl_api():

    @task()
    def fetch(**kwargs):
        """Fetch API endpoints and write CSVs per run-date"""
        logical_date = kwargs["logical_date"]
        date = logical_date.strftime("%Y-%m-%d")
        run_dir = os.path.join(DATA_DIR, date)
        os.makedirs(run_dir, exist_ok=True)

        def get_json(endpoint, params=None, retries=3, timeout=8):
            url = f"{API_URL}/{endpoint}"
            for attempt in range(1, retries + 1):
                try:
                    resp = requests.get(url, params=params, timeout=timeout)
                    resp.raise_for_status()
                    return resp.json()
                except Exception as e:
                    logger.warning("Fetch attempt %d failed for %s : %s", attempt, url, e)
                    if attempt == retries:
                        raise
            return []

        datasets = {}

        # suppliers & materials (no date param)
        suppliers_payload = get_json("suppliers")
        materials_payload = get_json("materials")

        # orders & experiments with date filter
        orders_payload = get_json("orders", params={"order_date": date})
        experiments_payload = get_json("experiments", params={"date": date})

        # convert to df safely
        df_suppliers = _safe_json_to_df(suppliers_payload)
        df_materials = _safe_json_to_df(materials_payload)
        df_orders = _safe_json_to_df(orders_payload)
        df_experiments = _safe_json_to_df(experiments_payload)

        # sanitize column names and write CSVs
        df_suppliers = _sanitize_cols(df_suppliers)
        df_materials = _sanitize_cols(df_materials)
        df_orders = _sanitize_cols(df_orders)
        df_experiments = _sanitize_cols(df_experiments)

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
        """Enrich and normalize dataframes, prevent _x/_y collisions by prefixing supplier/material cols."""
        date = kwargs["logical_date"].strftime("%Y-%m-%d")

        def read_df(path, name):
            try:
                if not path or not os.path.exists(path):
                    logger.warning("Missing file for %s: %s", name, path)
                    return pd.DataFrame()
                df = pd.read_csv(path)
                df = _sanitize_cols(df)
                logger.info("%s loaded: %d rows, cols=%s", name, len(df), list(df.columns)[:10])
                return df
            except Exception as e:
                logger.exception("Failed reading %s from %s: %s", name, path, e)
                return pd.DataFrame()

        suppliers = read_df(file_paths.get("suppliers"), "suppliers")
        materials = read_df(file_paths.get("materials"), "materials")
        orders = read_df(file_paths.get("orders"), "orders")
        experiments = read_df(file_paths.get("experiments"), "experiments")

        # basic validation
        if suppliers.empty or materials.empty or orders.empty:
            raise AirflowSkipException("Missing required datasets; skipping transform.")

        # Ensure join key names exist and are normalized
        # expected join keys: supplier_id, material_id, order_id, experiment_id
        # If supplier/material ids are named differently, user must adapt – we assume canonical names.
        # Prefix supplier/material fields (except their id) to avoid collisions
        suppliers_pref = suppliers.rename(
            columns={c: ("supplier_" + c if c != "supplier_id" else "supplier_id") for c in suppliers.columns}
        )
        materials_pref = materials.rename(
            columns={c: ("material_" + c if c != "material_id" else "material_id") for c in materials.columns}
        )

        # Merge orders with supplier/material prefixed schemas
        enriched_orders = orders.merge(suppliers_pref, on="supplier_id", how="left")
        enriched_orders = enriched_orders.merge(materials_pref, on="material_id", how="left")

        # remove duplicate columns if any still appear
        enriched_orders = enriched_orders.loc[:, ~enriched_orders.columns.duplicated()]

        orders_file = os.path.join(DATA_DIR, f"transformed_orders-{date}.csv")
        enriched_orders.to_csv(orders_file, index=False)
        logger.info("Saved transformed orders: %s (%d rows)", orders_file, len(enriched_orders))

        experiments_file = None
        if not experiments.empty:
            # note: materials_pref is safe to join
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
        """Load transformed CSVs into single SQLite DB with upsert and schema-evolution support."""
        # Orders
        orders_path = transformed.get("orders")
        if orders_path and os.path.exists(orders_path):
            df_orders = pd.read_csv(orders_path)
            if df_orders.empty:
                logger.info("No orders rows to load.")
            else:
                # sanitize and ensure colnames safe
                df_orders.columns = [re.sub(r"[^\w]+", "_", str(c).strip().lower()).strip("_") or "col" for c in df_orders.columns]
                # build extra_columns mapping for creation / evolution
                extra = {col: SQL_TYPE_MAPPING.get(str(dtype), SQL_TYPE_MAPPING.get("object")) for col, dtype in df_orders.dtypes.items()}
                engine_orders, table_orders = setup_database_connection(
                    DATABASE_FILE, "orders", extra_columns=extra, primary_keys=["order_id", "material_id"]
                )
                upsert(engine_orders, table_orders, df_orders, primary_keys=["order_id", "material_id"])
                logger.info("Orders load finished.")

        else:
            logger.warning("No transformed orders file found; skipping orders load.")

        # Experiments
        experiments_path = transformed.get("experiments")
        if experiments_path and os.path.exists(experiments_path):
            df_exp = pd.read_csv(experiments_path)
            if df_exp.empty:
                logger.info("No experiments rows to load.")
            else:
                df_exp.columns = [re.sub(r"[^\w]+", "_", str(c).strip().lower()).strip("_") or "col" for c in df_exp.columns]
                extra = {col: SQL_TYPE_MAPPING.get(str(dtype), SQL_TYPE_MAPPING.get("object")) for col, dtype in df_exp.dtypes.items()}
                engine_exp, table_exp = setup_database_connection(
                    DATABASE_FILE, "experiments", extra_columns=extra, primary_keys=["experiment_id"]
                )
                upsert(engine_exp, table_exp, df_exp, primary_keys=["experiment_id"])
                logger.info("Experiments load finished.")
        else:
            logger.info("No transformed experiments file; skipping experiments load.")

    # DAG wiring
    files = fetch()
    transformed = transform(files)
    load(transformed)


materials_orders_experiments_etl_api()
