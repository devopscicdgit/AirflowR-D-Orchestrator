import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, Float, String, Date, DateTime, PrimaryKeyConstraint
import logging

logger = logging.getLogger(__name__)

DATABASE_FILE = '/opt/airflow/data/materials_experiments_database.db'

# Mapping Python / pandas dtypes to SQLAlchemy types
SQL_TYPE_MAPPING = {
    'object': String,
    'int64': Integer,
    'float64': Float,
    'datetime64[ns]': DateTime,
    'bool': Integer,  # SQLite has no BOOLEAN type
    'date': Date,
}


def setup_database_connection(table_name: str, extra_columns: dict = None, primary_keys: list = None):
    """
    Connect to SQLite database and create table if not exists.
    Dynamic columns are supported. Ensures primary keys are correctly set.
    """
    extra_columns = extra_columns or {}
    primary_keys = primary_keys or ["order_id", "material_id"]

    # Create engine and metadata
    engine = create_engine(f"sqlite:///{DATABASE_FILE}", echo=False)
    metadata = MetaData()

    # Base columns
    columns = [
        Column('order_id', String),
        Column('supplier_id', String),
        Column('material_id', String),
    ]

    # Extra columns
    for col_name, col_type in extra_columns.items():
        # Avoid duplicate columns
        if col_name not in [c.name for c in columns]:
            columns.append(Column(col_name, col_type))

    # Table definition
    table = Table(
        table_name,
        metadata,
        *columns,
        extend_existing=True
    )

    # Add primary key constraint safely
    if primary_keys:
        table.append_constraint(PrimaryKeyConstraint(*primary_keys, name=f'pk_{table_name}'))

    # Create table if it doesn't exist
    metadata.create_all(engine)
    table = Table(table_name, metadata, autoload_with=engine)

    conn = engine.connect()
    logger.info(f"Connected to DB and ensured table '{table_name}' exists with {len(columns)} columns")
    return conn, table


def upsert(conn, table, df: pd.DataFrame):
    """
    Bulk insert or replace rows in the given table based on primary keys.
    Uses SQLite 'INSERT OR REPLACE' to safely handle duplicates.
    """
    if df.empty:
        logger.info(f"No rows to upsert into '{table.name}'")
        return

    # Ensure DataFrame columns match table columns
    table_columns = table.columns.keys()
    missing_cols = set(table_columns) - set(df.columns)
    for col in missing_cols:
        df[col] = None  # Add missing columns with NULL values

    extra_cols = set(df.columns) - set(table_columns)
    if extra_cols:
        logger.warning(f"Extra columns in DataFrame not in table '{table.name}': {extra_cols}")
        df = df[[c for c in df.columns if c in table_columns]]  # Drop extra columns

    records = df.to_dict(orient="records")

    try:
        with conn.begin():  # Transaction-safe
            conn.execute(table.insert().prefix_with("OR REPLACE"), records)
        logger.info(f"Upserted {len(df)} rows into '{table.name}'")
    except Exception as e:
        logger.exception(f"Failed to upsert rows into '{table.name}': {e}")
