# utils/data_helpers.py
import logging
import re
from typing import Dict, Optional

import pandas as pd
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Integer,
    Float,
    DateTime,
    Boolean,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

# Mapping pandas dtype name -> SQLAlchemy type class (used by DAG to pass extra_columns)
SQL_TYPE_MAPPING = {
    "object": String,
    "int64": Integer,
    "Int64": Integer,  # pandas nullable int dtype name
    "float64": Float,
    "bool": Boolean,
    "boolean": Boolean,
    "datetime64[ns]": DateTime,
}


# For ALTER TABLE ADD COLUMN we need SQLite literal types
_SQLALCHEMY_TO_SQLITE = {
    String: "TEXT",
    Integer: "INTEGER",
    Float: "REAL",
    DateTime: "TEXT",
    Boolean: "INTEGER",
}


def _sanitize_col_name(name: str) -> str:
    """Make a column name SQL-friendly (lower, alnum + underscore)."""
    if not isinstance(name, str):
        name = str(name)
    name = name.strip().lower()
    name = re.sub(r"[^\w]+", "_", name)
    name = re.sub(r"__+", "_", name)
    name = name.strip("_")
    return name or "col"


def _sqlalchemy_type_to_sqlite_literal(col_type) -> str:
    """Return a SQLite literal type (TEXT/INTEGER/REAL) for a SQLAlchemy type/class or string."""
    # If caller passed a class from SQLAlchemy mapping
    if col_type in _SQLALCHEMY_TO_SQLITE:
        return _SQLALCHEMY_TO_SQLITE[col_type]
    # If caller passed a SQLAlchemy type class object (e.g., sqlalchemy.String)
    # Try matching by type name
    try:
        name = getattr(col_type, "__name__", None)
        if name:
            for k, v in _SQLALCHEMY_TO_SQLITE.items():
                if getattr(k, "__name__", "") == name:
                    return v
    except Exception:
        pass
    # If passed a string literal already
    if isinstance(col_type, str):
        return col_type.upper()
    # Fallback
    return "TEXT"


def setup_database_connection(
    db_file: str,
    table_name: str,
    extra_columns: Optional[Dict[str, object]] = None,
    primary_keys: Optional[list] = None,
):
    """
    Ensure a SQLite DB exists and table exists with provided columns.
    - db_file: path to .db file
    - extra_columns: mapping col_name -> SQLAlchemy type class OR string literal ("TEXT", "INTEGER")
    - primary_keys: list of columns to mark as PKs (only applied on table creation)
    Returns (engine, table) where table is reflected Table object.
    """
    extra_columns = extra_columns or {}
    primary_keys = primary_keys or []

    engine = create_engine(f"sqlite:///{db_file}", connect_args={"check_same_thread": False})
    metadata = MetaData()

    # reflect existing tables
    metadata.reflect(bind=engine)

    if table_name in metadata.tables:
        # table exists: add missing columns (ALTER TABLE) if needed
        table = metadata.tables[table_name]
        existing_cols = set(table.columns.keys())
        missing = [c for c in extra_columns.keys() if c not in existing_cols]

        if missing:
            logger.info("Table exists but missing columns will be added: %s", missing)
            with engine.begin() as conn:
                for col in missing:
                    col_type = extra_columns[col]
                    sql_type = _sqlalchemy_type_to_sqlite_literal(col_type)
                    # SQLite ALTER TABLE ADD COLUMN syntax - simple types only
                    sql = f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {sql_type}'
                    logger.info("Running SQL: %s", sql)
                    conn.exec_driver_sql(sql)

            # re-reflect to update table object
            metadata = MetaData()
            metadata.reflect(bind=engine)
            table = metadata.tables[table_name]

        logger.info("Ensured table '%s' exists in %s (cols=%s)", table_name, db_file, list(table.columns.keys()))
        return engine, table

    # table doesn't exist -> create with columns
    cols = []
    for col_name, col_type in extra_columns.items():
        # get SQLAlchemy type class if passed as string or class
        if isinstance(col_type, str):
            tname = col_type.upper()
            if tname == "TEXT":
                sa_type = String
            elif tname == "INTEGER":
                sa_type = Integer
            elif tname == "REAL":
                sa_type = Float
            elif tname == "BOOLEAN":
                sa_type = Boolean
            else:
                sa_type = String
        else:
            sa_type = col_type or String

        is_pk = col_name in primary_keys
        cols.append(Column(col_name, sa_type, primary_key=is_pk))

    # create the table
    table = Table(table_name, metadata, *cols, extend_existing=True)
    metadata.create_all(engine)
    # reflect created table
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables[table_name]
    logger.info("Created table '%s' in %s with cols=%s", table_name, db_file, list(table.columns.keys()))
    return engine, table


def upsert(engine, table: Table, df: pd.DataFrame, primary_keys: list):
    """
    Upsert (insert or update) DataFrame rows into the given table.
    - automatically adds missing columns (ALTER TABLE) if necessary.
    - uses SQLite dialect insert(...).on_conflict_do_update
    """
    if df is None or df.empty:
        logger.info("Empty dataframe -> nothing to upsert for table %s", table.name)
        return

    # sanitize column names of DataFrame (they should already be sanitized by DAG but double-check)
    df = df.copy()
    df.columns = [_sanitize_col_name(c) for c in df.columns]

    # ensure table has all columns present in df; if not, add them
    existing_cols = set(c.name for c in table.columns)
    df_cols = list(df.columns)
    missing = [c for c in df_cols if c not in existing_cols]

    if missing:
        logger.info("Adding missing columns to table %s: %s", table.name, missing)
        with engine.begin() as conn:
            for col in missing:
                # derive SQL literal type from pandas dtype
                pd_dtype = str(df[col].dtype)
                # map pandas dtype -> SQLAlchemy type class
                sa_type = SQL_TYPE_MAPPING.get(pd_dtype, String)
                sql_type = _sqlalchemy_type_to_sqlite_literal(sa_type)
                sql = f'ALTER TABLE "{table.name}" ADD COLUMN "{col}" {sql_type}'
                logger.info("ALTER TABLE add column SQL: %s", sql)
                conn.exec_driver_sql(sql)
        # reflect to get updated table
        metadata = MetaData()
        metadata.reflect(bind=engine)
        table = metadata.tables[table.name]

    # prepare insert
    records = df.to_dict(orient="records")
    try:
        stmt = sqlite_insert(table).values(records)

        # build update dict using excluded
        update_dict = {}
        for c in table.columns:
            if c.name in primary_keys:
                continue
            # use getattr(stmt.excluded, column) to be SQLAlchemy 2.x safe
            try:
                update_dict[c.name] = getattr(stmt.excluded, c.name)
            except AttributeError:
                # fallback: use the column label from VALUES (this is rare)
                update_dict[c.name] = stmt.excluded.__getattr__(c.name)

        stmt = stmt.on_conflict_do_update(index_elements=primary_keys, set_=update_dict)

        with engine.begin() as conn:
            conn.execute(stmt)
        logger.info("Upserted %d rows into %s", len(records), table.name)
    except SQLAlchemyError as e:
        logger.exception("Upsert failed for table %s: %s", table.name, e)
        raise
