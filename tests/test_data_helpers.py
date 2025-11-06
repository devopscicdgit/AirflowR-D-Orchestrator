import os
import tempfile
import pandas as pd
from sqlalchemy import inspect

from dags.utils.data_helpers import setup_database_connection, upsert


def test_setup_and_add_columns():
    tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    db_path = tmp.name
    tmp.close()
    try:
        engine, table = setup_database_connection(
            db_file=db_path,
            table_name='materials',
            extra_columns={'material_id': 'INTEGER', 'name': 'TEXT'},
            primary_keys=['material_id']
        )
        assert 'material_id' in table.columns
        assert 'name' in table.columns
        # Add new column via second call
        engine2, table2 = setup_database_connection(
            db_file=db_path,
            table_name='materials',
            extra_columns={'material_id': 'INTEGER', 'name': 'TEXT', 'category': 'TEXT'},
            primary_keys=['material_id']
        )
        assert 'category' in table2.columns
    finally:
        os.unlink(db_path)


def test_upsert_and_schema_evolution():
    tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    db_path = tmp.name
    tmp.close()
    try:
        engine, table = setup_database_connection(
            db_file=db_path,
            table_name='experiments',
            extra_columns={'experiment_id': 'INTEGER', 'title': 'TEXT'},
            primary_keys=['experiment_id']
        )
        # initial insert
        df1 = pd.DataFrame([
            {'experiment_id': 1, 'title': 'Alpha'},
            {'experiment_id': 2, 'title': 'Beta'}
        ])
        upsert(engine, table, df1, primary_keys=['experiment_id'])
        # evolve schema with new column auto-added
        df2 = pd.DataFrame([
            {'experiment_id': 1, 'title': 'Alpha v2', 'status': 'done'},
            {'experiment_id': 3, 'title': 'Gamma', 'status': 'pending'}
        ])
        upsert(engine, table, df2, primary_keys=['experiment_id'])
        inspector = inspect(engine)
        cols = [c['name'] for c in inspector.get_columns('experiments')]
        assert 'status' in cols
        # verify upsert result row count (should be 3 unique PKs)
        with engine.connect() as conn:
            count = conn.execute(table.select()).fetchall()
        assert len(count) == 3
    finally:
        os.unlink(db_path)


def test_upsert_empty_dataframe_noop():
    import tempfile, os
    tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    db_path = tmp.name
    tmp.close()
    try:
        engine, table = setup_database_connection(
            db_file=db_path,
            table_name='empty_case',
            extra_columns={'id': 'INTEGER'},
            primary_keys=['id']
        )
        # capture columns before
        before_cols = [c.name for c in table.columns]
        # perform upsert with empty df
        upsert(engine, table, pd.DataFrame(), primary_keys=['id'])
        after_cols = [c.name for c in table.columns]
        assert before_cols == after_cols
    finally:
        os.unlink(db_path)


def test_upsert_updates_existing_row():
    import tempfile, os
    tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    db_path = tmp.name
    tmp.close()
    try:
        engine, table = setup_database_connection(
            db_file=db_path,
            table_name='metrics',
            extra_columns={'id': 'INTEGER', 'value': 'INTEGER'},
            primary_keys=['id']
        )
        df_insert = pd.DataFrame([{'id': 1, 'value': 10}, {'id': 2, 'value': 20}])
        upsert(engine, table, df_insert, primary_keys=['id'])
        df_update = pd.DataFrame([{'id': 2, 'value': 25}])
        upsert(engine, table, df_update, primary_keys=['id'])
        with engine.connect() as conn:
            rows = conn.exec_driver_sql('SELECT id, value FROM metrics ORDER BY id').fetchall()
        assert rows == [(1, 10), (2, 25)]
    finally:
        os.unlink(db_path)
