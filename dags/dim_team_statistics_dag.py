# DAG Imports
from airflow import DAG
from datetime import timedelta, date, datetime
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Directory, Transformations, Postgres Imports
import os
import pathlib
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# ETL & formatting Imports
from development.etl.raw_ETL_dim_team_statistics import dim_team_final
from development.functions.postgres_to_csv import postgres_to_csv, drop_postgres_tables
from development.dag_sql.create_tbl_dim_team_statistics import create_tbl

# Directory Structuring
folder_path = str(pathlib.PureWindowsPath(os.path.abspath(os.path.dirname(__file__))).as_posix())
prod_file_prefix = folder_path+"/development/data/production/"

# Postgres Connection
conn_string = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'
db = create_engine(conn_string)
conn = db.connect()
conn.autocommit = True

def _read_csv_transform_load():
    # Write this dataframe to postgres
    dim_team_final.to_sql('pre_load_df', conn, if_exists ='replace')

def _store_table_as_csv():
    postgres_to_csv(
        folder_path=folder_path
        , postgres_tbl='pre_load_df'
        , output_csv='dim_team_statistics'
    )

def _drop_interim_tbls():
    drop_postgres_tables(table1='pre_load_df')

with DAG(
    dag_id="DAG_dim_team_statistics"
    , start_date=datetime(2023,12,12)
    , schedule_interval='@daily'
    , catchup=False) as dag:

    create_pg_table = PostgresOperator(
        task_id = 'create_pg_table'
        , postgres_conn_id = 'postgres'
        , sql = create_tbl
    )

    read_transform_load = PythonOperator(
        task_id='read_transform_load'
        , python_callable=_read_csv_transform_load
    )

    process_dimension = PythonOperator(
        task_id='process_dimension'
        , python_callable=_store_table_as_csv
    )

    drop_pg_tables = PythonOperator(
        task_id='drop_pg_tables'
        , python_callable=_drop_interim_tbls
    )

    create_pg_table >> read_transform_load >> process_dimension >> drop_pg_tables
