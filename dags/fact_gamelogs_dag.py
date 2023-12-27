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
from development.etl.raw_ETL_fact_gamelogs import fact_gamelogs, gamelogs_transform
from development.functions.postgres_to_csv import postgres_to_csv, drop_postgres_tables
from development.dag_sql.create_tbl_fact_gamelogs import create_tbl_gamelogs_base, create_tbl_preload_final

# Directory Structuring
folder_path = str(pathlib.PureWindowsPath(os.path.abspath(os.path.dirname(__file__))).as_posix())
prod_file_prefix = folder_path+"/development/data/production/"

# Postgres Connection
conn_string = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'
db = create_engine(conn_string)
conn = db.connect()
conn.autocommit = True

def _read_csv_transform_load_1():
    # Write this dataframe to postgres
    fact_gamelogs.to_sql('gamelogs_base_df', conn, if_exists ='replace', index=False)

def _read_csv_transform_load_2():
    fact_gamelogs_pg = pd.read_sql('gamelogs_base_df', conn)
    preload_df = gamelogs_transform(fact_gamelogs=fact_gamelogs_pg)
    # Write this dataframe to postgres
    preload_df.to_sql('gamelogs_preload_df', conn, if_exists ='replace', index=False)

def _store_table_as_csv():
    postgres_to_csv(
        folder_path=folder_path
        , postgres_tbl='gamelogs_preload_df'
        , output_csv='fact_gamelogs'
    )

def _drop_interim_tbls():
    drop_postgres_tables(table1='gamelogs_base_df', table2='gamelogs_preload_df')

with DAG(
    dag_id="DAG_fact_gamelogs"
    , start_date=datetime(2023,12,12)
    , schedule_interval='@daily'
    , catchup=False) as dag:

    create_pg_table_1 = PostgresOperator(
        task_id = 'create_pg_table_1'
        , postgres_conn_id = 'postgres'
        , sql = create_tbl_gamelogs_base
    )

    create_pg_table_2 = PostgresOperator(
        task_id = 'create_pg_table_2'
        , postgres_conn_id = 'postgres'
        , sql = create_tbl_preload_final
    )

    read_transform_load_1 = PythonOperator(
        task_id='read_transform_load_1'
        , python_callable=_read_csv_transform_load_1
    )

    read_transform_load_2 = PythonOperator(
        task_id='read_transform_load_2'
        , python_callable=_read_csv_transform_load_2
    )

    process_fact = PythonOperator(
        task_id='process_fact'
        , python_callable=_store_table_as_csv
    )

    drop_pg_tables = PythonOperator(
        task_id='drop_pg_tables'
        , python_callable=_drop_interim_tbls
    )

    [create_pg_table_1, create_pg_table_2] >> read_transform_load_1 >> read_transform_load_2 >> process_fact >> drop_pg_tables