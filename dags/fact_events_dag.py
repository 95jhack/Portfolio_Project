# DAG Imports
from airflow import DAG
from datetime import timedelta, date, datetime
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Directory, Transformations, Postgres Imports
import os
import pathlib
from pathlib import Path, PurePosixPath, PureWindowsPath
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# ETL & formatting Imports
from development.etl.raw_ETL_fact_events import build_file_df, construct_file_list
from development.etl.raw_ETL_fact_events import build_initial_df_from_file, iterate_col, final_filter, stat_generation, run_etl, run_append_etl
from development.dag_sql.create_tbl_events import create_tbl, create_tbl_list

# Directory Structuring
folder_path = str(pathlib.PureWindowsPath(os.path.abspath(os.path.dirname(__file__))).as_posix())
prod_file_prefix = folder_path+"/development/data/production/"
event_file_prefix = folder_path+"/development/data/2022eventfiles/"

# Postgres Connection
conn_string = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'
db = create_engine(conn_string)
conn = db.connect()
conn.autocommit = True

def _read_csv_to_postgres_1():
    # Write this dataframe to postgres
    file_df = build_file_df(folder_path=event_file_prefix)
    file_df.to_sql('file_df', conn, if_exists ='replace')

def _read_csv_to_postgres_2():
    # Write this dataframe to postgres
    read_file_df = pd.read_sql('file_df', conn)
    listed_files = construct_file_list(df=read_file_df)
    file_list_df = pd.DataFrame(listed_files)
    file_list_df.to_sql('file_list_df', conn, if_exists ='replace')

def _run_etl_loop():
    read_file_list_df = pd.read_sql('file_list_df', conn)
    file_list = read_file_list_df['0'].values.tolist()
    for idx, file in enumerate(file_list):
        initial_df = build_initial_df_from_file(file=file)
        iterated_df = iterate_col(input_df=initial_df)
        filtered_df = final_filter(input_df=iterated_df)
        stat_df = stat_generation(input_df=filtered_df)
        if idx == 0:
            run_etl(df=stat_df,file_path=prod_file_prefix+'fact_game_events')
        else:
            run_append_etl(df=stat_df,file_path=prod_file_prefix+'fact_game_events')

def _drop_interim_tbls():
    drop_postgres_tables(table1='file_df', table2='file_list_df')

with DAG(
    dag_id="DAG_fact_game_events"
    , start_date=datetime(2023,12,12)
    , schedule_interval='@daily'
    , catchup=False) as dag:

    create_pg_table_1 = PostgresOperator(
        task_id = 'create_pg_table_1'
        , postgres_conn_id = 'postgres'
        , sql = create_tbl
    )

    create_pg_table_2 = PostgresOperator(
        task_id = 'create_pg_table_2'
        , postgres_conn_id = 'postgres'
        , sql = create_tbl_list
    )

    read_transform_load_1 = PythonOperator(
        task_id='read_transform_load_1'
        , python_callable=_read_csv_to_postgres_1
    )

    read_transform_load_2 = PythonOperator(
        task_id='read_transform_load_2'
        , python_callable=_read_csv_to_postgres_2
    )

    process_fact = PythonOperator(
        task_id='process_fact'
        , python_callable=_run_etl_loop
    )

    drop_pg_tables = PythonOperator(
        task_id='drop_pg_tables'
        , python_callable=_drop_interim_tbls
    )

    [create_pg_table_1, create_pg_table_2] >> read_transform_load_1 >> read_transform_load_2 >> process_fact >> drop_pg_tables 

