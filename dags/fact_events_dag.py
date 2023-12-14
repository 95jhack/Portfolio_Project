from airflow import DAG
from datetime import timedelta, date, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd

import os
import pathlib
from pathlib import Path, PurePosixPath, PureWindowsPath

folder_path = str(pathlib.PureWindowsPath(os.path.abspath(os.path.dirname(__file__))).as_posix())
raw_file_path = folder_path+"/development/data/"
prod_file_prefix = folder_path+"/development/data/production/"
event_file_prefix = folder_path+"/development/data/2022eventfiles/"

from development.etl.raw_ETL_fact_events import build_file_df, construct_file_list
from development.etl.raw_ETL_fact_events import build_initial_df_from_file, iterate_col, final_filter, stat_generation, run_etl, run_append_etl

def run_etl_loop(file_list):
    for idx, file in enumerate(file_list):
        initial_df = build_initial_df_from_file(file)
        iterated_df = iterate_col(input_df=initial_df)
        filtered_df = final_filter(input_df=iterated_df)
        stat_df = stat_generation(input_df=filtered_df)
        if idx == 0:
            run_etl(df=stat_df,file_path=prod_file_prefix+'fact_game_events')
        else:
            run_append_etl(df=stat_df,file_path=prod_file_prefix+'fact_game_events')

def _run_etl():
# Initial Events - Script Execution
    file_df = build_file_df(folder_path=event_file_prefix)

    listed_files = construct_file_list(df=file_df)

    run_etl_loop(file_list=listed_files)


with DAG(
    dag_id="fact_game_events"
    , start_date=datetime(2023,12,12)
    , schedule_interval='@daily'
    , catchup=False) as dag:

    process_fact = PythonOperator(
        task_id='process_fact'
        , python_callable=_run_etl
    )

    process_fact