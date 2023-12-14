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

from development.etl.raw_ETL_dim_rosters import build_file_df, construct_file_list
from development.etl.raw_ETL_dim_rosters import build_initial_df_from_file, run_etl, run_append_etl

def run_etl_loop(file_list):
    for idx, file in enumerate(file_list):
        initial_df = build_initial_df_from_file(file)
        if idx == 0:
            run_etl(df=initial_df,file_path=prod_file_prefix+'dim_team_rosters')
        else:
            run_append_etl(df=initial_df,file_path=prod_file_prefix+'dim_team_rosters')


def _run_etl():
# Initial Events - Script Execution
    file_df = build_file_df(folder_path=event_file_prefix)

    listed_files = construct_file_list(df=file_df)

    run_etl_loop(file_list=listed_files)


with DAG(
    dag_id="dim_team_rosters"
    , start_date=datetime(2023,12,12)
    , schedule_interval='@daily'
    , catchup=False) as dag:

    process_dim = PythonOperator(
        task_id='process_dim'
        , python_callable=_run_etl
    )

    process_dim