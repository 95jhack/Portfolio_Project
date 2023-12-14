from airflow import DAG
from datetime import timedelta, date, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd

import os
import pathlib

folder_path = str(pathlib.PureWindowsPath(os.path.abspath(os.path.dirname(__file__))).as_posix())
prod_file_prefix = folder_path+"/development/data/production/"
from development.functions.raw_data_extraction_functions import DateDimDFGenerator, CSVWriter

#########################################################
########### Object: dim_date ############################
#########################################################

# Enter pertaining dates for the Date Dimension
dim_start_date = datetime(2020, 1, 1)
dim_end_date = datetime(2025, 12, 31)
# Generate the date series based on the inputted start & end dates
generator = DateDimDFGenerator(dim_start_date, dim_end_date)
# Use this date series to populate a sample pandas dataframe for upload.
date_dimension_df = generator.dim_date_dataframe()

def _run_etl():
    # Write this dataframe to a CSV file in the listed location.
    df = date_dimension_df
    file_path = prod_file_prefix+'dim_date.csv'
    df_to_csv = CSVWriter(df)
    df_to_csv.write_to_csv(file_path)

with DAG(
    dag_id="dim_date"
    , start_date=datetime(2023,12,12)
    , schedule_interval='@daily'
    , catchup=False) as dag:

    process_dimension = PythonOperator(
        task_id='process_dimension'
        , python_callable=_run_etl
    )

    process_dimension