# DAG Imports
from airflow import DAG
from datetime import timedelta, date, datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Directory, Transformations, Postgres Imports
import os
import pathlib
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def postgres_to_csv(folder_path, postgres_tbl, output_csv):
    try:
        # Directory Structuring
        prod_file_prefix = folder_path+"/development/data/production/"

        # Postgres Connection
        conn_string = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'
        db = create_engine(conn_string)
        conn = db.connect()
        conn.autocommit = True

        pg_hook = PostgresHook(postgres_conn_id='postgres')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {postgres_tbl} LIMIT 0")
        column_names = [desc[0] for desc in cursor.description]

        copy_cmd = f"COPY {postgres_tbl} TO STDOUT WITH DELIMITER ',' CSV HEADER"

        with open(prod_file_prefix+f'{output_csv}.csv', 'w') as f:
            cursor.copy_expert(copy_cmd,f)

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")
        print("Table(s) could not be dropped.")

def drop_postgres_tables(table1, table2=None):
    try:
        if table2 is not None:    
            drop_query = f'''DROP TABLE IF EXISTS {table1}, {table2} '''    
            print_statement = f"The {table1} and {table2} tables have been dropped from the Postgres DB."
        else:
            drop_query = f'''DROP TABLE IF EXISTS {table1} '''
            print_statement =  f"The {table1} table has been dropped from the Postgres DB."     
        
        pg_hook = PostgresHook(postgres_conn_id='postgres')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(drop_query) 
        print(print_statement) 
        conn.close() 

    except Exception as e:
        print(f"Error: {e}")
        print("Table(s) could not be dropped.")