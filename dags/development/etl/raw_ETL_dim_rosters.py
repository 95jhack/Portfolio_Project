"""
Purpose:
This file is used for 


"""

###############
### Imports ###
###############
import os
import pathlib
from pathlib import Path, PurePosixPath, PureWindowsPath

folder_path = str(pathlib.PureWindowsPath(os.path.abspath(os.path.dirname(__file__))).as_posix()).replace('etl','data/2022eventfiles')

import pandas as pd
from development.functions.raw_data_extraction_functions import CSVReader, CSVWriter, create_id_from_text_column 

def build_file_df(folder_path):
    # Read the raw gamelogs data to a pandas dataframe
    csv_reader = CSVReader(
        file_path = folder_path+"TEAM2022.csv"
        , column_names=['col1','col2','col3','col4']
    )
    file_df = csv_reader.read_csv()
    return file_df

def construct_file_list(df):
    file_list = []

    for index, row in df.iterrows():
        
        value_to_append_rosters = str(row['col1'])+str(2022)+'.csv'

        # Append the value to the list
        file_list.append(value_to_append_rosters)
    
    return file_list

def build_initial_df_from_file(file):
    # Read the raw gamelogs data to a pandas dataframe
    csv_reader = CSVReader(
        file_path = folder_path+"/"+file
        , column_names=['player_id','lastname','firstname','handedness_batting','handedness_throwing','team','position']
    )
    df = csv_reader.read_csv()
    return df

def run_etl(df, file_path):
    # Write this dataframe to a CSV file in the listed location.
    df_to_ETL = CSVWriter(df)
    df_to_ETL.write_to_csv(file_path)

def run_append_etl(df, file_path):
    # append this dataframe to an existing CSV file in the listed location.
    df_to_ETL_through_append = CSVWriter(df)
    df_to_ETL_through_append.append_to_csv(file_path)
    # Append the DataFrame to the existing CSV file