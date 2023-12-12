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

# Restructure the Teams file, and use it to build out the file list.
def update_file_extension(folder_path):
    current_file = Path(folder_path+"/TEAM2022")
    current_file.rename(current_file.with_suffix('.csv'))

def build_file_df(folder_path):
    # Read the raw gamelogs data to a pandas dataframe
    csv_reader = CSVReader(
        file_path = folder_path+"/TEAM2022.csv"
        , column_names=['col1','col2','col3','col4']
    )
    file_df = csv_reader.read_csv()
    print()
    return file_df

def construct_file_list(df):
    file_list = []

    for index, row in df.iterrows():
        
        value_to_append_events = str(2022)+str(row['col1'])+'.EV'+str(row['col2'])
        value_to_append_rosters = str(row['col1'])+str(2022)+'.ROS'

        # Append the value to the list
        file_list.append(value_to_append_events)
        file_list.append(value_to_append_rosters)
        # file_list.remove("2022ANA.EVA")
    
    return file_list

def iterate_over_list(file_list, folder_path):
    for file in file_list:

        current_file = Path(folder_path+"/"+file)
        if PurePosixPath(current_file).suffix == '.csv':
            continue
        else:
            current_file.rename(current_file.with_suffix('.csv'))
