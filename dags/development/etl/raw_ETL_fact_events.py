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
        , column_names=['team','league','city','teamlogo']
    )
    file_df = csv_reader.read_csv()
    return file_df

def construct_file_list(df):
    file_list = []

    for index, row in df.iterrows():
        
        value_to_append_events = str(2022)+str(row['team'])+'.csv'

        # Append the value to the list
        file_list.append(value_to_append_events)
    
    return file_list

def build_initial_df_from_file(file):
    # Read the raw gamelogs data to a pandas dataframe
    csv_reader = CSVReader(
        file_path = folder_path+"/"+file
        , column_names=['col1','col2','col3','col4','col5','col6','col7']
    )
    df = csv_reader.read_csv()
    # filter df to id and play events 
    df2 = df[(df['col1'] == 'id') | (df['col1'] == 'info') | (df['col1'] == 'play')].reset_index()

    def recent_id_field(row):
        if row['col1'] == 'id':
            return row['col2']
        else:
            return 'see_prev_date'

    def visiting_team(row):
        if (row['col1'] == 'info' and row['col2'] == 'visteam'):
            return row['col3']
        else:
            return 'see_prev_visiting_team'

    df2['team_date_id'] = df2.apply(recent_id_field, axis=1)
    df2['visiting_team'] = df2.apply(visiting_team, axis=1)

    return df2

def iterate_col(input_df):
    # Iterate through the DataFrame
    # ACTION POTENTIAL CHANGE BELOW, remove "1,"
    for i in range(1, len(input_df)):
        if input_df.loc[i, 'team_date_id'] == 'see_prev_date':
            # Replace 'see_prev_date' with the value from the previous row
            input_df.at[i, 'team_date_id'] = input_df.at[i - 1, 'team_date_id']

    for i in range(1, len(input_df)):
        if input_df.loc[i, 'visiting_team'] == 'see_prev_visiting_team':
            # Replace 'see_prev_date' with the value from the previous row
            input_df.at[i, 'visiting_team'] = input_df.at[i - 1, 'visiting_team']

    return input_df

def final_filter(input_df):
    filtered_df = input_df[(input_df['col1'] == 'play')]
    
    renamed_filtered_df = filtered_df.rename(columns={
        "col1": "event_type"
        , "col2": "inning_no"
        , "col3": "home_team"
        , "col4": "player_id"
        , "col5": "count_on_batter"
        , "col6": "pitches_to_batter"
        , "col7": "event_describer"
    })
    renamed_filtered_df['team'] = renamed_filtered_df['team_date_id'].str[:3]
    renamed_filtered_df['date_id'] = renamed_filtered_df['team_date_id'].str[3:11]
    renamed_filtered_df = renamed_filtered_df.drop(['team_date_id'], axis=1).reset_index(drop=True)
    
    for i in range(len(renamed_filtered_df)):
        if renamed_filtered_df.loc[i, 'home_team'] == '0':
            # Replace 'see_prev_date' with the value from the previous row
            renamed_filtered_df.at[i, 'team'] = renamed_filtered_df.at[i, 'visiting_team']
    
    return renamed_filtered_df.drop(['visiting_team'], axis=1)

def stat_generation(input_df):
    input_df["PA"] = 1
    input_df["AB"] = 1
    input_df["H"] = 0
    input_df["Double"] = 0
    input_df["Triple"] = 0
    input_df["HR"] = 0
    input_df["1-H"] = 0
    input_df["2-H"] = 0
    input_df["3-H"] = 0
    input_df["ER"] = 0
    input_df["RBI"] = 0
    input_df["BB"] = 0
    input_df["BB_int"] = 0
    input_df["HBP"] = 0
    input_df["SH"] = 0
    input_df["SF"] = 0
    input_df["SB"] = 0
    input_df["SB2"] = 0
    input_df["SB3"] = 0    
    input_df["SBH"] = 0
    input_df["CS"] = 0
    input_df["GIDP"] = 0
    input_df["SO"] = 0

    for i in range(len(input_df)):

        if any(ab in input_df.loc[i, "event_describer"] for ab in ["W", "HP", "SF","SH", "I", "IW", "C/E3", "C/E2", "C/E1"]):
            input_df.at[i, "AB"] = 0

        if any(s in input_df.loc[i, "event_describer"] for s in ["S0", "S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9"]):
            input_df.at[i, "H"] = 1

        if any(d in input_df.loc[i, "event_describer"] for d in ["D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9", "DGR"]):
            input_df.at[i, "H"] = 1
            input_df.at[i, "Double"] = 1

        if any(t in input_df.loc[i, "event_describer"] for t in ["T0", "T1", "T2", "T3", "T4", "T5", "T6", "T7", "T8", "T9"]):
            input_df.at[i, "H"] = 1
            input_df.at[i, "Triple"] = 1

        if "HR" in input_df.loc[i, "event_describer"]:
            input_df.at[i, "H"] = 1
            input_df.at[i, "HR"] = 1

        if "3-H" in input_df.loc[i, "event_describer"]:
            input_df.at[i, "3-H"] = 1

        if "2-H" in input_df.loc[i, "event_describer"]:
            input_df.at[i, "2-H"] = 1

        if "1-H" in input_df.loc[i, "event_describer"]:
            input_df.at[i, "1-H"] = 1

        if any(er in input_df.loc[i, "event_describer"] for er in ["E0", "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9"]):
            input_df.at[i, "ER"] = 1

        input_df.at[i, "RBI"] = input_df.at[i, "HR"] + input_df.at[i, "3-H"] + input_df.at[i, "2-H"] + input_df.at[i, "1-H"] - input_df.at[i, "ER"]

        if "W" in input_df.loc[i, "event_describer"]:
            input_df.at[i, "BB"] = 1        
        
        if any(int_w in input_df.loc[i, "event_describer"] for int_w in ["I", "IW"]):
            input_df.at[i, "BB_int"] = 1

        if "HP" in input_df.loc[i, "event_describer"]:
            input_df.at[i, "HBP"] = 1

        if "SH" in input_df.loc[i, "event_describer"]:
            input_df.at[i, "SH"] = 1

        if "SF" in input_df.loc[i, "event_describer"]:
            input_df.at[i, "SF"] = 1

        if "SB2" in input_df.loc[i, "event_describer"]:
            input_df.at[i, "SB2"] = 1

        if "SB3" in input_df.loc[i, "event_describer"]:
            input_df.at[i, "SB3"] = 1

        if "SBH" in input_df.loc[i, "event_describer"]:
            input_df.at[i, "SBH"] = 1

        input_df.at[i, "SB"] = input_df.at[i, "SB2"] + input_df.at[i, "SB3"] + input_df.at[i, "SBH"]

        if "CS" in input_df.loc[i, "event_describer"]:
            input_df.at[i, "CS"] = 1

        if any(gidp in input_df.loc[i, "event_describer"] for gidp in ["GDP", "LDP"]):
            input_df.at[i, "GIDP"] = 1

        if "K" in input_df.loc[i, "event_describer"]:
            input_df.at[i, "SO"] = 1

    return input_df.drop(['3-H','2-H','1-H', 'SB2', 'SB3', 'SBH', 'ER'], axis=1)

def run_etl(df, file_path):
    # Write this dataframe to a CSV file in the listed location.
    df_to_ETL = CSVWriter(df)
    df_to_ETL.write_to_csv(file_path)

def run_append_etl(df, file_path):
    # append this dataframe to an existing CSV file in the listed location.
    df_to_ETL_through_append = CSVWriter(df)
    df_to_ETL_through_append.append_to_csv(file_path)
    # Append the DataFrame to the existing CSV file

