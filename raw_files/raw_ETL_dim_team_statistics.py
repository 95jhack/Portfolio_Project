"""
Purpose:
This file is used for 


"""

###############
### Imports ###
###############

import pandas as pd
from raw_data_extraction_functions import CSVReader, DataframeGlueOps, CSVWriter
from raw_data_column_organization import gamelogs_df_column_names, merged_columns, dim_team_stats_home_col, dim_team_stats_visitors_col, fact_gamelogs_cols

# Read the raw gamelogs data to a pandas dataframe
csv_reader = CSVReader(
    file_path = "C:/Users/95jha/Documents/Learning/JHack_Portfolio/raw_files/gl2022.csv"
    , column_names=gamelogs_df_column_names
)
df = csv_reader.read_csv()

#########################################################
########### Object: dim_team_statistics #################
#########################################################

# Filter the raw_gamelogs data to focus solely on the pertaining data required at this stage.
fact_gamelogs = df[fact_gamelogs_cols]

# Create a dataframe focused on the visitors statistics, based on the raw dataframe.
dim_team_statistics_visitors = df[dim_team_stats_visitors_col].groupby(by=["visiting_team","visiting_league"], as_index=False).sum()

# Create a dataframe focused on the home team's statistics, based on the raw dataframe.
dim_team_statistics_home = df[dim_team_stats_home_col].groupby(by=["home_team","home_league"], as_index=False).sum()

# Use the Dataframe Glue Ops object to rename the columns and UNION the dataframes.
df_ops = DataframeGlueOps(df1=dim_team_statistics_home,df2=dim_team_statistics_visitors)
dim_team_statistics_final = df_ops.merge_and_rename(df1=dim_team_statistics_home,df2=dim_team_statistics_visitors, new_column_names = merged_columns)

# Summarize the data based on the pertaining teams and their respective league that they are in within Major League Baseball 
dim_team_statistics_final = dim_team_statistics_final.groupby(by=['team','league'], as_index=False).sum()

# Next we seperate of the statistics each team generates during play, it is important to understand how many wins and losses teams had during the season.

# Function to compare home_team_score against visiting_team_score within the gamelogs
def winning_team(row):
    if row['home_team_score'] > row['visiting_team_score']:
        return row['home_team'] 
    else:
        return row['visiting_team']

# Create the column based on the above function.
fact_gamelogs["winning_team"] = fact_gamelogs.apply(winning_team, axis=1)

# Focus on certain columns from the initial gamelogs object listed above.
dim_record_df = fact_gamelogs[["date", "game_no", "dow", "visiting_team", "home_team", "winning_team"]]

# Create a column focused on which team within the game is the losing team.
dim_record_df['losing_team'] = dim_record_df.apply(lambda row: row['visiting_team'] if row['winning_team'] == row['home_team'] else row['home_team'], axis=1)

# The dataframe is pivoted on whether they won or lost the respective game, this way each team had all 162 games of their considered.
dim_record_df_melted = pd.melt(dim_record_df, id_vars=["date", "game_no", "dow","visiting_team", "home_team"], value_vars=["winning_team", "losing_team"])

# Create columns for both wins and losses so that the values of each can be summarized.
dim_record_df_melted['wins'] = dim_record_df_melted[["variable","value"]].apply(lambda row: 0 if row['variable'] == 'losing_team' else 1, axis=1)
dim_record_df_melted['losses'] = dim_record_df_melted.apply(lambda row: 0 if row['variable'] == 'winning_team' else 1, axis=1)

# Drop the unnecessary columns for this analysis focused on the fields to calculate a team's record
dim_team_rec = dim_record_df_melted.drop(['date', 'game_no','dow','visiting_team','home_team','variable'], axis=1).groupby(by=['value'], as_index=False).sum()

# Rename certain fields, sort based on the more important stat - wins 
dim_team_rec = dim_team_rec.rename(columns={"value": "team"}).sort_values(by='wins', ascending=False).reset_index(drop=True)

# Concatenate the team's record values with the statistics as new columns.
dim_team_final = pd.concat([dim_team_rec, dim_team_statistics_final], axis=1)

# Write this dataframe to a CSV file in the listed location.
df_to_csv_dim_team_final = CSVWriter(dim_team_final)
df_to_csv_dim_team_final.write_to_csv('C:/Users/95jha/Documents/Learning/JHack_Portfolio/cleaned_files/dim_team_statistics')
