"""
Purpose:
This file is used for 


"""

###############
### Imports ###
###############

import pandas as pd
from raw_data_extraction_functions import CSVReader, CSVWriter
from raw_data_column_organization import gamelogs_df_column_names, merged_columns, fact_gamelogs_cols

# Read the raw gamelogs data to a pandas dataframe
csv_reader = CSVReader(
    file_path = "C:/Users/95jha/Documents/Learning/JHack_Portfolio/raw_files/gl2022.csv"
    , column_names=gamelogs_df_column_names
)
df = csv_reader.read_csv()

#########################################################
########### Object: fact_gamelogs #######################
#########################################################

# Filter the raw_gamelogs data to focus solely on the pertaining data required at this stage.
fact_gamelogs = df[fact_gamelogs_cols]

# Create a copy of the base data set, for home and away.
home_gamelogs_transform = fact_gamelogs
away_gamelogs_transform = fact_gamelogs

# Rename both the home and away gamelogs to focus on a "team" of interest and their opponent.
home_gamelogs = home_gamelogs_transform.rename(
    columns={
        "home_team": "team"
        ,"home_league": "league"
        ,"visiting_team": "opponent_team"
        ,"visiting_league": "opponent_league"
        ,"visiting_team_score": "opponent_score"
        ,"home_team_score": "team_score"
        ,"home_league_game_no":"team_game_no"
    }).drop(['dow','visiting_league_game_no','game_no'], axis=1)
away_gamelogs = away_gamelogs_transform.rename(
    columns={
        "visiting_team": "team"
        ,"visiting_league": "league"
        ,"home_team": "opponent_team"
        ,"home_league": "opponent_league"
        ,"home_team_score": "opponent_score"
        ,"visiting_team_score": "team_score"
        ,"visiting_league_game_no":"team_game_no"
    }).drop(['dow','home_league_game_no','game_no'], axis=1)

# Function to compare home_team_score against visiting_team_score
def compare_scores(row):
    if row['team_score'] > row['opponent_score']:
        return 1
    else:
        return 0
 
# Apply the compare_scores function to create a new flag column determining whether it was a win or loss.
home_gamelogs['win_loss'] = home_gamelogs.apply(compare_scores, axis=1)
away_gamelogs['win_loss'] = away_gamelogs.apply(compare_scores, axis=1)

# Apply a lambda function to discern whether the "team" of interest was the home or away team.
home_gamelogs['is_home'] = home_gamelogs[['date','team','league','team_game_no','opponent_team','opponent_league','team_score','opponent_score','win_loss']].apply(lambda row: 1, axis=1)
away_gamelogs['is_home'] = away_gamelogs[['date','team','league','team_game_no','opponent_team','opponent_league','team_score','opponent_score','win_loss']].apply(lambda row: 0, axis=1)

# Concatenate the home and away gamelogs -- effectively this acts as a UNION ALL 
total_gamelogs = pd.concat([home_gamelogs, away_gamelogs])

# Write this dataframe to a CSV file in the listed location.
df_to_csv_total_gamelogs = CSVWriter(total_gamelogs)
df_to_csv_total_gamelogs.write_to_csv('C:/Users/95jha/Documents/Learning/JHack_Portfolio/cleaned_files/fact_gamelogs')
