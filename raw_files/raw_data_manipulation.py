import pandas as pd
from raw_data_extraction_functions import CSVReader, DataframeGlueOps, CSVWriter
from raw_data_gamelogs_organization import gamelogs_df_column_names, merged_columns, dim_team_stats_home_col, dim_team_stats_visitors_col, fact_gamelogs_cols

csv_reader = CSVReader(
    file_path = "C:/Users/95jha/Documents/Learning/JHack_Portfolio/raw_files/gl2022.csv"
    , column_names=gamelogs_df_column_names
)
df = csv_reader.read_csv()

#########################################################
# Fact_gamelogs
#########################################################

fact_gamelogs = df[fact_gamelogs_cols]

# Function to compare home_team_score against visiting_team_score
def compare_scores(row):
    if row['home_team_score'] > row['visiting_team_score']:
        return row['home_team']
    else:
        return row['visiting_team']
    
# Apply the function to create a new column 'Result'
fact_gamelogs['winning_team'] = fact_gamelogs.apply(compare_scores, axis=1)

df_to_csv_fact_gamelogs = CSVWriter(fact_gamelogs)
df_to_csv_fact_gamelogs.write_to_csv('C:/Users/95jha/Documents/Learning/JHack_Portfolio/cleaned_files/fact_gamelogs')

#########################################################
# Dim_Team_statistics
#########################################################

dim_team_statistics_visitors = df[dim_team_stats_visitors_col].groupby(by=["visiting_team","visiting_league"], as_index=False).sum()
# print(dim_team_statistics_visitors.head(30))

dim_team_statistics_home = df[dim_team_stats_home_col].groupby(by=["home_team","home_league"], as_index=False).sum()

df_ops = DataframeGlueOps(df1=dim_team_statistics_home,df2=dim_team_statistics_visitors)
dim_team_statistics_final = df_ops.merge_and_rename(df1=dim_team_statistics_home,df2=dim_team_statistics_visitors, new_column_names = merged_columns)

dim_team_statistics_final = dim_team_statistics_final.groupby(by=['team','league'], as_index=False).sum()
#print(dim_team_statistics_final.head(30))

# df_to_csv_dim_team_statistics = CSVWriter(dim_team_statistics_final)
# df_to_csv_dim_team_statistics.write_to_csv('C:/Users/95jha/Documents/Learning/JHack_Portfolio/cleaned_files/dim_team_statistics')

#########################################################
# Dim_Team_record
#########################################################

dim_record_df = fact_gamelogs[["date", "game_no", "dow", "visiting_team", "home_team", "winning_team"]]
dim_record_df['losing_team'] = dim_record_df.apply(lambda row: row['visiting_team'] if row['winning_team'] == row['home_team'] else row['home_team'], axis=1)
dim_record_df_melted = pd.melt(dim_record_df, id_vars=["date", "game_no", "dow","visiting_team", "home_team"], value_vars=["winning_team", "losing_team"])
dim_record_df_melted['win'] = dim_record_df_melted[["variable","value"]].apply(lambda row: 0 if row['variable'] == 'losing_team' else 1, axis=1)
dim_record_df_melted['loss'] = dim_record_df_melted.apply(lambda row: 0 if row['variable'] == 'winning_team' else 1, axis=1)
dim_team_rec = dim_record_df_melted.drop(['date', 'game_no','dow','visiting_team','home_team','variable'], axis=1).groupby(by=['value'], as_index=False).sum()
dim_team_rec = dim_team_rec.rename(columns={"value": "team","win": "wins","loss": "losses"}).sort_values(by='wins', ascending=False).reset_index(drop=True)

dim_team_final = pd.concat([dim_team_rec, dim_team_statistics_final], axis=1)
df_to_csv_dim_team_final = CSVWriter(dim_team_final)
df_to_csv_dim_team_final.write_to_csv('C:/Users/95jha/Documents/Learning/JHack_Portfolio/cleaned_files/dim_team_statistics')

#########################################################
# Dim_season_dates
#########################################################

df_date = fact_gamelogs[['date']].drop_duplicates(['date']).reset_index(drop=True)
#print(df_date)
df_to_csv_dim_date = CSVWriter(df_date)
df_to_csv_dim_date.write_to_csv('C:/Users/95jha/Documents/Learning/JHack_Portfolio/cleaned_files/dim_date')