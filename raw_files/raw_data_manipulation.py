from raw_data_extraction_functions import CSVReader, DataframeGlueOps, CSVWriter
from raw_data_gamelogs_organization import gamelogs_df_column_names, merged_columns, dim_team_stats_home_col, dim_team_stats_visitors_col, fact_gamelogs_cols

csv_reader = CSVReader("JHack_Portfolio/raw_files/gl2022.csv", column_names=gamelogs_df_column_names)
df = csv_reader.read_csv()

# Display the data, that was read into df
# csv_reader.display_data()

columns = csv_reader.get_column_names()
#print(f"Column Names: {columns}")

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
df_to_csv_fact_gamelogs.write_to_csv('JHack_Portfolio/cleaned_files/fact_gamelogs')

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

df_to_csv_dim_team_statistics = CSVWriter(dim_team_statistics_final)
df_to_csv_dim_team_statistics.write_to_csv('JHack_Portfolio/cleaned_files/dim_team_statistics')