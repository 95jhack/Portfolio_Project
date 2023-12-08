from development.etl.raw_ETL_dim_date import run_etl, date_dimension_df
from development.etl.raw_ETL_dim_team_statistics import run_etl, dim_team_final
from development.etl.raw_ETL_fact_gamelogs import run_etl, total_gamelogs

if __name__ == "__main__":
    run_etl(df=date_dimension_df,file_path='C:/Users/95jha/Documents/Learning/JHack_Portfolio/development/data/production/dim_date')
    run_etl(df=dim_team_final,file_path='C:/Users/95jha/Documents/Learning/JHack_Portfolio/development/data/production/dim_team_statistics')
    run_etl(df=total_gamelogs,file_path='C:/Users/95jha/Documents/Learning/JHack_Portfolio/development/data/production/fact_gamelogs')




        #df_to_csv_dim_date = CSVWriter(date_dimension_df)
    #df_to_csv_dim_date.write_to_csv('C:/Users/95jha/Documents/Learning/JHack_Portfolio/development_package/data/dim_date')


#     # Write this dataframe to a CSV file in the listed location.
# df_to_csv_dim_team_final = CSVWriter(dim_team_final)
# df_to_csv_dim_team_final.write_to_csv('C:/Users/95jha/Documents/Learning/JHack_Portfolio/development_package/data/dim_team_statistics')