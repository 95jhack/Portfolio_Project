import os
import pathlib

folder_path = str(pathlib.PureWindowsPath(os.path.abspath(os.path.dirname(__file__))).as_posix())
prod_file_prefix = folder_path+"/development/data/production/"

from development.etl.raw_ETL_dim_date import run_etl, date_dimension_df
from development.etl.raw_ETL_dim_team_statistics import run_etl, dim_team_final
from development.etl.raw_ETL_fact_gamelogs import run_etl, total_gamelogs

if __name__ == "__main__":
    run_etl(df=date_dimension_df,file_path=prod_file_prefix+'dim_date')
    run_etl(df=dim_team_final,file_path=prod_file_prefix+'dim_team_statistics')
    run_etl(df=total_gamelogs,file_path=prod_file_prefix+'fact_gamelogs')
