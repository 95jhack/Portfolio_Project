import os
import pathlib
from pathlib import Path, PurePosixPath, PureWindowsPath

folder_path = str(pathlib.PureWindowsPath(os.path.abspath(os.path.dirname(__file__))).as_posix())
prod_file_prefix = folder_path+"/development/data/2022eventfiles"
output_path = "c:/Users/95jha/Documents/Learning/Portfolio_Project/development/data/production/"

from development.etl.raw_ETL_dim_rosters_trial import build_file_df, construct_file_list
from development.etl.raw_ETL_dim_rosters_trial import build_initial_df_from_file, run_etl, run_append_etl

def run_etl_loop(file_list):
    for idx, file in enumerate(file_list):
        initial_df = build_initial_df_from_file(file)
        if idx == 0:
            run_etl(df=initial_df,file_path=output_path+'dim_team_rosters')
        else:
            run_append_etl(df=initial_df,file_path=output_path+'dim_team_rosters')
        

if __name__ == "__main__":
# Initial Events - Script Execution
    file_df = build_file_df(folder_path=prod_file_prefix)

    listed_files = construct_file_list(df=file_df)

    run_etl_loop(file_list=listed_files)
