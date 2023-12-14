import os
import pathlib
from pathlib import Path, PurePosixPath, PureWindowsPath

folder_path = str(pathlib.PureWindowsPath(os.path.abspath(os.path.dirname(__file__))).as_posix())
prod_file_prefix = folder_path+"/development/data/2022eventfiles"

from development.etl.raw_ETL_fact_events_trial import df2, print_df, iterate_col, final_filter, stat_generation
from development.etl.raw_file_extension_update import CSVReader, update_file_extension,build_file_df, construct_file_list, iterate_over_list


if __name__ == "__main__":
    file_df = build_file_df(folder_path=prod_file_prefix)
    print(file_df.head())
    listed_files = construct_file_list(df=file_df)
    iterate_over_list(file_list=listed_files[1:], folder_path=prod_file_prefix)
