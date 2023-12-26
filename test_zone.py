'''
Test Plans based on the schema of each object.
1. Duplication Testing 
2. Test column Set
3. Null Testing 
4. Variable Type Testing
5. Aggregated Testing
6. Logic-based Testing
7. Illegal Column Values Testing
8. Accepted Value Testing
9. Refrential Integrity Testing
10. Multi-data source testing ---> Compare against API
'''
import os
import pathlib
import numpy as np 
import pandas as pd
import unittest

# from dags.development.tests import testing_gamelogs as tg
# from dags.development.tests import testing_teams as tt
# from dags.development.tests import testing_dates as td
from dags.development.functions.raw_data_extraction_functions import CSVReader

folder_path = str(pathlib.PureWindowsPath(os.path.abspath(os.path.dirname(__file__))).as_posix())
prod_file_prefix = folder_path+"/dags/development/data/production/"

class TestEvents(unittest.TestCase):
    # Read the cleaned dim team statistics data to a pandas dataframe
    csv_reader1 = CSVReader(
        file_path = prod_file_prefix+"dim_team_statistics.csv"
    )
    dim_team_stats_df = csv_reader1.read_csv()

    # Read the cleaned fact events data to a pandas dataframe
    csv_reader2 = CSVReader(
        file_path = prod_file_prefix+"fact_game_events.csv"
    )
    fact_game_events_df = csv_reader2.read_csv()

    def test_duplicate_check(self):
        expected_result_no_dupes = len(self.fact_game_events_df)-len(self.fact_game_events_df['index'].drop_duplicates())
        self.assertTrue(expected_result_no_dupes == 0)

    def test_null_column_check(self):
        nulls_for_entire_table = self.fact_game_events_df.isnull().sum().sum()
        self.assertTrue(nulls_for_entire_table == 0)

    def test_fact_game_events_field_logic_1(self):
        fact_game_events_field_sum = self.fact_game_events_df['HR'].sum()
        dim_team_stats_field_sum = self.dim_team_stats_df['hr'].sum()
        # if fact_game_events_field_sum != dim_team_stats_field_sum:
        #     return f"Fields are not currently aligned. Summed Team Statistics shows field total as {dim_team_stats_field_sum}, whereas the summed events files show the total as {fact_game_events_field_sum}, please investigate the logic accordingly."
        # else:
        #     return "Results are aligned. Test is passing."
        self.assertTrue(fact_game_events_field_sum == dim_team_stats_field_sum)

if __name__ == '__main__':
    unittest.main()