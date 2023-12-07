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

import unittest
import testing_teams as tt
import testing_gamelogs as tg
import pandas as pd
from ..raw_data_extraction_functions import CSVReader


class TestTeams(unittest.TestCase):

    def test_duplicate_check(self):
        expected_result_no_dupes = tt.dup_check_result
        self.assertTrue(expected_result_no_dupes == 0)

    def test_team_count(self):
        mlb_team_count = 30
        testing_teams = tt.rows
        self.assertEqual(testing_teams,mlb_team_count)

class TestGameLogs(unittest.TestCase):

    def test_duplicate_check(self):
        expected_result_no_dupes = tg.dup_check_result
        self.assertTrue(expected_result_no_dupes == 0)

class TestDates(unittest.TestCase):
    def test_duplicate_check(self):
        # Read the raw gamelogs data to a pandas dataframe
        csv_reader = CSVReader(
            file_path = "C:/Users/95jha/Documents/Learning/JHack_Portfolio/cleaned_files/dim_date.csv"
            , column_names=gamelogs_df_column_names
        )
        date_df = csv_reader.read_csv()
        expected_result_no_dupes = len(date_df['date_id'])-len(date_df['date_id'].drop_duplicates())
        self.assertTrue(expected_result_no_dupes == 0)

    # def acceptable_values_check(self):


    # def not_null_check(self):
        

    # def columns_match_schema(self):
        

    # def logic_based_testing(self):

    #     def testing_gamelogs_total_game_count(self):
    #         away_games_per_year = 81 # This is correct for this year, but not the case for all seasons. Ex: COVID-19 impacted season duration.
    #         home_games_per_year = 81 # This is correct for this year, but not the case for all seasons. Ex: COVID-19 impacted season duration.
    #         testing_gamelogs_games_per_year = tg.rows_away+tg.rows_home
    #         self.assertEqual(testing_gamelogs_games_per_year,home_games_per_year+away_games_per_year)




# class TestDates(unittest.TestCase):
#     def test_team_count(self):
#         mlb_team_count = 30
#         testing_teams = tt.rows
#         self.assertEqual(testing_teams,mlb_team_count)


if __name__ == '__main__':
    unittest.main()