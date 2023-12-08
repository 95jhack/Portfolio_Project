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
prod_file_prefix = "C:/Users/95jha/Documents/Learning/JHack_Portfolio/development/data/production/"
import pandas as pd
import unittest
# from development.functions.raw_data_column_organization import gamelogs_df_column_names
from development.tests import testing_gamelogs as tg
from development.tests import testing_teams as tt
from development.functions.raw_data_extraction_functions import CSVReader


#########################################################
########### Testing Object: dim_team_statistics #########
#########################################################

class TestTeams(unittest.TestCase):

    def test_duplicate_check(self):
        expected_result_no_dupes = tt.dup_check_result
        self.assertTrue(expected_result_no_dupes == 0)

    def test_null_column_check(self):
        # Read the cleaned dim date data to a pandas dataframe
        csv_reader = CSVReader(
            file_path = prod_file_prefix+"dim_team_statistics.csv"
        )
        teams_df = csv_reader.read_csv()
        nulls_for_entire_table = teams_df.isnull().sum().sum()
        self.assertTrue(nulls_for_entire_table == 0)

    def test_team_count(self):
        mlb_team_count = 30
        testing_teams = tt.rows
        self.assertEqual(testing_teams,mlb_team_count)


#########################################################
########### Testing Object: fact_gamelogs ###############
#########################################################

class TestGameLogs(unittest.TestCase):

    def test_duplicate_check(self):
        expected_result_no_dupes = tg.dup_check_result
        self.assertTrue(expected_result_no_dupes == 0)

    def test_null_column_check(self):
        # Read the cleaned dim date data to a pandas dataframe
        csv_reader = CSVReader(
            file_path =  prod_file_prefix+"fact_gamelogs.csv"
        )
        gamelogs_df = csv_reader.read_csv()
        nulls_for_entire_table = gamelogs_df.isnull().sum().sum()
        self.assertTrue(nulls_for_entire_table == 0)


#########################################################
########### Testing Object: dim_date ####################
#########################################################

class TestDates(unittest.TestCase):

    def test_columns_match_schema(self):
        # Read the cleaned dim date data to a pandas dataframe
        csv_reader = CSVReader(
            file_path =  prod_file_prefix+"dim_date.csv"
        )
        date_df = csv_reader.read_csv()
        df_cols = list(date_df.columns.values)
        expected_column_set = ['Date','date_id','Year','Month','Day','Month_Name'
        ,'Weekday','dayofyear','Quarter','days_in_month','is_month_start'
        ,'is_month_end','is_year_start','is_year_end','is_leap_year']
        self.assertEqual(df_cols,expected_column_set)

    def test_duplicate_check(self):
        # Read the cleaned dim date data to a pandas dataframe
        csv_reader = CSVReader(
            file_path =  prod_file_prefix+"dim_date.csv"
        )
        date_df = csv_reader.read_csv()
        expected_result_no_dupes = len(date_df)-len(date_df['date_id'].drop_duplicates())
        self.assertTrue(expected_result_no_dupes == 0)

    def test_null_column_check(self):
        # Read the cleaned dim date data to a pandas dataframe
        csv_reader = CSVReader(
            file_path =  prod_file_prefix+"dim_date.csv"
        )
        date_df = csv_reader.read_csv()
        nulls_for_entire_table = date_df.isnull().sum().sum()
        self.assertTrue(nulls_for_entire_table == 0)

    def test_for_acceptable_values(self):
        # Read the cleaned dim date data to a pandas dataframe
        csv_reader = CSVReader(
            file_path =  prod_file_prefix+"dim_date.csv"
        )
        date_df = csv_reader.read_csv()
        # df_cols = date_df['is_month_start','is_month_end','is_year_start','is_year_end','is_leap_year']
        print(date_df.head())
       # self.assertEqual(df_cols,expected_column_set)

    
    '''     
        * Accepted Values Tests [T/F] ---> All Boolean columns
        * Logic Based Testing:
            - Day between 1 and 31
            - Month between 1 and 12
            - Quarter between 1 and 4
            - dayofyear between 1 and 365, if is_leap_year = False, else between 1 and 366
            - days_in_month between 28 and 31
            - if is_month_start = TRUE, Day must equal 1
            - if is_year_start = TRUE, Day must equal 1, dayofyear must equal 1, Month = 1, is_month_start = TRUE
            - if is_year_end = True, Day must equal 31, Month = 12
            - if is_month_end = True, Day must be between 28 and 31.
    '''
    # def logic_based_testing(self):

    #     def testing_gamelogs_total_game_count(self):
    #         away_games_per_year = 81 # This is correct for this year, but not the case for all seasons. Ex: COVID-19 impacted season duration.
    #         home_games_per_year = 81 # This is correct for this year, but not the case for all seasons. Ex: COVID-19 impacted season duration.
    #         testing_gamelogs_games_per_year = tg.rows_away+tg.rows_home
    #         self.assertEqual(testing_gamelogs_games_per_year,home_games_per_year+away_games_per_year)




if __name__ == '__main__':
    unittest.main()