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
# Directory, Transformations, and Other Imports
import os
import pathlib
import numpy as np 
import pandas as pd
import unittest

# ETL & formatting Imports
from dags.development.functions.raw_data_extraction_functions import CSVReader

# Directory Structuring
folder_path = str(pathlib.PureWindowsPath(os.path.abspath(os.path.dirname(__file__))).as_posix())
prod_file_prefix = folder_path+"/dags/development/data/production/"

#########################################################
########### Testing Object: dim_date ####################
#########################################################

# ACTION - MOVE THIS CODE TO OTHER FILE, IMPORT TO TEST EXECUTION FILE. 

class TestDates(unittest.TestCase):
    # Read the cleaned dim date data to a pandas dataframe
    csv_reader = CSVReader(
        file_path =  prod_file_prefix+"dim_date.csv"
    )
    date_df = csv_reader.read_csv()
    print('Data from the dim_date object has been read to a DataFrame. Testing to begin.')

    boolean_expected_values = ['f','t']

    def test_columns_match_schema(self):
        df_cols = list(self.date_df.columns.values)
        expected_column_set = ['Date','date_id','Year','Month','Day','Month_Name'
        ,'Weekday','dayofyear','Quarter','days_in_month','is_month_start'
        ,'is_month_end','is_year_start','is_year_end','is_leap_year']
        self.assertEqual(df_cols,expected_column_set)

    def test_duplicate_check(self):
        expected_result_no_dupes = len(self.date_df)-len(self.date_df['date_id'].drop_duplicates())
        self.assertTrue(expected_result_no_dupes == 0)

    def test_null_column_check(self):
        nulls_for_entire_table = self.date_df.isnull().sum().sum()
        self.assertTrue(nulls_for_entire_table == 0)

    def test_for_acceptable_values_1(self):
        boolean_field_1 = list(np.sort(self.date_df['is_month_start'].unique()))
        self.assertTrue(boolean_field_1[0]==self.boolean_expected_values[0], boolean_field_1[1]==self.boolean_expected_values[1])

    def test_for_acceptable_values_2(self):
        boolean_field_2 = list(np.sort(self.date_df['is_month_end'].unique()))
        self.assertTrue(boolean_field_2[0]==self.boolean_expected_values[0], boolean_field_2[1]==self.boolean_expected_values[1])

    def test_for_acceptable_values_3(self):
        boolean_field_3 = list(np.sort(self.date_df['is_year_start'].unique()))
        self.assertTrue(boolean_field_3[0]==self.boolean_expected_values[0], boolean_field_3[1]==self.boolean_expected_values[1])

    def test_for_acceptable_values_4(self):
        boolean_field_4 = list(np.sort(self.date_df['is_year_end'].unique()))
        self.assertTrue(boolean_field_4[0]==self.boolean_expected_values[0], boolean_field_4[1]==self.boolean_expected_values[1])

    def test_for_acceptable_values_5(self):
        boolean_field_5 = list(np.sort(self.date_df['is_leap_year'].unique()))
        self.assertTrue(boolean_field_5[0]==self.boolean_expected_values[0], boolean_field_5[1]==self.boolean_expected_values[1])

    def test_field_logic_1(self):
        field = list(np.sort(self.date_df['Day'].unique()))
        self.assertTrue(max(field) == 31, min(field) == 1) 

    def test_field_logic_2(self):
        field = list(np.sort(self.date_df['Month'].unique()))
        self.assertTrue(max(field) == 12, min(field) == 1)  

    def test_field_logic_3(self):
        field = list(np.sort(self.date_df['Quarter'].unique()))
        self.assertTrue(max(field) == 4, min(field) == 1)

    def test_field_logic_4a(self):
        field = list(np.sort(self.date_df[self.date_df['is_leap_year'] == 'f']['dayofyear'].unique()))
        self.assertTrue(max(field) == 365, min(field) == 1)

    def test_field_logic_4b(self):
        field = list(np.sort(self.date_df[self.date_df['is_leap_year'] == 't']['dayofyear'].unique()))
        self.assertTrue(max(field) == 366, min(field) == 1)  

    def test_field_logic_5(self):
        field = list(np.sort(self.date_df['days_in_month'].unique()))
        self.assertTrue(max(field) == 31, min(field) == 28)

    def test_field_logic_6(self):
        field = list(np.sort(self.date_df[self.date_df['is_month_start'] == 't']['Day'].unique()))
        self.assertTrue(max(field) == 1, min(field) == 1)  

    def test_field_logic_7a(self):
        day = list(np.sort(self.date_df[self.date_df['is_year_start'] == 't']['Day'].unique()))
        self.assertTrue(max(day) == 1, min(day) == 1)

    def test_field_logic_7b(self):
        dayofyear = list(np.sort(self.date_df[self.date_df['is_year_start'] == 't']['dayofyear'].unique()))
        self.assertTrue(max(dayofyear) == 1, min(dayofyear) == 1)

    def test_field_logic_7c(self):
        Month = list(np.sort(self.date_df[self.date_df['is_year_start'] == 't']['Month'].unique()))
        self.assertTrue(max(Month) == 1, max(Month) == 1)
     

#########################################################
########### Testing Object: fact_gamelogs ###############
#########################################################

# ACTION - MOVE THIS CODE TO OTHER FILE, IMPORT TO TEST EXECUTION FILE. 

class TestGameLogs(unittest.TestCase):
    # Read the cleaned dim date data to a pandas dataframe
    csv_reader = CSVReader(
        file_path =  prod_file_prefix+"fact_gamelogs.csv"
    )
    gamelogs_df = csv_reader.read_csv()
    print('Data from the fact_gamelogs object has been read to a DataFrame. Testing to begin.')

    def test_columns_match_schema(self):
        df_cols = list(self.gamelogs_df.columns.values)
        expected_column_set = ['date','opponent_team','opponent_league','team','league'
        ,'team_game_no','opponent_score','team_score','win_loss','is_home']
        self.assertEqual(df_cols,expected_column_set)

    def test_duplicate_check(self):
        expected_result_no_dupes = len(self.gamelogs_df)-len(self.gamelogs_df[['date','opponent_team','team_game_no']].drop_duplicates())
        self.assertTrue(expected_result_no_dupes == 0)

    def test_null_column_check(self):
        nulls_for_entire_table = self.gamelogs_df.isnull().sum().sum()
        self.assertTrue(nulls_for_entire_table == 0)

#########################################################
########### Testing Object: dim_team_statistics #########
#########################################################

class TestTeams(unittest.TestCase):
    # Read the cleaned dim date data to a pandas dataframe
    csv_reader = CSVReader(
        file_path = prod_file_prefix+"dim_team_statistics.csv"
    )
    teams_df = csv_reader.read_csv()
    print('Data from the dim_team_statistics object has been read to a DataFrame. Testing to begin.')

    def test_columns_match_schema(self):
        df_cols = list(self.teams_df.columns.values)
        expected_column_set = ['team','wins','losses','team_id','league','ab','h','dbl','trpl','hr','rbi','sh','sf','hbp','bb','bb_int','so','sb','cs','gidp','ci','lob','ptchrs','er','er_team','wp','balk','po','asst','err','pb','dbl_def','trpl_def']
        self.assertEqual(df_cols,expected_column_set)

    def test_duplicate_check(self):
        expected_result_no_dupes = len(self.teams_df)-len(self.teams_df['team_id'].drop_duplicates())
        self.assertTrue(expected_result_no_dupes == 0)

    def test_null_column_check(self):
        nulls_for_entire_table = self.teams_df.isnull().sum().sum()
        self.assertTrue(nulls_for_entire_table == 0)

    def test_field_logic_dim_team_statistics_1(self):
        field = list(np.sort(self.teams_df['team_id'].unique()))
        self.assertTrue(max(field) == 30, min(field) == 1)

#########################################################
########### Testing Object: fact_game_events ############
#########################################################

class TestFactEvents(unittest.TestCase):
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
    print('Data from the fact_game_events object has been read to a DataFrame. Testing to begin.')

    def test_columns_match_schema(self):
        df_cols = list(self.fact_game_events_df.columns.values)
        expected_column_set = ['index','event_type','inning_no','home_team','player_id','count_on_batter','pitches_to_batter','event_describer','team','date_id','PA','AB','H','Double','Triple','HR','RBI','BB','BB_int','HBP','SH','SF','SB','CS','GIDP','SO']
        self.assertEqual(df_cols,expected_column_set)

    def test_duplicate_check(self):
        expected_result_no_dupes = len(self.fact_game_events_df)-len(self.fact_game_events_df[['index','inning_no','player_id','event_describer','date_id']].drop_duplicates())
        self.assertTrue(expected_result_no_dupes == 0)

    def test_null_column_check(self):
        # 1 Known column that can take null values. Events where no pitches are thrown to batter.
        nulls_for_entire_table = self.fact_game_events_df.drop(['pitches_to_batter'], axis=1).isnull().sum().sum()
        self.assertTrue(nulls_for_entire_table == 0)
 
    def test_fact_game_events_field_logic_1(self):
        # Testing the fact game events object against the dim team statistics object
        # These objects must have matching values when summarized by Team.
        fact_game_events_field_sum = self.fact_game_events_df['HR'].sum()
        dim_team_stats_field_sum = self.dim_team_stats_df['hr'].sum()
        self.assertTrue(fact_game_events_field_sum == dim_team_stats_field_sum)

#########################################################
########### Testing Object: dim_team_rosters ############
#########################################################

class TestRosters(unittest.TestCase):
    # Read the cleaned dim team statistics data to a pandas dataframe
    csv_reader = CSVReader(
        file_path = prod_file_prefix+"dim_team_rosters.csv"
    )
    dim_rosters_df = csv_reader.read_csv()
    print('Data from the dim_team_rosters object has been read to a DataFrame. Testing to begin.')

    def test_columns_match_schema(self):
        df_cols = list(self.dim_rosters_df.columns.values)
        expected_column_set = ['player_id','lastname','firstname','handedness_batting','handedness_throwing','team','position']
        self.assertEqual(df_cols,expected_column_set)

    def test_duplicate_check(self):
        expected_result_no_dupes = len(self.dim_rosters_df)-len(self.dim_rosters_df[['player_id','team']].drop_duplicates())
        self.assertTrue(expected_result_no_dupes == 0)

    def test_null_column_check(self):
        nulls_for_entire_table = self.dim_rosters_df.isnull().sum().sum()
        self.assertTrue(nulls_for_entire_table == 0)

if __name__ == '__main__':
    unittest.main()