# Directory, Transformations, and Other Imports
import os
import pathlib
import numpy as np 
import pandas as pd
import unittest

# ETL & formatting Imports
from dags.development.functions.raw_data_extraction_functions import CSVReader
from dags.development.functions.testing_data_organization import expected_df_dates, expected_df_gamelogs, expected_df_statistics, expected_df_events, expected_df_rosters
from dags.development.functions.testing_functions import column_select_var_type_testing, referential_integrity_testing

# Directory Structuring
folder_path = str(pathlib.PureWindowsPath(os.path.abspath(os.path.dirname(__file__))).as_posix())
prod_file_prefix = folder_path+"/dags/development/data/production/"

#########################################################
########### Testing Object: dim_date ####################
#########################################################

class TestDates(unittest.TestCase):
    # Read the cleaned dim date data to a pandas dataframe
    csv_reader = CSVReader(
        file_path =  prod_file_prefix+"dim_date.csv"
    )
    date_df = csv_reader.read_csv()
    print('Data from the dim_date object has been read to a DataFrame. Testing to begin.')

    boolean_expected_values = ['f','t']

    def test_table_column_select_and_var_types(self):
        column_var_type_alignment = column_select_var_type_testing(
            df_to_test = self.date_df
            , expected_df_dict = expected_df_dates
        )
        self.assertTrue(column_var_type_alignment == True)

    def test_duplicate_check(self):
        expected_result_no_dupes = len(self.date_df)-len(self.date_df['date_id'].drop_duplicates())
        self.assertTrue(expected_result_no_dupes == 0)

    def test_null_column_check(self):
        nulls_for_entire_table = self.date_df.isnull().sum().sum()
        self.assertTrue(nulls_for_entire_table == 0)

    def test_for_acceptable_values_1(self):
        boolean_field_1 = list(np.sort(self.date_df['is_month_start'].unique()))
        self.assertTrue(
            boolean_field_1[0]==self.boolean_expected_values[0]
            , boolean_field_1[1]==self.boolean_expected_values[1]
        )

    def test_for_acceptable_values_2(self):
        boolean_field_2 = list(np.sort(self.date_df['is_month_end'].unique()))
        self.assertTrue(
            boolean_field_2[0]==self.boolean_expected_values[0]
            , boolean_field_2[1]==self.boolean_expected_values[1]
        )

    def test_for_acceptable_values_3(self):
        boolean_field_3 = list(np.sort(self.date_df['is_year_start'].unique()))
        self.assertTrue(
            boolean_field_3[0]==self.boolean_expected_values[0]
            , boolean_field_3[1]==self.boolean_expected_values[1]
        )

    def test_for_acceptable_values_4(self):
        boolean_field_4 = list(np.sort(self.date_df['is_year_end'].unique()))
        self.assertTrue(
            boolean_field_4[0]==self.boolean_expected_values[0]
            , boolean_field_4[1]==self.boolean_expected_values[1]
        )

    def test_for_acceptable_values_5(self):
        boolean_field_5 = list(np.sort(self.date_df['is_leap_year'].unique()))
        self.assertTrue(
            boolean_field_5[0]==self.boolean_expected_values[0]
            , boolean_field_5[1]==self.boolean_expected_values[1]
        )

    def test_field_logic_1(self):
        field = list(np.sort(self.date_df['Day'].unique()))
        self.assertTrue(
            max(field) == 31
            , min(field) == 1
        ) 

    def test_field_logic_2(self):
        field = list(np.sort(self.date_df['Month'].unique()))
        self.assertTrue(
            max(field) == 12
            , min(field) == 1
        )  

    def test_field_logic_3(self):
        field = list(np.sort(self.date_df['Quarter'].unique()))
        self.assertTrue(
            max(field) == 4
            , min(field) == 1
        )

    def test_field_logic_4a(self):
        field = list(np.sort(self.date_df[self.date_df['is_leap_year'] == 'f']['dayofyear'].unique()))
        self.assertTrue(
            max(field) == 365
            , min(field) == 1
        )

    def test_field_logic_4b(self):
        field = list(np.sort(self.date_df[self.date_df['is_leap_year'] == 't']['dayofyear'].unique()))
        self.assertTrue(
            max(field) == 366
            , min(field) == 1
        )  

    def test_field_logic_5(self):
        field = list(np.sort(self.date_df['days_in_month'].unique()))
        self.assertTrue(
            max(field) == 31
            , min(field) == 28
        )

    def test_field_logic_6(self):
        field = list(np.sort(self.date_df[self.date_df['is_month_start'] == 't']['Day'].unique()))
        self.assertTrue(
            max(field) == 1
            , min(field) == 1
        )  

    def test_field_logic_7a(self):
        day = list(np.sort(self.date_df[self.date_df['is_year_start'] == 't']['Day'].unique()))
        self.assertTrue(
            max(day) == 1
            , min(day) == 1
        )

    def test_field_logic_7b(self):
        dayofyear = list(np.sort(self.date_df[self.date_df['is_year_start'] == 't']['dayofyear'].unique()))
        self.assertTrue(
            max(dayofyear) == 1
            , min(dayofyear) == 1
        )

    def test_field_logic_7c(self):
        Month = list(np.sort(self.date_df[self.date_df['is_year_start'] == 't']['Month'].unique()))
        self.assertTrue(
            max(Month) == 1
            , max(Month) == 1
        )  

#########################################################
########### Testing Object: fact_gamelogs ###############
#########################################################

class TestGameLogs(unittest.TestCase):
    # Read the cleaned fact gamelogs data to a pandas dataframe
    csv_reader = CSVReader(
        file_path =  prod_file_prefix+"fact_gamelogs.csv"
    )
    gamelogs_df = csv_reader.read_csv()
    print('Data from the fact_gamelogs object has been read to a DataFrame. Testing to begin.')
    
    # Read the cleaned dim date data to a pandas dataframe
    csv_reader1 = CSVReader(
        file_path =  prod_file_prefix+"dim_date.csv"
    )
    date_df = csv_reader1.read_csv()
    
    # Read the cleaned dim team statistics data to a pandas dataframe
    csv_reader2 = CSVReader(
        file_path =  prod_file_prefix+"dim_team_statistics.csv"
    )
    teams_df = csv_reader2.read_csv()

    def test_table_column_select_and_var_types(self):
        column_var_type_alignment = column_select_var_type_testing(
            df_to_test = self.gamelogs_df
            , expected_df_dict = expected_df_gamelogs
        )
        self.assertTrue(column_var_type_alignment == True)

    # referential integrity test on dates within the fact gamelogs object
    def test_referential_integrity_dates(self):
        gamelogs_df_dates = pd.DataFrame(self.gamelogs_df['date']).sort_values(by='date', ascending=True).drop_duplicates().reset_index(drop=True).rename(columns={"date": "date_id"})
        date_df_compare = pd.DataFrame(self.date_df['date_id'])
        test_result = referential_integrity_testing(
            df_to_test = gamelogs_df_dates
            , df_to_compare_against = date_df_compare
            , join_field = 'date_id'
        )
        self.assertTrue(test_result == 'aligned')

    # referential integrity test on teams within the fact gamelogs object
    def test_referential_integrity_teams(self):
        gamelogs_df_teams = pd.DataFrame(self.gamelogs_df['team']).sort_values(by='team', ascending=True).drop_duplicates().reset_index(drop=True)
        teams_df_compare = pd.DataFrame(self.teams_df['team'])
        test_result = referential_integrity_testing(
            df_to_test = gamelogs_df_teams
            , df_to_compare_against = teams_df_compare
            , join_field = 'team'
        )
        self.assertTrue(test_result == 'aligned')

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

    def test_table_column_select_and_var_types(self):
        column_var_type_alignment = column_select_var_type_testing(
            df_to_test = self.teams_df
            , expected_df_dict = expected_df_statistics
        )
        self.assertTrue(column_var_type_alignment == True)

    def test_duplicate_check(self):
        expected_result_no_dupes = len(self.teams_df)-len(self.teams_df['team_id'].drop_duplicates())
        self.assertTrue(expected_result_no_dupes == 0)

    def test_null_column_check(self):
        nulls_for_entire_table = self.teams_df.isnull().sum().sum()
        self.assertTrue(nulls_for_entire_table == 0)

    def test_field_logic_dim_team_statistics_1(self):
        field = list(np.sort(self.teams_df['team_id'].unique()))
        self.assertTrue(
            max(field) == 30
            , min(field) == 1
        )

#########################################################
########### Testing Object: fact_game_events ############
#########################################################

class TestFactEvents(unittest.TestCase):
    
    # Read the cleaned dim team statistics data to a pandas dataframe
    csv_reader0 = CSVReader(
        file_path = prod_file_prefix+"dim_date.csv"
    )
    date_df = csv_reader0.read_csv()    
    
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

    # Read the cleaned dim team rosters data to a pandas dataframe
    csv_reader3 = CSVReader(
        file_path = prod_file_prefix+"dim_team_rosters.csv"
    )
    dim_rosters_df = csv_reader3.read_csv()

    def test_table_column_select_and_var_types(self):
        column_var_type_alignment = column_select_var_type_testing(
            df_to_test = self.fact_game_events_df
            , expected_df_dict = expected_df_events
        )
        self.assertTrue(column_var_type_alignment == True)

    # referential integrity test on dates within the fact gamelogs object
    def test_referential_integrity_dates(self):
        game_events_df_dates = pd.DataFrame(self.fact_game_events_df['date_id']).sort_values(by='date_id', ascending=True).drop_duplicates().reset_index(drop=True)
        date_df_compare = pd.DataFrame(self.date_df['date_id'])
        test_result = referential_integrity_testing(
            df_to_test = game_events_df_dates
            , df_to_compare_against = date_df_compare
            , join_field = 'date_id'
        )
        self.assertTrue(test_result == 'aligned')

    # referential integrity test on teams within the fact gamelogs object
    def test_referential_integrity_teams(self):
        game_events_df_teams = pd.DataFrame(self.fact_game_events_df['team']).sort_values(by='team', ascending=True).drop_duplicates().reset_index(drop=True)
        teams_df_compare = pd.DataFrame(self.dim_team_stats_df['team'])
        test_result = referential_integrity_testing(
            df_to_test = game_events_df_teams
            , df_to_compare_against = teams_df_compare
            , join_field = 'team'
        )
        self.assertTrue(test_result == 'aligned')

    # referential integrity test on players within the fact gamelogs object
    def test_referential_integrity_players(self):
        game_events_df_players = pd.DataFrame(self.fact_game_events_df['player_id']).sort_values(by='player_id', ascending=True).drop_duplicates().reset_index(drop=True)
        players_df_compare = pd.DataFrame(self.dim_rosters_df['player_id']).sort_values(by='player_id', ascending=True).drop_duplicates()
        test_result = referential_integrity_testing(
            df_to_test = game_events_df_players
            , df_to_compare_against = players_df_compare
            , join_field = 'player_id'
        )
        self.assertTrue(test_result == 'aligned')

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
    # Read the cleaned dim team rosters data to a pandas dataframe
    csv_reader = CSVReader(
        file_path = prod_file_prefix+"dim_team_rosters.csv"
    )
    dim_rosters_df = csv_reader.read_csv()
    print('Data from the dim_team_rosters object has been read to a DataFrame. Testing to begin.')

    def test_table_column_select_and_var_types(self):
        column_var_type_alignment = column_select_var_type_testing(
            df_to_test = self.dim_rosters_df
            , expected_df_dict = expected_df_rosters
        )
        self.assertTrue(column_var_type_alignment == True)

    def test_duplicate_check(self):
        expected_result_no_dupes = len(self.dim_rosters_df)-len(self.dim_rosters_df[['player_id','team']].drop_duplicates())
        self.assertTrue(expected_result_no_dupes == 0)

    def test_null_column_check(self):
        nulls_for_entire_table = self.dim_rosters_df.isnull().sum().sum()
        self.assertTrue(nulls_for_entire_table == 0)

if __name__ == '__main__':
    unittest.main()