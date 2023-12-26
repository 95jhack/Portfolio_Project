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
folder_path = str(pathlib.PureWindowsPath(os.path.abspath(os.path.dirname(__file__))).as_posix())
prod_file_prefix = folder_path+"/dags/development/data/production/"

import numpy as np 
import pandas as pd
import unittest

from development.functions.raw_data_extraction_functions import CSVReader

#########################################################
########### Testing Object: dim_date ####################
#########################################################

class TestDates(unittest.TestCase):
    # Read the cleaned dim date data to a pandas dataframe
    csv_reader = CSVReader(
        file_path =  prod_file_prefix+"dim_date.csv"
    )
    date_df = csv_reader.read_csv()
    boolean_expected_values = [False, True]

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
        field = list(np.sort(self.date_df[self.date_df['is_leap_year'] == False]['dayofyear'].unique()))
        self.assertTrue(max(field) == 365, min(field) == 1)

    def test_field_logic_4b(self):
        field = list(np.sort(self.date_df[self.date_df['is_leap_year'] == True]['dayofyear'].unique()))
        self.assertTrue(max(field) == 366, min(field) == 1)  

    def test_field_logic_5(self):
        field = list(np.sort(self.date_df['days_in_month'].unique()))
        self.assertTrue(max(field) == 31, min(field) == 28)

    def test_field_logic_6(self):
        field = list(np.sort(self.date_df[self.date_df['is_month_start'] == True]['Day'].unique()))
        self.assertTrue(max(field) == 1, min(field) == 1)  

    def test_field_logic_7(self):
        day = list(np.sort(self.date_df[self.date_df['is_year_start'] == True]['Day'].unique()))
        dayofyear = list(np.sort(self.date_df[self.date_df['is_year_start'] == True]['dayofyear'].unique()))
        Month = list(np.sort(self.date_df[self.date_df['is_year_start'] == True]['Month'].unique()))
        # is_month_start = list(np.sort(self.date_df[self.date_df['is_year_start'] == True]['is_month_start'].unique()))        
        self.assertTrue(max(day) == 1, min(day) == 1)
        self.assertTrue(max(dayofyear) == 1, min(dayofyear) == 1)
        self.assertTrue(max(Month) == 1, max(Month) == 1)
        # self.assertTrue(is_month_start[:] == True)       

    def test_field_logic_8(self):
        pass

    def test_field_logic_9(self):
        pass

    '''     
        * Accepted Values Tests [T/F] ---> All Boolean columns
        * Logic Based Testing:
            - Day between 1 and 31
            - Month between 1 and 12
            - Quarter between 1 and 4
            - dayofyear between 1 and 365, if is_leap_year = False, else Day between 1 and 366
            - days_in_month between 28 and 31
            - if is_month_start = TRUE, Day must equal 1
            - if is_year_start = TRUE, Day must equal 1, dayofyear must equal 1, Month = 1, is_month_start = TRUE
            - if is_year_end = True, Day must equal 31, Month = 12
            - if is_month_end = True, Day must be between 28 and 31.
    '''