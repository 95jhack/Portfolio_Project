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
prod_file_prefix = folder_path+"/development/data/production/"

import numpy as np 
import pandas as pd
import unittest
# from development.functions.raw_data_column_organization import gamelogs_df_column_names
from development.tests import testing_gamelogs as tg
from development.tests import testing_teams as tt
from development.functions.raw_data_extraction_functions import CSVReader

class TestDates(unittest.TestCase):

    def test_all_field_logic(self):
        # Read the cleaned dim date data to a pandas dataframe
        csv_reader = CSVReader(
            file_path =  prod_file_prefix+"dim_date.csv"
        )
        date_df = csv_reader.read_csv()

        def test_field_logic(self):
            field = list(np.sort(date_df['Day'].unique()))
            self.assertTrue(max(field) == 31, min(field) == 1) 

        def test_field_logic(self):
            field = list(np.sort(date_df['Month'].unique()))
            self.assertTrue(max(field) == 12, min(field) == 1) 

if __name__ == '__main__':
    unittest.main()