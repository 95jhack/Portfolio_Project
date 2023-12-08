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

class TestDates(unittest.TestCase):

    def test_for_acceptable_values(self):
        # Read the cleaned dim date data to a pandas dataframe
        csv_reader = CSVReader(
            file_path =  prod_file_prefix+"dim_date.csv"
        )
        date_df = csv_reader.read_csv()
        # df_cols = date_df['is_month_start','is_month_end','is_year_start','is_year_end','is_leap_year']
        print(date_df.head())
       # self.assertEqual(df_cols,expected_column_set)

if __name__ == '__main__':
    unittest.main()