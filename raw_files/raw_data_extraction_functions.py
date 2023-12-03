"""
Purpose:
This file is used to house a number of objects that are used to take the raw csv file
used in the original analysis of this project, and ETL pertaining data into facts
and dimensions that are more usable for this project.

There are 4 objects currently within this object:

1. CSV Reader - taking in a CSV, output a pandas dataframe.

2. DataframeGlueOps - take in 2 dataframes that are similarly structured other than column names. 
The columns are renamed, and the Dataframes are unioned using the Pandas concat method.

3. CSVWriter - taking in a pandas dataframe, write out to CSV within the selected directory.

4. DateDimDFGenerator - Take in a start and end date for your project. Create the pertaining 
date dimension as a pandas dataframe. Write this out as a CSV file.
"""

############ REMAINING ACTIONS:
# Add further exception handling. 
# Add further testing
# Add further commenting

###############
### Imports ###
###############

import pandas as pd
from datetime import timedelta, date, datetime


class CSVReader:

    def __init__(self, file_path, column_names=None):
        self.file_path = file_path
        self.column_names = column_names
        self.data = None

    def read_csv(self):
        try:
            if self.column_names is not None:
                self.data = pd.read_csv(self.file_path, names=self.column_names)
            else:
                self.data = pd.read_csv(self.file_path)

            rows = len(self.data)
            cols = len(self.data.columns)
            print(f"CSV file '{self.file_path}' successfully loaded.")
            print(f"{rows} rows were imported from the raw file.")
            print(f"{cols} columns were imported from the raw file.")   
            return self.data
        except FileNotFoundError:
            print(f"Error: File '{self.file_path}' not found.")
        except pd.errors.EmptyDataError:
            print(f"Error: File '{self.file_path}' is empty.")
        except pd.errors.ParserError:
            print(f"Error: Unable to parse the CSV file '{self.file_path}'.")
        return None

    def display_data(self):
        if self.data is not None:
            print(self.data.head())
        else:
            print("Error: No data loaded. Use read_csv method first.")

    def get_column_names(self):
        if self.data is not None:
            return list(self.data.columns)
        else:
            print("Error: No data loaded. Use read_csv method first.")
            return []

class DataframeGlueOps:
    def __init__(self, df1, df2, new_column_names=None):
        self.df1 = df1
        self.df2 = df2
        self.new_column_names = None

    def merge_and_rename(self, df1, df2, new_column_names):
        """
        Update the naming of both individual dataframes. Merge the two DataFrames.

        Parameters:
        - df1, df2: pandas DataFrames
        - new_column_names: list of str, shared column names for the merged DataFrame

        Returns:
        - merged_df: pandas DataFrame, the merged DataFrame with new column names
        """
        # Drop original column names
        self.df1.columns=new_column_names
        self.df2.columns=new_column_names

        # Append DataFrames
        merged_df = pd.concat([self.df1, self.df2])

        return merged_df

class CSVWriter:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def write_to_csv(self, file_name):
        try:
            if not file_name.endswith('.csv'):
                file_name += '.csv'
                
            self.dataframe.to_csv(file_name, index=False)
            print(f"Data written to {file_name} successfully.")
        except Exception as e:
            print(f"Error: {e}")

class DateDimDFGenerator:
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    # Function to generate date range
    def daterange(self):
        for n in range(int((self.end_date - self.start_date).days) + 1):
            yield self.start_date + timedelta(n)

    def dim_date_dataframe(self):
        # Generate date dimension
        date_dimension = pd.DataFrame(self.daterange(), columns=['Date'])
        
        # Extract additional date-related attributes
        date_dimension['date_id'] = date_dimension['Date'].dt.strftime('%Y%m%d')
        date_dimension['Date'].dt.year
        date_dimension['Year'] = date_dimension['Date'].dt.year
        date_dimension['Month'] = date_dimension['Date'].dt.month
        date_dimension['Day'] = date_dimension['Date'].dt.day
        date_dimension['Month_Name'] = date_dimension['Date'].dt.strftime('%B')
        date_dimension['Weekday'] = date_dimension['Date'].dt.dayofweek
        date_dimension['dayofyear'] = date_dimension['Date'].dt.dayofyear
        date_dimension['Quarter'] = date_dimension['Date'].dt.quarter
        date_dimension['days_in_month'] = date_dimension['Date'].dt.days_in_month
        date_dimension['is_month_start'] = date_dimension['Date'].dt.is_month_start
        date_dimension['is_month_end'] = date_dimension['Date'].dt.is_month_end
        date_dimension['is_year_start'] = date_dimension['Date'].dt.is_year_start
        date_dimension['is_year_end'] = date_dimension['Date'].dt.is_year_end
        date_dimension['is_leap_year'] = date_dimension['Date'].dt.is_leap_year
        return date_dimension