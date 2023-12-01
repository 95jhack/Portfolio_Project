import pandas as pd

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
            