import pandas as pd 

class CsvImport():

    def __init__(self, name):
        self.name = name
        #self.header = header

    def Csv_to_df(self):
        df = pd.read_csv(self.name) #,self.header)
        rows = len(df)
        cols = len(df.columns)
        print(f"Data from {self.name} has been read from a Raw CSV file to a Pandas Dataframe.")
        print(f"{rows} rows were imported from the raw file.")
        print(f"{cols} columns were imported from the raw file.")        
        return df
