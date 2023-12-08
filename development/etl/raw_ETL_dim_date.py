"""
Purpose:
This file is used to populate a generic date dimension that can be used 
for any project that requires date / time series analysis.
"""

###############
### Imports ###
###############

import pandas as pd
from datetime import timedelta, date, datetime
from development.functions.raw_data_extraction_functions import DateDimDFGenerator, CSVWriter

#########################################################
########### Object: dim_date ############################
#########################################################

# Enter pertaining dates for the Date Dimension
start_date = datetime(2020, 1, 1)
end_date = datetime(2025, 12, 31)

# Generate the date series based on the inputted start & end dates
generator = DateDimDFGenerator(start_date, end_date)

# Use this date series to populate a sample pandas dataframe for upload.
date_dimension_df = generator.dim_date_dataframe()

def run_etl(df, file_path):
    # Write this dataframe to a CSV file in the listed location.
    df_to_csv_dim_date = CSVWriter(df)
    df_to_csv_dim_date.write_to_csv(file_path)
