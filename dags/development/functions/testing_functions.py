import pandas as pd

def column_select_var_type_testing(df_to_test,expected_df_dict):
    # Create the produced df from the underlying ETL processes. 
    # This df will use the series focused on the column names and dtypes, 
    # this will then have its index reset so the column names are moved from the index to a column within the dataframe.
    # The columns are then renamed accordingly.
    produced_df = pd.DataFrame(df_to_test.dtypes).reset_index().rename(
        columns={"index": "column_name",0: "data_type"}
    )
    # Using the expected df structure, create the resulting dataframe. 
    expected_df = pd.DataFrame.from_dict(expected_df_dict)
    variable_type_check_result = expected_df.equals(produced_df)

    return variable_type_check_result

def referential_integrity_testing(df_to_test,df_to_compare_against,join_field):
    # Original unique row count of date_id's in the produced DF
    produced_df_length = df_to_test.shape[0]
    # Number of matching records between the 2 dataframes. This is determined based on the use of an inner join.
    matching_records_df_length = pd.merge(df_to_test, df_to_compare_against, on=[join_field], how='inner').shape[0]
    if produced_df_length == matching_records_df_length:
        return 'aligned'
    else:
        return 'unaligned'