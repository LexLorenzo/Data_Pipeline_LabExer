import pandas as pd
import numpy as np

def rounding_decimals():
    # df_branch_service = pd.read_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects"
    #                                         "/labexer/parquet_files/branch_txn_unrealistic_dates_removed.parquet")
    df_branch_service = pd.read_parquet('/opt/airflow/includes/parquet_files/branch_txn_remove_price_less_than_0.parquet')
    print("Successfully loaded the file.")

    print('Before: \n', df_branch_service)
    # Rounding the decimals to 0
    print("Rounding the decimals to 0")
    df_branch_service['price'] = df_branch_service['price'].round(2)
    print('After: \n', df_branch_service)

    # Saving data
    # df_branch_service.to_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects/"
    #                                 "labexer/parquet_files/branch_txn_rounding_decimal.parquet")
    df_branch_service.to_parquet('/opt/airflow/includes/parquet_files/branch_txn_rounding_decimal.parquet')
    print("Successfully saved the data.")