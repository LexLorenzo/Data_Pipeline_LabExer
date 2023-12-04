import pandas as pd
import numpy as np

def remove_price_less_than_0():
    # df_branch_service = pd.read_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects"
    #                                           "/labexer/parquet_files/branch_txn_unrealistic_dates_removed.parquet")
    df_branch_service = pd.read_parquet('/opt/airflow/includes/parquet_files/branch_txn_remove_duplicates.parquet')
    print("Successfully loaded the file.")

    print('Before: \n', df_branch_service)
    # Removing prices that is less than/equal to 0
    print("Removing prices that is less than/equal to 0")
    df_branch_service = df_branch_service[df_branch_service['price'] > 0]
    print('After: \n', df_branch_service)

    # Saving data
    # df_branch_service.to_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects/"
    #                                    "labexer/parquet_files/branch_txn_remove_price_less_than_0.parquet")
    df_branch_service.to_parquet('/opt/airflow/includes/parquet_files/branch_txn_remove_price_less_than_0.parquet')
    print("Successfully saved the data.")