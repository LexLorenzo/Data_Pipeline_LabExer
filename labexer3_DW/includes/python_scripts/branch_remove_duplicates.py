import pandas as pd
import numpy as np

def remove_duplicates_branch():
    # df_branch_service = pd.read_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects"
    #                                           "/labexer/parquet_files/branch_txn_unrealistic_dates_removed.parquet")
    df_branch_service = pd.read_parquet('/opt/airflow/includes/parquet_files/branch_txn_capitalizing_mall.parquet')
    print("Successfully loaded the file.")

    print('Before: \n', df_branch_service.shape)
    # Removing duplicates
    print("Removing duplicates")
    df_branch_service = df_branch_service.drop_duplicates()
    df_branch_service = df_branch_service.drop_duplicates(subset='txn_id', keep='first')
    print('After: \n', df_branch_service.shape)

    # Saving data
    # df_branch_service.to_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects/"
    #                                    "labexer/parquet_files/branch_txn_remove_duplicates.parquet")
    df_branch_service.to_parquet('/opt/airflow/includes/parquet_files/branch_txn_remove_duplicates.parquet')
    print("Successfully saved the data.")