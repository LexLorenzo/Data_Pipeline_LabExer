import pandas as pd
import numpy as np

def remove_empty():
    # df_branch_service = pd.read_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects"
    #                                         "/labexer/parquet_files/branch_txn_unrealistic_dates_removed.parquet")
    df_branch_service = pd.read_json('/opt/airflow/branch_service_transaction_info.json')
    print("Successfully loaded the file.")

    print('Before: \n', df_branch_service.shape)
    # Removing N/A, empty spaces, and null spaces
    print("Removing N/A, empty spaces, and null spaces")
    df_branch_service = df_branch_service.dropna()
    df_branch_service = df_branch_service[df_branch_service['branch_name'] != "N/A"] 
    df_branch_service = df_branch_service[df_branch_service['branch_name'] != ""] 
    print('After: \n', df_branch_service.shape)

    # Saving data
    # df_branch_service.to_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects/"
    #                                 "labexer/parquet_files/branch_txn_remove_empty.parquet")
    df_branch_service.to_parquet('/opt/airflow/includes/parquet_files/branch_txn_remove_empty.parquet')
    print("Successfully saved the data.")
