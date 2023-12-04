import pandas as pd
import numpy as np

def capitalize_malls():
    # df_branch_service = pd.read_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects"
    #                                         "/labexer/parquet_files/branch_txn_unrealistic_dates_removed.parquet")
    df_branch_service = pd.read_parquet('/opt/airflow/includes/parquet_files/branch_txn_remove_empty.parquet')
    print("Successfully loaded the file.")

    print('Before: \n', df_branch_service)
    # Capitalizing the mall
    print("Capitalizing the mall")
    df_branch_service.loc[df_branch_service['branch_name'] == 'Megamall' ,'branch_name'] = "MegaMall"
    df_branch_service.loc[df_branch_service['branch_name'] == 'Starmall' ,'branch_name'] = "StarMall"
    print('After: \n', df_branch_service)

    # Saving data
    # df_branch_service.to_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects/"
    #                                 "labexer/parquet_files/branch_txn_capitalizing_mall.parquet")
    df_branch_service.to_parquet('/opt/airflow/includes/parquet_files/branch_txn_capitalizing_mall.parquet')
    print("Successfully saved the data.")