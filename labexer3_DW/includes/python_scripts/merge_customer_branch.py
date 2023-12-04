import pandas as pd
import numpy as np

def merge_data():
    # df_customer_transaction = pd.read_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects"
    #                                         "/labexer/parquet_files/customer_txn_unrealistic_dates_removed.parquet")
    # df_branch_service = pd.read_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects"
    #                                         "/labexer/parquet_files/branch_txn_unrealistic_dates_removed.parquet")
    df_branch_service = pd.read_parquet('/opt/airflow/includes/parquet_files/branch_txn_rounding_decimal.parquet')
    df_customer_transaction = pd.read_parquet('/opt/airflow/includes/parquet_files/customer_txn_capitalized_names.parquet')
    print("Successfully loaded the file.")

    print('Before: \n', df_customer_transaction)
    # Loading the cleaned customer
    print("Rounding the decimals to 0")
    df_customer_transaction['avail_date'] = pd.to_datetime(df_customer_transaction['avail_date'])
    df_customer_transaction['birthday'] = pd.to_datetime(df_customer_transaction['birthday'])
    print('After: \n', df_customer_transaction)

    # Merging the customer and branch table
    df_merge = pd.merge(df_customer_transaction, df_branch_service)
    df_merge['first_name'] = df_merge['first_name'].str.capitalize()
    df_merge['last_name'] = df_merge['last_name'].str.capitalize()
    print('Merged Table: \n', df_merge)

    # Saving data
    # df_merge.to_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects/"
    #                                 "labexer/parquet_files/merge_txn_customer_branch.parquet")
    df_merge.to_parquet('/opt/airflow/includes/parquet_files/merged_file.parquet')
    print("Successfully saved the data.")
