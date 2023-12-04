import pandas as pd
import numpy as np


def remove_duplicates_nulls():
    # df_customer_transaction = pd.read_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects"
    #                                           "/vs-airflow-demo-main/airflow-data/includes/parquet_files/customer_txn_unrealistic_dates_removed.parquet")
    df_customer_transaction = pd.read_parquet('/opt/airflow/includes/parquet_files/customer_txn_unrealistic_dates_removed.parquet')
    print("Successfully loaded the file.")

    print('Before: ', df_customer_transaction.shape)
    # Removing duplicates and null rows
    print("Removing duplicates and null rows")
    df_customer_transaction = df_customer_transaction.dropna()
    df_customer_transaction = df_customer_transaction.drop_duplicates()
    df_customer_transaction = df_customer_transaction.drop_duplicates(subset='txn_id', keep='first')
    print('After: ', df_customer_transaction.shape)

    # Saving data
    # df_customer_transaction.to_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects/"
    #                                    "vs-airflow-demo-main/airflow-data/includes/parquet_files/customer_txn_duplicates_nulls_removed.parquet")
    df_customer_transaction.to_parquet('/opt/airflow/includes/parquet_files/customer_txn_duplicates_nulls_removed.parquet')
    print("Successfully saved the data.")


if __name__ == '__main__':
    remove_duplicates_nulls()
