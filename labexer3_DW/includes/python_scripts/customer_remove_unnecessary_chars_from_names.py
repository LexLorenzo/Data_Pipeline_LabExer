import pandas as pd
import numpy as np


def remove_unnecessary():
    # df_customer_transaction = pd.read_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects"
    #                                           "/vs-airflow-demo-main/airflow-data/includes/parquet_files/customer_txn_duplicates_nulls_removed.parquet")
    df_customer_transaction = pd.read_parquet('/opt/airflow/includes/parquet_files/customer_txn_duplicates_nulls_removed.parquet')
    print("Successfully loaded the file.")

    print('Before:\n', df_customer_transaction)
    # Remove unnecessary spaces, commas, and periods
    print("Remove unnecessary spaces, commas, and periods")
    df_customer_transaction['first_name'] = df_customer_transaction['first_name'].str.replace(r'\s+', '', regex=True)
    df_customer_transaction['first_name'] = df_customer_transaction['first_name'].str.replace(r'\.{2,}', '', regex=True)
    df_customer_transaction['first_name'] = df_customer_transaction['first_name'].str.replace(r',+', '', regex=True)
    df_customer_transaction['last_name'] = df_customer_transaction['last_name'].str.replace(r'\s+', '', regex=True)
    df_customer_transaction['last_name'] = df_customer_transaction['last_name'].str.replace(r'\.{2,}', '', regex=True)
    df_customer_transaction['last_name'] = df_customer_transaction['last_name'].str.replace(r',+', '', regex=True)
    print('After:\n', df_customer_transaction)

    # Saving data
    # df_customer_transaction.to_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects/vs-airflow-demo-main/airflow-data/includes"
    #                                    "/parquet_files/customer_txn_unnecessary_chars_removed.parquet")
    df_customer_transaction.to_parquet('/opt/airflow/includes/parquet_files/customer_txn_unnecessary_chars_removed.parquet')
    print("Successfully saved the data.")


if __name__ == '__main__':
    remove_unnecessary()
