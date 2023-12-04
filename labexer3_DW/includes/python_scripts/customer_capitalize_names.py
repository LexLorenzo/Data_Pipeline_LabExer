import pandas as pd
import numpy as np


def capitalize_names():
    # df_customer_transaction = pd.read_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects"
    #                                           "/vs-airflow-demo-main/airflow-data/includes/parquet_files/customer_txn_unnecessary_chars_removed.parquet")
    df_customer_transaction = pd.read_parquet('/opt/airflow/includes/parquet_files/customer_txn_unnecessary_chars_removed.parquet')
    print("Successfully loaded the file.")

    print('Before', df_customer_transaction)
    # Capitalize the first name and last name
    print("Capitalize the first name and last name")
    df_customer_transaction['first_name'] = df_customer_transaction['first_name'].str.capitalize()
    df_customer_transaction['last_name'] = df_customer_transaction['last_name'].str.capitalize()
    print('After', df_customer_transaction)

    # Saving data
    # df_customer_transaction.to_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects/vs-airflow-demo-main/airflow-data/includes"
    #                                    "/parquet_files/customer_txn_capitalized_names.parquet")
    df_customer_transaction.to_parquet('/opt/airflow/includes/parquet_files/customer_txn_capitalized_names.parquet')
    print("Successfully saved the data.")


if __name__ == '__main__':
    capitalize_names()
