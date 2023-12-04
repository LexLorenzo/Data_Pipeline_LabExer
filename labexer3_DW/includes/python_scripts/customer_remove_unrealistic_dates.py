import pandas as pd
import numpy as np
from datetime import datetime


def unrealistic_dates():
    # df_customer_transaction = pd.read_json("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects/"
                                        #    "vs-airflow-demo-main/airflow-data/includes/customer_transaction_info.json")
    df_customer_transaction = pd.read_json('/opt/airflow/customer_transaction_info.json')
    print("Successfully loaded the file.")

    print('Before:')
    print(df_customer_transaction)
    # Convert 'birthday' and 'avail_date' columns to datetime
    df_customer_transaction['birthday'] = pd.to_datetime(df_customer_transaction['birthday'])
    df_customer_transaction['avail_date'] = pd.to_datetime(df_customer_transaction['avail_date'])

    # Filter customers based on age and dates not in the future
    now = datetime.now()
    df_customer_transaction = df_customer_transaction[
        (df_customer_transaction['birthday'] < now) &
        (df_customer_transaction['avail_date'] < now) &
        ((df_customer_transaction['avail_date'] - df_customer_transaction['birthday']).dt.days // 365.25 >= 10)
    ]
    print('\nAfter:\n', df_customer_transaction)

    # Saving data
    # df_customer_transaction.to_parquet("C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects/vs-airflow-demo-main/airflow-data/includes"
    #                                    "/parquet_files/customer_txn_unrealistic_dates_removed.parquet")
    df_customer_transaction.to_parquet('/opt/airflow/includes/parquet_files/customer_txn_unrealistic_dates_removed.parquet')
    print("Successfully saved the data.")


if __name__ == '__main__':
    unrealistic_dates()
