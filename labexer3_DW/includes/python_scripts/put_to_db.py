import pandas as pd
import sqlite3

connection = sqlite3.connect('/opt/airflow/airflow-db/labexer3_sqlite.db')
cursor = connection.cursor()

cursor.execute(''' CREATE TABLE IF NOT EXISTS Merged_table (
txn_id TEXT NOT NULL PRIMARY KEY, 
avail_date DATE NOT NULL,
last_name TEXT NOT NULL,
first_name TEXT NOT NULL,
birthday DATE NOT NULL,
branch_name TEXT NOT NULL,
service TEXT NOT NULL,
price FLOAT NOT NULL
)''')

def insert_to_db():
    df_merge = pd.read_parquet('/opt/airflow/includes/parquet_files/merged_file.parquet')
    selected_columns = ['txn_id', 'avail_date', 'last_name', 'first_name', 'birthday', 'branch_name', 'service', 'price']

    try:
        connection.execute('BEGIN TRANSACTION')
        df_merge[selected_columns].to_sql('Merged_table', con=connection, if_exists='replace', index=False)
    except Exception as e:
        print('An error has occurred. Proceeding to rollback.')
        connection.rollback()
    else:
        print('Committed successfully')
        connection.commit()

    data = cursor.execute("SELECT * FROM Merged_table")
    print(data.fetchone())

    cursor.close()
    connection.close()
