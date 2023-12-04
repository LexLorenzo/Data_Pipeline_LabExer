import pandas as pd
import sqlite3

connection = sqlite3.connect('/opt/airflow/airflow-db/labexer3_sqlite.db')
cursor = connection.cursor()

def create_a_view():
    create_view_query = "CREATE VIEW IF NOT EXISTS Weekly_View AS Select branch_name, strftime('%Y-%W', avail_date) As \"Week\", service, SUM(price) AS \"Total Price\" FROM Merged_table GROUP BY branch_name, service, Week ORDER BY week,branch_name;"
    connection.execute(create_view_query)
    connection.commit()

    query = "SELECT * FROM Weekly_View"
    df = pd.read_sql_query(query, connection)
    print(df)

    cursor.close()
    connection.close()


if __name__ == '__main__':
    create_a_view()