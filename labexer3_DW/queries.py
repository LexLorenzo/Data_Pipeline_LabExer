import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

connection = sqlite3.connect('C:/Users/Lex Zedrick Lorenzo/OneDrive/Documents/DataWarehouse_Projects/labexer3_DW/includes/airflow-db/labexer3_sqlite.db')
cursor = connection.cursor()
# cursor.execute("SELECT * FROM Merged_table")
# cursor.execute("SELECT branch_name, COUNT(DISTINCT txn_id) AS \"number of transactions\" from Merged_table GROUP BY branch_name ORDER BY \"number of transactions\" DESC")

# number of transactions per branch
# df = pd.read_sql_query("SELECT branch_name, COUNT(DISTINCT txn_id) AS \"number of transactions\"" 
#                        "from Merged_table GROUP BY branch_name order by \"number of transactions\" desc"
#                        , connection)

# total sales per branch
# df = pd.read_sql_query("Select branch_name, SUM(price) AS \"total sales\" from Merged_table GROUP BY branch_name order by \"total sales\" desc"
#                        , connection)

#frequency of availed service from all branches
# df = pd.read_sql_query("Select service , count(service) as \"frequency of availed service\" from Merged_table group by service order by \"frequency of availed service\" desc"
#                        , connection)

# Age group distribution
# df = pd.read_sql_query("SELECT CASE WHEN strftime('%Y-%m-%d', 'now') - strftime('%Y-%m-%d', birthday) BETWEEN 10 AND 18 THEN '10-18'"
#                         "WHEN strftime('%Y-%m-%d', 'now') - strftime('%Y-%m-%d', birthday) BETWEEN 19 AND 30 THEN '19-30'"
#                                 "WHEN strftime('%Y-%m-%d', 'now') - strftime('%Y-%m-%d', birthday) BETWEEN 31 AND 50 THEN '31-50'"
#                                         "ELSE 'Over 50' END AS age_group, COUNT(*) AS count FROM Merged_table GROUP BY age_group;"
#                        , connection)

# count each services age group puta di ko madescribe
# df = pd.read_sql_query("SELECT CASE WHEN strftime('%Y-%m-%d', 'now') - strftime('%Y-%m-%d', birthday) BETWEEN 10 AND 18 THEN '10-18'"
#                         "WHEN strftime('%Y-%m-%d', 'now') - strftime('%Y-%m-%d', birthday) BETWEEN 19 AND 30 THEN '19-30'"
#                                 "WHEN strftime('%Y-%m-%d', 'now') - strftime('%Y-%m-%d', birthday) BETWEEN 31 AND 50 THEN '31-50'"
#                                         "ELSE 'Over 50' END AS age_group,"
#                         "service,  COUNT(*) AS service_count FROM Merged_table GROUP BY age_group, service;"
#                        , connection)

# df = pd.read_sql_query(""
#                        , connection)

# Weekly View per mall
# df = pd.read_sql_query("Select branch_name, strftime('%Y-%W', avail_date) As "
#                        "\"Week\", SUM(price) AS \"Total Price\" FROM Merged_table GROUP BY branch_name, Week ORDER BY week,branch_name;"
#                        , connection)

# Weekly view per service
# df = pd.read_sql_query("Select branch_name, strftime('%Y-%W', avail_date) As "
#    "\"Week\", service, SUM(price) AS \"Total Price\" FROM Merged_table GROUP BY branch_name, service, Week ORDER BY week,branch_name;"
#    , connection)
# print('Weekly View\n', df[0:20])

create_view_query = "Select branch_name, strftime('%Y-%W', avail_date) As \"Week\", service, SUM(price) AS \"Total Price\" FROM Merged_table GROUP BY branch_name, service, Week ORDER BY week,branch_name;"
connection.execute(create_view_query)
connection.commit()


# di nainclude
# df = pd.read_sql_query("SELECT branch_name, first_name, last_name, COUNT(*) AS shopping_frequency "
#                        "FROM Merged_table GROUP BY branch_name, first_name, last_name ORDER BY "
#                            "branch_name, shopping_frequency DESC Limit 2"
#                        , connection)
# print(cursor.fetchall())