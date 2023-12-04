from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from includes.python_scripts.customer_remove_unrealistic_dates import unrealistic_dates
from includes.python_scripts.customer_remove_duplicates_nulls import remove_duplicates_nulls
from includes.python_scripts.customer_remove_unnecessary_chars_from_names import remove_unnecessary
from includes.python_scripts.customer_capitalize_names import capitalize_names
from includes.python_scripts.branch_capitalizing_mall import capitalize_malls
from includes.python_scripts.branch_remove_duplicates import remove_duplicates_branch
from includes.python_scripts.branch_remove_empty import remove_empty
from includes.python_scripts.branch_remove_price_less_than_zero import remove_price_less_than_0
from includes.python_scripts.branch_rounding_decimal import rounding_decimals
from includes.python_scripts.merge_customer_branch import merge_data
from includes.python_scripts.put_to_db import insert_to_db
from includes.python_scripts.create_view import create_a_view


dag = DAG(
    'labexer-dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='*/2 * * * *',
    catchup=False
)


unrealistic = PythonOperator(
    task_id='remove_unrealistic_dates',
    python_callable=unrealistic_dates,
    dag=dag
)


duplicates = PythonOperator(
    task_id='remove_duplicates_nulls',
    python_callable=remove_duplicates_nulls,
    dag=dag
)


unnecessary = PythonOperator(
    task_id='print_random_quote',
    python_callable=remove_unnecessary,
    dag=dag
)


capitalize_the_names = PythonOperator(
    task_id='capitalize_names',
    python_callable=capitalize_names,
    dag=dag
)


remove_empty_branch = PythonOperator(
    task_id='remove_empty',
    python_callable=remove_empty,
    dag=dag
)


capitalize_mall_branch = PythonOperator(
    task_id='capitalize_malls',
    python_callable=capitalize_malls,
    dag=dag
)


remove_duplicates_from_branch = PythonOperator(
    task_id='remove_duplicates_branch',
    python_callable=remove_duplicates_branch,
    dag=dag
)


remove_prices = PythonOperator(
    task_id='remove_price_less_than_0',
    python_callable=remove_price_less_than_0,
    dag=dag
)


rounding_the_decimals = PythonOperator(
    task_id='rounding_decimals',
    python_callable=rounding_decimals,
    dag=dag
)


merge_tables = PythonOperator(
    task_id='merge_data',
    python_callable=merge_data,
    dag=dag
)


insert = PythonOperator(
    task_id='insert_to_db',
    python_callable=insert_to_db,
    dag=dag
)

weekly = PythonOperator(
    task_id='create_a_view',
    python_callable=create_a_view,
    dag=dag
)

# Set the dependencies between the tasks
unrealistic >> duplicates >> unnecessary >> capitalize_the_names >> remove_empty_branch >> capitalize_mall_branch >> \
    remove_duplicates_from_branch >> remove_prices >> rounding_the_decimals >> merge_tables >> insert >> weekly