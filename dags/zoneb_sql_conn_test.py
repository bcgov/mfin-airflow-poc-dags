from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import pymssql

# Define a function to test MSSQL connection
# def test_mssql_connection():
#     conn_id = 'mssql_default'  # Replace with your connection ID
#     conn = BaseHook.get_connection(conn_id)

#     # Construct the connection parameters
#     host = conn.host
#     #database = conn.schema
#     user = conn.login
#     password = conn.password

#     try:
#         with pymssql.connect(host=host, database=database, user=user, password=password) as connection:
#             cursor = connection.cursor()
#             cursor.execute("SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES")

#             # Fetch result
#             row = cursor.fetchone()
#             print('Number of tables:', row[0])

#             # Close connection
#             conn.close()
#     except Exception as e:
#         print(f"An error occurred: {e}")

# Define a function to test MSSQL connection for a list of databases
# def test_mssql_connections_for_databases(database_list):
#     conn_id = 'mssql_default'  # Replace with your connection ID
#     conn = BaseHook.get_connection(conn_id)

#     # Construct the connection parameters
#     host = conn.host
#     user = conn.login
#     password = conn.password

#     for database in database_list:
#         try:
#             with pymssql.connect(host=host, database=database, user=user, password=password) as connection:
#                 cursor = connection.cursor()
#                 cursor.execute("SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES")

#                 # Fetch result
#                 row = cursor.fetchone()
#                 print(f'Database: {database} - Number of tables:', row[0])

#                 # Connection is closed automatically by 'with' block
#         except Exception as e:
#             print(f"Database: {database} - An error occurred: {e}")
#         finally:
#             # Ensure the connection is closed
#             if connection:
#                 connection.close()
#                 print(f'Database: {database} - Connection closed.')

# Define a function to test MSSQL connection for a single database
def test_mssql_connection_for_database(database):
    conn_id = 'mssql_default'  # Replace with your connection ID
    conn = BaseHook.get_connection(conn_id)

    # Construct the connection parameters
    host = conn.host
    user = conn.login
    password = conn.password

    connection = None
    try:
        # Establish the connection
        connection = pymssql.connect(host=host, database=database, user=user, password=password)
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES")

        # Fetch result
        row = cursor.fetchone()
        print(f'Database: {database} - Number of tables:', row[0])
    except Exception as e:
        print(f"Database: {database} - An error occurred: {e}")
    finally:
        # Ensure the connection is closed
        if connection:
            connection.close()
            print(f'Database: {database} - Connection closed.')

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'test_mssql_connection',
    default_args=default_args,
    description='A simple DAG to test MSSQL connection using pymssql',
    schedule_interval=None,  # Set to None to run manually or specify a cron schedule
    start_date=days_ago(1),
    catchup=False,
)

# List of databases to test
database_list = ['FIN_SHARED_LANDING_DEV', 'FIN_SHARED_STAGING_DEV', 'FIN_SHARED_DATA_DEV', 'FIN_SHARED_OPS_DEV', 'FIN_SHARED_FREDA_STATIC_PROD','TACS_BI_EXTRACT']

# Define the task
# test_connection_task = PythonOperator(
#     task_id='test_mssql_connection_task',
#     python_callable=test_mssql_connection,
#     dag=dag,
# )

# Create a dictionary to hold the tasks
tasks = {}

# test_connections_task = PythonOperator(
#     task_id='test_mssql_connections_task',
#     python_callable=test_mssql_connections_for_databases,
#     op_kwargs={'database_list': database_list},
#     dag=dag,
# )

for database in database_list:
    task = PythonOperator(
        task_id=f'test_mssql_connection_{database}',
        python_callable=test_mssql_connection_for_database,
        op_kwargs={'database': database},
        dag=dag,
    )
    tasks[database] = task

# Set task dependencies (all tasks are independent)
for task in tasks.values():
    task  # No dependencies between tasks, so they run independently

# Set task dependencies
#test_connections_task