from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

# Configure logging to print everything to stdout
root = logging.getLogger()
root.setLevel(logging.DEBUG)

handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

root.addHandler(handler)

def print_s3_objects(**context):
    try:
        s3_objects = context['task_instance'].xcom_pull(task_ids='list_s3_objects')
        logging.info(f"Retrieved objects from S3: {s3_objects}")
        if s3_objects:
            logging.info("Connection to S3 was successful. Objects in the bucket:")
            for obj in s3_objects:
                logging.info(obj)
        else:
            logging.warning("No objects found in the S3 bucket.")
    except Exception as e:
        logging.error(f"Error connecting to S3 or retrieving objects: {str(e)}")

# Define the DAG
with DAG(
    dag_id='s3_connection_test',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Define the S3ListOperator task
    list_s3_objects = S3ListOperator(
        task_id='list_s3_objects',
        bucket='FIN_DATA_STORE',  # Replace with your bucket name
        aws_conn_id='aws_default',  # Replace with your AWS connection ID
        verify=False  # Disable SSL verification for testing purposes
    )

    # Define the PythonOperator task to print the results
    print_results = PythonOperator(
        task_id='print_results',
        python_callable=print_s3_objects,
        provide_context=True,
    )

    # Set the task dependencies
    list_s3_objects >> print_results
