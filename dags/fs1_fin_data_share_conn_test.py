from datetime import datetime
from airflow import DAG
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.operators.python_operator import PythonOperator

def test_smb_connection():
    # Replace this with your SMB connection ID
    smb_conn_id = 'fs1_fin_data_store'
    
    # Initialize the SambaHook using the connection ID
    hook = SambaHook(smb_conn_id)
    
    # List files in the root directory of the share
    path = '/'  # or specify another directory if needed
    files = hook.listdir(path)
    
    print("Files in the given directory:")
    for f in files:
        print(f)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'Test_FS1_FileShare_Connectivity_DEV',
    default_args=default_args,
    description='A DAG to test FS1 FIN data share connection',
    schedule_interval=None,
)

test_task = PythonOperator(
    task_id='test_smb_connection',
    python_callable=test_smb_connection,
    dag=dag,
)

test_task
