import os
import datetime as dt
from airflow import DAG
from airflow.contrib.hooks.shh_hook import SSHHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def remote_powershell():
    # Replace these with your SMB server details
    conn_id = 'fs1_fin_data_store'
      
    directory = '/rmo_ct_prod/'
    path = directory
    ssh = SSHHook(ssh_conn_id=conn_id)
    
    ssh_client = None
    
    try:
        ssh_client = ssh.get_conn()
        ssh_cclient.load_system_host_keys()
        ssh_client.exec_command('ps_remote.ps1')
    finally:
        if ssh_client:
            ssh_client.close()
        

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'Remote_PowerShell_script',
    default_args=default_args,
    description='Executing remote PowerShell script',
    schedule_interval="* * * * *",
)

shh_task = PythonOperator(
    task_id='ssh task',
    python_callable=remote_powershell,
    dag=dag,
)

ssh_task