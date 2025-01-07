import os
import datetime as dt
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from datetime import datetime

conn_id = 'fs1_fin_data_store'
ssh = SSHHook(ssh_conn_id=conn_id)


def remote_powershell():
    # Replace these with your SMB server details
#    conn_id = 'fs1_fin_data_store'
      
    directory = '/rmo_ct_prod/'
    path = directory
#    ssh = SSHHook(ssh_conn_id=conn_id)
    
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

ssh_task = SSHOperator(
    task_id = 'ssh_task',
    ssh_conn_id = 'fs1_fin_data_store',
    command = 'echo "Hello World"',
    dag=dag,
)

ssh_task