import os
import datetime as dt
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from datetime import datetime

#conn_id = 'ssh_powershell'
sshHook = SSHHook(ssh_conn_id="ssh_powershell")


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
    schedule=None,
)

ssh_task = SSHOperator(
    task_id = 'ssh_task',
    ssh_hook = sshHook,
    command = 'dir /rmo_ct_prod/*.* > files.txt"',
    dag=dag,
)

ssh_task