import os
import datetime as dt
#import subprocess
import logging
from zipfile import ZipFile
from airflow import DAG
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


remote_path = "/rmo_ct_prod/iceDB_ICE_BCMOFRMO.zip"
local_path = "/tmp/iceDB_ICE_BCMOFRMO.zip"

total_downloaded = 0
output_handle = open(local_path, "wb")

def print_progress(percent_progress):
    self.log.info("Percent Downloaded: %s%%" % percent_progress)
    
    
def write_to_file_with_progress(data):
    total_download += len(data)
    output_handle.write(data)
    percent_progress = (total_download / total_file_size) * 100
    print_progress(percent_progress)
    

def ice_rmo_unzip3():
    
    total_downloaded = 0
    # Replace these with your SMB server details
    conn_id = 'fs1_rmo_ice'      

    hook = SambaHook(conn_id)

    #total_file_size = hook.get_size(remote_path)

    #print('total_file size = ', total_file_size)
    
    hook.push_from_local("/tmp/iceDB_ICE_BCMOFRMO.zip","/rmo_ct_prod/iceDB_ICE_BCMOFRMO.zip")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'ice_rmo_unzip3',
    #local_tz=pendulum.timezone("America/Vancouver"),
    default_args=default_args,
    description='Backup CT source file in the completed folder',
    schedule_interval=None,
)

test_unzip3_task = PythonOperator(
    task_id='ice_rmo_unzip3',
    python_callable=ice_rmo_unzip3,
    dag=dag,
)

test_unzip3_task