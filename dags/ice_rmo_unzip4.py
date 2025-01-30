import os
import datetime as dt
#import subprocess
import logging
from zipfile import ZipFile
from airflow import DAG
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

    

def ice_rmo_unzip4():    
    # Replace these with your SMB server details
    conn_id = 'fs1_rmo_ice'
      
    directory = '/rmo_ct_prod/'
    path = directory

    hook = SambaHook(conn_id)
    files = hook.listdir(path)
    destination = '/tmp/'


    print("Files in the rmo_ct_prod directory:")
    dYmd = dt.datetime.today().strftime('%Y%m%d')

    for f in files:
        if f == 'iceDB_ICE_BCMOFRMO.zip' :
            hook.push_from_local('\\tmp','\\rmo_ct_prod\\'{f})
            #hook.replace(path + f, destination + 'iceDB_ICE_BCMOFRMO.zip')
            print('File copied --> ',f)
        else:
            print('File skipped', f)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'ice_rmo_unzip4',
    default_args=default_args,
    description='Backup CT source file in the completed folder',
    schedule_interval=None,
)

test_unzip4_task = PythonOperator(
    task_id='ice_rmo_unzip4',
    python_callable=ice_rmo_unzip4,
    dag=dag,
)

test_unzip4_task