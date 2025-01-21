import os
import datetime as dt
import subprocess
from airflow import DAG
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def fs1_ice_connection():
    # Replace these with your SMB server details
    conn_id = 'fs1_rmo_ice_copy1'
      
    # share_name = 'fs1.fin.gov.bc.ca'
    directory = '/rmo_ct_prod/'
    #path = share_name + directory
    path = directory
    hook = SambaHook(conn_id)
    files = hook.listdir(path)
    destination = "'$AIRFLOW_HOME/rmo_ct_prod/'"


    print("Files in the rmo_ct_prod directory:")
    dYmd = dt.datetime.today().strftime('%Y%m%d')

    for f in files:
        if f == 'iceDB_ICE_BCMOFRMO.zip' :
            hook.replace(path + f, destination + 'iceDB_ICE_BCMOFRMO-' + dYmd+'.zip')
            print('File copied ',f)
        else:
            print('File skipped', f)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'ice_move_source_server',
    #local_tz=pendulum.timezone("America/Vancouver"),
    default_args=default_args,
    description='move zip file to /$AIRFLOW/rmo_ct_prod/ folder',
    schedule_interval="01 15 * * *",
)

move_server_task = PythonOperator(
    task_id='fs1_ice_connection',
    python_callable=fs1_ice_connection,
    dag=dag,
)

move_server_task