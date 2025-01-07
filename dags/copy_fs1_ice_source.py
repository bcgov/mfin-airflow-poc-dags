import os
import datetime as dt
import pendulum
from airflow import DAG
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def test_ice_connection():
    # Replace these with your SMB server details
    conn_id = 'fs1_fin_data_store'
      
    # share_name = 'fs1.fin.gov.bc.ca'
    directory = '/rmo_ct_prod/'
    #path = share_name + directory
    path = directory
    hook = SambaHook(conn_id)
    files = hook.listdir(path)
    destination = '/rmo_ct_prod/completed/'

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
    'start_date': datetime(2024, 9, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'copy_fs1_ice_source',
    #local_tz=pendulum.timezone("America/Vancouver"),
    default_args=default_args,
    description='Backup CT source file in the completed folder',
    schedule_interval="01 15 * * *",
)

test_task = PythonOperator(
    task_id='test_ice_connection',
    python_callable=test_ice_connection,
    dag=dag,
)

test_task