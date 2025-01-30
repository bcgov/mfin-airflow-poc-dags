import os
import datetime as dt
from airflow import DAG
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import timedelta
import zipfile


def ice_rmo_unzip5():
    # Replace these with your SMB server details
    conn_id = 'fs1_rmo_ice'
      
    # share_name = 'fs1.fin.gov.bc.ca'
    directory = '/rmo_ct_prod/'
    path = directory
    hook = SambaHook(conn_id)
    files = hook.listdir(path)
    destination = '/rmo_ct_prod/completed/'

    print("Files in the rmo_ct_prod directory:")
    #dYmd = dt.datetime.today().strftime('%Y%m%d')
    dYmd = (dt.datetime.today() + timedelta(days=-1)).strftime('%Y%m%d') #CT source file with yesterday's date
    
    for f in files:
        if f == 'iceDB_ICE_BCMOFRMO.zip' :
            with zipfile.zipfile(hook.replace(path + f, destination + 'iceDB_ICE_BCMOFRMO-' + dYmd+'.zip'),'r') as zip_ref:
                zip_ref.extractall("/tmp")
            print('File copied --> ',f)
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
    'ice_rmo_unzip5',
    #local_tz=pendulum.timezone("America/Vancouver"),
    default_args=default_args,
    description='Copy source zip file to /rmo_ct_prod/completed/ folder',
    schedule = NONE,
)

move_rmo_unzip5 = PythonOperator(
    task_id='ice_rmo_unzip5',
    python_callable=ice_rmo_unzip5,
    dag=dag,
)

move_rmo_unzip5