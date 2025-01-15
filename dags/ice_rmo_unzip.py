import os
import datetime as dt
import subprocess
import logging
from zipfile import ZipFile
from airflow import DAG
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def ice_rmo_unzip():
    # Replace these with your SMB server details
    conn_id = 'fs1_rmo_ice_dev'
      
    # share_name = 'fs1.fin.gov.bc.ca'
    directory_zip_file = '/rmo_ct_prod/inprogress/'
    directory_unzip_file = '/rmo_ct_prod/inprogress/'

    self.path_zip = directory_zip_file
    self.path_unzip = directory_unzip_file

    hook = SambaHook(conn_id)

    files = hook.listdir(self.path_zip)


    dYmd = dt.datetime.today().strftime('%Y%m%d')

    for f in files:
        if f == 'iceDB_ICE_BCMOFRMO.zip' :
            logging.info("Extracting all the contect to'" + str(self.path_unzip_file) +"'")
            
            
            with ZipFile(self.path_zip,'r') as zip_file:
                zip_file.extractall(self.path_unzip)
                
                zip_file.close()
            #hook.replace(path + f, destination + 'iceDB_ICE_BCMOFRMO-' + dYmd+'.zip')
            #print('File copied ',f)
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
    'ice_rmo_unzip',
    #local_tz=pendulum.timezone("America/Vancouver"),
    default_args=default_args,
    description='Backup CT source file in the completed folder',
    schedule_interval=None,
)

test_unzip_task = PythonOperator(
    task_id='ice_rmo_unzip',
    python_callable=ice_rmo_unzip,
    dag=dag,
)

test_unzip_task