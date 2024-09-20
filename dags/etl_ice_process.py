import os
import datetime as dt
from airflow.operators import UnzipOperator
from airflow import DAG
from airflow.decorators import task, dag
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
 
# Defining DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# DAG definition
@dag = DAG(
    'etl_ice_process',
    default_args=default_args,
    description='Expand .zip and backup CT source file in the completed folder',
    schedule_interval="01 15 * * *", # UTC 15:01 = PDT 08:01
)

def etl_ice_process():
    conn_id = 'fs1_rmo_ice'
      
    directory = '/rmo_ct_prod/'
    path = directory
    hook = SambaHook(conn_id)
    files = hook.listdir(path)
    destination = '/rmo_ct_prod/completed/'
    destination_unzip = '/rmo_ct_prod/inprogress'

    @task
    def etl_ice_backup:
        # Backing up daily CT source file in \\fs1.fin.gov.bc.ca\rmo_ct_prod\completed
        dYmd = dt.datetime.today().strftime('%Y%m%d')
        for f in files:
            if f == 'iceDB_ICE_BCMOFRMO.zip' :
                hook.replace(path + f, destination + 'iceDB_ICE_BCMOFRMO-' + dYmd+'.zip')
 
    @task
    def etl_ice_unzip():
        unzip_task = UnzipOperator(
            task_id = 'unzip_task',
            path_to_zip_file=path,
            path_to_unzip = destination_unzip,
            dag = dag)            
        
#etl_ice_process = PythonOperator(
#    task_id='etl_ice_process',
#    python_callable=etl_ice_process,
#    dag=dag,
)
    
    etl_ice_unzip >> etl_ice_backup

# call task
etl_ice_process()
