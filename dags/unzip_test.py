import os
import datetime as dt
import subprocess
import logging
from zipfile import ZipFile
from airflow import DAG
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.fs_hook import FSHook
from datetime import datetime


def unzip_test():
    # Replace these with your SMB server details
    conn_id = 'fs1_rmo_ice_copy1'
      
    # share_name = 'fs1.fin.gov.bc.ca'
    directory_zip_file = '/rmo_ct_prod/'
    #directory_unzip_file = '/rmo_ct_prod/'

    path_zip = directory_zip_file
    #path_unzip = directory_unzip_file

    #hook = SambaHook(conn_id)
    
    hook2 = FSHook(conn_id)
    base_path = hook2.get_path()
    with open(os.path.join(base_path, 'iceDB_ICE_BCMOFRMO.zip'), 'r') as fp:
        print(fp.read())
    
    #files = hook.listdir(path_zip)


    #dYmd = dt.datetime.today().strftime('%Y%m%d')

    #for f in files:
    #    dir_unzip = hook.path
 
     #   print('Current directory: ',dir_unzip)
        
#        if f == 'iceDB_ICE_BCMOFRMO.zip' :
#            logging.info("Extracting all the content '"+ f +"' to '"+ str(path_unzip) +"'")
#            print('opening zip file')          
#            with ZipFile(f,'r') as zip_file:
#                print('Printing all contents of the zip file')
#                zip_file.print()
#                zip_file.extractall()
                #zip_file.extractall(path_unzip)
             #   myzip.readlines
                #zip_file.close()
            #hook.replace(path + f, destination + 'iceDB_ICE_BCMOFRMO-' + dYmd+'.zip')
            #print('File copied ',f)
 #       else:
 #           print('File skipped', f)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'unzip_test',
    #local_tz=pendulum.timezone("America/Vancouver"),
    default_args=default_args,
    description='Unzip file test',
    schedule_interval=None,
)

test_unzip_task = PythonOperator(
    task_id='unzip_test',
    python_callable=unzip_test,
    dag=dag,
)

test_unzip_task