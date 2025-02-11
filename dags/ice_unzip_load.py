import os
from airflow.decorators import dag, task
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.utils.dates import days_ago
import zipfile
import logging
import io

# Define the DAG
@dag(
    dag_id="move_and_unzip_file_dag",
    schedule_interval=None,  # Set your schedule interval or leave as None for manual trigger
    start_date=days_ago(1),
    catchup=False,
    tags=["ice", "unzip", "inprogress_folder"]
)
def ice_unzip_load_daily():
    # Log all steps at INFO level
    logging.basicConfig(level=logging.INFO)

    # Task 1: Move file from source to destination (using SambaHook)
    @task
    def move_file():
        source_path = r'/rmo_ct_prod/'
        dest_path = r'/rmo_ct_prod/inprogress/'
        file = 'iceDB_ICE_BCMOFRMO.zip'
        zip_loc = '/tmp'

        # Initialize SambaHook with your credentials and connection details
        with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
            with fs_hook.open_file(source_path + file,'rb') as f:
                z = zipfile.ZipFile(f)
                for iceTable in z.infolist():
                    logging.info(iceTable.filename)
                    z.extract(iceTable.filename,path=zip_loc)

                    fs_hook.push_from_local(dest_path+thing.filename, os.path.join(zip_loc,iceTable.filename))
                    
        
        #logging.info(f"File moved from {source_path} to {dest_path}")
        
        return

    # Task orchestration
    moved_file_path = move_file()

# Instantiate the DAG
dag_instance = ice_unzip_load_daily()