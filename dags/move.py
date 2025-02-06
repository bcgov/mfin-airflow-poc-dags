import os
from airflow.decorators import dag, task
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.utils.dates import days_ago
import zipfile
import logging
import io
import zipfile

# Define the DAG
@dag(
    dag_id="move_and_unzip_file_dag",
    schedule_interval=None,  # Set your schedule interval or leave as None for manual trigger
    start_date=days_ago(1),
    catchup=False,
    tags=["example", "move_file", "unzip"]
)
def move_and_unzip_file():
    # Log all steps at INFO level
    logging.basicConfig(level=logging.INFO)

    # Task 1: Move file from source to destination (using SambaHook)
    @task
    def move_file():
        source_path = r'/bulk_test/fabric_test/'
        dest_path = r'/bulk_test/airflow_test/'
        file = 'bulk_insert_test_file.zip'
        zip_loc = '/tmp'

        # Initialize SambaHook with your credentials and connection details
        with SambaHook(samba_conn_id="FS1_test") as fs_hook:
            with fs_hook.open_file(source_path + file,'rb') as f:
                z = zipfile.ZipFile(f)
                for thing in z.infolist():
                    logging.info(thing.filename)
                    z.extract(thing.filename,path=zip_loc)

                    fs_hook.push_from_local(dest_path+thing.filename, os.path.join(zip_loc,thing.filename))
                    
        
        #logging.info(f"File moved from {source_path} to {dest_path}")
        
        return

    # Task orchestration
    moved_file_path = move_file()

# Instantiate the DAG
dag_instance = move_and_unzip_file()
