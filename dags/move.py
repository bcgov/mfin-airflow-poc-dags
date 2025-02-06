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
        file = 'titleEventExport_2025-1-1-0-0-0___2025-1-1-23-59-59.zip'

        # Initialize SambaHook with your credentials and connection details
        with SambaHook(samba_conn_id="FS1_test") as fs_hook:
            with fs_hook.open_file(source_path + file,'rb') as f:
                z = zipfile.ZipFile(f)
                for thing in z.infolist():
                    logging.info(thing.filename)
                    
        
        #logging.info(f"File moved from {source_path} to {dest_path}")
        
        return

    # Task 2: Unzip the file after it's moved
    @task
    def unzip_file(file_path):
        dest_unzip_dir = '/tmp/destination/unzipped/'
        
        # Ensure the destination unzip directory exists
        os.makedirs(dest_unzip_dir, exist_ok=True)
        
        # Unzip the file
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(dest_unzip_dir)
            logging.info(f"File unzipped to {dest_unzip_dir}")
        
        return dest_unzip_dir

    # Task orchestration
    moved_file_path = move_file()
    #unzip_file(moved_file_path)

# Instantiate the DAG
dag_instance = move_and_unzip_file()
