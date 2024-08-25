from datetime import datetime
from airflow import DAG
from airflow.providers.samba.hooks.samba import SambaHook
import datetime as dt
import pandas as pd
import os
import requests

smb_conn_id = 'test_fs1'

with DAG(
    dag_id='lfs_poc_download',
    start_date=dt.datetime(2024,8,22,9),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 0,
        "retry_delay" : dt.timedelta(minutes=2)
    }
) as dag: 
    #download last month's LFS file via the task API
    @task()
    def fileshare_test():
        hook = SambaHook(smb_conn_id)
        files = hook.listdir('Airflow_POC_Dev/')
        print("Files in the given directory:")
        for f in files:
            print(f)

    fileshare_test()