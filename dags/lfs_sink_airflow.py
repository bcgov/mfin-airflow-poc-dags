from airflow import DAG
from airflow.decorators import task
from airflow.providers.samba.hooks.samba import SambaHook
import datetime as dt
import pandas as pd
import os
import requests


with DAG(
    dag_id='lfs_poc_download',
    start_date=dt.datetime(2024,8,22,9),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay" : dt.timedelta(minutes=2)
    }
) as dag:
    #download last month's LFS file via the task API
    @task()
    def extract_lfs():
        #get the year-month of the prior month based on today's date
        date = str(pd.Period(dt.datetime.now(), 'M') - 1)

         #fail case
        #date = str(pd.Period(dt.datetime.now(), 'M') + 1)

        filename = f"{date}-CSV.zip"

        url = f"https://www150.statcan.gc.ca/n1/pub/71m0001x/2021001/{filename}"

        try:
            r = requests.get(url)
            r.raise_for_status()
        #should exceptions be logged?
        except requests.exceptions.HTTPError as errh:
            print ("Http Error:",errh)
        except requests.exceptions.ConnectionError as errc:
            print ("Error Connecting:",errc)
        except requests.exceptions.Timeout as errt:
            print ("Timeout Error:",errt)
        except requests.exceptions.RequestException as err:
            print ("OOps: Something Else",err)
        else:
            #TODO make path based on config file or tied to connection definition
            path = os.path.join('\\\\test.fs1.fin.gov.bc.ca\\DevOps\\Airflow_POC_Dev\\LFS',filename)

            #get the test FS1 connection
            smb_conn_id = 'fs1_test_conn'
        '''
        with open(filename,'wb') as f:
                f.write(r.content)
        '''
        try:
            
            with SambaHook(smb_conn_id) as hook:
                hook.listdir(path)
                print("Files in the directory:")
                for f in files:
                    print(f)
                #hook.replace(path, r.content)
        except Exception as X:
            print(X)

    #call task
    extract_lfs()