from airflow import DAG
from airflow.decorators import task
from airflow.io.path import ObjectStoragePath
import datetime as dt
import pandas as pd
import os
import requests

base = ObjectStoragePath("s3://aws_default@FREDA_DATA/")

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
    def extract_lfs():
        #get the year-month of the prior month based on today's date
        date = str(pd.Period(dt.datetime.now(), 'M') - 1)

         #fail case
        #date = str(pd.Period(dt.datetime.now(), 'M') + 1)

        filename = f"./{date}-CSV.zip"

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

            # ensure the bucket exists
            #base.mkdir(exist_ok=True)
            
            path = f"{base}/{filename}"

            with path.open("wb") as file:
                file.write(r.content)

    #call task
    extract_lfs()