from airflow.decorators import task, dag
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import datetime as dt
import zipfile
import boto3
from io import BytesIO

#get the year-month of the prior month based on today's date
date = str(pd.Period(dt.datetime.now(), 'M') - 1)
filename = f"{date}-CSV.zip"
bucket_path = "FREDA_DATA"

@dag(
    description="DAG to process LFS file",
    start_date=dt.datetime(2024, 8, 24),
    schedule="@daily",
    catchup=False,
)
def lfs_load ():
    waiting_for_lfs_file = S3KeySensor(
        task_id="sensor_one_key",
        bucket_name="FREDA_DATA",
        bucket_key=filename,
        aws_conn_id = 'aws_default',
        poke_interval=200,
        mode="reschedule",
        timeout= 60 * 5,
    )

    @task
    def get_file(filename,bucket_path):
        source_s3 = S3Hook('aws_default')
        zf = source_s3.get_key(key=filename,bucket_name=bucket_path)

        filebytes = BytesIO()

        zf.download_fileobj(filebytes)
        zip_file = zipfile.ZipFile(filebytes)

        df = pd.read_csv(zip_file.open('pub0724.csv'))
        return df
        
    '''
    @task
    def load_file(df):
        pass
    
    @task
    def clean_up(filename):
        pass
    '''
    
    waiting_for_lfs_file >> get_file(filename,bucket_path)

lfs_load()