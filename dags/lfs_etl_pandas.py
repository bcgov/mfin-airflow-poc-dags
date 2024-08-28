from airflow.decorators import task, dag
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import datetime as dt
import zipfile
import boto3
from sqlalchemy import create_engine
from io import BytesIO
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

#get the year-month of the prior month based on today's date
date = str(pd.Period(dt.datetime.now(), 'M') - 1)
filename = f"{date}-CSV.zip"
bucket_path = "FREDA_DATA"

@dag(
    description="DAG to process LFS file via pandas",
    start_date=dt.datetime(2024, 8, 24),
    schedule=None,
    catchup=False,
)
def lfs_load_pandas():
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

        df = pd.read_csv(zip_file.open('pub0724.csv'),nrows=10,usecols=['REC_NUM','SURVYEAR','SURVMNTH','LFSSTAT','PROV','FINALWT'], dtype='str')
        return df
        
    
    @task
    def load_file(df):
        #print(df.head())
        sql_hook = MsSqlHook(mssql_conn_id='mssql_default')

        #fails complaining about __extra__ in connection string
        #engine = sql_hook.get_sqlalchemy_engine()

        #create engine manually
        #remove the extra info from the conn string
        uri = sql_hook.get_uri()
        print(uri)
        con_uri = uri.split('?',1)[0]
        enginer = create_engine(con_uri)

        df.to_sql('AIRFLOW_TEST',con=engine,if_exists='append', index=False)

    
    @task
    def clean_up(filename):
        pass

    data_frame = get_file(filename,bucket_path)
    load_data = load_file(data_frame)
    
    waiting_for_lfs_file >> data_frame >> load_data

lfs_load_pandas()