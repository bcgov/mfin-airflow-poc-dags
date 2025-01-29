from airflow.decorators import task, dag
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import time

@dag(
    description="DAG to stress test - pandas",
    schedule=None,
    catchup=False,
)
def airflow_stress_test_pandas ():
    
    def load_file(rows):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')

        try:
            smb_conn_id = 'fs1_fin_data_store'
            hook = SambaHook(smb_conn_id)
            files = hook.listdir('.')
            print("Files in the given directory:")
            for f in files:
                print(f)
            file = hook.open_file("bulk_test/airflow_stress_file.csv")
            
            #load data into data frame
            start_time = time.time()
            df = pd.read_csv(file, nrows=rows, dtype='str')
            print(f"loading dataframe {rows} rows test, duration: --- {time.time() - start_time} seconds ---")
            df = df.fillna("0")
            print(df.head())
            data = list(df.itertuples(index=False, name=None))
        except Exception as e:
            print(e)

    @task
    def file_runner():

        test_size = [10,100,1000]

        for size in test_size:
            load_file(size)

    file_runner()

airflow_stress_test_pandas()
