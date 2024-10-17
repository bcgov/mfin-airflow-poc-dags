from airflow.decorators import task, dag
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import time

@dag(
    description="DAG to bulk insert",
    schedule=None,
    catchup=False,
)
def airflow_stress_test_bulk():
    
    def load_file(rows):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')

        try:
            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            query = f""" BULK INSERT [dbo].[AIRFLOW_STRESS_TEST_TARGET]
                    FROM '\\\\fs1.fin.gov.bc.ca\Finance_Data_Store\\bulk_test\\airflow_stress_file.csv'
                    WITH
	                ( FORMAT = 'CSV'
	                  ,FIRSTROW = 2
	                  ,LASTROW = {rows}
	                );
                """

            start_time = time.time()
            cursor.execute(query)
            conn.commit()
            print(f"bulk insert {rows} rows test, duration: --- {time.time() - start_time} seconds ---")
        
        except Exception as e:
            print(e)


    @task
    def file_runner():

        test_size = [1000,10000,100000]

        for size in test_size:
            load_file(size)

    file_runner()

airflow_stress_test_bulk()