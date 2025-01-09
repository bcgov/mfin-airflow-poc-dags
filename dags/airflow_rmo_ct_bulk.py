from airflow.decorators import task, dag
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import time

@dag(
    description="DAG to RMO CT bulk insert",
    schedule=None,
    catchup=False,
)

def airflow_rmo_ct_bulk():
    
    def load_ct_source(rows):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')

        try:
            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            query = f""" BULK INSERT [FIN_SHARED_LANDING_DEV].[dbo].[Stat_QueueActivity_M]
                    FROM '\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\inprogress\\Stat_QueueActivity_M202410.csv'
                    WITH
	                ( FORMAT = 'CSV'
	                  ,FIRSTROW = 1
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
        
        load_ct_source(91)

    file_runner()
    
airflow_rmo_ct_bulk()