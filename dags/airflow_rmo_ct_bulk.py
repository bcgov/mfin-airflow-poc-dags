from airflow.decorators import task, dag
#from airflow.providers.samba.hooks.samba import SambaHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import datetime as dt
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import time

@dag(
    description="DAG to RMO CT bulk insert",
    schedule=None,
    catchup=False,
)

def airflow_rmo_ct_bulk():
    
    def load_ct_source(psource_file):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')
        #myvar = "Stat_QueueActivity_M202410.csv"

        try:
            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            query = f""" BULK INSERT [FIN_SHARED_LANDING_DEV].[dbo].[Stat_QueueActivity_M]
                    FROM '\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\inprogress\\{psource_file}'
                    WITH
	                ( FORMAT = 'CSV'
	                );
                """

            start_time = time.time()
            cursor.execute(query)
            conn.commit()
            
                      
            print(f"bulk insert duration: --- {time.time() - start_time} seconds ---")
            #print(f"bulk insert {rows} rows test, duration: --- {time.time() - start_time} seconds ---")
        
        
        except Exception as e:
            print(e)


    @task
    def load_daily():
        
        source_file_set = ["Stat_QueueActivity_M202409.csv","Stat_QueueActivity_M202410.csv","Stat_QueueActivity_M202411.csv","Stat_QueueActivity_M202412.csv","Stat_QueueActivity_M202501.csv"]
        
        for source_file in source_file_set:
            load_ct_source(source_file)

    load_daily()
    
airflow_rmo_ct_bulk()