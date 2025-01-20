from airflow.decorators import task, dag
#from airflow.providers.samba.hooks.samba import SambaHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import datetime as dt
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import time

@dag(
    description="DAG - RMO CT daily bulk insert",
    schedule=None,
    catchup=False,
)

def ice_rmo_load_ondemand():
    
    def ondemand_load_source(psource_file):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')
        #myvar = "Stat_QueueActivity_M202410.csv"

        try:
            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            query = f""" BULK INSERT [RMO_ICE_HISTORY].[dbo].[Stat_AgentNotReadyBreakdown_D]
                    FROM '\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\ondemand\\{psource_file}'
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
    def ondemand_load_data():
        
        source_file_set = ["Stat_AgentNotReadyBreakdown_D20241227.csv","Stat_AgentNotReadyBreakdown_D20241228.csv","Stat_AgentNotReadyBreakdown_D20241229.csv",
                           "Stat_AgentNotReadyBreakdown_D20241230.csv"]
        
        for source_file in source_file_set:
            ondemand_load_source(source_file)

    ondemand_load_data()
    
ice_rmo_load_ondemand()