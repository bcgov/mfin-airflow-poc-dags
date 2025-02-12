from airflow.decorators import task, dag
#from airflow.providers.samba.hooks.samba import SambaHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import datetime as dt
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import time
import logging

@dag(
    description="DAG - RMO CT specific dates bulk insert",
    schedule=None,
    catchup=False,
    tags=["ice","ondemand","load","source file"]
)

def ice_rmo_load_ondemand():
    logging.basicConfig(level=logging.INFO)
    
    def ondemand_load_source(psource_file):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')

        try:
            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            query = f""" BULK INSERT [RMO_ICE_HISTORY].[dbo].[Stat_QueueActivity_D]
                    FROM '\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\ondemand\\{psource_file}'
                    WITH
	                ( FORMAT = 'CSV'
	                );
                """
            logging.info({psource_file})
            start_time = time.time()
            cursor.execute(query)
            conn.commit()
            
                      
            print(f"bulk insert duration: --- {time.time() - start_time} seconds ---")
            #print(f"bulk insert {rows} rows test, duration: --- {time.time() - start_time} seconds ---")
        
        
        except Exception as e:
            logging.info(f"Error bulk loading {psource_file}")
 

    @task
    def ondemand_load_data():
        
        source_file_set = ["Stat_QueueActivity_D20241013.csv", "Stat_QueueActivity_D20241014.csv", "Stat_QueueActivity_D20241015.csv", "Stat_QueueActivity_D20241016.csv",
                           "Stat_QueueActivity_D20241017.csv", "Stat_QueueActivity_D20241018.csv", "Stat_QueueActivity_D20241019.csv", "Stat_QueueActivity_D20241020.csv",
                           "Stat_QueueActivity_D20241021.csv", "Stat_QueueActivity_D20241022.csv", "Stat_QueueActivity_D20241023.csv", "Stat_QueueActivity_D20241024.csv",
                           "Stat_QueueActivity_D20241025.csv", "Stat_QueueActivity_D20241026.csv", "Stat_QueueActivity_D20241027.csv", "Stat_QueueActivity_D20241028.csv",
                           "Stat_QueueActivity_D20241029.csv", "Stat_QueueActivity_D20241030.csv", "Stat_QueueActivity_D20241031.csv"]
                           

                           
                           #, "icePay_D20241220.csv",
                           #"icePay_D20241221.csv", "icePay_D20241222.csv", "icePay_D20241223.csv", "icePay_D20241224.csv"]
                           
                           #"icePay_D20240919.csv", "icePay_D20240920.csv"]
        #source_file_set = ["Stat_AgentNotReadyBreakdown_D20250121.csv","Stat_AgentNotReadyBreakdown_D20250122.csv","Stat_AgentNotReadyBreakdown_D20250123.csv",
        #                   "Stat_AgentNotReadyBreakdown_D20250124.csv","Stat_AgentNotReadyBreakdown_D20250125.csv","Stat_AgentNotReadyBreakdown_D20250126.csv",
        #                   "Stat_AgentNotReadyBreakdown_D20250127.csv","Stat_AgentNotReadyBreakdown_D20250128.csv"]
        
        for source_file in source_file_set:
            ondemand_load_source(source_file)

    ondemand_load_data()
    
ice_rmo_load_ondemand()