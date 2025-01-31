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

        try:
            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            query = f""" BULK INSERT [RMO_ICE_HISTORY].[dbo].[icePay]
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
        
        source_file_set = ["icePay_D20241001.csv", "icePay_D20241002.csv", "icePay_D20241003.csv", "icePay_D20241004.csv",
                           "icePay_D20241005.csv", "icePay_D20241006.csv", "icePay_D20241007.csv", "icePay_D20241008.csv",
                           "icePay_D20241009.csv", "icePay_D20241010.csv", "icePay_D20241011.csv", "icePay_D20241012.csv",
                           "icePay_D20241013.csv", "icePay_D20241014.csv", "icePay_D20241015.csv", "icePay_D20241016.csv"]

                           #"icePay_D20240919.csv", "icePay_D20240920.csv"]
        #source_file_set = ["Stat_AgentNotReadyBreakdown_D20250121.csv","Stat_AgentNotReadyBreakdown_D20250122.csv","Stat_AgentNotReadyBreakdown_D20250123.csv",
        #                   "Stat_AgentNotReadyBreakdown_D20250124.csv","Stat_AgentNotReadyBreakdown_D20250125.csv","Stat_AgentNotReadyBreakdown_D20250126.csv",
        #                   "Stat_AgentNotReadyBreakdown_D20250127.csv","Stat_AgentNotReadyBreakdown_D20250128.csv"]
        
        for source_file in source_file_set:
            ondemand_load_source(source_file)

    ondemand_load_data()
    
ice_rmo_load_ondemand()