from airflow.decorators import task, dag
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import datetime as dt
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import time

@dag(
    description="RMO CT daily table delete and bulk insert test",
    schedule=None,
    catchup=False,
)

def ice_rmo_single():
    
    def single_table_delete(ptable_delete):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')

        try:
            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            query = f""" DELETE FROM [ARCHITECT_SANDBOX].[dbo].{ptable_delete};"""

            start_time = time.time()
            cursor.execute(query)
            conn.commit()
            
            #print(f"bulk insert duration: --- {time.time() - start_time} seconds ---")
            #print(f"bulk insert {rows} rows test, duration: --- {time.time() - start_time} seconds ---")
        
        
        except Exception as e:
            print(e)
 
 
    def single_load_source(psource_file):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')

        try:
            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            query = f""" BULK INSERT [ARCHITECT_SANDBOX].[dbo].{psource_file}
                    FROM '\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\inprogress\\{psource_file}.csv'
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
    def single_load_test():
        # Tables not included in the daily delete statement
        # "AgentAssignment","TeamAssignment", *** investigate further
        
        
        delete_tables_set = ["ACDQueue"]
        
        for delete_table in delete_table_set:
            single_table_delete(delete_table)                     
                             
        
        source_file_set = ["ACDQueue"]

        
        for source_file in source_file_set:
            single_load_source(source_file)

    single_load_test()
    
ice_rmo_single()