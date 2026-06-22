import csv, sys, argparse
import os
import numpy as np
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.utils.dates import days_ago
import zipfile
import logging
import io
import time
import datetime as dt
from datetime import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s -%(message)s')
handler.setFormatter(formatter)
root.addHandler(handler) 

@dag(
    description="DAG - RMO CT specific Stat dates bulk insert",
    schedule=None,
    catchup=False,
    tags=["ice","ondemand","load","source file"]
)

def ice_rmo_stat_load_ondemand():
    logging.basicConfig(level=logging.INFO)
    
  
    #                WITH
	#                ( FIELDTERMINATOR = '|',
    #                  ROWTERMINATOR = '\r\n',                         
    #                  MAXERRORS = 20, 
    #                  ERRORFILE='\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\log\\{psource_file}.log',
    #                  TABLOCK
	
    
    
    def ondemand_stat_load_source(psource_file):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')

        try:
            
            logging.info(f"loading table: ICE_Transactions")
            
            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            query = f""" BULK INSERT [FIN_SHARED_LANDING_DEV].[dbo].[ICE_Transactions]
                    FROM '\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\ondemand\\{psource_file}'
                    WITH
	                ( FORMAT='CSV',
                      ROWTERMINATOR = '\r\n',                         
                      MAXERRORS = 100, 
                      ERRORFILE='\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\log\\{psource_file}.log',
                      TABLOCK
	                );
                """
            logging.info(f"query: {query}")
            logging.info(f"inserting table:  ICE_Transactions")
            start_time = time.time()
            cursor.execute(query)
            conn.commit()
            
                      
            logging.info(f"bulk insert {time.time() - start_time} seconds")
       
        
        except Exception as e:
            logging.error(f"Error bulk loading table: ICE_Transactions source file: {psource_file} {e}")
 

    @task
    def ondemand_stat_load_data():
        
        source_file_set = ["Transactions01.csv","Transactions02.csv","Transactions03.csv", "Transactions04.csv","Transactions05.csv",
                           "Transactions06.csv","Transactions07.csv","Transactions08.csv", "Transactions09.csv","Transactions10.csv",
                           "Transactions11.csv","Transactions12.csv","Transactions13.csv", "Transactions14.csv","Transactions15.csv",
                           "Transactions16.csv","Transactions17.csv","Transactions18.csv", "Transactions19.csv","Transactions20.csv",
                           "Transactions21.csv","Transactions22.csv","Transactions23.csv", "Transactions24.csv","Transactions25.csv",
                           "Transactions26.csv","Transactions27.csv","Transactions28.csv", "Transactions29.csv","Transactions30.csv"]
        
        
        for source_file in source_file_set:
            ondemand_stat_load_source(source_file)

    ondemand_stat_load_data()
    
ice_rmo_stat_load_ondemand()