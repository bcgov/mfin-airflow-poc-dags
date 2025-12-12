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
    
  
    
    def ondemand_stat_load_source(psource_file):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')

        try:
            
            logging.info(f"loading table: ICE_Stat_AgentActivityByQueue_I")
            
            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            query = f""" BULK INSERT [FIN_SHARED_LANDING_DEV].[dbo].[ICE_Stat_AgentActivityByQueue_I]
                    FROM '\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\ondemand\\{psource_file}'
                    WITH
	                ( FIELDTERMINATOR = '|',
                      ROWTERMINATOR = '\r\n',                         
                      MAXERRORS = 20, 
                      ERRORFILE='\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\log\\{psource_file}.log',
                      TABLOCK
	                );
                """
            logging.info(f"query: {query}")
            logging.info(f"inserting table:  ICE_Stat_AgentActivityByQueue_I")
            start_time = time.time()
            cursor.execute(query)
            conn.commit()
            
                      
            logging.info(f"bulk insert {time.time() - start_time} seconds")
       
        
        except Exception as e:
            logging.error(f"Error bulk loading table: ICE_Stat_AgentActivityByQueue_I source file: {psource_file} {e}")
 

    @task
    def ondemand_stat_load_data():
        
        source_file_set = ["Stat_AgentActivityByQueue_I_01.csv","Stat_AgentActivityByQueue_I_02.csv","Stat_AgentActivityByQueue_I_03.csv", "Stat_AgentActivityByQueue_I_04.csv",
                           "Stat_AgentActivityByQueue_I_04.csv","Stat_AgentActivityByQueue_I_05.csv","Stat_AgentActivityByQueue_I_06.csv", "Stat_AgentActivityByQueue_I_07.csv",
                           "Stat_AgentActivityByQueue_I_08.csv","Stat_AgentActivityByQueue_I_09.csv","Stat_AgentActivityByQueue_I_10.csv", "Stat_AgentActivityByQueue_I_11.csv",
                           "Stat_AgentActivityByQueue_I_12.csv","Stat_AgentActivityByQueue_I_13.csv","Stat_AgentActivityByQueue_I_14.csv", "Stat_AgentActivityByQueue_I_15.csv",
                           "Stat_AgentActivityByQueue_I_16.csv","Stat_AgentActivityByQueue_I_17.csv","Stat_AgentActivityByQueue_I_18.csv", "Stat_AgentActivityByQueue_I_19.csv",
                           "Stat_AgentActivityByQueue_I_20.csv","Stat_AgentActivityByQueue_I_21.csv","Stat_AgentActivityByQueue_I_22.csv", "Stat_AgentActivityByQueue_I_23.csv",
                           "Stat_AgentActivityByQueue_I_24.csv","Stat_AgentActivityByQueue_I_25.csv","Stat_AgentActivityByQueue_I_26.csv", "Stat_AgentActivityByQueue_I_27.csv",
                           "Stat_AgentActivityByQueue_I_28.csv","Stat_AgentActivityByQueue_I_29.csv","Stat_AgentActivityByQueue_I_30.csv", "Stat_AgentActivityByQueue_I_31.csv"]
        
        
        for source_file in source_file_set:
            ondemand_stat_load_source(source_file)

    ondemand_stat_load_data()
    
ice_rmo_stat_load_ondemand()