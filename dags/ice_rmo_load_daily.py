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
    description="RMO CT Stat_CDR test",
    schedule=None,
    catchup=False,
)

def daily_load_data():
   
   @task   
   def daily_load_source(psource_file):
       sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')
       
       conn = sql_hook.get_conn()
       cursor = conn.cursor()
       
       source_path = r'/rmo_ct_prod/inprogress/'
       file = 'Stat_CDR.csv'
       output_file = 'Stat_CDR_fixed.csv'       

       try:
           with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
               try:
                   with fs_hook.open_file(source_path + file,'r') as f:
                       csv_reader = pd.read_csv(f, header = None, usecols=[i for i in range(21)], quoting=1, on_bad_lines = 'skip')
                       
               except Exception as e:
                   logging.error(f"Error opening {file} wirh error: {e}")
                   
               try:
                   with fs_hook.open_file(source_path + output_file, 'w') as outfile:
                       csv_reader.to_csv(outfile, header=False,index=False)
        
               except Exception as e:
                   logging.error(f"Error opening output file {output_file} with error: {e}")  
       
       except Exception as e:
           logging.error(f"error hooking to fs1_rmo_ice")
           
       
       try:
           conn = sql_hook.get_conn()
           cursor = conn.cursor()
            
           query = f""" BULK INSERT [FIN_SHARED_LANDING_DEV].[dbo].[ICE_Stat_CDR]
                        FROM '\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\inprogress\\Stat_CDR_fixed.csv'
                        WITH
	                  ( FORMAT='CSV', 
                        MAXERRORS=100, 
                        ERRORFILE='\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\log\\ICE_STAT_CDR.log',
                        TABLOCK                               
	                  );
                    """
           logging.info(f"query: {query}")
           logging.info(f"inserting table:  {psource_file}")
           start_time = time.time()
           cursor.execute(query)
           conn.commit()

       except Exception as e:
           logging.error(f"error Bulk Insert with error: {e}")
           
                    
       #sql = f"""
       #         BEGIN TRY
       #           INSERT INTO [FIN_SHARED_LANDING_DEV].[dbo].[Stat_CDR]
       #               (PrimaryKey,EventTime,DSTStatus,ContactID,EventID,
       #                SwitchID,ContactType,CurrentState,LastState,
       #                LastStateDuration,QueueID,
       #                IntData1,IntData2,IntData3,IntData4,
       #                StrData1,StrData2,StrData3,StrData4, 
       #                EventSequence,ServerId,RolledUp) 
       #                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,
       #                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
       #         END TRY
           
       #         BEGIN CATCH
       #           logging.error(f"skipping failed insert');
       #         END CATCH;
       #       """
           
       #try:
       #    for record in csv_reader:
       #        cursor.execute(sql, record)                   
               
       #except Exception as e:
       #    logging.error(f"Skipping record {record} due to error:{e}")
           
       #conn.commit()
                   
#                   conn.commit()
#                   cursor.close()
#                   conn.close()
           
       
       
       return
           


   source_file_set = ["Stat_CDR"]
  
   for source_file in source_file_set:
       daily_load_source(source_file)

        
dag = daily_load_data()