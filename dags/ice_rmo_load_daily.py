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

def ice_rmo_load_daily():
   
   @task   
   def daily_load_source(psource_file):
       sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')
       
       conn = sql_hook.get_conn()
       cursor = conn.cursor()
       
       source_path = r'/rmo_ct_prod/inprogress/'
       file = 'Stat_CDR.csv'

       try:
           with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
               with fs_hook.open_file(source_path + file,'r') as f:
                   csv_reader = pd.read_csv(f, header = None, usecols=[i for i in range(22)])
                   
           sql = """
           BEGIN TRY
             INSERT INTO [FIN_SHARED_LANDING_DEV].[dbo].[Stat_CDR]
                     (PrimaryKey,EventTime,DSTStatus,ContactID,EventID,
                      SwitchID,ContactType,CurrentState,LastState,
                      LastStateDuration,QueueID,
                      IntData1,IntData2,IntData3,IntData4,
                      StrData1,StrData2,StrData3,StrData4, 
                      EventSequence,ServerId,RolledUp) 
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,
                              %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
           END TRY
           
           BEGIN CATCH
             logging.error(f"skipping failed insert');
           END CATCH;
           """
           
           for record in csv_reader:
               try:
                   cursor.execute(sql,record)
               except Expection as e:
                   logging.error(f"Skipping record {record} due to error:{e}")
                   
           conn.commit()
           cursor.close()
           conn.close()
           
       except Exception as d:
           logging.error(f"Error opening source folder {d}")
       
       return
           

    @task
    def daily_load_data():
        
        source_file_set = ["Stat_CDR"]

        
        for source_file in source_file_set:
            daily_load_source(source_file)

    daily_load_data(), 
    
ice_rmo_load_daily()