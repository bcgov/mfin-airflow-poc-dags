import csv, sys, argparse
from airflow import DAG
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
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.smtp.hooks.smtp import SmtpHook
from email.message import EmailMessage
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
import pymssql



root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s -%(message)s')
handler.setFormatter(formatter)
root.addHandler(handler) 

@dag(
    description="DAG - PTB CT specific dates bulk insert",
    schedule=None,
    catchup=False,
    tags=["ice","ondemand","load","PTB source file"]
)

def ice_ptb_load_ondemand_CDR_Summary():
    logging.basicConfig(level=logging.INFO)
    

    def log_remove():
        with SambaHook(samba_conn_id='fs1_prod_conn') as fs_hook:
            DeletePath = Variable.get("vPTBLogPath")
            files = fs_hook.listdir(DeletePath)

            try:
                for file in files:
                    file_path = f"{DeletePath}/{file}"
                    fs_hook.remove(file_path)
        
            except Exception as e:
                logging.error(f"Error {e} removing files in log folder: {DeletePath}")
            
        return

    
    def etl_truncate():
        logging.basicConfig(level=logging.INFO) 
        logging.info(f"truncate_landing_tables procedure")

        conn_id = 'mssql_default'
#       conn_id = 'mssql_conn_finafdbt
#       conn_id = 'mssql_conn_finafdbp    
        conn = BaseHook.get_connection(conn_id)
        dbname = Variable.get("vDatabaseName")
        host = conn.host
        user = conn.login
        password = conn.password        
        connection = None
                
        try:
            connection =  pymssql.connect(host = host, database = dbname, user = user, password = password)
            cursor = connection.cursor()                    
            start_time = time.time()
            cursor.execute("EXEC [dbo].[PROC_TELEPHONY_ICE_TRUNCATE]")            
            connection.commit()                                  
            logging.info(f"truncate landing tables {time.time() - start_time} seconds")
        
        except Exception as e:
            logging.error(f"Task 5: Error truncating landing tables {e}")
        
        finally:
            if connection:
                connection.close()
                logging.info(f"Database {dbname} - Connection closed")
 
        return



    def loading_target_tables_db():
        logging.basicConfig(level=logging.INFO) 
        logging.info(f"loading_db_data_procedure")

        conn_id = 'mssql_default'
#       conn_id = 'mssql_conn_finafdbt
#       conn_id = 'mssql_conn_finafdbp     
        conn = BaseHook.get_connection(conn_id)
        dbname = Variable.get("vDatabaseName")
        host = conn.host
        user = conn.login
        password = conn.password        
        connection = None
                
        try:
            connection =  pymssql.connect(host = host, database = dbname, user = user, password = password)
            cursor = connection.cursor()                    
            start_time = time.time()
            cursor.execute("EXEC [FIN_SHARED_STAGING_DEV].[dbo].[PROC_TELEPHONY_PTB_BUILD_ALL]")            
            connection.commit()                                  
            logging.info(f"Truncate PTB landing tables {time.time() - start_time} seconds")
        
        except Exception as e:
            logging.error(f"Task 6: Error loading PTB data to db target tables {e}")
        
        finally:
            if connection:
                connection.close()
                logging.info(f"Database {dbname} - Connection closed")
 
        return

    
    def ondemand_load_source(psource_file):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')
        dYmd = (dt.datetime.today() + timedelta(days = -1)).strftime('%Y%m%d')

        try:
            pTableName = "ICE_Stat_CDR_Summary"            
            logging.info(f"loading table: {pTableName}")
            
            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            query = f""" BULK INSERT [FIN_SHARED_LANDING_DEV].[dbo].[{pTableName}]
                    FROM '\\\\fs1.fin.gov.bc.ca\\ptb_ct_prod\\ondemand\\{psource_file}'
                    WITH
	                ( FIELDTERMINATOR = '|',
                      ROWTERMINATOR = '\r\n',                         
                      MAXERRORS = 20, 
                      ERRORFILE='\\\\fs1.fin.gov.bc.ca\\ptb_ct_prod\\log\\{psource_file}_{dYmd}.log',
                      TABLOCK 
	                );
                """
            logging.info(f"query: {query}")
            logging.info(f"inserting table:  {pTableName}")
            start_time = time.time()
            cursor.execute(query)
            conn.commit()
            
                      
            logging.info(f"bulk insert {time.time() - start_time} seconds")
       
        
        except Exception as e:
            logging.error(f"Error bulk loading table: {pTableName} source file: {psource_file} {e}")
 

    @task
    def ondemand_load_data():
        
        source_file_set = ["Stat_CDR_Summary01.csv","Stat_CDR_Summary02.csv","Stat_CDR_Summary03.csv","Stat_CDR_Summary04.csv","Stat_CDR_Summary05.csv","Stat_CDR_Summary06.csv","Stat_CDR_Summary07.csv","Stat_CDR_Summary08.csv","Stat_CDR_Summary09.csv","Stat_CDR_Summary10.csv",
                           "Stat_CDR_Summary11.csv","Stat_CDR_Summary12.csv","Stat_CDR_Summary13.csv","Stat_CDR_Summary14.csv","Stat_CDR_Summary15.csv","Stat_CDR_Summary16.csv","Stat_CDR_Summary17.csv","Stat_CDR_Summary18.csv","Stat_CDR_Summary19.csv","Stat_CDR_Summary20.csv",
                           "Stat_CDR_Summary21.csv","Stat_CDR_Summary22.csv","Stat_CDR_Summary23.csv","Stat_CDR_Summary24.csv","Stat_CDR_Summary25.csv","Stat_CDR_Summary26.csv","Stat_CDR_Summary27.csv","Stat_CDR_Summary28.csv","Stat_CDR_Summary29.csv","Stat_CDR_Summary30.csv"
                          ]
        
        log_remove()
        etl_truncate()       
        
        for source_file in source_file_set:
            ondemand_load_source(source_file)

        loading_target_tables_db() 
        
        return

    ondemand_load_data()
    
ice_ptb_load_ondemand_CDR_Summary()