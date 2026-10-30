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

#
# Author: Eloy Mendez
# Date  : October 29, 2026 
# Assumptions:
#         Prior to executing this DAG source files need to be located in RMO's ondemand folder
#         Include all required csv source files in the \\fs1.fin.gov.bc.ca\rmo_ct_prod\ondemand folder
#         All files need to have a unique csv file name
#

root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s -%(message)s')
handler.setFormatter(formatter)
root.addHandler(handler) 

@dag(
    description="DAG - Ondemand loading source stat tables",
    schedule=None,
    catchup=False,
    tags=["ice","ondemand","load","source file"]
)

def ice_rmo_stat_load_ondemand():
    logging.basicConfig(level=logging.INFO)
    
  
    
    def ondemand_stat_load_source(pSourceFile):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')
        dYmd = (dt.datetime.today() + timedelta(days = -1)).strftime('%Y%m%d')

        try:
            xlen = len(pSourceFile)-4
            vTableName = 'ICE_' + pSourceFile[:xlen]
            vSourceFile = pSourceFile
            logging.info(f"loading table: {vTableName}")            

            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            query = f""" BULK INSERT [FIN_SHARED_LANDING_DEV].[dbo].[{vTableName}]
                    FROM '\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\ondemand\\{vSourceFile}'
                    WITH
	                ( FIELDTERMINATOR = '|',
                      ROWTERMINATOR = '\r\n',
                      MAXERRORS = 20,  
                      ERRORFILE='\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\log\\{vTableName}_{dYmd}.log',
                      TABLOCK
	                );
                """
            logging.info(f"query: {query}")
            logging.info(f"inserting table: {vTableName}")
            start_time = time.time()
            cursor.execute(query)
            conn.commit()
                            
            logging.info(f"bulk insert {time.time() - start_time} seconds")
       
        
        except Exception as e:
            logging.error(f"Error bulk loading ondemand tables: {vTableName} source file: {vSourceFile} {e}")
        
        return
 
 
    def truncate_target_tables_db():
        logging.basicConfig(level=logging.INFO) 
        logging.info(f"loading_ondemand_db_data")

        conn_id = 'mssql_default'
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
            cursor.execute("EXEC [FIN_SHARED_STAGING_DEV].[dbo].[PROC_TELEPHONY_ICE_ONDEMAND_TRUNCATE]")            
            connection.commit()                                  
            logging.info(f"Load OnDemand truncate tables {time.time() - start_time} seconds")
        
        except Exception as e:
            logging.error(f"Task 6: Error OnDemand truncate target tables {e}")
        
        finally:
            if connection:
                connection.close()
                logging.info(f"Database {dbname} - Connection closed")
 
        return
 
 
    def load_target_tables_db():
        logging.basicConfig(level=logging.INFO) 
        logging.info(f"loading_ondemand_db_data")

        conn_id = 'mssql_default'
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
            cursor.execute("EXEC [FIN_SHARED_STAGING_DEV].[dbo].[PROC_TELEPHONY_ICE_ONDEMAND_LOAD]")            
            connection.commit()                                  
            logging.info(f"Load OnDemand landing tables {time.time() - start_time} seconds")
        
        except Exception as e:
            logging.error(f"Task 6: Error OnDemand data to db target tables {e}")
        
        finally:
            if connection:
                connection.close()
                logging.info(f"Database {dbname} - Connection closed")
 
        return
       

    @task
    def ondemand_stat_load_data():
        
        conn_id = 'fs1_prod_conn'
        ConfigPath = Variable.get("vRMOConfigPath")
        FileName = 'ondemand_file_names.csv'
        
        truncate_target_tables_db()
        
        with SambaHook(samba_conn_id=conn_id) as fs_hook:
            with fs_hook.open_file(ConfigPath + FileName,'r') as f:
                source_file_set = pd.read_csv(f, header = None, quoting=1)
                data = source_file_set.values.flatten().tolist()
                data_set = [x for x in data if str(x) != 'nan']
        
                #source_file_set =["Stat_ADR.csv",
                #                  "Stat_AgentActivity_D.csv","Stat_AgentActivity_I.csv", "Stat_AgentActivity_M.csv","Stat_AgentActivity_W.csv","Stat_AgentActivity_Y.csv",
                #                  "Stat_AgentActivityByQueue_D.csv","Stat_AgentActivityByQueue_I.csv","Stat_AgentActivityByQueue_M.csv", "Stat_AgentActivityByQueue_W.csv","Stat_AgentActivityByQueue_Y.csv",
                #                  "Stat_AgentLineOfBusiness_D.csv","Stat_AgentLineOfBusiness_I.csv","Stat_AgentLineOfBusiness_M.csv", "Stat_AgentLineOfBusiness_W","Stat_AgentLineOfBusiness_Y.csv",
                #                  "Stat_AgentNotReadyBreakdown_D.csv","Stat_AgentNotReadyBreakdown_I.csv","Stat_AgentNotReadyBreakdown_M.csv", "Stat_AgentNotReadyBreakdown_W.csv","Stat_AgentNotReadyBreakdown_Y.csv",
                #                  "Stat_CDR.csv","Stat_CDR_Summary.csv",
                #                  "Stat_DNISActivity_D.csv","Stat_DNISActivity_I.csv","Stat_DNISActivity_M.csv","Stat_DNISActivity_W.csv","Stat_DNISActivity_Y.csv",
                #                  "Stat_QueueActivity_D.csv","Stat_QueueActivity_I.csv","Stat_QueueActivity_M.csv","Stat_QueueActivity_W.csv","Stat_QueueActivity_Y.csv",
                #                  "Stat_SkillActivity_D.csv","Stat_SkillActivity_I.csv","Stat_SkillActivity_M.csv","Stat_SkillActivity_W.csv","Stat_SkillActivity_Y.csv",
                #                  "Stat_WorkflowActionActivity_D.csv","Stat_WorkflowActionActivity_I.csv","Stat_WorkflowActionActivity_M.csv","Stat_WorkflowActionActivity_W.csv","Stat_WorkflowActionActivity_Y.csv"
                #                 ]
        
                for SourceFile in data_set:
                    ondemand_stat_load_source(SourceFile)
                    
        
        load_target_tables_db()

    ondemand_stat_load_data()
    
ice_rmo_stat_load_ondemand()