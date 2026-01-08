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


def email_completion():
    dYmdHMS = (dt.datetime.today()+ timedelta(days=-1)).strftime('%Y-%m-%d:%H%M%S')
    
    with SmtpHook(smtp_conn_id = 'Email_Notification') as sh:
        sh.send_email_smtp(
           to=['eloy.mendez@gov.bc.ca'],
           subject='Airflow ETL Process Notification',
           html_content='<html><body><h2>Airflow RMO-ETL daily source file completion</h2><p>CT iceDB_ICE_BCMOFRMO-' + dYmdHMS + '.zip daily file processed succesfully </p></body></html>'
    )        
    return

def email_notification():
    LogPath = Variable.get("vRMOLogPath")
    LogName = 'daily_etl.txt'
    conn_id = 'fs1_prod_conn'
    dYmdHMS = (dt.datetime.today() - timedelta(hours=7)).strftime('%Y-%m-%d:%H%M%S')
        
    with SambaHook(samba_conn_id=conn_id) as fs_hook:
        with fs_hook.open_file(LogPath + LogName,'a') as outfile:
            outfile.writelines("Time:%s,Step:2,Task:ETL process failed,Description:ETL process stops NO daily extract available for processing\n" % dYmdHMS)
            
    outfile.close()

    dYmd = (dt.datetime.today()+ timedelta(days=-1)).strftime('%Y-%m-%d')
    
    with SmtpHook(smtp_conn_id = 'Email_Notification') as sh:
        sh.send_email_smtp(
           to=['eloy.mendez@gov.bc.ca'],
           subject='Airflow Email Notification',
           html_content='<html><body><h2>Airflow RMO daily source file failure</h2><p>CT iceDB_ICE_BCMOFRMO-' + dYmd + '.zip file not received/available</p></body></html>'
    )        
    return


def choose_path():
    LogPath = Variable.get("vRMOLogPath")
    SourcePath = Variable.get("vRMOSourcePath")
    LogName = 'daily_etl.txt'
    conn_id = 'fs1_prod_conn'
    filefound = 0        
    dYmdHMS = (dt.datetime.today() - timedelta(hours=7)).strftime('%Y-%m-%d:%H%M%S')
        
    with SambaHook(samba_conn_id=conn_id) as fs_hook:
        with fs_hook.open_file(LogPath + LogName,'w') as outfile:
            outfile.writelines("Time:%s,Step:1,Task:ETL process,Description:Starting ETL process\n" % dYmdHMS)

        outfile.close()
        files = fs_hook.listdir(SourcePath)
        for f in files:
            if f == 'iceDB_ICE_BCMOFRMO.zip':
                filefound = 1
				
        if filefound == 0:		    
            return 'path_email'
        else:
            return 'path_daily_load'


def etl_remove(pconn_id):
        
    with SambaHook(samba_conn_id=pconn_id) as fs_hook:
        ProgressPath = Variable.get("vRMOProgressPath")
        files = fs_hook.listdir(ProgressPath)

        try:
            for file in files:
                file_path = f"{ProgressPath}/{file}"
                fs_hook.remove(file_path)
        
        except Exception as e:
            logging.error(f"Error {e} removing file: {r'/rmo_ct_prod/inprogress'}")
            
    return
    

def log_remove(pconn_id):
        
    with SambaHook(samba_conn_id=pconn_id) as fs_hook:
        DeletePath = Variable.get("vPTBLogPath")
        files = fs_hook.listdir(DeletePath)

        try:
            for file in files:
                file_path = f"{DeletePath}/{file}"
                fs_hook.remove(file_path)
        
        except Exception as e:
            logging.error(f"Error {e} removing files in log folder: {DeletePath}")
            
    return

    
#Task 3: Unzip and Move files from source to destination (using SambaHook)    
def etl_unzip(pconn_id):
    SourcePath = Variable.get("vRMOSourcePath")
    DestPath = Variable.get("vRMOProgressPath")
    file = 'iceDB_ICE_BCMOFRMO.zip'
    zip_loc = r'/tmp/'
    logging.info("Unzip daily file")  
        
    with SambaHook(samba_conn_id=pconn_id) as fs_hook:            
        try:
            # Initialize SambaHook with your credentials and connection details
            with fs_hook.open_file(SourcePath + file,'rb') as f:
                z = zipfile.ZipFile(f)
                for iceTable in z.infolist():
                    logging.info(iceTable.filename)
                    z.extract(iceTable.filename,path=zip_loc)
            
                    fs_hook.push_from_local(DestPath + iceTable.filename, os.path.join(zip_loc,iceTable.filename))
                    
        except Exception as e:
            logging.error(f"Task 3: Error unzipping files: {e}")  

    return       
    
# Task 4: Backup iceDB_ICE_BCMOFRMO-YYYYMMDD.zip to the completed folder    
def etl_backup(pconn_id):
        
    with SambaHook(samba_conn_id=pconn_id) as fs_hook:
        SourcePath = Variable.get("vRMOSourcePath")
        DestPath = Variable.get("vRMOCompletePath")
        file = 'iceDB_ICE_BCMOFRMO.zip'
        # Set dYmd to yesterdays date
        dYmd = (dt.datetime.today() + timedelta(days=-1)).strftime('%Y%m%d')
        try:
            files = fs_hook.listdir(SourcePath)
 
            for f in files:
                if f == 'iceDB_ICE_BCMOFRMO.zip':
                    fs_hook.replace(SourcePath + f, DestPath + 'iceDB_ICE_BCMOFRMO-' + dYmd+'.zip') 
                    
        except Exception as e:
            logging.error(f"Task 4: Error backing up {file}-{dYmd}.zip source file")
        
    return   
   

# Task 5: Truncate landing tables prior loading next daily source files    
def etl_truncate():
    logging.basicConfig(level=logging.INFO) 
    logging.info(f"truncate_landing_tables procedure")

    conn_id = 'mssql_default'
#   conn_id = 'mssql_conn_finafdbt
#   conn_id = 'mssql_conn_finafdbp    
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


# Task 6: Loading daily data from landing tables to target tables in the database     
def loading_target_tables_db():
    logging.basicConfig(level=logging.INFO) 
    logging.info(f"loading_db_data_procedure")

    conn_id = 'mssql_default'
#   conn_id = 'mssql_conn_finafdbt
#   conn_id = 'mssql_conn_finafdbp     
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
        cursor.execute("EXEC [FIN_SHARED_STAGING_DEV].[dbo].[PROC_TELEPHONY_RMO_BUILD_ALL]")            
        connection.commit()                                  
        logging.info(f"Truncate RMO landing tables {time.time() - start_time} seconds")
        
    except Exception as e:
        logging.error(f"Task 6: Error loading RMO data to db target tables {e}")
        
    finally:
        if connection:
            connection.close()
            logging.info(f"Database {dbname} - Connection closed")
 
    return

   

# Task 7: Loading daily csv data files to FIN_SHARED SQL Server database
def etl_daily_load():
    # Log all steps at INFO level
    logging.basicConfig(level=logging.INFO)
        
       
       
    def load_db_source(pSourceFile, pDBName):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')
        dYmd = (dt.datetime.today() + timedelta(days = -1)).strftime('%Y%m%d')

        try:
            xlen = len(pSourceFile)-4
            vTableName = 'ICE_' + pSourceFile[:xlen]
            vSourceFile = pSourceFile
            logging.info(f"loading table: {vTableName}")            
            conn = sql_hook.get_conn()
            cursor = conn.cursor()                
            vRMOInProgress = Variable.get("vRMOProgressPath")
            vRMOLog = Variable.get("vRMOLogPath")
            vFS1 = Variable.get("vFS1")
            
            query = f""" BULK INSERT [{pDBName}].[dbo].[{vTableName}]
                         FROM '\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\inprogress\\{vSourceFile}'
                         WITH
	                     ( FORMAT = 'CSV',
                           ROWTERMINATOR = '\r\n',
                           MAXERRORS = 100, 
                           ERRORFILE='\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\log\\{vTableName}_{dYmd}.log',
                           TABLOCK 
	                     );
                     """

            logging.info(f"query: {query}")
            logging.info(f"inserting table:  {vSourceFile}")
            start_time = time.time()
            cursor.execute(query)
            conn.commit()                                  
            logging.info(f"bulk insert {time.time() - start_time} seconds")
        
        except Exception as e:
            logging.error(f"Task 7: Error bulk loading table: {vTableName} source file: {vSourceFile} {e}")
               
        return
              
        
    conn_id = 'fs1_prod_conn'
    LogPath = Variable.get("vRMOLogPath")
    #log_path = '/rmo_ct_prod/log/'
    ConfigPath = Variable.get("vRMOConfigPath")
    FileName = Variable.get("vConfigName")
    SourcePath = Variable.get("vRMOSourcePath")                
    DBName = Variable.get("vDatabaseName")

    log_name = 'daily_set.txt'  
    log_etl = 'daily_etl.txt'    
    dYmdHMS = (dt.datetime.today() - timedelta(hours=7)).strftime('%Y-%m-%d:%H%M%S')    
    source_file_set=[] 
    data = []      

    with SambaHook(samba_conn_id=conn_id) as fs_hook:                
        with fs_hook.open_file(ConfigPath + FileName,'r') as f:
            source_file_set = pd.read_csv(f, header = None, quoting=1)
            data = source_file_set.values.flatten().tolist()
            data_set = [x for x in data if str(x) != 'nan']
            
        with fs_hook.open_file(LogPath + log_etl,'a') as outfile:
            dYmdHMS = (dt.datetime.today() - timedelta(hours=7)).strftime('%Y-%m-%d:%H%M%S')
            outfile.writelines("Time:%s,Step:2,Task:Removing old data extract,Description:Remove old CSV file Inprogress folder task\n" % dYmdHMS)
            etl_remove(conn_id)

            dYmdHMS = (dt.datetime.today() - timedelta(hours=7)).strftime('%Y-%m-%d:%H%M%S')
            outfile.writelines("Time:%s,Step:2.1,Task:Removing previous log data,Description:Remove old Log files Log folder task\n" % dYmdHMS)
            log_remove(conn_id)    
            
            dYmdHMS = (dt.datetime.today() - timedelta(hours=7)).strftime('%Y-%m-%d:%H%M%S')
            outfile.writelines("Time:%s,Step:3,Task:Unzipping CSV files,Description:Unzipping daily CSV extract file task\n" % dYmdHMS)
            etl_unzip(conn_id)

            dYmdHMS = (dt.datetime.today() - timedelta(hours=7)).strftime('%Y-%m-%d:%H%M%S')
            outfile.writelines("Time:%s,Step:4,Task:Backing up extract,Description:Backing up CT daily source file task\n" % dYmdHMS)
            etl_backup(conn_id)
            
            dYmdHMS = (dt.datetime.today() - timedelta(hours=7)).strftime('%Y-%m-%d:%H%M%S')
            outfile.writelines("Time:%s,Step:5,Task:cTruncating tables,Description:Truncating landing tables in DB task\n" % dYmdHMS)
            etl_truncate()
            
            dYmdHMS = (dt.datetime.today() - timedelta(hours=7)).strftime('%Y-%m-%d:%H%M%S')
            outfile.writelines("Time:%s,Step:6,Task:DB data load,Description:Loading csv daily data to FIN_SHARED_LANDING_Env tables task\n" % dYmdHMS)
    
            for source_file in data_set:
                load_db_source(source_file, DBName)
                
            dYmdHMS = (dt.datetime.today() - timedelta(hours=7)).strftime('%Y-%m-%d:%H%M%S')
            outfile.writelines("Time:%s,Step:7,Task:Loading targte tables in database,Description: Loading target tables from landing tables in DB task\n" % dYmdHMS)
            loading_target_tables_db()                        
            
            dYmdHMS = (dt.datetime.today() - timedelta(hours=7)).strftime('%Y-%m-%d:%H%M%S')
            outfile.writelines("Time:%s,Step:8,Task:ETL process completed,Description:ETL process completed successfully task\n" % dYmdHMS)
        
    outfile.close()    
    
    email_completion()
    
    return
    

def create_csv_dag():
    dag = DAG(
        dag_id = 'ice_rmo_CSV_task',
        start_date = days_ago(1),
#        schedule_interval = "01 15 * * *",
        schedule_interval = None,
        catchup = False,
        tags = ["ice","rmo","etl","CSV task"]
    )
    
    start = DummyOperator(
        task_id = 'start',
        dag=dag
    )

    branch_decision = BranchPythonOperator(
        task_id = 'branch_decision',
        python_callable = choose_path,
        dag = dag  
    ) 
    
    path_daily_load = PythonOperator(
        task_id = 'path_daily_load',
        python_callable = etl_daily_load,
        dag = dag
    )
    
    path_email = PythonOperator(
        task_id = 'path_email',
        python_callable = email_notification,
        dag = dag
    )
    
    end = DummyOperator(
        task_id = 'end',
        trigger_rule = 'none_failed_min_one_success',
        dag = dag
    )
    
    
    start >> branch_decision >> [path_email, path_daily_load] >> end
    
    return dag
    
    
dag = create_csv_dag()