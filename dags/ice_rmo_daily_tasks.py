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
        log_path = r'/rmo_ct_prod/log/'
        log_name = 'daily_etl.txt'
        conn_id = 'fs1_prod_conn'
        dYmdHMS = (dt.datetime.today()).strftime('%Y-%m-%d:%H%M%S')
        
        with SambaHook(samba_conn_id=conn_id) as fs_hook:
            with fs_hook.open_file(log_path + log_name,'a') as outfile:
                outfile.writelines("ETL setp: 2, Task: ETL process stops NO daily extract available for processing, Time: %s\n" % dYmdHMS)
            
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
    log_path = r'/rmo_ct_prod/log/'
    log_name = 'daily_etl.txt'
    SourcePath = '/rmo_ct_prod/'  
    conn_id = 'fs1_prod_conn'
    filefound = 0        
    dYmdHMS = (dt.datetime.today()).strftime('%Y-%m-%d:%H%M%S')
        
    with SambaHook(samba_conn_id=conn_id) as fs_hook:
        with fs_hook.open_file(log_path + log_name,'w') as outfile:
            outfile.writelines("ETL setp: 1, Task: ETL process task begins, Time: %s\n" % dYmdHMS)
            
        outfile.close()
        files = fs_hook.listdir(SourcePath)
        for f in files:
            if f == 'iceDB_ICE_BCMOFRMO.zip':
                filefound = 1
				
        if filefound == 0:		    
            return 'path_email'
        else:
            return 'path_daily_load'


def etl_remove(pconn_id, plog_path, plog_name):
        
    with SambaHook(samba_conn_id=pconn_id) as fs_hook:
        DeletePath = Variable.get("vRMOSourcePath")
        files = fs_hook.listdir(DeletePath)

        try:
            for file in files:
                file_path = f"{DeletePath}/{file}"
                fs_hook.remove(file_path)
        
        except Exception as e:
            logging.error(f"Error {e} removing file: {DeletePath}")
            
    return
    
#Task 3: Unzip and Move files from source to destination (using SambaHook)    
def etl_unzip(pconn_id, plog_path, plog_name):
    source_path = r'/rmo_ct_prod/'
    dest_path = r'/rmo_ct_prod/inprogress/'
    file = 'iceDB_ICE_BCMOFRMO.zip'
    zip_loc = r'/tmp/'
    logging.info("Unzip daily file")  
        
    with SambaHook(samba_conn_id=pconn_id) as fs_hook:
            
        try:
            # Initialize SambaHook with your credentials and connection details
            with fs_hook.open_file(source_path + file,'rb') as f:
                z = zipfile.ZipFile(f)
                for iceTable in z.infolist():
                    logging.info(iceTable.filename)
                    z.extract(iceTable.filename,path=zip_loc)
            
                    fs_hook.push_from_local(dest_path+iceTable.filename, os.path.join(zip_loc,iceTable.filename))
                    
        except Exception as e:
            logging.error(f"Error unzipping files: {e}")  

    return       
    
# Task 4: Backup iceDB_ICE_BCMOFRMO-YYYYMMDD.zip to the completed folder    
def etl_backup(pconn_id, plog_path, plog_name):
        
    with SambaHook(samba_conn_id=pconn_id) as fs_hook:
        SourcePath = '/rmo_ct_prod/'
        DestPath = '/rmo_ct_prod/completed/'
        file = 'iceDB_ICE_BCMOFRMO.zip'
        # Set dYmd to yesterdays date
        dYmd = (dt.datetime.today() + timedelta(days=-1)).strftime('%Y%m%d')
        try:
            files = fs_hook.listdir(SourcePath)
 
            for f in files:
                if f == 'iceDB_ICE_BCMOFRMO.zip':
                    fs_hook.replace(SourcePath + f, DestPath + 'iceDB_ICE_BCMOFRMO-' + dYmd+'.zip') 
                    
        except Exception as e:
            logging.error(f"Error backing up {file}-{dYmd}.zip source file")
        
    return   
   

# Task 5: Truncate landing tables prior loading next daily source files    
def etl_truncate(pconn_id, plog_path, plog_name):
    logging.basicConfig(level=logging.INFO) 
    logging.info(f"truncate_landing_tables procedure")

    conn_id = 'mssql_default'
    conn = BaseHook.get_connection(pconn_id)
    dbname = 'FIN_SHARED_LANDING_DEV'
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
        logging.error(f"Error truncating landing tables {e}")
        
    finally:
        if connection:
            connection.close()
            logging.info(f"Database {dbname} - Connection closed")
 
    return
   

# Task 6: Loading daily csv data files to FIN_SHARED SQL Server database
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
            vRMOInProgress = Variable.get("vRMOInProgressFolder")
            vRMOLog = Variable.get("vRMOLogFolder")
            
            query = f""" BULK INSERT [{pDBName}].[dbo].[{vTableName}]
                         FROM '{vRMOInProgress}{vSourceFile}'
                         WITH
	                     ( FIELDTERMINATOR = '|',
                           ROWTERMINATOR = '\r\n',                         
                           MAXERRORS = 20, 
                           ERRORFILE='{vRMOLog}{vTableName}_{dYmd}.log',
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
            logging.error(f"Error bulk loading table: {vTableName} source file: {vSourceFile} {e}")
               
        return
              
        
    conn_id = 'fs1_prod_conn'
    log_path = '/rmo_ct_prod/log/'
    log_name = 'daily_set.txt'  
    log_etl = 'daily_etl.txt'    
    dYmdHMS = (dt.datetime.today()).strftime('%Y-%m-%d:%H%M%S')
    
    with SambaHook(samba_conn_id=conn_id) as fs_hook:
        with fs_hook.open_file(log_path + log_name,'a') as outfile:
                outfile.writelines("ETL step: 6, Task: Loading csv data to FIN_SHARED_LANDING_DEV tables, Time: %s\n" % dYmdHMS)

    outfile.close()        
    ConfigPath = Variable.get("vRMOConfigPath")
    FileName = Variable.get("vConfigName")
    SourcePath = Variable.get("vRMOSourcePath")                
    source_file_set=[] 
    data = []      
    DBName = Variable.get("vDatabaseName")

    with SambaHook(samba_conn_id=conn_id) as fs_hook:                
        with fs_hook.open_file(ConfigPath + FileName,'r') as f:
            source_file_set = pd.read_csv(f, header = None, quoting=1)
            data = source_file_set.values.flatten().tolist()
            data_set = [x for x in data if str(x) != 'nan']
            
        with fs_hook.open_file(log_path + log_name,'a') as outfile:
            dYmdHMS = (dt.datetime.today()).strftime('%Y-%m-%d:%H%M%S')
            outfile.writelines("ETL Remove Step: 2, Task: Remove CSV Inprogress task, Time: %s\n" % dYmdHMS)   
            etl_remove(conn_id, log_path, log_name)
    
            dYmdHMS = (dt.datetime.today()).strftime('%Y-%m-%d:%H%M%S')
            outfile.writelines("ETL Unzip Step: 3, Task: Unzipping daily CSV file task, Time: %s\n" % dYmdHMS)   
            etl_unzip(conn_id, log_path, log_name)

            dYmdHMS = (dt.datetime.today()).strftime('%Y-%m-%d:%H%M%S')
            outfile.writelines("ETL Backup Step: 4, Task: Backing up CT daily source file task, Time: %s\n" % dYmdHMS)            
            etl_backup(conn_id, log_path, log_name)
            
            dYmdHMS = (dt.datetime.today()).strftime('%Y-%m-%d:%H%M%S')
            outfile.writelines("ETL Truncating Landing tables Step: 5, Task: Truncating landing tables in DB task, Time: %s\n" % dYmdHMS)            
            etl_truncate(conn_id, log_path, log_name)
    
            for source_file in data_set:
                load_db_source(source_file, DBName)
        
            dYmdHMS = (dt.datetime.today()).strftime('%Y-%m-%d:%H%M%S')
            outfile.writelines("ETL step: 8, Task: ETL process completed, Time: %s\n" % dYmdHMS)

    outfile.close()    
    
    email_completion()
    
    return
    

def create_dag():
    dag = DAG(
        dag_id = 'ice_rmo_daily_task',
        start_date = days_ago(1),
        schedule_interval = None,
        catchup = False,
        tags = ["ice","rmo","etl","daily_task"]
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
    
    
dag = create_dag()