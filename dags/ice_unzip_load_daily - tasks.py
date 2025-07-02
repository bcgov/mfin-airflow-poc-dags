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
from airflow.providers.smtp.hooks.smtp import SmtpHook
from email.message import EmailMessage
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
import pymssql


root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s -%(message)s')
handler.setFormatter(formatter)
root.addHandler(handler) 

@dag(
    dag_id="ice_etl_load_daily_tasks",
    schedule_interval=None,  # Set your schedule interval or leave as None for manual trigger
    start_date=days_ago(1),
    catchup=False,
    tags=["ice", "etl", "unzip","load","inprogress_folder"]
)

       
def daily_load_data():
    # Log all steps at INFO level
    logging.basicConfig(level=logging.INFO)

    # Task 0: Email notification process
    @task
    def email_notification():
        dYmd = (dt.datetime.today()).strftime('%Y%m%d')
    
        with SmtpHook(smtp_conn_id = 'Email_Notification') as sh:
            sh.send_email_smtp(
               to=['eloy.mendez@gov.bc.ca'],
               subject='Airflow email test',
               html_content='<html><body><h2>Airflow load daily source file failure</h2><p>CT iceDB_ICE_BCMOFRMO-' + dYmd + '.zip file not received</p></body></html>'
        )        
        return
    
    
    # Task 1: ETL process begins - when daily file is not availableRemoving csv data files       
    @task
    def ETLbegin():
        log_path = r'/rmo_ct_prod/log/'
        log_name = 'daily_backup.txt'
        SourcePath = '/rmo_ct_prod/'  
        conn_id = 'fs1_rmo_ice'
        filefound = 0
        
        dYmdHMS = (dt.datetime.today()).strftime('%Y%m%d%H%M%S')
        
        with SambaHook(samba_conn_id=conn_id) as fs_hook:
            with fs_hook.open_file(log_path + log_name,'a') as outfile:
                outfile.write("ETL process begins %s\n" % dYmdHMS)
            
        outfile.close()

        with SambaHook(samba_conn_id=conn_id) as fs_hook:
            files = fs_hook.listdir(SourcePath)
            for f in files:
                if f == 'iceDB_ICE_BCMOFRMO.zip':
                    filefound = 1
                    
        if filefound == 0:
            email_notification()
            raise AirflowFailException ("CT daily extract file not available. ETL stops")
                
        return

    @task
    def ETLend():
        log_path = r'/rmo_ct_prod/log/'
        log_name = 'daily_backup.txt'
        dYmdHMS = (dt.datetime.today()).strftime('%Y%m%d%H%M%S')
        
        with fs_hook.open_file(log_path + log_name,'a') as outfile:
            outfile.write("ETL process ends %s\n" % dYmdHMS)
            
        outfile.close()        
        return
         
    
    # Task 2: Inprogress subfolder - Removing csv data files    
    @task
    def remove_csv_inprogress():
        conn_id = 'fs1_rmo_ice'
        log_path = r'/rmo_ct_prod/log/'
        log_name = 'daily_backup.txt'
        dYmdHMS = (dt.datetime.today()).strftime('%Y%m%d%H%M%S')
        
        with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
            with fs_hook.open_file(log_path + log_name,'a') as outfile:
                outfile.write("ETL step: 2; Task: Remove CSV Inprogress task; Time: %s\n" % dYmdHMS)
      
        outfile.close()
        #DeletePath = r'/rmo_ct_prod/inprogress/'
        DeletePath = Variable.get("vRMOSourcePath")
        hook = SambaHook(conn_id)
        files = hook.listdir(DeletePath)

        try:
            for file in files:
                file_path = f"{DeletePath}/{file}"
                hook.remove(file_path)
        
        except Exception as e:
            logging.error(f"Error {e} removing file: {DeletePath}")
            
        return


    #Task 3: Unzip and Move files from source to destination (using SambaHook)
    @task
    def unzip_move_file():        
        #logging.basicConfig(level=logging.INFO)

        source_path = r'/rmo_ct_prod/'
        dest_path = r'/rmo_ct_prod/inprogress/'
        file = 'iceDB_ICE_BCMOFRMO.zip'
        zip_loc = r'/tmp/'
        log_path = r'/rmo_ct_prod/log/'
        log_name = 'daily_backup.txt'
        dYmdHMS = (dt.datetime.today()).strftime('%Y%m%d%H%M%S')
        
        logging.info("Unzip daily file")  
        
        with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
            with fs_hook.open_file(log_path + log_name,'a') as outfile:
                outfile.write("ETL step: 3; Task: Unzipping and moving file task; Time: %s\n" % dYmdHMS) 
        
        outfile.close()        
            
        try:
            # Initialize SambaHook with your credentials and connection details
            with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
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
    @task
    def backup_daily_source_file():
        log_path = r'/rmo_ct_prod/log/'
        log_name = 'daily_backup.txt'
        
        with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
            with fs_hook.open_file(log_path + log_name,'a') as outfile:
                      
                SourcePath = '/rmo_ct_prod/'
                DestPath = '/rmo_ct_prod/completed/'
                conn_id = 'fs1_rmo_ice'
                file = 'iceDB_ICE_BCMOFRMO.zip'
                hook = SambaHook(conn_id)
                outfile.write("creating SambaHook\n")
                #   Set dYmd to yesterdays date
                dYmd = (dt.datetime.today() + timedelta(days=+2)).strftime('%Y%m%d')
                outfile.write("Setting date extension\n")   
                try:
                    files = hook.listdir(SourcePath)
                    outfile.write("getting hook.listdir\n")

                    for f in files:
                        outfile.write("looking for CT daily RMO source file %s\n" % f)
                        if f == 'iceDB_ICE_BCMOFRMO.zip':
                            hook.replace(SourcePath + f, DestPath + 'iceDB_ICE_BCMOFRMO-' + dYmd+'.zip') 
                            outfile.write("copying file %s to completed folder\n" % f)
                    
                except Exception as e:
                    logging.error(f"Error backing up {file}-{dYmd}.zip source file")
        
                outfile.write(str(foundDailyExtract))
        return foundDailyExtract
   
    
 

    # Task 5: Truncate landing tables prior loading next daily source files    
    @task
    def truncate_landing_tables():
        logging.basicConfig(level=logging.INFO) 
        logging.info(f"truncate_landing_tables procedure")

        #sql_hook = MsSqlHook(mssql_conn_id='mssql_default')
        conn_id = 'mssql_default'
        conn = BaseHook.get_connection(conn_id)
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
            
    # Task 6: Loading csv data files to SQL Server database       
    @task
    def daily_load_source():
        logging.basicConfig(level=logging.INFO)                

        def Agent_Datafix(pSourcePath):
            file = 'Agent.csv'
            output_file = 'Agent_fixed.csv'
            
            logging.info("Agent fixing code")
            
            try:
                # Initialize SambaHook with your credentials and connection details
                with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
                    
                    cols = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47]
                    
                    with fs_hook.open_file(pSourcePath + file,'r') as f:
                        csv_reader = pd.read_csv(f, header = None, usecols=cols, quoting=1)

                    df1 = csv_reader.loc[(csv_reader[1] ==  1137) | (csv_reader[1] == 1888) | (csv_reader[1] == 1889) | (csv_reader[1] == 1890) | (csv_reader[1] == 2001) | (csv_reader[1] == 2003) | (csv_reader[1] == 9985)]    
                    df1 = df1.iloc[:,[0,1,2,3,4,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,32,33,34,35,36,37,38,39,41,42,43]]
                                        
                    df2 = csv_reader.loc[(csv_reader[1] !=  1137) & (csv_reader[1] !=  1148) & (csv_reader[1] != 1888) & (csv_reader[1] != 1889) & (csv_reader[1] != 1890) & (csv_reader[1] != 2001) & (csv_reader[1] != 2003) & (csv_reader[1] != 9985)]    
                    df2 = df2.iloc[:,[0,1,2,3,4,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,36,37,38,39,40,41,42,43,45,46,47]]

                    df3 = csv_reader.loc[(csv_reader[1] ==  1148)]  
                    df3 = df3.iloc[:,[0,1,2,3,4,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,34,35,36,37,38,39,40,41,43,44,45]]

 
                    with fs_hook.open_file(pSourcePath + output_file, 'w') as outfile:
                        df1.to_csv(outfile, header=False,index=False,lineterminator='\r\n')
                                
                    with fs_hook.open_file(pSourcePath + output_file, 'a') as outfile:
                        df2.to_csv(outfile, header=False,index=False,lineterminator='\r\n')
                    
                    with fs_hook.open_file(pSourcePath + output_file, 'a') as outfile:
                        df3.to_csv(outfile, header=False,index=False,lineterminator='\r\n')
                                
                
            except Exception as e:
                logging.error(f"Error data fixing table Agent: {e}")
                
            return   



        def Stat_CDR_Datafix(pSourcePath):
            file = 'Stat_CDR.csv'
            output_file = 'Stat_CDR_fixed.csv'

            logging.info("Stat_CDR fixing code")
            try:
                # Initialize SambaHook with your credentials and connection details
                with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
                    
                    cols = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22]                  
                    
                    with fs_hook.open_file(pSourcePath + file,'r') as f:
                        csv_reader = pd.read_csv(f, header = None, usecols=[i for i in range(22)], quoting=1)
 
                    df1 = csv_reader.loc[:, [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21]]
 
                    with fs_hook.open_file(pSourcePath + output_file, 'w') as outfile:
                            df1.to_csv(outfile, header=False,index=False,lineterminator='\r\n')
  
                                
                outfile.close()
                
            except Exception as e:
                logging.error(f"Error data fixing table Stat_CDR: {e}")
                
            return                     

            
        def Stat_CDR_Summary_Datafix(pSourcePath):
            file = 'Stat_CDR_Summary.csv'
            output_file = 'Stat_CDR_Summary_fixed.csv'

            logging.info("Stat_CDR_Summary_fixing code")
            try:
                # Initialize SambaHook with your credentials and connection details
                with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
                    
                    with fs_hook.open_file(pSourcePath + file,'r') as f:
                        csv_reader = pd.read_csv(f, header = None, usecols=[i for i in range(70)], quoting=1)
 
                    df1 = csv_reader.loc[:,[0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,15,16,17,18,19,
                                            20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,
                                            40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,
                                            60,61,62,63,64,65,66,67,68,69]]                      
                            

                    with fs_hook.open_file(pSourcePath + output_file, 'w') as outfile:
                        df1.to_csv(outfile, header=False,index=False,lineterminator='\r\n')
  
                                
                outfile.close()
                
            except Exception as e:
                logging.error(f"Error data fixing table Stat_CDR_Summary: {e}")
                
            return   
        

        def LOBCodeLangString(pSourcePath):
            file = 'LOBCodeLangString.csv'
            output_file = 'LOBCodeLangString_fixed.csv'

            logging.info("LOBCodeLangString_fixing code")
            try:
                # Initialize SambaHook with your credentials and connection details
                with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
                               
                    with fs_hook.open_file(pSourcePath + file,'r') as f:
                        csv_reader = pd.read_csv(f, header = None, usecols=[i for i in range(3)], quoting=1)   

                    df1 = csv_reader.loc[:,[0,1,2]]
                    
                    with fs_hook.open_file(pSourcePath + output_file, 'w') as outfile:
                        df1.to_csv(outfile, header=False,index=False,lineterminator='\r\n')
                                
                outfile.close()                    
        
            except Exception as e:
                logging.error(f"Error data fixing table LOBCodeLangString {e}")
                
            return   

            
        def EvalCriteriaLangString(pSourcePath):
            file = 'EvalCriteriaLangString.csv'
            output_file = 'EvalCriteriaLangString_fixed.csv'

            logging.info("EvalCriteriaLangString_fixing code")
            try:
                # Initialize SambaHook with your credentials and connection details
                with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
                    
                    with fs_hook.open_file(pSourcePath + file,'r') as f:
                        csv_reader = pd.read_csv(f, header = None, usecols=[i for i in range(3)], quoting=1)   

                    df1 = csv_reader.loc[:,[0,1,2]]
                    
                    with fs_hook.open_file(pSourcePath + output_file, 'w') as outfile:
                        df1.to_csv(outfile, header=False,index=False,lineterminator='\r\n')
  
                                
                outfile.close()                    
        
            except Exception as e:
                logging.error(f"Error data fixing table LOBCodeLangString {e}")
                
            return       

        
        def load_db_source(pSourceFile, pDBName):
            sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')
            dYmd = (dt.datetime.today() + timedelta(days = -1)).strftime('%Y%m%d')

            try:
                if pSourceFile == 'Stat_CDR.csv':
                    vTableName = 'ICE_Stat_CDR'
                    vSourceFile = 'Stat_CDR_fixed.csv'
                elif pSourceFile == 'Agent.csv':
                    vTableName = 'ICE_Agent'
                    vSourceFile = 'Agent_fixed.csv'
                elif pSourceFile == 'Stat_CDR_Summary.csv':
                    vTableName = 'ICE_Stat_CDR_Summary'
                    vSourceFile = 'Stat_CDR_Summary_fixed.csv' 
                elif pSourceFile == 'LOBCodeLangString.csv':   
                    vTableName = 'ICE_LOBCodeLangString'
                    vSourceFile = 'LOBCodeLangString_fixed.csv'
                elif pSourceFile == 'EvalCriteriaLangString.csv':   
                    vTableName = 'ICE_EvalCriteriaLangString'
                    vSourceFile = 'EvalCriteriaLangString_fixed.csv'     
                else:
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
	                         ( FORMAT='CSV', 
                               MAXERRORS=100, 
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
              
        
        #log_path = r'/rmo_ct_prod/log/'
        #log_name = 'daily_set.txt'
        
        ConfigPath = Variable.get("vRMOConfigPath")
        FileName = Variable.get("vConfigName")
        SourcePath = Variable.get("vRMOSourcePath")
                
        source_file_set=[] 
        data = []
      
        DBName = Variable.get("vDatabaseName")

        with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
                
            with fs_hook.open_file(ConfigPath + FileName,'r') as f:
                source_file_set = pd.read_csv(f, header = None, quoting=1)
                data = source_file_set.values.flatten().tolist()   
                
                data_set = [x for x in data if str(x) != 'nan']
                
                #with fs_hook.open_file(log_path + log_name,'w') as outfile:
                #    for item in data_set:
                #        outfile.write("%s\n" % item)
                        
       
        # Data fixes required for relevant daily table process 
        Agent_Datafix(SourcePath)
        Stat_CDR_Datafix(SourcePath)
        Stat_CDR_Summary_Datafix(SourcePath)
        LOBCodeLangString(SourcePath)
        EvalCriteriaLangString(SourcePath)
              
        for source_file in data_set:
            load_db_source(source_file, DBName)
 
 
 #Set task dependencies
 
    ETLbegin() >> remove_csv_inprogress() >> unzip_move_file() >> backup_daily_source_file() >> truncate_landing_tables() >> daily_load_source() >> ETLend()
    
dag = daily_load_data()