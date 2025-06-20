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
from airflow.operators.email import EmailOperator
from airflow.utils.context import Context
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
    dag_id="ice_etl_load_daily_tasks",
    schedule_interval=None,  # Set your schedule interval or leave as None for manual trigger
    start_date=days_ago(1),
    catchup=False,
    tags=["ice", "etl", "unzip","load","inprogress_folder"]
)


def daily_load_data():
    # Log all steps at INFO level
    logging.basicConfig(level=logging.INFO)
    
    #Task 1: Unzip and Move files from source to destination (using SambaHook)
    @task
    def unzip_move_file():        
        #logging.basicConfig(level=logging.INFO)

        source_path = r'/rmo_ct_prod/'
        dest_path = r'/rmo_ct_prod/inprogress/'
        file = 'iceDB_ICE_BCMOFRMO.zip'
        zip_loc = '/tmp'
        
        logging.info("Unzip daily file")        
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
    

    # Task 2: Backup iceDB_ICE_BCMOFRMO-YYYYMMDD.zip to the completed folder 
    @task
    def backup_daily_source_file():
        log_path = r'/rmo_ct_prod/log/'
        log_name = 'daily_backup.txt'
        
        with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
            with fs_hook.open_file(log_path + log_name,'w') as outfile:
                      
                SourcePath = '/rmo_ct_prod/'
                DestPath = '/rmo_ct_prod/completed/'
                conn_id = 'fs1_rmo_ice'
                file = 'iceDB_ICE_BCMOFRMO.zip'
                hook = SambaHook(conn_id)
                outfile.write("creating SambaHook\n")
                #   Set dYmd to yesterdays date
                dYmd = (dt.datetime.today() + timedelta(days=2)).strftime('%Y%m%d')
                outfile.write("Setting date extension\n")   
                foundflag = 0                
                try:
                    files = hook.listdir(SourcePath)
                    outfile.write("getting hook.listdir\n")

                    for f in files:
                        outfile.write("looking for daily RMO file %s\n" % f)
                        if f == 'iceDB_ICE_BCMOFRMO.zip':
                            hook.replace(SourcePath + f, DestPath + 'iceDB_ICE_BCMOFRMO-' + dYmd+'.zip') 
                            outfile.write("copying file %s to completed folder\n" % f)
                            foundflag = 1
                    
                    if foundflag == 0:
                        email_operator = EmailOperator(
                                   task_id = 'Source file not available',
                                   to = 'eloy.mendez@gov.bc.ca',
                                   subject = 'Missing daily iceDB_ICE_BCMOFRMO.zip file',
                                   html_content = '<p>iceDB_ICE_BCMOFRM.zip missing in target folder!/p>',
                                   dag = None
                                   )
                        
                        context = {'ds' : str(datetime.today()),
                                   'ts' : str(datetime.now()),
                                   'task_instance' : None,
                                  }
                        
                        email_operator.execute(context = context)
                    
                except Exception as e:
                    logging.error(f"Error backing up {file}-{dYmd}.zip source file")
        
        return
 
    # Task 3: Inprogress subfolder - Removing csv data files    
    @task
    def remove_csv_inprogress():
        conn_id = 'fs1_rmo_ice'
      
        #DeletePath = r'/rmo_ct_prod/inprogress/'
        DeletePath = Variable.get("vRMOSourcePath")
        hook = SambaHook(conn_id)
        files = hook.listdir(DeletePath)

        try:
            for file in files:
                file_path = f"{DeletePath}/{file}"
                hook.remove(file_path)
        except:
            logging.error(f"Error {e} removing file: {DeletePath}")
            
        return

    # Task 4: Truncate landing tables prior loading next daily source files    
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
 
            #conn = sql_hook.get_conn()
            #cursor = conn.cursor()
            #query = f"""EXEC [FIN_SHARED_LANDING_DEV].[dbo].[PROC_TELEPHONY_ICE_TRUNCATE];"""
            #cursor.execute(query)
            #conn.commit()
            #cursor.execute("EXECUTE PROC_TELEPHONY_ICE_TRUNCATE")
            #cursor.execute("TRUNCATE TABLE FIN_SHARED_LANDING_DEV.dbo.ICE_Stat_QueueActivity_D")
 
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
            
    # Task 5: Loading csv data files to SQL Server database       
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
    remove_csv_inprogress() >> unzip_move_file() >> backup_daily_source_file() >> truncate_landing_tables() >> daily_load_source() 
    
dag = daily_load_data()