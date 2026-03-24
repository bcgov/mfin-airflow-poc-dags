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
    description="DAG - RMO CT specific dates bulk insert",
    schedule=None,
    catchup=False,
    tags=["ice","ondemand","load","source file"]
)

def ice_rmo_load_ondemand_icepay():
    logging.basicConfig(level=logging.INFO)
    
    def Agent_Datafix():
            source_path = r'/rmo_ct_prod/ondemand/'
            file = 'Agent.csv'
            output_file = 'Agent_fixed.csv'            

            logging.info("Agent fixing code")
            
            try:
                # Initialize SambaHook with your credentials and connection details
                with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
                    
                    names = ["SwitchID","AgentID","AgentName","AgentType","ClassOfService","pw1","pw2","pw3"
                            ,"AutoLogonAddress","AutoLogonQueue","PAQOverflowThreshold","NoAnswerThreshold"
                            ,"CfacDn","CfnaDn","CfpoDn","CfnlDn","CfState","EmailAddress"
                            ,"RemoteDn","VoiceMailDN","NumVoiceMailCalls","CallerNumPBX"
                            ,"CallerNumPSTN","AgentAlias","ImageURL","OutboundWorkflowDN"
                            ,"OutboundWorkflowMode","HotlineDN","CallerName","PlacedCallAutoWrapTimer"
                            ,"UpdateCount","LogonToNotReadyReason","IMAddress","pw4","pw5","pw6"
                            ,"PasswordCOS","PasswordLastChanged","PasswordAbsoluteLockedOutDate"
                            ,"PasswordLockedOutExpireDateTime","ClassOfService2","ADFQDN"
                            ,"ADGUID","LanguageCode","version","AzureADGuid","MaxImConcurrency","MaxEmailConcurrency"]
                               
                    cols = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47]
                    
                    with fs_hook.open_file(source_path + file,'r') as f:
                        csv_reader = pd.read_csv(f, header = None, usecols=cols, quoting=1)
                    
                    # Agent: 1137 - Colin Klingspohn; 1148	Jasmyn Carnwell
                    # Agent: 1888 - Revenu Service BC; 1889 - Health Insurance BC; 1890 - Enrolment Specialists; 2001 - Taxpayer Services
                    # Agent: 2003 - eTax Team; 9985 - 9985
 
                    df1 = csv_reader.loc[(csv_reader[1] ==  1137) | (csv_reader[1] == 1888) | (csv_reader[1] == 1889) | (csv_reader[1] == 1890) | (csv_reader[1] == 2001) | (csv_reader[1] == 2003) | (csv_reader[1] == 9985)]    
                    df1 = df1.iloc[:,[0,1,2,3,4,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,32,33,34,35,36,37,38,39,41,42,43]]
                                        
                    df2 = csv_reader.loc[(csv_reader[1] !=  1137) & (csv_reader[1] !=  1148) & (csv_reader[1] != 1888) & (csv_reader[1] != 1889) & (csv_reader[1] != 1890) & (csv_reader[1] != 2001) & (csv_reader[1] != 2003) & (csv_reader[1] != 9985)]    
                    df2 = df2.iloc[:,[0,1,2,3,4,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,36,37,38,39,40,41,42,43,45,46,47]]

                    df3 = csv_reader.loc[(csv_reader[1] ==  1148)]  
                    df3 = df3.iloc[:,[0,1,2,3,4,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,34,35,36,37,38,39,40,41,43,44,45]]

 
                    with fs_hook.open_file(source_path + output_file, 'w') as outfile:
                        df1.to_csv(outfile, header=False,index=False,lineterminator='\r\n')
                                
                    with fs_hook.open_file(source_path + output_file, 'a') as outfile:
                        df2.to_csv(outfile, header=False,index=False,lineterminator='\r\n')
                    
                    with fs_hook.open_file(source_path + output_file, 'a') as outfile:
                        df3.to_csv(outfile, header=False,index=False,lineterminator='\r\n')
                                
                
            except Exception as e:
                logging.error(f"Error data fixing table Agent: {e}")
                
            return   
          

    def Stat_CDR_Datafix():
            source_path = r'/rmo_ct_prod/ondemand/'
            file = 'Stat_CDR.csv'
            output_file = 'Stat_CDR_fixed.csv'

            logging.info("Stat_CDR fixing code")
            try:
                # Initialize SambaHook with your credentials and connection details
                with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
                    
                    names = ["PrimaryKey","EventTime","DSTStatus","ContactID","EventID"
                            ,"SwitchID","ContactType","CurrentState","LastState","LastStateDuration"
                            ,"QueueID","IntData1","IntData2","IntData3","IntData4","StrData1"
                            ,"StrData2","StrData3","StrData4","EventSequence","ServerId","RolledUp"]
                               
                    cols = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22]                  
                    
                    with fs_hook.open_file(source_path + file,'r') as f:
                        csv_reader = pd.read_csv(f, header = None, usecols=[i for i in range(22)], quoting=1)
 
                    df1 = csv_reader.loc[:, [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21]]
 
                    with fs_hook.open_file(source_path + output_file, 'w') as outfile:
                            df1.to_csv(outfile, header=False,index=False,lineterminator='\r\n')
  
                                
                outfile.close()
                
            except Exception as e:
                logging.error(f"Error data fixing table Stat_CDR: {e}")
                
            return   
            

    def Stat_CDR_Summary_Datafix():
            source_path = r'/rmo_ct_prod/ondemand/'
            file = 'Stat_CDR_Summary.csv'
            output_file = 'Stat_CDR_Summary_fixed.csv'

            logging.info("Stat_CDR_Summary_fixing code")
            try:
                # Initialize SambaHook with your credentials and connection details
                with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
                    
                    names = ["SwitchID","ContactID","ContactType","ContactTypeString ","CreatedDateTime",
                             "CreatedReason","CreatedReasonString","CreatedContactGroupID","CreatedAddressID",
                             "Duration","ReleasedReason","ReleasedReasonString","ReleasedDateTime",
                             "OriginatorAddress","OriginatorName","ReceivingAddress","RedirectAddress","NumTimesInWorkflow",
                             "TimeInWorkflow","NumTimesRouted","TimeInRouting","NumTimesInPAQ","TimeInPAQ",
                             "NumTimesOnOutbound","TimeOnOutbound","NumTimesHandledByAgent","TimeHandledByAgent",
                             "NumTimesQueued","NumTimesReturned","OriginalQueueID","OriginalQueueName","NumTimesHandledFromQueue",
                             "TotalTimeQueuedHandled","NumTimesAbandonedFromQueue","TotalTimeQueuedAbandoned",
                             "NumTimesRemovedFromQueue","TotalTimeQueuedRemoved","NumTimesSetUserData","NumTimesActionCompleted",
                             "OriginalHandledQueueID","OriginalHandledQueueName","OriginalHandlingAgentID","OriginalHandlingAgentName",
                             "OriginalHandlingAgentSkillScore","OriginalOutboundContactGroupID","OriginalOutboundAddressID", 
                             "OriginalOutboundNumber", "OriginalRoutedAddressID","OriginalRoutedResult","OriginalRoutedResultString",
                             "OriginalRoutedReason","OriginalRoutedReasonString", "OriginalRoutedDestination","OriginalSetUserData",
                             "LastSetUserData","OriginalLoggedActionWfID","OriginalLoggedActionPageID", "OriginalLoggedActionActionID",
                             "OriginalLoggedActionDuration","OriginalLoggedActionName","OriginalLoggedActionData","OriginalLoggedActionResult",
                             "LastLoggedActionWfID","LastLoggedActionPageID","LastLoggedActionActionID","LastLoggedActionDuration",
                             "LastLoggedActionName","LastLoggedActionData","LastLoggedActionResult","ServerId"]
                               
                    
                    with fs_hook.open_file(source_path + file,'r') as f:
                        csv_reader = pd.read_csv(f, header = None, usecols=[i for i in range(70)], quoting=1)
 
                    df1 = csv_reader.loc[:,[0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,15,16,17,18,19,
                                            20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,
                                            40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,
                                            60,61,62,63,64,65,66,67,68,69]]                      
                            

                    with fs_hook.open_file(source_path + output_file, 'w') as outfile:
                        df1.to_csv(outfile, header=False,index=False,lineterminator='\r\n')
  
                                
                outfile.close()
                
            except Exception as e:
                logging.error(f"Error data fixing table Stat_CDR_Summary: {e}")
                
            return   
            
    def LOBCodeLangString():
            source_path = r'/rmo_ct_prod/ondemand/'
            file = 'LOBCodeLangString.csv'
            output_file = 'LOBCodeLangString_fixed.csv'

            logging.info("LOBCodeLangString_fixing code")
            try:
                # Initialize SambaHook with your credentials and connection details
                with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
                    names = ["CodeID","Lang","Value"]                               
                    
                    with fs_hook.open_file(source_path + file,'r') as f:
                        csv_reader = pd.read_csv(f, header = None, usecols=[i for i in range(3)], quoting=1)   

                    df1 = csv_reader.loc[:,[0,1,2]]
                    
                    with fs_hook.open_file(source_path + output_file, 'w') as outfile:
                        df1.to_csv(outfile, header=False,index=False,lineterminator='\r\n')  
                                
                outfile.close()                    
        
            except Exception as e:
                logging.error(f"Error data fixing table LOBCodeLangString {e}")
                
            return   
           

    def EvalCriteriaLangString():
            source_path = r'/rmo_ct_prod/ondemand/'
            file = 'EvalCriteriaLangString.csv'
            output_file = 'EvalCriteriaLangString_fixed.csv'

            logging.info("EvalCriteriaLangString_fixing code")
            try:
                # Initialize SambaHook with your credentials and connection details
                with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
                    names = ["ID","Lang","Value"]
                               
                    
                    with fs_hook.open_file(source_path + file,'r') as f:
                        csv_reader = pd.read_csv(f, header = None, usecols=[i for i in range(3)], quoting=1)   

                    df1 = csv_reader.loc[:,[0,1,2]]
                    
                    with fs_hook.open_file(source_path + output_file, 'w') as outfile:
                        df1.to_csv(outfile, header=False,index=False,lineterminator='\r\n')
  
                                
                outfile.close()                    
        
            except Exception as e:
                logging.error(f"Error data fixing table LOBCodeLangString {e}")
                
            return   
    
    

    def log_remove():
        with SambaHook(samba_conn_id='fs1_prod_conn') as fs_hook:
            DeletePath = Variable.get("vRMOLogPath")
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

    
    def ondemand_load_source(psource_file):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')
        dYmd = (dt.datetime.today() + timedelta(days = -1)).strftime('%Y%m%d')


        try:
            if psource_file == "Stat_CDR6.csv":
                pTableName = "ICE_Stat_CDR"
                psource_file = "Stat_CDR_fixed.csv"
            elif psource_file == "Agent6.csv":
                pTableName = "ICE_Agent"
                psource_file = "Agent_fixed.csv"
            elif psource_file == "Stat_CDR_Summary6.csv":
                pTableName = "ICE_Stat_CDR_Summary"
                psource_file = "Stat_CDR_Summary_fixed.csv" 
            elif psource_file == "LOBCodeLangString6.csv":   
                pTableName = "ICE_LOBCodeLangString"
                psource_file = "LOBCodeLangString_fixed.csv"
            elif psource_file == "EvalCriteriaLangString6.csv":   
                pTableName = "ICE_EvalCriteriaLangString"
                psource_file = "EvalCriteriaLangString_fixed.csv"                
            else:
                xlen = len(psource_file)-4
                pTableName = "ICE_" + psource_file[:xlen]
            
            logging.info(f"loading table: {pTableName}")
            
            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            pTableName = "ICE_icePay"
            
            query = f""" BULK INSERT [FIN_SHARED_LANDING_DEV].[dbo].[{pTableName}]
                    FROM '\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\ondemand\\{psource_file}'
                    WITH
	                     ( FIELDTERMINATOR = '|',
                           ROWTERMINATOR = '\r\n',
                           MAXERRORS = 20, 
                           ERRORFILE='\\\\fs1.fin.gov.bc.ca\\ptb_ct_prod\\log\\{pTableName}_{dYmd}.log',
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
        
        source_file_set = ["icePay03.csv","icePay04.csv","icePay05.csv","icePay06.csv","icePay07.csv","icePay08.csv","icePay09.csv","icePay10.csv",
                           "icePay11.csv","icePay12.csv","icePay13.csv","icePay14.csv","icePay15.csv","icePay16.csv","icePay17.csv","icePay18.csv","icePay19.csv","icePay20.csv", 
                           "icePay21.csv","icePay22.csv","icePay23.csv","icePay24.csv","icePay25.csv","icePay26.csv","icePay27.csv","icePay28.csv","icePay29.csv","icePay30.csv",
                           "icePay31.csv"
                           ]
        
        
        log_remove()
        etl_truncate()       
        
        for source_file in source_file_set:
            ondemand_load_source(source_file)

        loading_target_tables_db() 
        
        return

    ondemand_load_data()
    
ice_rmo_load_ondemand_icepay()