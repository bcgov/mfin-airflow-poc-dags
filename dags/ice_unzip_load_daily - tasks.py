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
    dag_id="ice_etl_load_daily_tasks",
    schedule_interval=None,  # Set your schedule interval or leave as None for manual trigger
    start_date=days_ago(1),
    catchup=False,
    tags=["ice", "etl", "unzip","load","inprogress_folder"]
)


def daily_load_data():
    #logging.basicConfig(level=logging.INFO)
    
    #Task 1: Unzip and Move files from source to destination (using SambaHook)
    @task
    def unzip_move_file():
        # Log all steps at INFO level
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
                    
                #logging.info(f"File moved from {source_path} to {dest_path}")
        except Exception as e:
            logging.error(f"Error unzipping files: {e}")        

    

    # Task 2: Backup iceDB_ICE_BCMOFRMO-YYYYMMDD.zip to the copleted folder 
    @task
    def backup_file():
        source_path = 'r/rmo_ct_prod/'
        dest_path = r'/rmo_ct_prod/completed/'
        conn_id = 'fs1_rmo_ice'
        file = 'iceDB_ICE_BCMOFRMO.zip'
        hook = SambaHook(conn_id)
    #   Set dYmd to yesterdays date
        dYmd = (dt.datetime.today() + timedelta(days=-1)).strftime('%Y%m%d')
        
        try:
            files = hook.listdir(source_path)
            for f in files:
                if f == 'iceDB_ICE_BCMOFRMO.zip':
                    hook.replace(source_path + f, dest_path + 'iceDB_ICE_BCMOFRMO-' + dYmd+'.zip') 
                    
        except Exception as e:
            logging.error(f"Error backing up {dYmd} source file")
 
 
    # Task 3: Removing 103 csv data files from \\fs1.fin.gov.bc.ca\rmo_ct_prod\inprogress\ subfolder    
    @task
    def remove_csv_inprogress():
        conn_id = 'fs1_rmo_ice'
      
        delete_path = r'/rmo_ct_prod/inprogress/'
        hook = SambaHook(conn_id)
        files = hook.listdir(delete_path)

        try:
            for file in files:
                file_path = f"{delete_path}/{file}"
                hook.remove(file_path)
        except:
            logging.error(f"Error {e} removing file: {file_path}")
            
        return
            

    # Task 4: Truncate landing tables prior loading next daily source files    
    @task
    def truncate_landing_tables():
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')
      
        try:
            logging.info(f"truncating landing tables")
            
            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            query = f"""EXEC [FIN_SHARED_STAGING_DEV].[dbo].[PROC_TELEPHONY_ICE_TRUNCATE];"""
            
            start_time = time.time()
            cursor.execute(query)
            conn.commit()
                                  
            logging.info(f"truncate landing tables {time.time() - start_time} seconds")
        
        except Exception as e:
                logging.error(f"Error truncati landing tables {e}")
        
        return

            
    # Task 5: Loading 103 csv data files to [IAPETUS\FINDATA].[dbo].[FIN_SHARED_LANDING_DEV]      
    @task
    # Slowly changin dimension TBD on AgentAssignment, TeamAssignment
    def daily_load_source():
        logging.basicConfig(level=logging.INFO)                

        def Agent_Datafix():
            source_path = r'/rmo_ct_prod/inprogress/'
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
            source_path = r'/rmo_ct_prod/inprogress/'
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
            source_path = r'/rmo_ct_prod/inprogress/'
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
            source_path = r'/rmo_ct_prod/inprogress/'
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
            source_path = r'/rmo_ct_prod/inprogress/'
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

        
        def load_db_source(psource_file):
            sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')
            dYmd = (dt.datetime.today() + timedelta(days = -1)).strftime('%Y%m%d')

            try:
                if psource_file == "Stat_CDR.csv":
                    pTableName = "ICE_Stat_CDR"
                    psource_file = "Stat_CDR_fixed.csv"
                elif psource_file == "Agent.csv":
                    pTableName = "ICE_Agent"
                    psource_file = "Agent_fixed.csv"
                elif psource_file == "Stat_CDR_Summary.csv":
                    pTableName = "ICE_Stat_CDR_Summary"
                    psource_file = "Stat_CDR_Summary_fixed.csv" 
                elif psource_file == "LOBCodeLangString.csv":   
                    pTableName = "ICE_LOBCodeLangString"
                    psource_file = "LOBCodeLangString_fixed.csv"
                elif psource_file == "EvalCriteriaLangString.csv":   
                    pTableName = "ICE_EvalCriteriaLangString"
                    psource_file = "EvalCriteriaLangString_fixed.csv"     
                else:
                    xlen = len(psource_file)-4
                    pTableName = "ICE_" + psource_file[:xlen]

                logging.info(f"loading table: {pTableName}")
            
                conn = sql_hook.get_conn()
                cursor = conn.cursor()
            
                query = f""" BULK INSERT [FIN_SHARED_LANDING_DEV].[dbo].[{pTableName}]
                             FROM '\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\inprogress\\{psource_file}'
                             WITH
	                         ( FORMAT='CSV', 
                               MAXERRORS=100, 
                               ERRORFILE='\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\log\\{pTableName}_{dYmd}.log',
                               TABLOCK                               
	                         );
                         """
                logging.info(f"query: {query}")
                logging.info(f"inserting table:  {psource_file}")
                start_time = time.time()
                cursor.execute(query)
                conn.commit()
                                  
                logging.info(f"bulk insert {time.time() - start_time} seconds")
        
            except Exception as e:
                logging.error(f"Error bulk loading table: {pTableName} source file: {psource_file} {e}")
               
                
            return
            
            
        # Date: Mar 06, 2025
        # Annual table load 
        #     Holiday, 
        #     Server,
        #     Site,
        #     Switch,
        #     OperatingDates,
        #     QueueIDLookup
        
        #
        # Implemented tables data fix processes:
        #             - Agent.csv --> Agent_fixed.csv
        #             - Stat_CDR.csv --> Stat_CDR_fixed.csv
        #             - Stat_CDR-Summary.csv --> Stat_CDR-Summary_fixed.csv
        #             - LOBCodeLangString.csv ---> LOBCodeLangString_fixed.csv 
        #             - EvalCriteriaLangString ---> EvalCriteriaLangString_fixed.csv
        #
        # RMO resource (Sofia Polar) implementing/testing data fix code:
        #             - WfAction.csv
        #             - WfAttributeDetail.csv
        #             - WfLink.csv
        #             - WfSubAppMethod.csv
        #             - WfSubApplication.csv
        #             - WfVariables.csv
        #
        # Tables not loaded and/not functional at this moment
        #             - AudioMessage.csv
        #             - AgentSkills.csv
        #             - RequiredSkills.csv   
        #             - Recordings.csv
        #             - RecodringFaultedFiles.csv        
        #             - Skill.csv        
            
        source_file_set = ["ACDQueue.csv","Agent.csv",
                           #"AudioMessage.csv", 
                           "AgentAssignment.csv", 
                           #"AgentSkill.csv",
                           "ContactLink.csv","ContactSegment.csv",
                           "Email.csv","EmailGroup.csv","Eval_Contact.csv","EvalScore.csv","EvalCategory.csv","EvalCategoryLangString.csv",
                           "EvalCriteria.csv","EvalCriteriaValue.csv","EvalCriteriaValueLangString.csv",
                           "EvalCriteriaLangString.csv",
                           "EvalEvaluation.csv","EvalForm.csv","EvalFormLangString.csv",
                           #"Holiday.csv",
                           "IMRecording.csv","icePay.csv",
                           "Languages.csv","LOBCategory.csv","LOBCategoryLangString.csv","LOBCode.csv",
                           "LOBCodeLangString.csv",
                           "Node.csv","NotReadyReason.csv","NotReadyReasonLangString.csv",
                           #"OperatingDates.csv",
                           #"Recordings.csv",
                           #"RecordingsFaultedFiles.csv",
                           #"RequiredSkill.csv", 
                           "Stat_ADR.csv",
                           "SegmentAgent.csv","SegmentQueue.csv",
                           #"Skill.csv",
                           #"Server.csv","Site.csv","Switch.csv",
                           "Stat_AgentActivity_D.csv", "Stat_AgentActivity_I.csv", "Stat_AgentActivity_M.csv", "Stat_AgentActivity_W.csv", "Stat_AgentActivity_Y.csv",
                           "Stat_AgentActivityByQueue_D.csv", "Stat_AgentActivityByQueue_I.csv", "Stat_AgentActivityByQueue_M.csv", "Stat_AgentActivityByQueue_W.csv", "Stat_AgentActivityByQueue_Y.csv",
                           "Stat_AgentLineOfBusiness_D.csv", "Stat_AgentLineOfBusiness_I.csv", "Stat_AgentLineOfBusiness_M.csv", "Stat_AgentLineOfBusiness_W.csv", "Stat_AgentLineOfBusiness_Y.csv",
                           "Stat_AgentNotReadyBreakdown_D.csv", #2024 missing Jan30-Mar, 
                           "Stat_AgentNotReadyBreakdown_M.csv","Stat_AgentNotReadyBreakdown_I.csv", "Stat_AgentNotReadyBreakdown_W.csv", "Stat_AgentNotReadyBreakdown_Y.csv",
                           "Stat_CDR.csv","Stat_CDR_LastSummarized.csv","Stat_CDR_Summary.csv",                           
                           "Stat_DNISActivity_D.csv", "Stat_DNISActivity_I.csv", "Stat_DNISActivity_M.csv", "Stat_DNISActivity_W.csv", "Stat_DNISActivity_Y.csv",
                           "Stat_QueueActivity_D.csv","Stat_QueueActivity_M.csv","Stat_QueueActivity_I.csv", "Stat_QueueActivity_W.csv", "Stat_QueueActivity_Y.csv",
                           "Stat_SkillActivity_D.csv", "Stat_SkillActivity_I.csv", "Stat_SkillActivity_M.csv", "Stat_SkillActivity_W.csv", "Stat_SkillActivity_Y.csv",
                           "Stat_TrunkActivity_D.csv", "Stat_TrunkActivity_I.csv", "Stat_TrunkActivity_M.csv", "Stat_TrunkActivity_W.csv", "Stat_TrunkActivity_Y.csv",    
                           "Stat_WorkflowActionActivity_D.csv", "Stat_WorkflowActionActivity_I.csv", "Stat_WorkflowActionActivity_M.csv", "Stat_WorkflowActionActivity_W.csv", "Stat_WorkflowActionActivity_Y.csv",
                           "Team.csv","TeamAssignment.csv",
                           "UCAddress.csv","UCGroup.csv",
                           "WfAttributeDetail.csv","WfLinkDetail.csv","WfLink.csv","WfAction.csv","WfPage.csv","WfGraph.csv",
                           "WfSubAppMethod.csv","WfSubApplication.csv","WfVariables.csv"]
 
        
        # Data fixes required for relevant daily table process 
        Agent_Datafix()
        Stat_CDR_Datafix()
        Stat_CDR_Summary_Datafix()
        LOBCodeLangString()
        EvalCriteriaLangString()
              
        for source_file in source_file_set:
            load_db_source(source_file)
 
 
 #Set task dependencies
    remove_csv_inprogress() >> unzip_move_file() >> truncate_landing_tables >> daily_load_source() #>> remove_csv_inprogress()
    
dag = daily_load_data()