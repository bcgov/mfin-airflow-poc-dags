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
    description="DAG - RMO CT specific dates bulk insert",
    schedule=None,
    catchup=False,
    tags=["ice","ondemand","load","source file"]
)

def ice_rmo_load_ondemand():
    logging.basicConfig(level=logging.INFO)
    
    def Agent_Datafix():
            source_path = r'/rmo_ct_prod/inprogress/'
            file = 'Agent.csv'
            output_file = 'Agent_fixed.csv'

            logging.info("Agent fixing code")
            
            try:
                # Initialize SambaHook with your credentials and connection details
                with SambaHook(samba_conn_id="fs1_rmo_ice") as fs_hook:
                    
                    names = ["SwitchID","AgentID","AgentName","AgentType","ClassOfService"
                            ,"AutoLogonAddress","AutoLogonQueue","PAQOverflowThreshold","NoAnswerThreshold"
                            ,"CfacDn","CfnaDn","CfpoDn","CfnlDn","CfState","EmailAddress"
                            ,"RemoteDn","VoiceMailDN","NumVoiceMailCalls","CallerNumPBX"
                            ,"CallerNumPSTN","AgentAlias","ImageURL","OutboundWorkflowDN"
                            ,"OutboundWorkflowMode","HotlineDN","CallerName","PlacedCallAutoWrapTimer"
                            ,"UpdateCount","LogonToNotReadyReason","IMAddress","PasswordCOS"
                            ,"PasswordLastChanged","PasswordAbsoluteLockedOutDate"
                            ,"PasswordLockedOutExpireDateTime","ClassOfService2","ADFQDN"
                            ,"ADGUID","LanguageCode","AzureADGuid","MaxImConcurrency","MaxEmailConcurrency"]
                              
                    cols = [1,2,3,4,5,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,37,38,39,40,41,42,43,44,46,47,48]
                              
                    with fs_hook.open_file(source_path + file,'r') as f:
                        csv_reader = pd.read_csv(f, header = None, usecols=cols, quoting=1)
                        
 
                        with fs_hook.open_file(source_path + output_file, 'w') as outfile:
                            csv_reader.to_csv(outfile, header=False,index=False,lineterminator='\r\n')
                                
  
                                
                outfile.close()
                
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
                    
                    usecols = ["PrimaryKey","EventTime","DSTStatus","ContactID","EventID","SwitchID","ContactType","CurrentState",
                               "LastState","LastStateDuration","QueueID","IntData1","InData2","IntDate3","IntData4",
                               "StrData1","StrData2","StrData3","StrData4","EventSequence","ServerId","RolledUp","Extra"]
                    with fs_hook.open_file(source_path + file,'r') as f:
                        csv_reader = pd.read_csv(f, header = None, usecols=[i for i in range(22)], quoting=1)
 
                        with fs_hook.open_file(source_path + output_file, 'w') as outfile:
                            csv_reader.to_csv(outfile, header=False,index=False,lineterminator='\r\n')
                                
  
                                
                outfile.close()
                
            except Exception as e:
                logging.error(f"Error data fixing table Stat_CDR: {e}")
                
            return   
            
    
    def ondemand_load_source(psource_file):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')

        try:
            if psource_file == "Stat_CDR.csv":
                pTableName = "ICE_Stat_CDR"
                psource_file = "ICE_Stat_fixed.csv"
            elif psource_file == "Agent.csv":
                pTableName = "ICE_Agent"
                psource_file = "ICE_Agent_fixed.csv"
            else:
                xlen = len(psource_file)-4
                pTableName = "ICE_" + psource_file[:xlen]
            
            logging.info(f"loading table: {pTableName}")
            
            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            query = f""" BULK INSERT [FIN_SHARED_LANDING_DEV].[dbo].[{pTableName}]
                    FROM '\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\inprogress\\{psource_file}'
                    WITH
	                ( FORMAT = 'CSV',
                      MAXERRORS = 20, 
                      ERRORFILE='\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\log\\{psource_file}.log'
	                );
                """
            logging.info(f"query: {query}")
            logging.info(f"inserting table:  {pTableName}")
            start_time = time.time()
            cursor.execute(query)
            conn.commit()
            
                      
            logging.info(f"bulk insert {time.time() - start_time} seconds")
            #print(f"bulk insert duration: --- {time.time() - start_time} seconds ---")
            #print(f"bulk insert {rows} rows test, duration: --- {time.time() - start_time} seconds ---")
        
        
        except Exception as e:
            logging.error(f"Error bulk loading table: {pTableName} source file: {psource_file} {e}")
 

    @task
    def ondemand_load_data():
        
        #source_file_set = ["ACDQueue.csv","Agent.csv","AudioMessage.csv", "AgentAssignment.csv", "AgentSkill.csv",
        #                   "ContactLink.csv","ContactSegment.csv",
        #                   "Email.csv","EmailGroup.csv","Eval_Contact.csv","EvalScore.csv","EvalCategory.csv","EvalCategoryLangString.csv",
        #                   "EvalCriteria.csv","EvalCriteriaLangString.csv","EvalCriteriaValue.csv","EvalCriteriaValueLangString.csv",
        #                   "EvalEvaluation.csv","EvalForm.csv","EvalFormLangString.csv",
        #                   "Holiday.csv","IMRecording.csv","icePay.csv",
        #                   "Languages.csv","LOBCategory.csv","LOBCategoryLangString.csv","LOBCode.csv","LOBCodeLangString.csv",
        #                   "Node.csv","NotReadyReason.csv","NotReadyReasonLangString.csv",
        #                   "OperatingDates.csv",
        #                   "Recordings.csv","RecordingsFaultedFiles.csv","RequiredSkill.csv", 
        #                   "SegmentAgent.csv","SegmentQueue.csv","Server.csv","Site.csv","Skill.csv","Stat_CDR_LastSummarized.csv","Switch.csv",
        #                   "Stat_AgentActivity_D.csv", "Stat_AgentActivity_I.csv", "Stat_AgentActivity_M.csv", "Stat_AgentActivity_W.csv", "Stat_AgentActivity_Y.csv",
        #                   "Stat_AgentActivityByQueue_D.csv", "Stat_AgentActivityByQueue_I.csv", "Stat_AgentActivityByQueue_M.csv", "Stat_AgentActivityByQueue_W.csv", "Stat_AgentActivityByQueue_Y.csv",
        #                   "Stat_AgentLineOfBusiness_D.csv", "Stat_AgentLineOfBusiness_I.csv", "Stat_AgentLineOfBusiness_M.csv", "Stat_AgentLineOfBusiness_W.csv", "Stat_AgentLineOfBusiness_Y.csv",
                          #"Stat_AgentNotReadyBreakdown_D" 2024 missing Jan30-Feb, 
        #                   "Stat_AgentNotReadyBreakdown_M.csv",
        #                   "Stat_AgentNotReadyBreakdown_I.csv", "Stat_AgentNotReadyBreakdown_W.csv", "Stat_AgentNotReadyBreakdown_Y.csv",
        #                   "Stat_DNISActivity_D.csv", "Stat_DNISActivity_I.csv", "Stat_DNISActivity_M.csv", "Stat_DNISActivity_W.csv", "Stat_DNISActivity_Y.csv",
        #                   "Stat_CDR.csv","Stat_CDR_LastSummarized.csv","Stat_CDR_Summary.csv",
        #                   "Stat_ADR.csv",
        #                   "Stat_QueueActivity_D.csv","Stat_QueueActivity_M.csv","Stat_QueueActivity_I.csv", "Stat_QueueActivity_W.csv", "Stat_QueueActivity_Y.csv",
        #                   "Stat_SkillActivity_D.csv", "Stat_SkillActivity_I.csv", "Stat_SkillActivity_M.csv", "Stat_SkillActivity_W.csv", "Stat_SkillActivity_Y.csv",
        #                   "Stat_TrunkActivity_D.csv", "Stat_TrunkActivity_I.csv", "Stat_TrunkActivity_M.csv", "Stat_TrunkActivity_W.csv", "Stat_TrunkActivity_Y.csv",    
        #                   "Stat_WorkflowActionActivity_D.csv", "Stat_WorkflowActionActivity_I.csv", "Stat_WorkflowActionActivity_M.csv", "Stat_WorkflowActionActivity_W.csv", "Stat_WorkflowActionActivity_Y.csv",
        #                   "Team.csv","TeamAssignment.csv",
        #                   "UCAddress.csv","UCGroup.csv",
        #                   "WfAttributeDetail.csv","WfLinkDetail.csv","WfLink.csv","WfAction.csv","WfPage.csv","WfGraph.csv",
        #                   "WfSubAppMethod.csv","WfSubApplication.csv","WfVariables.csv"]
        source_file_set = ["Stat_CDR.csv","Agent.csv"]                   
        
        Stat_CDR_Datafix()
        Agent_Datafix()
        
        for source_file in source_file_set:
            ondemand_load_source(source_file)

    ondemand_load_data()
    
ice_rmo_load_ondemand()