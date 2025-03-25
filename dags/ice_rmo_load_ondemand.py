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
 
                        with fs_hook.open_file(source_path + output_file, 'w') as outfile:
                            csv_reader.to_csv(outfile, header=False,index=False,lineterminator='\r\n')
                            
                        df1 = csv_reader.loc[csv_reader[19].find("sip:")]    
                        df1 = df1.iloc[:,[0,1,2,3,4,6,7,8,9,10,11,12,13,14,15,16,17,18,20,21,22]]
                                        
                        df2 = csv_reader.loc[(csv_reader[19] >= 0) & (csv_reader[19] <= 100)]    
                        df2 = df2.iloc[:,[0,1,2,3,4,8,9,10,11,12,13,14,15,16,17,18,19,20,21]]
                        
                        with fs_hook.open_file(source_path + output_file, 'w') as outfile:
                            df1.to_csv(outfile, header=False,index=False,lineterminator='\r\n')
                                
                        with fs_hook.open_file(source_path + output_file, 'a') as outfile:
                            df2.to_csv(outfile, header=False,index=False,lineterminator='\r\n')

                                
  
                                
                outfile.close()
                
            except Exception as e:
                logging.error(f"Error data fixing table Stat_CDR: {e}")
                
            return   
            
    
    def ondemand_load_source(psource_file):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')

        try:
            if psource_file == "Stat_CDR.csv":
                pTableName = "ICE_Stat_CDR"
                psource_file = "Stat_CDR_fixed.csv"
            elif psource_file == "Agent.csv":
                pTableName = "ICE_Agent"
                psource_file = "Agent_fixed.csv"
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