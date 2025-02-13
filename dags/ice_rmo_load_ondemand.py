from airflow.decorators import task, dag
#from airflow.providers.samba.hooks.samba import SambaHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import datetime as dt
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import time
import logging

@dag(
    description="DAG - RMO CT specific dates bulk insert",
    schedule=None,
    catchup=False,
    tags=["ice","ondemand","load","source file"]
)

def ice_rmo_load_ondemand():
    logging.basicConfig(level=logging.INFO)
    
    def ondemand_load_source(psource_file):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')

        try:
            xlen = len(psource_file)-4
            pTableName = psource_file[:xlen]
            logging.info(f"loading table: {pTableName}")
            
            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            query = f""" BULK INSERT [RMO_ICE_HISTORY].[dbo].[{pTable}]
                    FROM '\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\inprogress\\{psource_file}'
                    WITH
	                ( FORMAT = 'CSV'
	                );
                """
            logging.info(f"query: {query}")
            logging.info(f"inserting table:  {psource_file}")
            start_time = time.time()
            cursor.execute(query)
            conn.commit()
            
                      
            logging.info(f"bulk insert {time.time() - start_time} seconds")
            #print(f"bulk insert duration: --- {time.time() - start_time} seconds ---")
            #print(f"bulk insert {rows} rows test, duration: --- {time.time() - start_time} seconds ---")
        
        
        except Exception as e:
            logging.info(f"Error bulk loading {psource_file} {e}")
 

    @task
    def ondemand_load_data():
        
        source_file_set = ["ACDQueue.csv","Agent.csv","AudioMessage.csv", "AgentAssignment.csv", "AgentSkill.csv",
                           "ContactLink.csv","ContactSegment.csv",
                           "Email.csv","EmailGroup.csv","Eval_Contact.csv","EvalScore.csv","EvalCategory.csv","EvalCategoryLangString.csv",
                           "EvalCriteria.csv","EvalCriteriaLangString.csv","EvalCriteriaValue.csv","EvalCriteriaValueLangString.csv",
                           "EvalEvaluation.csv","EvalForm.csv","EvalFormLangString.csv",
                           "Holiday.csv","IMRecording.csv","icePay.csv",
                           "Languages.csv","LOBCategory.csv","LOBCategoryLangString.csv","LOBCode.csv","LOBCodeLangString.csv",
                           "Node.csv","NotReadyReason.csv","NotReadyReasonLangString.csv",
                           "OperatingDates.csv",
                           "Recordings.csv","RecordingsFaultedFiles.csv","RequiredSkill.csv", 
                           "SegmentAgent.csv","SegmentQueue.csv","Server.csv","Site.csv","Skill.csv","Stat_CDR_LastSummarized.csv","Switch.csv",
                           "Stat_AgentActivity_D.csv", "Stat_AgentActivity_I.csv", "Stat_AgentActivity_M.csv", "Stat_AgentActivity_W.csv", "Stat_AgentActivity_Y.csv",
                           "Stat_AgentActivityByQueue_D.csv", "Stat_AgentActivityByQueue_I.csv", "Stat_AgentActivityByQueue_M.csv", "Stat_AgentActivityByQueue_W.csv", "Stat_AgentActivityByQueue_Y.csv",
                           "Stat_AgentLineOfBusiness_D.csv", "Stat_AgentLineOfBusiness_I.csv", "Stat_AgentLineOfBusiness_M.csv", "Stat_AgentLineOfBusiness_W.csv", "Stat_AgentLineOfBusiness_Y.csv",
                          #"Stat_AgentNotReadyBreakdown_D" 2024 missing Jan30-Feb, 
                           "Stat_AgentNotReadyBreakdown_M.csv",
                           "Stat_AgentNotReadyBreakdown_I.csv", "Stat_AgentNotReadyBreakdown_W.csv", "Stat_AgentNotReadyBreakdown_Y.csv",
                           "Stat_DNISActivity_D.csv", "Stat_DNISActivity_I.csv", "Stat_DNISActivity_M.csv", "Stat_DNISActivity_W.csv", "Stat_DNISActivity_Y.csv",
                           "Stat_CDR.csv","Stat_CDR_LastSummarize.csv","Stat_CDR_Summary.csv",
                           "Stat_ADR.csv",
                           "Stat_QueueActivity_D.csv","Stat_QueueActivity_M.csv","Stat_QueueActivity_I.csv", "Stat_QueueActivity_W.csv", "Stat_QueueActivity_Y.csv",
                           "Stat_SkillActivity_D.csv", "Stat_SkillActivity_I.csv", "Stat_SkillActivity_M.csv", "Stat_SkillActivity_W.csv", "Stat_SkillActivity_Y.csv",
                           "Stat_TrunckActivity_D.csv", "Stat_TrunckActivity_I.csv", "Stat_TrunckActivity_M.csv", "Stat_TrunckActivity_W.csv", "Stat_TrunckActivity_Y.csv",    
                           "Stat_WorkflowActionActivity_D.csv", "Stat_WorkflowActionActivity_I.csv", "Stat_WorkflowActionActivity_M.csv", "Stat_WorkflowActionActivity_W.csv", "Stat_WorkflowActionActivity_Y.csv",
                           "Team.csv","TeamAssignment.csv",
                           "UCAddress.csv","UCGroup.csv",
                           "WfAttributeDetail.csv","WfLinkDetail.csv","WfLink.csv","WfAction.csv","WfPage.csv","WfGraph.csv",
                           "WfSubAppMethod.csv","WfSubApplication.csv","WfVariables.csv"]
                           
        
        for source_file in source_file_set:
            ondemand_load_source(source_file)

    ondemand_load_data()
    
ice_rmo_load_ondemand()