from airflow.decorators import task, dag
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import datetime as dt
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import time

@dag(
    description="RMO CT daily table delete and bulk insert",
    schedule=None,
    catchup=False,
)

def ice_rmo_load_daily():
    
    def daily_taable_delete(ptable_delete):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')

        try:
            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            query = f""" DELETE FROM [RMO_ICE_HISTORY].[dbo].{ptable_delete};"""

            start_time = time.time()
            cursor.execute(query)
            conn.commit()
            
            #print(f"bulk insert duration: --- {time.time() - start_time} seconds ---")
            #print(f"bulk insert {rows} rows test, duration: --- {time.time() - start_time} seconds ---")
        
        
        except Exception as e:
            print(e)
 
 
    def daily_load_source(psource_file):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')

        try:
            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            query = f""" BULK INSERT [RMO_ICE_HISTORY].[dbo].{psource_file}
                    FROM '\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\inprogress\\{psource_file}.csv'
                    WITH
	                ( FORMAT = 'CSV'
	                );
                """

            start_time = time.time()
            cursor.execute(query)
            conn.commit()
            
                      
            print(f"bulk insert duration: --- {time.time() - start_time} seconds ---")
            #print(f"bulk insert {rows} rows test, duration: --- {time.time() - start_time} seconds ---")
        
        
        except Exception as e:
            print(e)
 

    @task
    def daily_load_data():
        # Tables not included in the daily delete statement
        # "ACDQueue","Agent","AgentAssignment","TeamAssignment", *** investigate further
        # "ContactSegment",
        
        
        delete_tables_set = ["AudioMessage", "AgentSkill",
                             "ContactLink",
                             "Email","EmailGroup","Eval_Contact","EvalScore","EvalCategory","EvalCategoryLangString",
                             "EvalCriteria","EvalCriteriaLangString","EvalCriteriaValue","EvalCriteriaValueLangString",
                             "EvalEvaluation","EvalForm","EvalFormLangString",
                             "Holiday","IMRecording",
                             "Languages","LOBCategory","LOBCategoryLangString","LOBCode","LOBCodeLangString",
                             "Node","NotReadyReason","NotReadyReasonLangString",
                             "OperatingDates",
                             "Recordings","RecordingsFaultedFiles","RequiredSkill", 
                             "SegmentAgent","SegmentQueue","Server","Site","Skill","Stat_CDR_LastSummarized","Switch",
                             "Team",
                             "UCAddress","UCGroup",
                             "WfAttributeDetail","WfLinkDetail","WfLink","WfAction","WfPage","WfGraph",
                             "WfSubAppMethod","WfSubApplication","WfVariables"]
        
        for delete_table in delete_table_set:
            daily_table_delete(delete_table)                     
                             
        
        source_file_set = ["AudioMessage", "AgentSkill",
                           "ContactLink","ContactSegment",
                           "Email","EmailGroup","Eval_Contact","EvalScore","EvalCategory","EvalCategoryLangString",
                           "EvalCriteria","EvalCriteriaLangString","EvalCriteriaValue","EvalCriteriaValueLangString",
                           "EvalEvaluation","EvalForm","EvalFormLangString",
                           "Holiday","IMRecording","icePay",
                           "Languages","LOBCategory","LOBCategoryLangString","LOBCode","LOBCodeLangString",
                           "Node","NotReadyReason","NotReadyReasonLangString",
                           "OperatingDates",
                           "Recordings","RecordingsFaultedFiles","RequiredSkill", 
                           "SegmentAgent","SegmentQueue","Server","Site","Skill","Stat_CDR_LastSummarized","Switch",
                           "Stat_AgentActivity_D", "Stat_AgentActivity_I", "Stat_AgentActivity_M", "Stat_AgentActivity_W", "Stat_AgentActivity_Y",
                           "Stat_AgentActivityByQueue_D", "Stat_AgentActivityByQueue_I", "Stat_AgentActivityByQueue_M", "Stat_AgentActivityByQueue_W", "Stat_AgentActivityByQueue_Y",
                           "Stat_AgentLineOfBusiness_D", "Stat_AgentLineOfBusiness_I", "Stat_AgentLineOfBusiness_M", "Stat_AgentLineOfBusiness_W", "Stat_AgentLineOfBusiness_Y",
                          #"Stat_AgentNotReadyBreakdown_D", "Stat_AgentNotReadyBreakdown_M" uncomment this line after loading all other days 
                           "Stat_AgentNotReadyBreakdown_I", "Stat_AgentNotReadyBreakdown_W", "Stat_AgentNotReadyBreakdown_Y",
                           "Stat_DNISActivity_D", "Stat_DNISActivity_I", "Stat_DNISActivity_M", "Stat_DNISActivity_W", "Stat_DNISActivity_Y",
                           "Stat_CDR","Stat_CDR_LastSummarize","Stat_CDR_Summary",
                           "Stat_ADR",
                           #"Stat_QueueActivity_D", "Stat_QueueActivity_M" uncomment this line after loading all other days 
                           "Stat_QueueActivity_I", "Stat_QueueActivity_W", "Stat_QueueActivity_Y",
                           "Stat_SkillActivity_D", "Stat_SkillActivity_I", "Stat_SkillActivity_M", "Stat_SkillActivity_W", "Stat_SkillActivity_Y",
                           "Stat_TrunckActivity_D", "Stat_TrunckActivity_I", "Stat_TrunckActivity_M", "Stat_TrunckActivity_W", "Stat_TrunckActivity_Y",    
                           "Stat_WorkflowActionActivity_D", "Stat_WorkflowActionActivity_I", "Stat_WorkflowActionActivity_M", "Stat_WorkflowActionActivity_W", "Stat_WorkflowActionActivity_Y",
                           "Team",
                           "UCAddress","UCGroup",
                           "WfAttributeDetail","WfLinkDetail","WfLink","WfAction","WfPage","WfGraph",
                           "WfSubAppMethod","WfSubApplication","WfVariables"]

        
        for source_file in source_file_set:
            daily_load_source(source_file)

    daily_load_data()
    
ice_rmo_load_daily()