import os
from airflow.decorators import dag, task
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.utils.dates import days_ago
import zipfile
import logging
import io
from datetime import datetime
from datetime import timedelta

# Define the DAG
@dag(
    dag_id="ice_etl_load_daily",
    schedule_interval=None,  # Set your schedule interval or leave as None for manual trigger
    start_date=days_ago(1),
    catchup=False,
    tags=["ice", "etl", "unzip","load","inprogress_folder"]
)
def ice_etl_load_daily():
    # Log all steps at INFO level
    logging.basicConfig(level=logging.INFO)

    # Task 1: Unzip and Move files from source to destination (using SambaHook)
    @task
    def unzip_move_file():
        source_path = r'/rmo_ct_prod/'
        dest_path = r'/rmo_ct_prod/inprogress/'
        file = 'iceDB_ICE_BCMOFRMO.zip'
        zip_loc = '/tmp'

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
                logging.info(f"Error unzipping files: {e}")
        
        return

    # Task 2: Loading 103 csv data files to [IAPETUS\FINDATA].[dbo].[FIN_SHARED_LANDING_DEV]      
    @task
    def daily_load_source(psource_file):
        sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')

        try:            
            xlen = len(psource_file)-4
            pTableName = psource_file[:xlen]
            logging.info(pTableName)
            conn = sql_hook.get_conn()
            cursor = conn.cursor()
            
            query = f""" BULK INSERT [FIN_SHARED_LANDING_DEV].[dbo].[{pTableName}]
                    FROM '\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\inprogress\\{psource_file}'
                    WITH
	                ( FORMAT = 'CSV'
	                );
                """

            start_time = time.time()
            cursor.execute(query)
            conn.commit()
            end_time = time.time()
            
            logging.info(f"bulk insert lapse: --- {psoiurce_file}.csv {time.time() - start_time} seconds ---")          
            #print(f"bulk insert duration: --- {time.time() - start_time} seconds ---")
        
        
        except Exception as e:
            print(f"Error loading {psource_file}")
            
        return
    
    
    @task
    def daily_load_data():
        # Slowly changin dimension TBD on AgentAssignment, TeamAssignment
                
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
            daily_load_source(source_file)
 
                
        return



    # Task orchestration
    unzip_file = unzip_move_file() 
    load_daily = daily_load_data()
    
    
# Instantiate the DAG
dag_instance = ice_etl_load_daily()