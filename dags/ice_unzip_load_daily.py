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

    # Task 2: Backup iceDB_ICE_BCMOFRMO-YYYYMMDD.zip to the copleted folder 
    @task
    def backup_file():
        source_path = 'r/rmo_ct_prod/'
        dest_path = r'/rmo_ct_prod/completed/'
        conn_id = 'fs1_rmo_ice'
        file = 'iceDB_ICE_BCMOFRMO.zip'
        hook = SambaHook(conn_id)
        #Set dYmd to yesterdays date
        dYmd = (dt.datetime.today() + timedelta(days=-1)).strftime('%Y%m%d')
        
        try:
            files = hook.listdir(source_path)
            for f in files:
                if f == 'iceDB_ICE_BCMOFRMO.zip':
                    hook.replace(source_path + f, dest_path + 'iceDB_ICE_BCMOFRMO-' + dYmd+'.zip') 
                    
        except Exception as e:
                logging.info(f"Error backing up {dYmd} source file")
                
        return

    # Task 3: Loading 103 csv data files to [IAPETUS\FINDATA].[dbo].[RMO_ICE_HISTORY]      
    @task
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
            end_time = time.time()
            
            logging.info(f"bulk insert lapse: --- {psoiurce_file}.csv {time.time() - start_time} seconds ---")          
            #print(f"bulk insert duration: --- {time.time() - start_time} seconds ---")
        
        
        except Exception as e:
            print(f"Error loading {psource_file}.csv")
            
        return
    
    
    @task
    def daily_load_data():
        # Slowly changin dimension TBD on AgentAssignment, TeamAssignment
                
        source_file_set = ["ACDQueue","Agent","AudioMessage", "AgentAssignment", "AgentSkill",
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
                          #"Stat_AgentNotReadyBreakdown_D" 2024 missing Jan30-Feb, 
                           "Stat_AgentNotReadyBreakdown_M",
                           "Stat_AgentNotReadyBreakdown_I", "Stat_AgentNotReadyBreakdown_W", "Stat_AgentNotReadyBreakdown_Y",
                           "Stat_DNISActivity_D", "Stat_DNISActivity_I", "Stat_DNISActivity_M", "Stat_DNISActivity_W", "Stat_DNISActivity_Y",
                           "Stat_CDR","Stat_CDR_LastSummarize","Stat_CDR_Summary",
                           "Stat_ADR",
                           "Stat_QueueActivity_D",
                           "Stat_QueueActivity_M", 
                           "Stat_QueueActivity_I", "Stat_QueueActivity_W", "Stat_QueueActivity_Y",
                           "Stat_SkillActivity_D", "Stat_SkillActivity_I", "Stat_SkillActivity_M", "Stat_SkillActivity_W", "Stat_SkillActivity_Y",
                           "Stat_TrunckActivity_D", "Stat_TrunckActivity_I", "Stat_TrunckActivity_M", "Stat_TrunckActivity_W", "Stat_TrunckActivity_Y",    
                           "Stat_WorkflowActionActivity_D", "Stat_WorkflowActionActivity_I", "Stat_WorkflowActionActivity_M", "Stat_WorkflowActionActivity_W", "Stat_WorkflowActionActivity_Y",
                           "Team","TeamAssignment",
                           "UCAddress","UCGroup",
                           "WfAttributeDetail","WfLinkDetail","WfLink","WfAction","WfPage","WfGraph",
                           "WfSubAppMethod","WfSubApplication","WfVariables"]

        
        for source_file in source_file_set:
            daily_load_source(source_file)

                
        return



    # Task orchestration
    unzip_move_file() 
    #backup_file()
    daily_load_data()
    
    unzip_move_file >> daily_load_data

# Instantiate the DAG
dag_instance = ice_etl_load_daily()