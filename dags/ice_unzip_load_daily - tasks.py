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
                
    

    # Task 3: Loading 103 csv data files to [IAPETUS\FINDATA].[dbo].[FIN_SHARED_LANDING_DEV]      
    @task
    # Slowly changin dimension TBD on AgentAssignment, TeamAssignment
    def daily_load_source():
                
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
                        csv_reader = pd.read_csv(f, header = None, usecols=[i for i in range(22)], quoting=1, on_bad_lines = 'skip')
                        #csv_reader = csv_reader.fillna(0)
                        #if len(csv_reader[12]) != 0:
                        #    csv_reader[12] = csv_reader[12].astype(int)
                        #if len(csv_reader[13]) != 0:
                        #    csv_reader[13] = csv_reader[13].astype(int)
                        #if len(csv_reader[14]) != 0:
                        #    csv_reader[14] = csv_reader[14].astype(int)
                        #if len(csv_reader[15]) != 0:
                        #    csv_reader[15] = csv_reader[15].astype(int)
                        
                        #csv_reader = csv.reader(f)
                            
                        #lst = ['']
                        #for index, row in csv_reader.iterrows():
                        with fs_hook.open_file(source_path + output_file, 'w') as outfile:
                            csv_reader.to_csv(outfile, header=False,index=False)
                                
                                #writer = csv.writer(outfile)
                                #writer.writerows(row["PrimaryKey"])
                                #if 'sip:' in row['EventSequence']:
                                #    d = ','.join(str(e) for e in row)
                                    
                                    #writer.writerows(row["PrimaryKey"],row["EventTim"],row["DSStatus"],row["ContactID"],
                                    #                row["EventID"],row["SwitchID"],row["ContactType"],row["CurrentState"],
                                    #                row["LastState"],row["LastStateDuration"],row["QueueID"],row["IntData1"],
                                    #                row["InData2"],row["IntDate3"],row["IntData4"],row["StrData1"],row["StrData2"],
                                    #                row["StrData3"],row["StrData4"],row["ServiceId"],row["RolledUp"],row["ContactType"])
                                #else:
                                    
                                #    writer.writerows(row["PrimaryKey"],row["EventTime"],row["DSTStatus"],row["ContactID"],
                                #                    row["EventID"],row["SwitchID"],row["ContactType"],row["CurrentState"],
                                #                    row["LastState"],row["LastStateDuration"],row["QueueID"],row["IntData1"],
                                #                    row["InData2"],row["IntDate3"],row["IntData4"],row["StrData1"],row["StrData2"],
                                #                    row["StrData3"],row["StrData4"],row["EventSequence"],row["ServerId"],row["RolledUp"])
                                    
                                     
                                     
                                
                                #writer = csv.writer(outfile)            
                                #writer.writerow(row['PrimaryKey'], row['EventTime'], row['DSStatus'])
                                #if 'sip:' in row[19]:
                                #    new_lst = [row[x] for x in range(19)]
                                #    new_lst = new_lst + lst
                                #    new_lst = new_lst + [row[x] for x in range(20,23)]
                                #    writer.writerow(new_lst)
                                #else:
                                #    new_lst = [row[x] for x in range(21)]
                                #    writer.writerow(new_lst)
                                
                                
            except Exception as e:
                logging.error(f"Error data fixing table Stat_CDR: {e}")
                
            return                    
            
        
        def load_db_source(psource_file):
            sql_hook = MsSqlHook(mssql_conn_id='mssql_conn_bulk')
            dYmd = (dt.datetime.today() + timedelta(days=-1)).strftime('%Y%m%d')

            try:
                #if psource_file == 'Stat_CDR_fixed.csv':
                #    psource_file = 'Stat_CDR_fixed.csv'
                #    pTableName = 'ICE_Stat_CDR'
                #elif psource_file == 'Stat_CDR_Summary.csv':
                #    psource_file = 'Stat_CDR_Summary_fixed.csv'
                #    pTableName = 'ICE_Stat_CDR'
                #elif psource_file == 'Agent.csv':
                #    psource_file = 'Agent_fixed.csv'
                #    pTableName = 'ICE_Agent'
                #else:
                xlen = len(psource_file) - 4    
                pTableName = "ICE_" + psource_file[:xlen]
                
                logging.info(f"loading table: {pTableName}")
            
                conn = sql_hook.get_conn()
                cursor = conn.cursor()
            
                query = f""" BULK INSERT [FIN_SHARED_LANDING_DEV].[dbo].[{pTableName}]
                             FROM '\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\inprogress\\{psource_file}'
                             WITH
	                         ( FORMAT = 'CSV', 
                               MAXERRORS = 100, 
                               ERRORFILE = '\\\\fs1.fin.gov.bc.ca\\rmo_ct_prod\\log\\{pTableName}_{dYmd}.log',
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
        
        # Preloaded tables - need to be reviewed on a regular basis: 
        #     ICE_CriteriaLangString
        #     ICE_LOBCodeLangString   
        #
        # Implemented data fix processes:
        #             - Agent.csv --> Agent_fixed.csv
        #             - Stat_CDR.csv --> Stat_CDR_fixed.csv
        #             - Stat_CDR-Summary.csv --> Stat_CDR-Summary_fixed.csv
        # RMO resource (Sofia Polar) implementing data fix code:
        #             - WfAction.csv
        #             - WfAttributeDetail.csv
        #             - WfLink.csv
        #             - WfSubAppMethod.csv
        #             - WfSubApplication.csv
        #             - WfVariables.csv
                 
            
        source_file_set = ["ACDQueue.csv","Agent.csv",
                           #"AudioMessage.csv", 
                           "AgentAssignment.csv", "AgentSkill.csv",
                           "ContactLink.csv","ContactSegment.csv",
                           "Email.csv","EmailGroup.csv","Eval_Contact.csv","EvalScore.csv","EvalCategory.csv","EvalCategoryLangString.csv",
                           "EvalCriteria.csv","EvalCriteriaValue.csv","EvalCriteriaValueLangString.csv",
                           #"EvalCriteriaLangString.csv",
                           "EvalEvaluation.csv","EvalForm.csv","EvalFormLangString.csv",
                           #"Holiday.csv",
                           "IMRecording.csv","icePay.csv",
                           "Languages.csv","LOBCategory.csv","LOBCategoryLangString.csv","LOBCode.csv",
                           #"LOBCodeLangString.csv",
                           "Node.csv","NotReadyReason.csv","NotReadyReasonLangString.csv",
                           #"OperatingDates.csv",
                           "Recordings.csv","RecordingsFaultedFiles.csv","RequiredSkill.csv", 
                           "Stat_ADR.csv",
                           "SegmentAgent.csv","SegmentQueue.csv","Skill.csv",
                           #"Server.csv","Site.csv","Switch.csv",
                           "Stat_AgentActivity_D.csv", "Stat_AgentActivity_I.csv", "Stat_AgentActivity_M.csv", "Stat_AgentActivity_W.csv", "Stat_AgentActivity_Y.csv",
                           "Stat_AgentActivityByQueue_D.csv", "Stat_AgentActivityByQueue_I.csv", "Stat_AgentActivityByQueue_M.csv", "Stat_AgentActivityByQueue_W.csv", "Stat_AgentActivityByQueue_Y.csv",
                           "Stat_AgentLineOfBusiness_D.csv", "Stat_AgentLineOfBusiness_I.csv", "Stat_AgentLineOfBusiness_M.csv", "Stat_AgentLineOfBusiness_W.csv", "Stat_AgentLineOfBusiness_Y.csv",
                      #    "Stat_AgentNotReadyBreakdown_D" 2024 missing Jan30-Mar, 
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
        #Agent_Datafix
        #logging.info(f"Calling Stat_CDR_Datafix")
        #Stat_CDR_Datafix()
        #logging.info(f"Returning from Stat_CDR_Datafix")
        #Stat_CDR_Summary_Datafix
        
        for source_file in source_file_set:
            load_db_source(source_file)
 
 
   # Task 4: Removing 103 csv data files from \\fs1.fin.gov.bc.ca\rmo_ct_prod\inprogress\ subfolder
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


#Set task dependencies
    unzip_move_file() >> daily_load_source() #>> remove_csv_inprogress()
    
dag = daily_load_data()