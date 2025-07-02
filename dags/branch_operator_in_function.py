from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.providers.smtp.hooks.smtp import SmtpHook
from email.message import EmailMessage

def email1():
    dYmd = (dt.datetime.today()).strftime('%Y%m%d')
    with SmtpHook(smtp_conn_id = 'Email_Notification') as sh:
        sh.send_email_smtp(
           to=['eloy.mendez@gov.bc.ca'],
           subject='Airflow email test',
           html_content='<html><body><h2>Email 1 Airflow load daily source file failure</h2><p>CT iceDB_ICE_BCMOFRMO-' + dYmd + '.zip file not received</p></body></html>'
        )
    return

def email2():
    dYmd = (dt.datetime.today()).strftime('%Y%m%d')
    with SmtpHook(smtp_conn_id = 'Email_Notification') as sh:
        sh.send_email_smtp(
           to=['eloy.mendez@gov.bc.ca'],
           subject='Airflow email test',
           html_content='<html><body><h2>Email 2 Airflow load daily source file failure</h2><p>CT iceDB_ICE_BCMOFRMO-' + dYmd + '.zip file not received</p></body></html>'
        )
    return


def choose_path():
    log_path = r'/rmo_ct_prod/log/'
    log_name = 'daily_backup.txt'
    SourcePath = '/rmo_ct_prod/'  
    conn_id = 'fs1_rmo_ice'
    filefound = 0
        
    dYmdHMS = (dt.datetime.today()).strftime('%Y%m%d%H%M%S')
        
    with SambaHook(samba_conn_id=conn_id) as fs_hook:
        with fs_hook.open_file(log_path + log_name,'a') as outfile:
            outfile.write("ETL process begins %s\n" % dYmdHMS)
            
        outfile.close()

        files = fs_hook.listdir(SourcePath)
        for f in files:
            if f == 'iceDB_ICE_BCMOFRMO.zip':
                filefound = 1
				
        if filefound == 0:		    
            return 'path_a'
        else:
            return 'path_b'


def create_dag():
    dag = DAG(
         dag_id = 'branch_operator_in_function',
         start_date = datetime(2025, 1, 1),
         schedule_interval = None,
         catchup = False
    )

    start = DummyOperator(
        task_id = 'start',
        dag=dag
    )

    branch = BranchPythonOperator(
         task_id = 'branch_decision',
         python_callable = choose_path,
         dag = dag  
    )

    path_a = PythonOperator(
      task_id = 'email1',
      python_callable = email1,
      dag = dag
    )	  

    path_b = PythonOperator(
      task_id = 'email2',
      python_callable = email2,
      dag = dag
    )

    end = DummyOperator(
        task_id = 'end',
        trigger_rule = 'none_failed_min_one_success',
        dag = dag
    )
	
    start >> branch >> [path_a, path_b] >> end
    
    return dag
    
dag = create_dag()
