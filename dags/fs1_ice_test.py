from datetime import datetime
from airflow import DAG
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.operators.python_operator import PythonOperator


def test_ice_connection():
    # Replace these with your SMB server details
    conn_id = 'fs1_rmo_ice'
      
    # share_name = 'fs1.fin.gov.bc.ca'
    directory = '/rmo_ct_prod/'
    #path = share_name + directory
    path = directory
    hook = SambaHook(conn_id)
    #files = hook.listdir(share_name, directory)
    files = hook.listdir(path)
    print("Files in the rmo_ct_prod directory:")
    #dt = str(pd.Period(datetime.datetime.now(),'%Y%m%d'))
    for f in files:
        newname = f+'_20240822.txt'
        hook.replace(f,newname)
        print(f)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'Test_FS1_ICE_connectivity',
    default_args=default_args,
    description='A DAG to test ICE FS1 connection',
    schedule_interval=None,
)

test_task = PythonOperator(
    task_id='test_ice_connection',
    python_callable=test_ice_connection,
    dag=dag,
)

test_task