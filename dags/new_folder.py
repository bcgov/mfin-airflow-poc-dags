from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import datetime

@dag(
  dag_id="new_rmo_ct_folder",
  start_date=datetime(2025, 1 ,20),
  schedule=None,
  )
  
def new_rmo_folder_dag():
    new_folder = BashOperator(
	    task_kd="new_rmo_folder",
		bash_command="mkdir $AIRFLOW_HOME/rmo_ct_prod",
	)

new_folder
	

