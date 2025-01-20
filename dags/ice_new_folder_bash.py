from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import datetime

@dag(
  dag_id="ice_new_folder_bash_dag",
  start_date=datetime(2025, 1 ,20),
  schedule=None,
)
  
def ice_new_folder_bash_dag():
    # Creating new folder in Airflow server
    execute_nu_folder = BashOperator(
	    task_id = "new_folder",
		bash_command = "mkdir $AIRFLOW_HOME/rmo_ct_prod",
	)

    execute_nu_folder
	
ice_new_folder_bash_dag()
