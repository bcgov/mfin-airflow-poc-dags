from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import datetime

@dag(
  dag_id="ice_bash_dag",
  start_date=datetime(2025, 1 ,20),
  schedule=None,
)
  
def ice_bash_dag():
    # Creating new folder in Airflow server
    execute_nu_folder = BashOperator(
	    task_id = "new_folder",
		bash_command = "mkdir ./tmp/rmo_ct_prod",
	)
 
    execute_ls_folder = BashOperator(
       task_id = "ls_folder",
       bash_command ="ls ./tmp",
    )
    
    execute_nu_folder >> execute_ls_folder

	
ice_bash_dag()
