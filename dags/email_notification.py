from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from datetime import datetime
import logging

root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s -%(message)s')
handler.setFormatter(formatter)
root.addHandler(handler) 


with DAG(
    dag_id="email_missing_daily_load",
    schedule_interval = None,  # Set your schedule interval or leave as None for manual trigger
    start_date = datetime(2025,1,1),
    catchup = False,
    tags = ["ice", "email", "missing","daily","load"],
    on_failure_callback=[
        send_smtp_notification(
            from_email="FINDAMSG@gov.bc.ca",
            to="eloy.mendez@gov.bc.ca",
            subject="Missing daily source file",
            html_content="daily source file iceDB_ICE_BCMOFRMO.zip not available for loading",
        )
    ],    
):
    BashOperator(
        task_id="mytask",
        on_failure_callback=[
            send_smtp_notification(
                from_email="FINDAMSG@gov.bc.ca",
                to="eloy.mendez@gov.bc.ca",
                subject="Missing daily source file",
                html_content="daily source file iceDB_ICE_BCMOFRMO.zip not available for loading",
            )
        ],
        bash_command="fail",
    )

