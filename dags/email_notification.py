from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.smtp.hooks.smtp import SmtpHook
from email.message import EmailMessage
from datetime import datetime
import datetime as dt
import logging

root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s -%(message)s')
handler.setFormatter(formatter)
root.addHandler(handler) 


def send_email_with_hook():
    
    dYmd = (dt.datetime.today()).strftime('%Y%m%d')
    
    with SmtpHook(smtp_conn_id = 'Email_Notification') as sh:
        
        
        sh.send_email_smtp(
           to=['nick.goodwin@gov.bc.ca','eloy.mendez@gov.bc.ca'],
           subject='Airflow email test',
           html_content='<p>This is a email test</p>'
        )


with DAG(
    dag_id="email_missing_daily_load",
    schedule_interval = None,  # Set your schedule interval or leave as None for manual trigger
    start_date = datetime(2025,1,1),
    catchup = False,
    tags = ["email", "notification", "missing","daily","load"],
) as dag:
    
    
    send_email = PythonOperator(
        task_id = 'send_email_task',
        python_callable = send_email_with_hook
        
    )

