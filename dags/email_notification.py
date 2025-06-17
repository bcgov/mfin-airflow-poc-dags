from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.smtp_hook import SmtpHook
from datetime import datetime
from email.mime.text import MIMEText
import logging

root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s -%(message)s')
handler.setFormatter(formatter)
root.addHandler(handler) 


def send_email_smtp():
    smtp_hook=SmtpHook(smtp_conn_id = 'Email_Notification')
	
	msg = MIMEText('<h3>Daily iceDB_ICE_BCMOFRMO.zip missing for ETL process</h3>', 'html')
    msg['Subject'] = 'Airflow SMTP Email'
    msg['From'] = 'FINDAMSG@gov.bc.ca'
	msg['To'] = 'eloy.mendez@gov.bc.ca'
	
	smtp_hook.send_email(
        to=['eloy.mendez@gov.bc.ca'],
        subject = msg['Missing daily source file iceDB_ICE_BCMOFRMO.zip'],
        html_content = msg.getpayload()
	)
	
with DAG(
    dag_id="email_missing_daily_load",
	owner = "airflow",    	
    schedule_interval = None,  # Set your schedule interval or leave as None for manual trigger
    start_date = days_ago(1),
    catchup = False,
    tags = ["ice", "email", "missing","daily","load"]
) as dag:
    
	email_task = PythonOperator(
	    task_id = 'send_email',
		python_callable = send_email_smtp

