from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.smtp.hooks.smtp import SmtpHook
from email.mime.text import MIMEText
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
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
    
    smtp_hook = SmtpHook(smtp_conn_id='Email_Notification')
    
    msg = MIMEText("Daily source file iceDB_ICE_BCMOFRMO.zip for {dYmd}  not available for loading")
    msg['Subject'] = "Missing daily source file"
    msg['From'] = "FINDAMSG@gov.bc.ca"
    msg['To'] = "eloy.mendez@gov.bc.ca"
    
    send_email_smtp(
        to = ['eloy.mendez@gov.bc.ca'],
        subject = 'Missing dailynsource file',
        html_content = """<h3>Email Notification</h3>
                          <p>Daily source file iceDB_ICE_BCMOFRMO.zip {dYmd} for not available for loading</p>""",
        from_email = ['FINDAMSG@gov.bc.ca']
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

