from datetime import datetime, timedelta
from airflow import DAG
import pandas as pd
import subprocess
from airflow.operators.python import PythonOperator
from clean_data import clean_data
import smtplib
import requests

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

def send_email_func():
    # Send email phamthixuanhien@iuh.edu.vn
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
            smtp.starttls()
            smtp.login("sonnees05@gmail.com", "qtoa dukb bezp wfdw")

            message = MIMEMultipart()
            message["From"] = "sonnees05@gmail.com"
            message["To"] = "phamthixuanhien@iuh.edu.vn"
            message["Subject"] = "DHKTPM16A - 4203002322905 - Nguyen Van Son - 20049819"

            body = "20049891, Nguyễn văn sơn"
            message.attach(MIMEText(body, "plain"))

            for path in ['/opt/airflow/data/nguyenvanson_filted.csv',
                        '/opt/airflow/data/nguyenvanson_stvl.csv',
                        '/opt/airflow/data/nguyenvanson_stvl.json']:
                with open(path, "rb") as attachment:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header("Content-Disposition", f"attachment; filename={path.split('/')[-1]}")
                    message.attach(part)

            text = message.as_string()
            smtp.sendmail("sonnees05@gmail.com", "phamthixuanhien@iuh.edu.vn", text)
            print("Email sent successfully!")

    except smtplib.SMTPException as e:
        print("Email sent failed:", e)
    except Exception as e:
        print("An unexpected error occurred:", e)


default_args = {
    "owner": "son",
    "depends_on_past": False,
    'start_date': datetime(2024, 5, 11),
    'email': ['sonnees05@gmail.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    'crawl_son',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
    start_date=datetime(2024, 5, 7),
    catchup=False
)


def crawl_funtion():
    command = ['scrapy', 'runspider', '/opt/airflow/crawldata/crawpy/crawpy/spiders/ck_son.py']
    subprocess.run(command)

# def download_file():
#     url = "https://drive.google.com/uc?id=1phaHg9objxK2MwaZmSUZAKQ8kVqlgng4&export=download"
#     save_path = "/opt/airflow/data/download_file.csv"

#     response = requests.get(url)
#     if response.status_code == 200:
#         with open(save_path, 'wb') as file:
#             file.write(response.content)
#         print("Downloaded successfully.")
#     else:
#         print("Failed to download file.")

crawl_task = PythonOperator(
    task_id='crawl_task',
    python_callable=crawl_funtion,
    dag=dag
)

clean_task = PythonOperator(
    task_id='clean_task',
    python_callable=clean_data,
    dag=dag
)

send_email = PythonOperator(
    task_id='send_email',
    python_callable=send_email_func,
    dag=dag
)


crawl_task >> clean_task >> send_email
