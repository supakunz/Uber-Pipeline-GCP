import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# on Docker
# เพิ่ม path ของ scripts เข้าไป
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../scripts")))

# on GCS
# เพิ่ม path ของ plugins เข้าไป
sys.path.append("/home/airflow/gcs/plugins")   

from extract import extract_data
from transform import transform_data


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 24),
    'catchup': False
}

with DAG('uber_extract_pipeline', default_args=default_args, schedule_interval=None) as dag:
    extract_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_data_task',
        python_callable=transform_data
    )

# dependencies
extract_task >> transform_task
