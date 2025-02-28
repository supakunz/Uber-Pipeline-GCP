import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator # Operator ของ Apache Airflow ที่ใช้สำหรับ ย้ายข้อมูล (Transfer Data) จาก Google Cloud Storage (GCS) ไปยัง BigQuery
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

    load_task = GCSToBigQueryOperator(
        task_id='load_data_task',
        bucket='us-central1-uber-pipline-gc-9fb54503-bucket', # bucket name
        source_objects=['data/uber_data_final.csv'],
        destination_project_dataset_table='uber_dataset.uber_data',  # [DATASET].[TABLE_NAME]
        skip_leading_rows=1,
        schema_fields=[
  {
    "mode": "NULLABLE",
    "name": "VendorID",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "tpep_pickup_datetime",
    "type": "DATETIME"
  },
  {
    "mode": "NULLABLE",
    "name": "tpep_dropoff_datetime",
    "type": "DATETIME"
  },
  {
    "mode": "NULLABLE",
    "name": "passenger_count",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "trip_distance",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "rate_code_name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "pickup_latitude",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "pickup_longitude",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "dropoff_latitude",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "dropoff_longitude",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "payment_type_name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "fare_amount",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "extra",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "mta_tax",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "tip_amount",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "tolls_amount",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "improvement_surcharge",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "total_amount",
    "type": "FLOAT"
  }
],
        write_disposition='WRITE_TRUNCATE', # ลบข้อมูลเดิมทั้งหมดในตาราง ก่อนจะเขียนข้อมูลชุดใหม่ลงไป
    )

# dependencies
extract_task >> transform_task >> load_task
