import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def extract_data():
   
   # Tip -> การปลงข้อมูลเป็น DataFrame 
   # pd.read_csv() -> ใช้สำหรับโหลดข้อมูลจากไฟล์ CSV และแปลงข้อมูลเหล่านั้นให้เป็น DataFrame
   # pd.DataFrame() -> ใช้สำหรับการสร้าง DataFrame ด้วยข้อมูลที่กำหนดเองในรูปของ dictionary, list, หรือ data structure อื่นๆ
   # * ทั้งสองวิธีสร้าง DataFrame ได้เหมือนกัน แต่แหล่งข้อมูลที่ใช้ต่างกัน *

   # df = pd.read_csv('../data/uber_data.csv') # แปลงข้อมูล CSV เป็น DataFrame

   # เชื่อมต่อ PostgreSQL ผ่าน Airflow Hook
    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    
    # ดึงข้อมูลจากตาราง uber_data
    df = postgres_hook.get_pandas_df(sql="SELECT * FROM uber_data")
    
    # แสดงตัวอย่างข้อมูล
   #  print(df.head())

    # กำหนด path สำหรับบันทึก CSV
    # raw_data_path = '../data/raw_uber_data.csv'

    # absolute path ใน container:
    raw_data_path = '/opt/airflow/data/raw_uber_data.csv'
    
    # บันทึก DataFrame ลงไฟล์ CSV
    df.to_csv(raw_data_path, index=False)
    print(f"✅ Data saved to {raw_data_path}")
