import os
import pandas as pd
from extract import extract_data
# from pandasgui import show # ใช้แสดง table

# Lib สำหรับใช้ทำ Visualizations สำหรับการทำ EDA
# import matplotlib.pyplot as plt
# import seaborn as sns

RAW_DATA_PATH = os.getenv("raw_data_path")
FINAL_DATA_PATH = os.getenv("final_data_path")

def transform_data():
   
  # df = pd.read_csv('../data/raw_uber_data.csv') # แปลงข้อมูล CSV เป็น DataFrame
  
  # Path on docker
  df = pd.read_csv(RAW_DATA_PATH) # แปลงข้อมูล CSV เป็น DataFrame
  # สามารถเปลี่ยนเป็น missing_uber_data.csv เพื่อดู data ที่มีข้อผิดพลาด

  # ------------- ทำ EDA(Exploratory Data Analysis) ก่อน Tranform Data -------------
  # ✅ ขั้นตอนของ EDA
  
  # 1.ดูข้อมูลพื้นฐาน
  # print(df.head()) 
  # print(df.tail()) # ใช้ df.head(), df.tail() เพื่อตรวจสอบแถวเริ่มต้นและแถวท้ายของข้อมูล 
  # print(df.shape) # ใช้ df.shape เพื่อตรวจสอบจำนวนแถวและคอลัมน์ในข้อมูล
  # print(df.info()) # เพื่อตรวจสอบข้อมูลประเภท (data types) และค่าที่ขาดหาย
  # print(df.describe()) # ใช้ df.describe() เพื่อสรุปสถิติพื้นฐาน เช่น ค่าเฉลี่ย, ค่ามากที่สุด, ค่าน้อยที่สุด, และการกระจาย
  
  # 2.ตรวจสอบค่า missing (ค่าที่หายไป)
  # print(df.isnull().sum()) # เพื่อตรวจสอบว่าในแต่ละคอลัมน์มีค่า missing เท่าไหร่
  # print(df[df.isnull().any(axis=1)]) # แสดงเฉพาะ rows ที่มีค่า null

  # 3.การตรวจสอบค่าผิดปกติ (Outliers)
  # สามารถใช้กราฟ boxplot เพื่อตรวจสอบ outliers หรือค่าผิดปกติในคอลัมน์ต่าง ๆ
  # สามารถใช้ scatter plot เพื่อตรวจสอบความสัมพันธ์ระหว่างตัวแปรต่าง ๆ
  # sns.boxplot(x=df['fare_amount'])
  # plt.show()

  #เมื่อมีค่าที่ผิดปกติ หาค่านั้นเพื่อตรวจสอบ
  # outliers = df[(df['fare_amount'] < -30) | (df['fare_amount'] > 800)]
  # print(outliers)

  # 4.การตรวจสอบการกระจายของข้อมูล (Data Distribution)
  # sns.histplot(df['column_name'])
  # sns.kdeplot(df['column_name'])
  # sns.scatterplot(x=df['Unnamed: 0'],y=df['total_amount'])
  # plt.show()

  # ** เมื่อไม่มีข้อมูลผิดปกติสามารถทำ Tranform Data ได้

  # -----------------------------------------------------

  # ------------------- Tranform Data -------------------

  # แปลง Type ของ Column จาก timestamp เป็น date ทั้ง 2 Column
  df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
  df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
   
  # print(df.dtypes) --> check type 
  # print(df.shape) --> check rows และ columns
   
  df = df.drop_duplicates().reset_index(drop=True) # ลบ rows ที่มี data เหมือนกัน
  # ✅ reset_index(drop=True) เป็นการ จัดเรียง index ใหม่ ให้เริ่มต้นจาก 0,1,2,... ตามลำดับ
  df['trip_id'] = df.index # เพิ่ม column trip_id มีค่าเท่ากับ index

  # สร้าง DataFrame ใหม่ <datetime_dim>
  datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
  datetime_dim['tpep_pickup_datetime'] = datetime_dim['tpep_pickup_datetime']
  datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour # สร้างคอลัมน์ใหม่ ชื่อ pick_hour โดย .dt.hour → ดึงเฉพาะ ชั่วโมง (hour) จากค่า datetime
  datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
  datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
  datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
  datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

  datetime_dim['tpep_dropoff_datetime'] = datetime_dim['tpep_dropoff_datetime']
  datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
  datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
  datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
  datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
  datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday

  datetime_dim['datetime_id'] = datetime_dim.index

  # จัดเรียงคอลัมน์ใหม่
  datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
                             'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]

  # สร้าง DataFrame ใหม่ <passenger_count_dim>
  passenger_count_dim = df[['passenger_count']].reset_index(drop=True)
  passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
  passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]
  
  # สร้าง DataFrame ใหม่ <trip_distance_dim>
  trip_distance_dim = df[['trip_distance']].reset_index(drop=True)
  trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
  trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]
  
  
  # สร้าง DataFrame ใหม่ <rate_code_dim>
  rate_code_dim = df[['RatecodeID']].reset_index(drop=True)
  rate_code_dim['rate_code_id'] = rate_code_dim.index
  
  # สร้าง dictionary
  rate_code_type = {
    1:"Standard rate",
    2:"JFK",
    3:"Newark",
    4:"Nassau or Westchester",
    5:"Negotiated fare",
    6:"Group ride"
}
  # map dictionary ลงใน column rate_code_name
  rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
  # จัดเรียงคอลัมน์ใหม่
  rate_code_dim = rate_code_dim[['rate_code_id','RatecodeID','rate_code_name']]

  # สร้าง DataFrame ใหม่ <pickup_location_dim>
  pickup_location_dim = df[['pickup_longitude','pickup_latitude']].reset_index(drop=True)
  pickup_location_dim['pickup_location_id'] = pickup_location_dim.index # .index เป็นการเพิ่ม index มา

  # จัดเรียงคอลัมน์ใหม่
  pickup_location_dim = pickup_location_dim[['pickup_location_id','pickup_latitude','pickup_longitude']]

  # สร้าง DataFrame ใหม่ <dropoff_location_dim>
  dropoff_location_dim = df[['dropoff_longitude','dropoff_latitude']].reset_index(drop=True)
  dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index # .index เป็นการเพิ่ม index มา

  # จัดเรียงคอลัมน์ใหม่
  dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','dropoff_latitude','dropoff_longitude']]
  
  # สร้าง DataFrame ใหม่ <payment_type_dim>
  payment_type_dim = df[['payment_type']].reset_index(drop=True)
  payment_type_dim['payment_type_id'] = payment_type_dim.index

  # สร้าง dictionary
  payment_type_name = {
    1:"Credit card",
    2:"Cash",
    3:"No charge",
    4:"Dispute",
    5:"Unknown",
    6:"Voided trip"
  }
  # map dictionary ลงใน column payment_type_name
  payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
  
  # จัดเรียงคอลัมน์ใหม่
  payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]


  # ตอนนี้มี table --> 
  # datetime_dim, passenger_count_dim, trip_distance_dim, 
  # rate_code_dim, pickup_location_dim, dropoff_location_dim, payment_type_dim

  # ** ทำการ Merge Table ทั้งหมดเข้าด้วยกัน **
  # Tip ! #Merge
  """ 
  🔹 สรุป how= ใน pd.merge()
  Join Type	คำอธิบาย
  inner	เอาเฉพาะค่าที่ตรงกัน (เหมือน INNER JOIN ใน SQL)
  left	เอาทุกแถวจากซ้าย (เหมือน LEFT JOIN)
  right	เอาทุกแถวจากขวา (เหมือน RIGHT JOIN)
  outer	รวมทุกแถวจากทั้งสองตาราง (เหมือน FULL OUTER JOIN)
  """
  # 🔹 ถ้าคอลัมน์ที่ใช้เชื่อมมีชื่อไม่ตรงกัน ใช้ left_on= และ right_on= -> จะไม่รวมคอลัมน์ของ left_on กับ right_on เป็นอันเดียวกัน
  # 🔹 ถ้าคอลัมน์มีชื่อตรงกันให้ใช่ on= -> จะรวมคอลัมน์เป็นอันเดียว
  
  # how -> ค่า default เป็น inner join ถ้าไม่กำหนด
  fact_table = df.merge(passenger_count_dim, left_on='trip_id', right_on='passenger_count_id', how='inner') \
               .merge(trip_distance_dim, left_on='trip_id', right_on='trip_distance_id') \
               .merge(rate_code_dim, left_on='trip_id', right_on='rate_code_id') \
               .merge(pickup_location_dim, left_on='trip_id', right_on='pickup_location_id') \
               .merge(dropoff_location_dim, left_on='trip_id', right_on='dropoff_location_id') \
               .merge(datetime_dim, left_on='trip_id', right_on='datetime_id') \
               .merge(payment_type_dim, left_on='trip_id', right_on='payment_type_id') \
               [['trip_id','VendorID','datetime_id','passenger_count_id','trip_distance_id','rate_code_id',
                 'store_and_fwd_flag','pickup_location_id','dropoff_location_id','payment_type_id','fare_amount',
                 'extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount'
                 ]]

  # ตรวจสอบชื่อ cloumns ของตาราง
  # print(payment_type_dim.columns)
  # Output => Index(['payment_type_id', 'payment_type', 'payment_type_name'], dtype='object')

  # print(fact_table.columns)
  # Output => Index(['trip_id', 'VendorID', 'datetime_id', 'passenger_count_id',
      # 'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag',
      # 'pickup_location_id', 'dropoff_location_id', 'payment_type_id',
      # 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
      # 'improvement_surcharge', 'total_amount'],
      # dtype='object')

  # ใช้ pandas GUI ในการแสดงข้อมูล Table
  # show(fact_table)

  # Save ไฟล์ CSV ไปที่ transformed_data_path ("../data/transformed_uber_data.csv")
  # final_data_path = '../data/uber_data_final.csv'
  final_data_path = FINAL_DATA_PATH
  fact_table.to_csv(final_data_path, index=False)

  print(f"✅ Tranform Data Successed!!")

