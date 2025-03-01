# Uber Data Engineering Pipeline on GCP

An end-to-end modern data engineering project, including deployment of an ETL pipeline on Google Cloud Platform. The data is extracted from Supabase (PostgreSQL), processed and loaded into BigQuery for analysis, and finally visualized using Power BI to generate an insight dashboard

## Architecture
<img width="1258" alt="Project Architecture" src="https://github.com/user-attachments/assets/9d65b70c-e944-414e-acca-a062d84b4deb">


## Technology Stack
Languages: 
* Python
* SQL

Google Cloud Platform: 
* Google Storage
* Google Composer
* Big Query

Visualization
* PowerBI

Database
* Supabase ( PostgreSQL )

## Data Storage
The raw data and output files are too large to store in the repository. They are stored on Google Drive.

- **Raw data** link : https://drive.google.com/file/d/14T4HoiK9dDom7Zh5HWhqkd5PObP-QsAu/view?usp=drive_link

- **Output** link : https://drive.google.com/file/d/1Ph0Bqiyb7LzqOo7dWSNTdvKG-5x1Sguf/view?usp=drive_link


## Data Modeling
![Uber Data Model](https://github.com/user-attachments/assets/515ce74d-6feb-46c1-8db1-38e050c2ae12)

## Airflow Pipeline
<img alt="ETL pipeline" src="https://github.com/user-attachments/assets/c800a9d0-16d2-4156-a53d-74023fab2b36">

## Output
[<img src="https://github.com/user-attachments/assets/f3002d49-57d4-406f-b882-3d2df7a14f6d" width=70% height=70%>](https://lookerstudio.google.com/reporting/06afc17e-5b14-40be-b797-47dc3729b332)
* The final output from Power BI can be accessed via the following link: [Download Dashboard](https://drive.google.com/file/d/1zyHMYYam206mGEXB5zWnpGLbbcIgAONA/view?usp=drive_link). Note: The dashboard reads data from a static CSV file exported from BigQuery.

## ❄️ Setup

1. Clone this repository :

```bash
git clone https://github.com/supakunz/Uber-Pipeline-GCP.git
```

2. Navigate to the project folder and Set up the environment variables :

```
cd Uber-Pipeline-GCP
```
- Create a `.env` file in the root directory.

- Add the following variables to the .env file, replacing the placeholder values with your own:

```
AIRFLOW_UID = 50000 # User ID for running Airflow in Docker
# raw_data_path = /opt/airflow/data/raw_uber_data.csv # Path if running on local Docker
raw_data_path = /home/airflow/gcs/data/raw_uber_data.csv # Path on GCS
final_data_path = /home/airflow/gcs/data/uber_data_final.csv # Processed output file path
```

## Contact
Supakun Thata (supakunt.thata@gmail.com)
