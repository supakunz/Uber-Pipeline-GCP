# End-to-End Employee Data Engineering Pipeline on GCP

## Project Overview

This is an end-to-end modern data engineering project deployed on Google Cloud Platform, including the deployment of an ETL pipeline using **Google Cloud Data Fusion** for data transformation. The transformed data is stored in **BigQuery** for analysis, and an interactive dashboard is generated using **Looker** to provide actionable insights.

## Architecture
<img width="1258" alt="Project Architecture" src="https://github.com/user-attachments/assets/48be681d-3ee2-4e4a-b0da-d52794ad39a0">


## Technology Stack
Languages: 
* Python
* SQL

Google Cloud Platform: 
* Google Storage
* Cloud Data Fusion
* Google Composer
* Big Query
* Looker Studio


## Data Storage

The raw data files are stored in the `data` folder of the repository. These files are used as input for the processing pipeline and are essential for the overall project. 


## Data Modeling
![Uber Data Model](https://github.com/user-attachments/assets/73b813d1-733a-4109-b925-51384fbf3a46)

## Data Fusion Pipeline
<img alt="ETL pipeline" src="https://github.com/user-attachments/assets/05b31dbd-444c-48d0-a476-5ae87818dec6">

## Airflow Pipeline
<img alt="ETL pipeline" src="https://github.com/user-attachments/assets/0c5abaa3-ee33-47a4-a863-034391b34e65">

## Output
[<img src="https://github.com/user-attachments/assets/4471ed9d-aa99-4a47-8c5d-3bbf6c48b629" width=70% height=70%>](https://lookerstudio.google.com/reporting/06afc17e-5b14-40be-b797-47dc3729b332)
* The final output from Looker Studio can be accessed via the following link: [View Dashboard](https://lookerstudio.google.com/reporting/06afc17e-5b14-40be-b797-47dc3729b332). Note: The dashboard reads data from a static CSV file exported from BigQuery.


## Contact
Supakun Thata (supakunt.thata@gmail.com)
