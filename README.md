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
![Uber Data Model](https://github.com/user-attachments/assets/515ce74d-6feb-46c1-8db1-38e050c2ae12)

## Airflow Pipeline
<img alt="ETL pipeline" src="https://github.com/user-attachments/assets/c800a9d0-16d2-4156-a53d-74023fab2b36">

## Output
[<img src="https://github.com/user-attachments/assets/f3002d49-57d4-406f-b882-3d2df7a14f6d" width=70% height=70%>](https://lookerstudio.google.com/reporting/06afc17e-5b14-40be-b797-47dc3729b332)
* The final output from Looker Studio can be accessed via the following link: [View Dashboard](https://lookerstudio.google.com/reporting/06afc17e-5b14-40be-b797-47dc3729b332). Note: The dashboard reads data from a static CSV file exported from BigQuery.


## Contact
Supakun Thata (supakunt.thata@gmail.com)
