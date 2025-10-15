"""
Noted: Airflow 2.10.x spport Python 3.9, 3.10, 3.11, 3.12

Set Up Airflow:
1. Download https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe
2. Install exe (Chose customize and for all user)
3. Python Create env: py -3.11 -m venv airflow_venv
4. Python Active Environment
5. Install Docker
6. Python Generate Enceypt Key 
7. Docker Create Airflow DB
8. Docker Create User Airflow
7. Docker Run Airflow Image
8. Docker Run Schedule

"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from Final_Project_Retail_Sale import *

path = r"/opt/airflow/retail_sales_dataset.csv"
db_connection = {
      "host": "host.docker.internal",
      "username": "root",
      "password": "1234",
      "database": ""
}

# --- Task functions ---
def excuteRetailSaleETL():
    data = etl_retail_sale(path)
    data.cleanning()
    data.loading(db_connection)


"""
Title: ETL Retail Sales
Description: 
    This DAG runs an ETL pipeline to process retail sales data.

Schedule: Every 1 minute (* * * * *)
Start Date: 2025-09-23
Catchup: False
Tags: ['ETL']
"""
with DAG(
    'etl_retail_sales',
    start_date=datetime(2025, 9, 23),
    schedule= '* * * * *', # every 1 minutes
    catchup=False,
    tags=['ETL']
) as dag:

    task_extract = PythonOperator(
        task_id='etl-retail-sale',
        python_callable=excuteRetailSaleETL
    )

