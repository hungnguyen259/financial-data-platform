from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
import os
from pyspark.sql import Row
    
default_args = {'owner': 'airflow','start_date': datetime(2023, 1, 1),}

def get_date(type):
    if type == 'today':
        value = date.today()
    elif type == 'yesterday':
        value = date.today() - timedelta(1)
    elif type == 'init':
        return "2000-01-01"
    formatted = value.strftime("%Y-%m-%d")
    return formatted

with DAG(
    dag_id='import_daily',
    description='Import quarter data to Postgresql',
    start_date=datetime(2023, 1, 1),
    schedule_interval='* 16 * * *', #Run at 4PM everyday
    tags=["final"]
) as dag:
    path = os.getcwd()
    name = "data_to_db.py"
    driver_name = "postgresql-42.5.1.jar"
    print(path)
    for root, dirs, files in os.walk(path):
        if name in files:
            app_path = (os.path.join(root, name))
        if driver_name in files:
            driver_path = (os.path.join(root, driver_name))

    
    stock_history = SparkSubmitOperator(task_id = "stock_history",
                                    application = app_path,
                                    jars = driver_path,
                                    num_executors = 4,
                                    executor_cores = 4,     
                                    conn_id = "spark_default",      
                                    application_args = ['--table', 'stock_history', '--start_date', get_date('yesterday'), '--end_date', get_date('today')], 
                                    dag = dag)


    stock_intraday_transaction = SparkSubmitOperator(task_id = "stock_intraday_transaction",
                                    application = app_path,
                                    jars = driver_path,
                                    conn_id = "spark_default",
                                    application_args = ['--table', 'stock_intraday_transaction'], 
                                    dag = dag)
    stock_history_calculation = SparkSubmitOperator(task_id = "stock_history_calculation",
                                    application = app_path,
                                    jars = driver_path,
                                    conn_id = "spark_default",
                                    application_args = ['--calculation', 'True'], 
                                    dag = dag)

    stock_intraday_transaction 
    stock_history>> stock_history_calculation