from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
import os
from pyspark.sql import Row
    
default_args = {'owner': 'airflow','start_date': datetime(2023, 1, 1),}



with DAG(
    dag_id='import_quarterly',
    description='Import quarter data to Postgresql',
    start_date=datetime(2023, 6, 1),
    schedule_interval='0 16 10 1,4,7,10 *',
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
    dummy_task = BashOperator(
        task_id='dummy_task',
        bash_command="echo dummy"
    )

    balance_sheet = SparkSubmitOperator(task_id = "balance_sheet",
                                    application = app_path,
                                    jars = driver_path,
                                    conn_id = "spark_default",
                                    num_executors = 1,
                                    executor_cores = 1,
                                    application_args = ['--table', 'balance_sheet', '--init', "False"], 
                                    dag = dag)

    
    income_statement = SparkSubmitOperator(task_id = "income_statement",
                                    application = app_path,
                                    jars = driver_path,
                                    conn_id = "spark_default",
                                    num_executors = 1,
                                    executor_cores = 1,
                                    application_args = ['--table', 'income_statement', '--init', "False"], 
                                    dag = dag)

    cash_flow = SparkSubmitOperator(task_id = "cash_flow",
                                    application = app_path,
                                    jars = driver_path,
                                    conn_id = "spark_default",
                                    num_executors = 1,
                                    executor_cores = 1,
                                    application_args = ['--table', 'cash_flow', '--init', "False"], 
                                    dag = dag)


    financial_ratio = SparkSubmitOperator(task_id = "financial_ratio",
                                    application = app_path,
                                    jars = driver_path,
                                    conn_id = "spark_default",
                                    num_executors = 1,
                                    executor_cores = 1,
                                    application_args = ['--table', 'financial_ratio', '--init', "False"], 
                                    dag = dag)

    general_rating = SparkSubmitOperator(task_id = "general_rating",
                                    application = app_path,
                                    jars = driver_path,
                                    conn_id = "spark_default",
                                    num_executors = 1,
                                    executor_cores = 1,
                                    application_args = ['--table', 'general_rating'], 
                                    dag = dag)

    business_model_rating = SparkSubmitOperator(task_id = "business_model_rating",
                                    application = app_path,
                                    conn_id = "spark_default",
                                    num_executors = 1,
                                    executor_cores = 1,
                                    application_args = ['--table', 'business_model_rating'], 
                                    dag = dag)

    business_operation_rating = SparkSubmitOperator(task_id = "business_operation_rating",
                                    application = app_path,
                                    jars = driver_path,
                                    conn_id = "spark_default",
                                    num_executors = 1,
                                    executor_cores = 1,
                                    application_args = ['--table', 'business_operation_rating'], 
                                    dag = dag)

    financial_health_rating = SparkSubmitOperator(task_id = "financial_health_rating",
                                    application = app_path,
                                    jars = driver_path,
                                    conn_id = "spark_default",
                                    num_executors = 1,
                                    executor_cores = 1,
                                    application_args = ['--table', 'financial_health_rating'], 
                                    dag = dag)

    valuation_rating = SparkSubmitOperator(task_id = "valuation_rating",
                                    application = app_path,
                                    jars = driver_path,
                                    conn_id = "spark_default",
                                    num_executors = 1,
                                    executor_cores = 1,
                                    application_args = ['--table', 'valuation_rating'], 
                                    dag = dag)

    industry_financial_health = SparkSubmitOperator(task_id = "industry_financial_health",
                                    application = app_path,
                                    jars = driver_path,
                                    conn_id = "spark_default",
                                    num_executors = 1,
                                    executor_cores = 1,
                                    application_args = ['--table', 'industry_financial_health'], 
                                    dag = dag)
    
    listing_companies = SparkSubmitOperator(task_id = "listing_companies",
                                    application = app_path,
                                    jars = driver_path,      
                                    conn_id = "spark_default",
                                    num_executors = 1,
                                    executor_cores = 1,
                                    application_args = ['--table', 'listing_companies'], 
                                    dag = dag)

    
    listing_companies >> [balance_sheet, income_statement, cash_flow, financial_ratio, general_rating]>> dummy_task>> [business_model_rating, business_operation_rating, financial_health_rating, valuation_rating, industry_financial_health]