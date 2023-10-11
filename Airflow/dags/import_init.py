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
    dag_id='import_initial',
    description='Import initial data to Postgresql',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    tags=["init"]
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
                                    num_executors = 4,
                                    executor_cores = 4,
                                    conn_id = "spark_default",
                                    application_args = ['--table', 'balance_sheet', '--init', "True"], 
                                    dag = dag)

    
    income_statement = SparkSubmitOperator(task_id = "income_statement",
                                    application = app_path,
                                    jars = driver_path,
                                    num_executors = 1,
                                    executor_cores = 1,
                                    conn_id = "spark_default",
                                    application_args = ['--table', 'income_statement', '--init', "True"], 
                                    dag = dag)

    cash_flow = SparkSubmitOperator(task_id = "cash_flow",
                                    application = app_path,
                                    jars = driver_path,
                                    num_executors = 1,
                                    executor_cores = 1,
                                    conn_id = "spark_default",
                                    application_args = ['--table', 'cash_flow', '--init', "True"], 
                                    dag = dag)


    financial_ratio = SparkSubmitOperator(task_id = "financial_ratio",
                                    application = app_path,
                                    jars = driver_path,
                                    num_executors = 1,
                                    executor_cores = 1,
                                    conn_id = "spark_default",
                                    application_args = ['--table', 'financial_ratio', '--init', "True"], 
                                    dag = dag)

    general_rating = SparkSubmitOperator(task_id = "general_rating",
                                    application = app_path,
                                    jars = driver_path,
                                    num_executors = 1,
                                    executor_cores = 1,
                                    conn_id = "spark_default",
                                    application_args = ['--table', 'general_rating', '--init', "True"], 
                                    dag = dag)

    business_model_rating = SparkSubmitOperator(task_id = "business_model_rating",
                                    application = app_path,
                                    jars = driver_path,
                                    num_executors = 1,
                                    executor_cores = 1,
                                    conn_id = "spark_default",
                                    application_args = ['--table', 'business_model_rating', '--init', "True"], 
                                    dag = dag)

    business_operation_rating = SparkSubmitOperator(task_id = "business_operation_rating",
                                    application = app_path,
                                    jars = driver_path,
                                    num_executors = 1,
                                    executor_cores = 1,
                                    conn_id = "spark_default",
                                    application_args = ['--table', 'business_operation_rating', '--init', "True"], 
                                    dag = dag)

    financial_health_rating = SparkSubmitOperator(task_id = "financial_health_rating",
                                    application = app_path,
                                    jars = driver_path,
                                    num_executors = 1,
                                    executor_cores = 1,
                                    conn_id = "spark_default",
                                    application_args = ['--table', 'financial_health_rating', '--init', "True"], 
                                    dag = dag)

    valuation_rating = SparkSubmitOperator(task_id = "valuation_rating",
                                    application = app_path,
                                    jars = driver_path,
                                    num_executors = 1,
                                    executor_cores = 1,
                                    conn_id = "spark_default",
                                    application_args = ['--table', 'valuation_rating', '--init', "True"],
                                    dag = dag)

    industry_financial_health = SparkSubmitOperator(task_id = "industry_financial_health",
                                    application = app_path,
                                    jars = driver_path,
                                    num_executors = 1,
                                    executor_cores = 1, 
                                    conn_id = "spark_default",
                                    application_args = ['--table', 'industry_financial_health', '--init', "True"], 
                                    dag = dag)
    
    listing_companies = SparkSubmitOperator(task_id = "listing_companies",
                                    application = app_path,
                                    jars = driver_path,
                                    num_executors = 4,
                                    executor_cores = 4,  
                                    conn_id = "spark_default",
                                    # conf = {"spark.driver.host":"$(hostname -i)"},
                                    application_args = ['--table', 'listing_companies', '--init', "True"], 
                                    dag = dag)
    
    stock_history = SparkSubmitOperator(task_id = "stock_history",
                                    application = app_path,
                                    jars = driver_path,
                                    num_executors = 4,
                                    executor_cores = 4,     
                                    conn_id = "spark_default",      
                                    application_args = ['--table', 'stock_history', '--start_date', get_date('init'), '--end_date', get_date('yesterday')], 
                                    dag = dag)
    
    stock_history_calculation = SparkSubmitOperator(task_id = "stock_history_calculation",
                                    application = app_path,
                                    jars = driver_path,
                                    num_executors = 4,
                                    executor_cores = 4, 
                                    conn_id = "spark_default",
                                    application_args = ['--calculation', 'True', '--init', "True"], 
                                    dag = dag)
        
    
    listing_companies >>  [ general_rating,business_operation_rating,valuation_rating, financial_health_rating] \
    >> dummy_task >> [industry_financial_health, income_statement, cash_flow, financial_ratio] >> stock_history\
    >> [business_model_rating,balance_sheet, stock_history_calculation]   #stock history is heavy