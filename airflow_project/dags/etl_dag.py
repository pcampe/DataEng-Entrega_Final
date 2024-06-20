from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from weather_data import fetch_weather_data
from stock_data import fetch_stock_data
from transform_data import transform_data
from load_data import load_data_to_dw
from alerts import check_alerts

def etl():
    weather_data = fetch_weather_data('New York')
    stock_data = fetch_stock_data()
    combined_data = transform_data(weather_data, stock_data)
    load_data_to_dw(combined_data)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

dag = DAG('etl_dag', default_args=default_args, schedule_interval='@daily')

etl_task = PythonOperator(task_id='etl_task', python_callable=etl, dag=dag)
alert_task = PythonOperator(task_id='alert_task', python_callable=check_alerts, dag=dag)

etl_task >> alert_task
