from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

import sys

sys.path.append("/opt/airflow/src")

from extract import extract_weather
from transform import transform_weather
from load import load_weather


with DAG(
    dag_id="weather_pipeline",
    description="Pipeline ETL météo Open-Meteo vers PostgreSQL",
    start_date=datetime(2026, 9, 10),
    schedule="@hourly",
    catchup=False,
    tags=["weather", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_weather,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_weather,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_weather,
    )

    extract_task >> transform_task >> load_task