from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.extract import extract_data
from include.transform import transform_data
from include.load import load_data
from airflow.sdk.bases.hook import BaseHook


default_args = {
    "owner": "dhimas",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "healthcare_etl_dag",
    default_args=default_args,
    description="A simple healthcare ETL DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 10, 4),
    catchup=False,
    tags=["healthcare", "etl"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task