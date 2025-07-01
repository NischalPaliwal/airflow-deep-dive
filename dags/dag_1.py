from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'nischalpaliwal7',
    'retries': 4,
    'retry_delay': timedelta(minutes=2)
}

def greet():
    print("Hello, World!")

with DAG(
    dag_id='DAG002',
    default_args=default_args,
    description="DAG using python operator!",
    start_date=datetime(2025, 7, 1, 7),
    schedule=timedelta(weeks=1)
) as dag:
    task1 = PythonOperator(
        task_id="T1",
        python_callable=greet
    )