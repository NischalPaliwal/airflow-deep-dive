from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'nischalpaliwal7',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id="DAG006",
    default_args=default_args,
    start_date=datetime(2025, 7, 1, 15),
    schedule='@daily'
) as dag:
    pass