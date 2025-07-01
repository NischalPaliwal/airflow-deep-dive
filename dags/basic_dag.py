from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'nischalpaliwal7',
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    dag_id="DAG001",
    default_args=default_args,
    description="Basic DAG for syntax understanding nothing much.",
    start_date=datetime(2025, 7, 1, 3),
    schedule=timedelta(weeks=1)
) as dag:
    task1 = BashOperator(
        task_id='T1',
        bash_command="echo Hello World, this is the first task!"
    )