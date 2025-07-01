from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'nischalpaliwal7',
    'retries': 4,
    'retry_delay': timedelta(minutes=2)
}

def greet(name, age):
    message = f"Hello World!, my name is {name} and I am {age} years old."
    return message

def get_name(ti):
    name_message = ti.xcom_pull(task_ids="T1")
    print(name_message)

with DAG(
    dag_id='DAG002',
    default_args=default_args,
    description="DAG using python operator!",
    start_date=datetime(2025, 7, 1, 7),
    schedule=timedelta(weeks=1)
) as dag:
    task1 = PythonOperator(
        task_id="T1",
        python_callable=greet,
        op_kwargs={
            'name': 'Lakshit Paliwal',
            'age': 15
        }
    )

    task2 = PythonOperator(
        task_id="T2",
        python_callable=get_name
    )

    task1 >> task2