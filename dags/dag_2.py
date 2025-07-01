from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'nischalpaliwal7',
    'retries': 4,
    'retry_delay': timedelta(minutes=2)
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids="T2", key="First_Name")
    last_name = ti.xcom_pull(task_ids="T2", key="Last_Name")
    return f"Hello World!, my name is {first_name} {last_name} and I am 20 years old."

def get_name(ti):
    ti.xcom_push(key="First_Name", value="Nischal")
    ti.xcom_push(key="Last_Name", value="Paliwal")

with DAG(
    dag_id='DAG003',
    default_args=default_args,
    description="DAG using python operator!",
    start_date=datetime(2025, 7, 1, 7),
    schedule=timedelta(weeks=1)
) as dag:
    task1 = PythonOperator(
        task_id="T1",
        python_callable=greet
    )

    task2 = PythonOperator(
        task_id="T2",
        python_callable=get_name
    )

    task1 >> task2