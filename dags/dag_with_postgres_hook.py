import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'nischalpaliwal7',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

def postgres_to_s3():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("select * from employees")
    
    with open('data/employee_data.csv', "w") as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
    
    cursor.close()
    connection.close()

with DAG(
    dag_id="DAG008",
    default_args=default_args,
    start_date=datetime(2025, 7, 1, 15),
    schedule='@weekly'
) as dag:
    task1 = PythonOperator(
        task_id="postgres_to_s3",
        python_callable=postgres_to_s3
    )
    task1