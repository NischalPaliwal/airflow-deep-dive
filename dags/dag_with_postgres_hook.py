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
    
    # Using hook.run to execute query and get results
    records = hook.run("SELECT * FROM employees")
    
    # Get column names separately (hook run doesn't return description by default)
    conn = hook.get_conn()
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM employees LIMIT 0")  # Get schema without data
        column_names = [desc[0] for desc in cursor.description]
    conn.close()
    
    # Write to CSV
    with open('data/employee_data.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(column_names)
        writer.writerows(records)

with DAG(
    dag_id="DAG008",
    default_args=default_args,
    start_date=datetime(2025, 7, 1, 15),
    schedule_interval='@weekly'
) as dag:
    task1 = PythonOperator(
        task_id="postgres_to_s3",
        python_callable=postgres_to_s3
    )
    task1