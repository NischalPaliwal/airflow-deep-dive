from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres import PostgresOperator

default_args = {
    'owner': 'nischalpaliwal7',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id="DAG007",
    default_args=default_args,
    start_date=datetime(2025, 7, 1, 15),
    schedule='@daily'
) as dag:
    task1 = PostgresOperator(
        task_id="T1",
        postgres_conn_id="my_postgres",
        sql='''
        CREATE TABLE IF NOT EXISTS dag_runs(
            dt date,
            dag_id VARCHAR(20),
            PRIMARY KEY (dt, dag_id)
        );
    '''
    )

    task1